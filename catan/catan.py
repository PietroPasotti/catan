import dataclasses
import logging
import sys
from importlib import import_module
from itertools import chain, starmap
from pathlib import Path
from typing import List, Dict, Union, Optional

from ops import CharmBase
from scenario import *
from scenario.state import _DCBase, _CharmSpec

logger = logging.getLogger("catan")


@dataclasses.dataclass(frozen=True)
class App(_DCBase):
    charm: _CharmSpec
    leader_id: int = None
    name: str = None

    @staticmethod
    def from_path(path: Union[str, Path], name: str = None, patches=None):
        charm_root = Path(path)
        if not charm_root.exists():
            raise RuntimeError(f"{charm_root} not found: cannot import charm from path.")
        sources_roots = tuple(map(str, [
            charm_root / 'src',
            charm_root / 'lib',
        ]))
        sys.path.extend(sources_roots)

        if patches:
            for patch in patches:
                patch.__enter__()

        module = import_module("charm")
        for sroot in sources_roots:
            sys.path.remove(sroot)

        charm_type = [t for t in module.__dict__.values() if isinstance(t, type) and issubclass(t, CharmBase) and
                      not t.__name__ == "CharmBase"]

        if not charm_type:
            raise RuntimeError(f"No charm could be loaded from {module}.")
        if len(charm_type) > 1:
            raise RuntimeError(f"Too many charms found in {module}: {charm_type}")

        spec = _CharmSpec.autoload(charm_type[0])

        # unimport charm else the next 'import' will pick it up again
        sys.modules.pop("charm")
        return App(
            spec,
            name=name or spec.meta['name']
        )

    def binding(self, name: str):
        raise NotImplementedError()

    @property
    def _true_name(self):
        return self.name or self.charm.meta["name"]

    def __eq__(self, other: "App"):
        return self._true_name == other._true_name

    def __hash__(self):
        # app names are going to be model-unique anyway.
        return hash(self._true_name)


@dataclasses.dataclass(frozen=True)
class Binding(_DCBase):
    app: App
    endpoint: str
    interface: str


@dataclasses.dataclass(frozen=True)
class Integration(_DCBase):
    binding1: Binding
    binding2: Binding

    def sync(self, c: "Catan", model_state_out: "ModelState", app: "App" = None, unit: int = None,
             queue:bool=True):
        """Sync this integration's bindings and queue any events on Catan."""

        def _sync(states_from, states_to, binding_from, binding_to):
            # simplifying a bit:
            # if b1's local app data is different from b2's remote app data:
            # copy it over and notify all b2 apps of the change in remote app data

            # this is the unit that's made changes to the state.
            # If not given, it means we're doing an initial sync (not as a consequence of a
            # specific charm executing) and so any unit will do.
            # this ASSUMES that all input states will be equivalent.
            master_state = states_from[unit or 0]
            any_b1_relation = master_state.get_relations(binding_from.endpoint)[0]

            # not nice, but inevitable?
            any_b2_state = states_to[0]
            any_b2_relation = any_b2_state.get_relations(binding_to.endpoint)[0]

            if any_b1_relation.local_app_data == any_b2_relation.remote_app_data:
                # no changes!
                logger.debug("nothing to sync: relation data didn't change")
                return

            # copy over all relations; the one this endpoint is about would be enough,
            # but we don't quite care as the rest shouldn't have changed either.
            new_states_1 = [follower_state.replace(relations=master_state.relations)
                            for follower_state in states_from]
            # we copy the local app data of any relation in the master state
            local_app_data = any_b1_relation.local_app_data
            # todo sync unit data

            # now we copy the local app data of the master state into the remote app data of each state
            # of the remote app
            # any state will do, they should all be
            # the same where the relation is concerned
            # all relations in binding_to stay the same for other endpoints, but for the binding_to endpoint that's being changed,
            # we replace remote app data with binding_from's local app data.
            new_relations_b2 = [r for r in states_to[0].relations if r.endpoint != binding_to.endpoint] + [
                r.replace(remote_app_data=local_app_data) for r in states_to[0].relations if
                r.endpoint == binding_to.endpoint]
            new_states_2 = [unit_state.replace(relations=new_relations_b2) for unit_state in states_to]

            model_state_out.unit_states[binding_from.app] = new_states_1
            model_state_out.unit_states[binding_to.app] = new_states_2

            if queue:
                c.queue(any_b2_relation.changed_event, binding_to.app)

        # we assume that every state transition only touches a single state: namely that of
        # app/unit.

        b1, b2 = self.binding1, self.binding2
        states_out_b1 = model_state_out.unit_states[b1.app]
        states_out_b2 = model_state_out.unit_states[b2.app]

        if app is None or app == b1.app:
            _sync(states_out_b1, states_out_b2, b1, b2)

        if app is None or app == b2.app:
            _sync(states_out_b2, states_out_b1, b2, b1)


@dataclasses.dataclass(frozen=True)
class ModelState(_DCBase):
    unit_states: Dict[App, List[State]] = dataclasses.field(default_factory=dict)
    """Mapping from apps to the states of their units."""

    integrations: List[Integration] = dataclasses.field(default_factory=list)
    model: Model = dataclasses.field(default_factory=Model)

    def __str__(self):
        no_units = sum(map(len, self.unit_states.values()))
        return f"<ModelState ({len(self.unit_states)} apps, {no_units} units)>"


class Catan:
    """Model-level State convergence tool."""

    def __init__(self):
        self._event_queue = []
        self._emitted = []

    def queue(self, event: Union[Event, str], app: App = None, unit: int = None):
        """Queue an event for emission on an app or a unit."""
        if isinstance(event, str):
            event = Event(event)
        self._queue(event, app, unit)

    def _queue(self, event: Event, app: App, unit: int = None):
        self._event_queue.append((event, app, unit))

    def _expand_event_queue(self, model_state: ModelState):
        def _expand(event: Event, app: Optional[App], unit: Optional[int]):
            if app is not None:
                if unit is not None:
                    return [(event, app, unit)]
                return [(event, app, unit_) for unit_ in range(len(model_state.unit_states[app]))]
            return chain(_expand(event, app, None) for app in model_state.unit_states)

        expanded = chain(*starmap(_expand, self._event_queue))
        self._event_queue = []
        return tuple(expanded)

    def settle(self, model_state: ModelState) -> ModelState:
        # lol

        model_state = self._initial_sync(model_state)

        ms_in = model_state
        i = 0

        while q := self._expand_event_queue(ms_in):
            i += 1
            q = list(q)
            logger.info(f"processing iteration {i} ({len(q)} events)...")

            for item in q:
                event, app, unit = item
                if unit is None:
                    # fire on all units
                    if not model_state.unit_states[app]:
                        logger.error(f"App {app} has no units. Pass some to ``ModelState.unit_states``.")
                        continue

                    for unit_id in range(app.scale):
                        ms_out = self._fire(ms_in, event, app, unit_id)
                    ms_in = ms_out
                    continue

                ms_out = self._fire(ms_in, event, app, unit)
                ms_out_synced = self._delta(ms_in, ms_out, app, unit)
                ms_in = ms_out_synced

        if i == 0:
            logger.warning("Event queue empty: converged in zero iterations. "
                           "Model state unchanged.")
            return ms_in

        logger.info(f"converged in {i} iterations")
        return ms_in

    def _run_scenario(self, app: App, unit_state: State, event: Event) -> State:
        logger.info("running scenario...")
        with Context(
                app.charm.charm_type,
                # pass meta, config, actions instead of letting _CharmSpec.autoload() again because
                # we've already removed the path from sys.modules, so attempting to find the source
                # of charm_type would incorrectly classify it as a built-in type.
                meta=app.charm.meta,
                config=app.charm.config,
                actions=app.charm.actions
        ).manager(event, unit_state) as mgr:
            c = mgr.charm
            # debugging breakpoint
            print(c)
            return mgr.run()

    def _fire(self, model_state: ModelState, event: Event, app: App, unit: int) -> ModelState:
        logger.info(f"firing {event} on {app}:{unit}")
        self._emitted.append((event, app, unit))
        # don't mutate: replace.
        ms_out = model_state.copy()
        units = ms_out.unit_states[app]
        state_in = units[unit]
        state_out = self._run_scenario(app, state_in, event)
        units[unit] = state_out
        ms_out.unit_states[app] = units
        return ms_out

    def _initial_sync(self, model_state: ModelState) -> ModelState:
        for i in model_state.integrations:
            # don't queue any events at this stage, we're just making sure the model is consistent.
            # todo: we could have a single relation instance to roll out to all units of an app,
            #  that way we can skip the initial sync.
            i.sync(self, model_state, queue=False)
        return model_state

    def _delta(self, model_state_in: ModelState, model_state_out: ModelState, app: App, unit: int):
        """Check what has changed between the model states and queue any generated events."""
        # fixme: no model_state_out mutation
        for i in model_state_out.integrations:
            i.sync(self, model_state_out, app, unit)
        return model_state_out
