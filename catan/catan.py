import dataclasses
import logging
import sys
from importlib import import_module
from itertools import chain, starmap
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from ops import CharmBase
from scenario import *
from scenario.state import _CharmSpec, _DCBase

logger = logging.getLogger("catan")


@dataclasses.dataclass(frozen=True)
class App(_DCBase):
    charm: _CharmSpec
    leader_id: int = None
    alias: str = None

    @staticmethod
    def from_path(path: Union[str, Path], name: str = None, patches=None):
        charm_root = Path(path)
        if not charm_root.exists():
            raise RuntimeError(
                f"{charm_root} not found: cannot import charm from path."
            )
        sources_roots = tuple(
            map(
                str,
                [
                    charm_root / "src",
                    charm_root / "lib",
                ],
            )
        )
        sys.path.extend(sources_roots)

        if patches:
            for patch in patches:
                patch.__enter__()

        module = import_module("charm")
        for sroot in sources_roots:
            sys.path.remove(sroot)

        charm_type = [
            t
            for t in module.__dict__.values()
            if isinstance(t, type)
            and issubclass(t, CharmBase)
            and not t.__name__ == "CharmBase"
        ]

        if not charm_type:
            raise RuntimeError(f"No charm could be loaded from {module}.")
        if len(charm_type) > 1:
            raise RuntimeError(f"Too many charms found in {module}: {charm_type}")

        spec = _CharmSpec.autoload(charm_type[0])

        # unimport charm else the next 'import' will pick it up again
        sys.modules.pop("charm")
        return App(spec, alias=name or spec.meta["name"])

    def binding(self, name: str):
        raise NotImplementedError()

    @property
    def name(self):
        return self.alias or self.charm.meta["name"]

    def __eq__(self, other: "App"):
        return self.name == other.name

    def __hash__(self):
        # app names are going to be model-unique anyway.
        return hash(self.name)


@dataclasses.dataclass(frozen=True)
class Binding(_DCBase):
    app: App
    relation: Relation


@dataclasses.dataclass(frozen=True)
class Integration(_DCBase):
    binding1: Binding
    binding2: Binding

    def sync(
        self,
        c: "Catan",
        model_state_out: "ModelState",
        app: "App" = None,
        unit_id: int = None,
        queue: bool = True,
    ):
        """Sync this integration's bindings and queue any events on Catan."""

        def _sync(states_from, states_to, binding_from: Binding, binding_to: Binding):
            # simplifying a bit:
            # if b1's local app data is different from b2's remote app data:
            # copy it over and notify all b2 apps of the change in remote app data

            # this is the unit that's made changes to the state.
            # If not given, it means we're doing an initial sync (not as a consequence of a
            # specific charm executing) and so any unit will do.
            # this ASSUMES that all input states will be equivalent.
            master_state = states_from[unit_id or 0]
            any_b1_relation = master_state.get_relations(
                binding_from.relation.endpoint
            )[0]

            # not nice, but inevitable?
            any_b2_state = next(iter(states_to.values()))
            any_b2_relation = any_b2_state.get_relations(binding_to.relation.endpoint)[
                0
            ]

            if any_b1_relation.local_app_data == any_b2_relation.remote_app_data:
                # no changes!
                logger.debug("nothing to sync: relation data didn't change")
                return

            # copy over all relations; the one this endpoint is about would be enough,
            # but we don't quite care as the rest shouldn't have changed either.
            new_states_1 = {
                uid: follower_state.replace(relations=master_state.relations)
                for uid, follower_state in states_from.items()
            }
            # we copy the local app data of any relation in the master state
            local_app_data = any_b1_relation.local_app_data
            # todo sync unit data

            # now we copy the local app data of the master state into the remote app data of each state
            # of the remote app
            # any state will do, they should all be
            # the same where the relation is concerned
            # all relations in binding_to stay the same for other endpoints, but for the binding_to endpoint that's being changed,
            # we replace remote app data with binding_from's local app data.
            any_state_to = next(iter(states_to.values()))
            new_relations_b2 = [
                r
                for r in any_state_to.relations
                if r.endpoint != binding_to.relation.endpoint
            ] + [
                r.replace(remote_app_data=local_app_data)
                for r in any_state_to.relations
                if r.endpoint == binding_to.relation.endpoint
            ]
            new_states_2 = {
                uid: unit_state.replace(relations=new_relations_b2)
                for uid, unit_state in states_to.items()
            }

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
    unit_states: Dict[App, Dict[int, State]] = dataclasses.field(default_factory=dict)
    """Mapping from apps to the states of their units."""

    integrations: List[Integration] = dataclasses.field(default_factory=list)
    model: Model = dataclasses.field(default_factory=Model)

    def __str__(self):
        no_units = sum(map(len, self.unit_states.values()))
        return f"<ModelState ({len(self.unit_states)} apps, {no_units} units)>"


class Catan:
    """Model-level State convergence tool."""

    def __init__(self, queue_sorting: bool = True):
        self._queue_sorting = queue_sorting
        self._event_queue = []
        self._emitted = []

    def queue(self, event: Union[Event, str], app: App = None, unit_id: int = None):
        """Queue an event for emission on an app or a unit."""
        if isinstance(event, str):
            event = Event(event)
        self._queue(event, app, unit_id)

    def _queue(self, event: Event, app: App, unit_id: int = None):
        self._event_queue.append((event, app, unit_id))

    @staticmethod
    def _get_priority(event: Event):
        suffixes_to_priority = {
            "_action": 150,
            "relation_created": 100,
            "relation_broken": 99,
            "relation_joined": 98,
            "relation_departed": 98,
            "relation_changed": 50,
            "config_changed": 49,
            "update_status": 0,
        }
        for suffix, prio in suffixes_to_priority.items():
            if event.name.endswith(suffix):
                return prio
        return 50

    def _expand_event_queue(
        self, model_state: ModelState
    ) -> List[Tuple[Event, App, int]]:
        """Expand the event queue until all elements are explicit."""

        def _expand(event: Event, app: Optional[App], unit_id: Optional[int]):
            if app is not None:
                if unit_id is not None:
                    return [(event, app, unit_id)]
                return [(event, app, unit_) for unit_ in model_state.unit_states[app]]
            return chain(
                *(_expand(event, app, None) for app in model_state.unit_states)
            )

        expanded = chain(*starmap(_expand, self._event_queue))
        self._event_queue = []

        def _sorting(elem: Tuple[Event, App, int]):
            e_, a_, u_ = elem
            # sort by event priority, then app name, then unit ID.
            # app name and unit ID could be ignored, but we like things deterministic
            return (-self._get_priority(e_), a_.name, u_)

        if self._queue_sorting:
            final = sorted(expanded, key=_sorting)
        else:
            final = list(expanded)
        return final

    def settle(self, model_state: ModelState) -> ModelState:
        """Settle Catan."""
        model_state = self._initial_sync(model_state)

        ms_in = model_state
        i = 0

        # todo: infinite loop detection.
        #  e.g. relation-changed ping-pong
        while q := self._expand_event_queue(ms_in):
            i += 1
            q = list(q)
            logger.info(f"processing iteration {i} ({len(q)} events)...")

            for item in q:
                event, app, unit_id = item
                ms_out = self._fire(ms_in, event, app, unit_id)
                ms_out_synced = self._sync(ms_out, app, unit_id)
                ms_in = ms_out_synced

        if i == 0:
            logger.warning(
                "Event queue empty: converged in zero iterations. "
                "Model state unchanged."
            )
            return ms_in

        logger.info(f"Catan settled in {i} iterations")
        log = "\t" + "\n\t".join(self._emitted_repr)
        logger.info(f"Emission log: \n {log}")
        return ms_in

    @staticmethod
    def _run_scenario(app: App, unit_id: int, unit_state: State, event: Event) -> State:
        logger.info("running scenario...")
        context = Context(
            app.charm.charm_type,
            # pass meta, config, actions instead of letting _CharmSpec.autoload() again because
            # we've already removed the path from sys.modules, so attempting to find the source
            # of charm_type would incorrectly classify it as a built-in type.
            meta=app.charm.meta,
            config=app.charm.config,
            actions=app.charm.actions,
            app_name=app.name,
            unit_id=unit_id,
        )
        if event._is_action_event:  # noqa
            return context.run_action(event.action, unit_state).state
        else:
            return context.run(event, unit_state)

    def _fire(
        self, model_state: ModelState, event: Event, app: App, unit_id: int
    ) -> ModelState:
        logger.info(f"firing {event} on {app}:{unit_id}")
        self._emitted.append((event, app, unit_id))
        # don't mutate: replace.
        ms_out = model_state.copy()
        units = ms_out.unit_states[app]
        state_in = units[unit_id]
        state_out = self._run_scenario(app, unit_id, state_in, event)
        units[unit_id] = state_out
        ms_out.unit_states[app] = units
        return ms_out

    def _initial_sync(self, model_state: ModelState) -> ModelState:
        def _sync(relation_1: Relation, relation_2: Relation, states: Dict[int, State]):
            # relation = relation_1.replace(remote_app_data=relation_2.local_app_data)
            return {
                uid: s.replace(relations=s.relations + [relation_1])
                for uid, s in states.items()
            }

        new_states = model_state.unit_states.copy()

        for i in model_state.integrations:
            b1 = i.binding1
            b2 = i.binding2
            new_states[b1.app] = _sync(b1.relation, b2.relation, new_states[b1.app])
            new_states[b2.app] = _sync(b2.relation, b1.relation, new_states[b2.app])

        return model_state.replace(unit_states=new_states)

    def _sync(self, model_state_out: ModelState, app: App, unit_id: int):
        """Check what has changed between the model states and queue any generated events."""
        # fixme: no model_state_out mutation
        for i in model_state_out.integrations:
            i.sync(self, model_state_out, app, unit_id)
        return model_state_out

    def integrate(
        self,
        model_state: ModelState,
        app1: App,
        endpoint1: str,
        app2: App,
        endpoint2: str,
    ):
        """Add an integration."""
        relation1 = Relation(endpoint1)
        relation2 = Relation(endpoint2)
        integration = Integration(Binding(app1, relation1), Binding(app2, relation2))
        ms_out = model_state.replace(
            integrations=model_state.integrations + [integration]
        )
        for relation, app in ((relation1, app1), (relation2, app2)):
            self.queue(relation.created_event, app)
            # todo do this for each <other app> unit
            self.queue(relation.joined_event, app)

            self.queue(relation.changed_event, app)
        return ms_out

    def disintegrate(
        self, model_state: ModelState, app1: App, endpoint: str, app2: App
    ) -> ModelState:
        """Remove an integration."""
        to_remove = None
        for i, integration in enumerate(model_state.integrations):
            if (
                integration.binding1.app == app1
                and integration.binding2.app == app2
                and integration.binding1.relation.endpoint == endpoint
            ):
                to_remove = model_state.integrations[i]
                break
            if (
                integration.binding1.app == app2
                and integration.binding2.app == app1
                and integration.binding1.relation.endpoint == endpoint
            ):
                app1, app2 = app2, app1
                to_remove = model_state.integrations[i]
                break

        if not to_remove:
            raise RuntimeError(
                f"Not found: cannot disintegrate {app1}:{endpoint} --> {app2}"
            )

        for app, relation in (
            (app1, to_remove.binding1.relation),
            (app2, to_remove.binding2.relation),
        ):
            # todo do this for all <other app> units
            self.queue(relation.departed_event)
            self.queue(relation.broken_event)

        return model_state.replace(
            integrations=[i for i in model_state.integrations if i is not to_remove]
        )

    def run_action(
        self, action: Union[str, Action], app: App, unit: Optional[int] = None
    ):
        """Run an action on all units or a specific one."""
        if not isinstance(action, Action):
            action = Action(action)

        self.queue(action.event, app, unit)

    @property
    def _emitted_repr(self):
        return [f"{e[1].name}/{e[2]} :: {e[0].path}" for e in self._emitted]
