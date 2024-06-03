import dataclasses
import logging
import random
import sys
from collections import defaultdict
from contextlib import contextmanager
from functools import partial
from importlib import import_module
from itertools import chain, count
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
)

import scenario  # pyright: ignore[reportMissingImports]
from ops import CharmBase
from scenario.state import _CharmSpec, _DCBase  # pyright: ignore[reportMissingImports]

logger = logging.getLogger("catan")


@dataclasses.dataclass(frozen=True)
class App(_DCBase):
    """Application."""

    charm: _CharmSpec
    leader_id: Optional[int] = None
    alias: Optional[str] = None

    @staticmethod
    def from_charm(
        charm_type: Type[CharmBase],
        name: Optional[str] = None,
        patches: Any = None,
        meta: Optional[Dict[str, Any]] = None,
    ):
        if patches:
            for patch in patches:
                patch.__enter__()
        if meta:
            spec = _CharmSpec(charm_type, meta=meta)
        else:
            spec = _CharmSpec.autoload(charm_type)

        return App(spec, alias=name or spec.meta["name"])

    @staticmethod
    def from_path(
        path: Union[str, Path], name: Optional[str] = None, patches: Any = None
    ):
        """Load app from local path."""
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

    @property
    def name(self):
        """App name as visible to the user."""
        return self.alias or self.charm.meta["name"]

    def __eq__(self, other: "App"):  # pyright: ignore[reportIncompatibleMethodOverride]
        if not isinstance(other, App):
            raise RuntimeError(f"cannot __eq__(App, {type(other).__name__}")
        return self.name == other.name

    def __hash__(self):
        # app names are going to be model-unique anyway.
        return hash(self.name)

    def __repr__(self):
        return f"<App: {self.name}>"


@dataclasses.dataclass(frozen=True)
class Binding(_DCBase):
    """Integration endpoint binding."""

    app: App
    endpoint: str
    relation_id: int = dataclasses.field(
        default_factory=scenario.state.next_relation_id
    )

    local_app_data: Dict[str, str] = dataclasses.field(default_factory=dict)
    remote_app_data: Dict[str, str] = dataclasses.field(default_factory=dict)
    local_units_data: Dict[int, Dict[str, str]] = dataclasses.field(
        default_factory=dict
    )
    remote_units_data: Dict[int, Dict[str, str]] = dataclasses.field(
        default_factory=dict
    )

    def __repr__(self):
        return f"<Binding {self.app.name}:{self.endpoint}>"


@dataclasses.dataclass(frozen=True)
class Integration(_DCBase):
    """Juju integration."""

    binding1: Binding
    binding2: Binding

    @property
    def apps(self) -> Tuple[App, App]:
        """Apps partaking in this integration."""
        return self.binding1.app, self.binding2.app

    @property
    def relations(self) -> Tuple[scenario.Relation, scenario.Relation]:
        """Relations."""
        return (
            scenario.Relation(
                endpoint=self.binding1.endpoint,
                remote_app_name=self.binding2.app.name,
                relation_id=self.binding1.relation_id,
            ),
            scenario.Relation(
                endpoint=self.binding2.endpoint,
                remote_app_name=self.binding1.app.name,
                relation_id=self.binding2.relation_id,
            ),
        )


@dataclasses.dataclass(frozen=True)
class ModelState(_DCBase):
    """Model state."""

    unit_states: Dict[App, Dict[int, scenario.State]] = dataclasses.field(
        default_factory=dict
    )
    """Mapping from apps to the states of their units."""

    integrations: List[Integration] = dataclasses.field(default_factory=list)
    model: scenario.Model = dataclasses.field(default_factory=scenario.Model)

    def __str__(self):
        no_units = sum(map(len, self.unit_states.values()))
        return f"<ModelState ({len(self.unit_states)} apps, {no_units} units)>"


_qitem_counter = count()


@dataclasses.dataclass
class _QueueItem:
    event: scenario.Event
    app: Optional[App]
    unit_id: Optional[Optional[int]]
    group: Optional[int]
    _index: int = dataclasses.field(default_factory=lambda: next(_qitem_counter))

    def __eq__(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, other: "_QueueItem"
    ):
        return (
            self.event.name == other.event.name
            # todo: any other event attr we should use here?
            and self.event.relation_remote_unit_id
            == other.event.relation_remote_unit_id
            and self.app == other.app
            and self.unit_id == other.unit_id
            and self.group == other.group
        )

    def __repr__(self):
        return f"Q<{self.event.name}: {getattr(self.app, 'name')}/{self.unit_id} ({self.group or '-'}/{self._index})>"


@dataclasses.dataclass(frozen=True)
class _HistoryItem(_DCBase):
    item: _QueueItem
    state_out: scenario.State


class Catan:
    """Model-level scenario.State convergence tool.

    Settlers of Juju unite!
    Like scenario, but for multiple charms.
    """

    def __init__(self, model_state: Optional[ModelState] = None):
        self._model_state = model_state or ModelState()

        # used to keep track of states we remove from self._model_state (e.g. with remove_unit)
        # but we still need in order to be able to scenario the charm.
        self._dead_unit_states: Dict[App, Dict[int, scenario.State]] = defaultdict(dict)
        self._event_queue: List[_QueueItem] = []
        self._emitted: List[_HistoryItem] = []

        self._fixed_sequence_counter = 0
        self._current_group = None

    @property
    def model_state(self) -> ModelState:
        """The model state attached to this Catan."""
        return self._model_state

    def queue(
        self,
        event: Union[scenario.Event, str],
        app: Optional[App] = None,
        unit_id: Optional[int] = None,
    ):
        """Queue an event for emission on an app or a unit."""
        self._queue(self._model_state, event, app, unit_id)

    def _queue(
        self,
        model_state: ModelState,
        event: Union[str, scenario.Event],
        app: Optional[App] = None,
        unit_id: Optional[int] = None,
    ):
        if isinstance(event, str):
            event = scenario.Event(event)

        qitem = _QueueItem(event, app, unit_id, group=self._current_group)
        expanded = self._expand_queue_item(model_state, qitem)
        self._extend_event_queue(expanded)
        return expanded

    def _extend_event_queue(self, expanded: Iterable[_QueueItem]):
        """Add to the end of the event queue, avoiding duplicates."""
        eq = self._event_queue
        for item in expanded:
            if item in eq:
                logger.debug(f"skipped {item} as it's already in queue.")
                continue
            eq.append(item)

    def _expand_queue_item(
        self, model_state: ModelState, item: _QueueItem
    ) -> Iterable[_QueueItem]:
        """Expand the queue item until all elements are explicit."""

        def _expand(ms: ModelState, qitem: _QueueItem):
            event = qitem.event
            app = qitem.app
            unit_id = qitem.unit_id
            group = qitem.group

            if app is not None:
                if unit_id is not None:
                    return [qitem]
                units = (
                    ms.unit_states[app]
                    if app in ms.unit_states
                    else self._dead_unit_states[app]
                )
                return [_QueueItem(event, app, unit_, group) for unit_ in units]
            return chain(
                *(
                    _expand(ms, _QueueItem(event, app, None, group))
                    for app in ms.unit_states
                )
            )

        return chain(*map(partial(_expand, model_state), [item]))

        # def _sorting(elem: _QueueItem):
        #     # sort by group event priority, then app name, then unit ID.
        #     # app name and unit ID could be ignored, but we like things deterministic
        #     return (-self._get_priority(elem.event), elem.app.name, elem.unit_id)
        #
        # if self._queue_sorting:
        #     final = sorted(expanded, key=_sorting)
        # else:
        #     final = list(expanded)

    @contextmanager
    def fixed_sequence(self):
        """Keep together any events queued within this context."""
        self._current_group = self._fixed_sequence_counter
        logger.debug(f"starting group sequence {self._fixed_sequence_counter}")

        yield

        self._fixed_sequence_counter += 1
        self._current_group = None
        logger.debug(f"exiting group sequence {self._fixed_sequence_counter}")

    def _get_next_queue_item(self) -> Optional[_QueueItem]:
        if self._event_queue:
            return self._event_queue.pop(0)

    @property
    def emitted(self) -> List[_HistoryItem]:
        """Inspect all events that have been emitted so far by this Catan."""
        return self._emitted

    def settle(self, steps: Optional[int] = None) -> ModelState:
        """Settle Catan."""
        model_state = self._initial_sync()

        i = 0

        # todo: infinite loop detection.
        #  e.g. relation-changed ping-pong

        while item := self._get_next_queue_item():
            i += 1
            logger.info(f"processing item {item} ({i}/{len(self._event_queue)})")

            # queue items are fully specified
            app = cast(App, item.app)
            unit_id = cast(int, item.unit_id)

            ms_out, unit_state_out = self._fire(model_state, item.event, app, unit_id)
            self._emitted.append(_HistoryItem(item, unit_state_out))

            ms_out_synced = self._model_reconcile(ms_out, app, unit_id)
            model_state = ms_out_synced

            if steps and i >= steps:
                break

        if i == 0:
            logger.warning(
                "scenario.Event queue empty: converged in zero iterations. "
                "scenario.Model state unchanged (modulo initial sync)."
            )
            return self._final_sync(model_state)

        logger.info(f"Catan settled in {i} iterations")
        log = "\t" + "\n\t".join(self._emitted_repr)
        logger.info(f"Emission log: \n {log}")
        return self._final_sync(model_state)

    @staticmethod
    def _run_scenario(
        app: App, unit_id: int, unit_state: scenario.State, event: scenario.Event
    ) -> scenario.State:
        logger.info("running scenario...")
        context = scenario.Context(
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
        self,
        model_state: ModelState,
        event: scenario.Event,
        app: App,
        unit_id: int,
    ) -> Tuple[ModelState, scenario.State]:
        logger.info(f"firing {event} on {app}:{unit_id}")
        # don't mutate: replace.
        ms_out = model_state.copy()
        dead_unit = False

        try:
            units = ms_out.unit_states[app]
            state_in = units[unit_id]
        except KeyError as e:
            try:
                state_in = self._dead_unit_states[app][unit_id]
                dead_unit = True
            except KeyError:
                raise RuntimeError(
                    f"{app}/{unit_id} not found in current model_state or dead_units_states."
                ) from e

        state_out = self._run_scenario(app, unit_id, state_in, event)

        if not dead_unit:
            units[unit_id] = state_out  # pyright: ignore[reportUnboundVariable]
            ms_out.unit_states[app] = units  # pyright: ignore[reportUnboundVariable]

        return ms_out, state_out

    def _initial_sync(self) -> ModelState:
        """Bring the unit states in sync with what's in the Integrations."""

        def _sync(relation, states: Dict[int, scenario.State]):
            return {
                uid: s.replace(relations=s.relations + [relation])
                for uid, s in states.items()
            }

        model_state = self._model_state
        new_states = model_state.unit_states.copy()

        for i in model_state.integrations:
            b1 = i.binding1
            b2 = i.binding2
            r1, r2 = i.relations
            new_states[b1.app] = _sync(r1, new_states[b1.app])
            new_states[b2.app] = _sync(r2, new_states[b2.app])

        return model_state.replace(unit_states=new_states)

    def _model_reconcile(self, model_state_out: ModelState, app: App, unit_id: int):
        """Check what has changed between the model states and queue any generated events."""
        # fixme: no model_state_out mutation
        for i in model_state_out.integrations:
            self._reconcile_integration(model_state_out, i, app, unit_id)
        return model_state_out

    @staticmethod
    def _find_relation(
        state: scenario.State, local_binding: Binding, remote_binding: Binding
    ):
        others = []
        found = None
        for rel in state.relations:
            if (
                rel.remote_app_name == remote_binding.app.name
                and rel.endpoint == local_binding.endpoint
            ) or (rel.endpoint == local_binding.endpoint):
                if found:
                    raise RuntimeError("found one already")
                found = rel
            else:
                others.append(rel)

        if not found:
            raise RuntimeError(
                f"relation {local_binding.endpoint} -> {remote_binding.app.name}:"
                f"{remote_binding.endpoint} not found in state"
            )
        return found, others

    def _sync_integration(
        self,
        unit_id: int,
        model_state_out: ModelState,
        states_from: Dict[int, scenario.State],
        binding_from: Binding,
        binding_to: Binding,
        queue: bool,
    ):
        # simplifying a bit:
        # if b1's local app data is different from b2's remote app data:
        # copy it over and notify all b2 apps of the change in remote app data

        # we assume that every state transition only touches a single state: namely that of the
        # unit that has just executed.

        # ``unit_id`` is the unit that's made changes to the state.
        # If not given, it means we're doing an initial sync (not as a consequence of a
        # specific charm executing) and so any unit of the provider app will do.
        # this ASSUMES that all input states will be equivalent so far as the relation is concerned.
        master_state = states_from[unit_id]
        relation_from, other_relations_from = self._find_relation(
            master_state, binding_from, binding_to
        )

        if master_state.leader:
            # If the leader has made changes to the app databag, we need to notify the peers
            new_states_from = {unit_id: master_state}

            for peer_unit_id, peer_state in model_state_out.unit_states[
                binding_from.app
            ].items():
                if peer_unit_id == unit_id:
                    continue
                peer_rel_from, other_peer_relations_from = self._find_relation(
                    peer_state, binding_from, binding_to
                )

                # check if app data has changed
                if peer_rel_from.local_app_data != relation_from.local_app_data:
                    new_peer_rel_from = peer_rel_from.replace(
                        local_app_data=relation_from.local_app_data
                    )
                    new_peer_state = peer_state.replace(
                        relations=other_peer_relations_from + [new_peer_rel_from]
                    )
                    # FIXME: unclear why this is NOT needed:
                    #  it results in duplicate events in the queue, but I'd
                    #  expect this to be necessary to trigger events in the peers
                    # if queue:
                    #     self.queue(
                    #         new_peer_rel_from.changed_event,
                    #         binding_from.app,
                    #         peer_unit_id,
                    #     )
                else:
                    new_peer_state = peer_state

                new_states_from[peer_unit_id] = new_peer_state

        else:
            # follower unit can only have changed unit databags, which won't notify the peers
            # we only need to notify the remotes.
            new_states_from = states_from

        new_states_to = {}
        any_changed = False

        for remote_unit_id, remote_state in model_state_out.unit_states[
            binding_to.app
        ].items():
            remote_rel_from, other_remote_relations_from = self._find_relation(
                remote_state, binding_to, binding_from
            )

            # copy or we'll be mutating in-place
            new_remote_units_data = remote_rel_from.remote_units_data.copy()
            new_remote_units_data[unit_id] = relation_from.local_unit_data

            if (
                # unit data changed
                remote_rel_from.remote_units_data != new_remote_units_data
            ) or (
                # app data changed
                remote_rel_from.remote_app_data != relation_from.local_app_data
            ):
                new_remote_rel_from = remote_rel_from.replace(
                    remote_units_data=new_remote_units_data,
                    remote_app_data=relation_from.local_app_data,
                )
                new_state_to = remote_state.replace(
                    relations=other_remote_relations_from + [new_remote_rel_from]
                )
                any_changed = new_remote_rel_from
            else:
                new_state_to = remote_state

            new_states_to[remote_unit_id] = new_state_to

        if any_changed and queue:
            # if one has changed, they all have.
            self.queue(any_changed.changed_event, binding_to.app)

        model_state_out.unit_states[binding_from.app] = new_states_from
        model_state_out.unit_states[binding_to.app] = new_states_to

    def _reconcile_integration(
        self,
        model_state_out: "ModelState",
        integration: Integration,
        app: Optional["App"] = None,
        unit_id: Optional[int] = None,
        queue: bool = True,
    ):
        """Sync this integration's bindings and queue any events on Catan."""
        unit_id = unit_id or 0

        b1, b2 = integration.binding1, integration.binding2
        states_out_b1 = model_state_out.unit_states[b1.app]
        states_out_b2 = model_state_out.unit_states[b2.app]

        if app is None or app == b1.app:
            self._sync_integration(
                unit_id, model_state_out, states_out_b1, b1, b2, queue=queue
            )

        if app is None or app == b2.app:
            self._sync_integration(
                unit_id, model_state_out, states_out_b2, b2, b1, queue=queue
            )

    def _final_sync(self, model_state: ModelState):
        """Bring the Integrations in sync with what's in the model's unit states."""
        new_integrations = []

        for i in model_state.integrations:
            b1_relations = {}
            b2_relations = {}

            for unit_id, state in model_state.unit_states[i.binding1.app].items():
                b1_relations[unit_id] = [
                    r
                    for r in state.get_relations(i.binding1.endpoint)
                    if r.remote_app_name == i.binding2.app.name
                    # hopefully there's only one in here!
                ][0]

            for unit_id, state in model_state.unit_states[i.binding2.app].items():
                b2_relations[unit_id] = [
                    r
                    for r in state.get_relations(i.binding2.endpoint)
                    if r.remote_app_name == i.binding1.app.name
                    # hopefully there's only one in here!
                ][0]

            # local and remote app data, and remote units data should be in sync already
            # the only thing we need to sync up is the local units data
            # (because scenario.Relation doesn't keep track of peers' unit data)
            relation_from = list(b1_relations.values())[0]
            any_b2_relation = list(b2_relations.values())[0]

            new_integrations.append(
                Integration(
                    i.binding1.replace(
                        local_app_data=relation_from.local_app_data,
                        remote_app_data=relation_from.remote_app_data,
                        local_units_data={
                            unit: rel.local_unit_data
                            for unit, rel in b1_relations.items()
                        },
                        remote_units_data=relation_from.remote_units_data,
                    ),
                    i.binding2.replace(
                        local_app_data=any_b2_relation.local_app_data,
                        remote_app_data=any_b2_relation.remote_app_data,
                        local_units_data={
                            unit: rel.local_unit_data
                            for unit, rel in b2_relations.items()
                        },
                        remote_units_data=any_b2_relation.remote_units_data,
                    ),
                )
            )

        ms_out = model_state.replace(integrations=new_integrations)
        self._model_state = ms_out
        return ms_out

    def _add_integration(self, integration: Integration):
        """Add an integration to the model."""
        ms_out = self._model_state.replace(
            integrations=self._model_state.integrations + [integration]
        )
        for relation, app, other_app in zip(
            integration.relations, integration.apps, reversed(integration.apps)
        ):
            self._queue(ms_out, relation.created_event, app)

            for remote_unit_id in ms_out.unit_states[other_app]:
                self._queue(
                    ms_out, relation.joined_event(remote_unit_id=remote_unit_id), app
                )

            self._queue(ms_out, relation.changed_event, app)

        self._model_state = ms_out
        return ms_out

    def integrate(
        self,
        app1: App,
        endpoint1: str,
        app2: App,
        endpoint2: str,
    ):
        """Integrate two apps."""
        integration = Integration(
            Binding(
                app1,
                endpoint1,
                local_units_data={
                    uid: {} for uid in self.model_state.unit_states[app1]
                },
                relation_id=scenario.state.next_relation_id(),
            ),
            Binding(
                app2,
                endpoint2,
                local_units_data={
                    uid: {} for uid in self.model_state.unit_states[app2]
                },
                relation_id=scenario.state.next_relation_id(),
            ),
        )
        return self._add_integration(integration)

    def disintegrate(self, integration: Integration) -> ModelState:
        """Remove an integration."""
        model_state = self._model_state

        for relation, app, other_app in zip(
            integration.relations, integration.apps, reversed(integration.apps)
        ):
            # if the remote app has scale zero, removing the integration will not fire any
            # departed events on this app
            # for remote_unit_id in model_state.unit_states.get(other_app, ()):
            departing_units_ids = list(
                model_state.unit_states.get(
                    other_app, self._dead_unit_states[other_app]
                )
            )
            for remote_unit_id in departing_units_ids:
                self._queue(
                    model_state,
                    relation.departed_event(remote_unit_id=remote_unit_id),
                    app,
                )

            self._queue(model_state, relation.broken_event, app)

        ms_out = model_state.replace(
            integrations=[i for i in model_state.integrations if i is not integration]
        )
        self._model_state = ms_out
        return ms_out

    def get_integration(self, app1: App, endpoint: str, app2: App) -> Integration:
        """Get an integration."""
        to_remove = None
        ms = self._model_state

        for integration in ms.integrations:
            if (
                integration.binding1.app == app1
                and integration.binding2.app == app2
                and integration.binding1.endpoint == endpoint
            ):
                to_remove = integration
                break
            if (
                integration.binding1.app == app2
                and integration.binding2.app == app1
                and integration.binding1.endpoint == endpoint
            ):
                app1, app2 = app2, app1
                to_remove = integration
                break

        if not to_remove:
            raise RuntimeError(f"Integration not found: {app1}:{endpoint} --> {app2}")

        return to_remove

    def run_action(
        self,
        action: Union[str, scenario.Action],
        app: App,
        unit: Optional[int] = None,
    ):
        """Run an action on all units or a specific one."""
        if not isinstance(action, scenario.Action):
            action = scenario.Action(action)

        self._queue(self._model_state, cast(scenario.Action, action).event, app, unit)

    def queue_setup_sequence(self, app: App, unit: Optional[int] = None):
        """Queues setup phase event sequence for this app/unit."""

        model_state = self._model_state

        if unit is None:
            for unit_id in model_state.unit_states[app]:
                self.queue_setup_sequence(app, unit_id)
            return

        with self.fixed_sequence():
            self._queue(model_state, "install", app, unit)
            # todo storage attached events
            # todo relation events
            is_leader = model_state.unit_states[app][unit].leader
            self._queue(
                model_state,
                "leader-elected" if is_leader else "leader-settings-changed",
                app,
                unit,
            )
            self._queue(model_state, "config-changed", app, unit)
            self._queue(model_state, "start", app, unit)

    def queue_teardown_sequence(self, app: App, unit: Optional[int] = None):
        """Queues teardown phase event sequence for this app/unit."""
        model_state = self._model_state

        if unit is None:
            for unit_id in model_state.unit_states[app]:
                self.queue_teardown_sequence(app, unit_id)
            return

        with self.fixed_sequence():
            # todo storage detached events
            # todo relation broken events
            self._queue(model_state, "stop", app, unit)
            self._queue(model_state, "remove", app, unit)

    def add_unit(
        self,
        app: App,
        unit: int,
        state: scenario.State,
    ):
        """Adds a unit to this application."""
        model_state = self._model_state
        new_states = model_state.unit_states.copy()

        if unit in new_states[app]:
            raise RuntimeError(f"{app.name}/{unit} exists already in the model state")

        if state.leader:
            raise RuntimeError("new units cannot join as leaders.")

        new_states[app][unit] = state
        new_model_state = model_state.replace(unit_states=new_states)

        self._model_state = new_model_state
        self.queue_setup_sequence(app, unit)
        return new_model_state

    def imatrix_clear(self, *app: App) -> List[Integration]:
        """Remove all relations (involving app)."""
        gone = []
        for integration in list(self._model_state.integrations):
            if app:
                if any(a in integration.apps for a in app) and integration not in gone:
                    self.disintegrate(integration)
                    gone.append(integration)
            else:
                self.disintegrate(integration)
                gone.append(integration)
        return gone

    def imatrix_fill(self, *app: App) -> List[Integration]:
        """Cross-relate all that's relatable (only including *app)."""

        # mapping from apps to interfaces to endpoints
        providers = {}
        requirers = {}

        pool = app or list(self._model_state.unit_states)

        for _app in pool:
            # mapping from interfaces to app, endpoint pairs
            providers_from_app = defaultdict(list)
            for endpoint, ep_meta in _app.charm.meta.get("provides", {}).items():
                providers_from_app[ep_meta["interface"]].append((_app, endpoint))
            providers.update(providers_from_app)

            requirers_from_app = defaultdict(list)
            for endpoint, ep_meta in _app.charm.meta.get("requires", {}).items():
                requirers_from_app[ep_meta["interface"]].append((_app, endpoint))
            requirers.update(requirers_from_app)

        # interfaces that have both a requirer and a provider in the pool of apps under consideration
        # sorting is to make our lives easier in testing
        shared_interfaces = sorted(set(providers).intersection(set(requirers)))

        integrations = []
        for interface in shared_interfaces:
            for prov_app, prov_ep in providers[interface]:
                for req_app, req_ep in requirers[interface]:
                    integration = Integration(
                        Binding(prov_app, prov_ep),
                        Binding(req_app, req_ep),
                    )
                    self._add_integration(integration)
                    integrations.append(integration)

        return integrations

    def remove_app(self, app: App):
        """Remove an app."""
        model_state = self._model_state
        new_states = model_state.unit_states.copy()

        if app not in new_states:
            raise RuntimeError(f"app {app} not in ModelState")

        all_unit_states = new_states.pop(app)
        # in case we've previously killed some units of this app:
        all_unit_states.update(self._dead_unit_states[app])
        self._dead_unit_states[app] = all_unit_states

        new_model_state: ModelState = model_state.replace(unit_states=new_states)
        # set this first so that queue_setup_sequence will use it
        self._model_state = new_model_state

        # remove all integrations where app is involved BEFORE nuking the app
        self.imatrix_clear(app)

        # we could call queue_teardown_sequence(app), but depending on who's the leader that might
        # trigger a lot more events than we'd like. If we're destroying the app there's no point
        # in electing a new leader each time, so instead we nuke the leader last,
        # to prevent any bouncing.
        follower_ids = list(all_unit_states)
        leader_id = [i for i, s in all_unit_states.items() if s.leader][0]
        follower_ids.remove(leader_id)
        for follower_id in follower_ids:
            self.queue_teardown_sequence(app, follower_id)
        self.queue_teardown_sequence(app, leader_id)

        return new_model_state

    def remove_unit(self, app: App, unit: int):
        """Remove an app."""
        model_state = self._model_state
        new_states = model_state.unit_states.copy()

        if app not in new_states:
            raise RuntimeError(f"app {app} not in ModelState")

        unit_state = new_states[app].pop(unit)
        self._dead_unit_states[app][unit] = unit_state

        new_model_state = model_state.replace(unit_states=new_states)

        if unit_state.leader:
            # killing the leader!
            if new_states[app]:
                new_leader_id = random.choice(list(new_states[app]))
                logger.debug(
                    f"killing the leader. Electing {new_leader_id} instead. "
                    f"Long live {app}/{new_leader_id}!"
                )

                new_states[app][new_leader_id] = new_states[app][new_leader_id].replace(
                    leader=True
                )
                self._queue(new_model_state, "leader-elected", app, new_leader_id)

            else:
                logger.debug(f"killing the last unit and leader, {app} scaled to zero.")

            self._queue(new_model_state, "leader-settings-changed", app, unit)

        # set this first so that queue_setup_sequence will use it
        self._model_state = new_model_state
        self.queue_teardown_sequence(app, unit)

        return new_model_state

    def clear_queue(self):
        """Delete all items from the queue."""
        self._event_queue.clear()
        logger.info("Event queue cleared.")

    def deploy(
        self,
        app: App,
        ids: Optional[Sequence[int]] = None,
        state_template: Optional[scenario.State] = None,
        leader_id: Optional[int] = None,
    ):
        """Deploy an app."""
        model_state = self._model_state
        new_states = model_state.unit_states.copy()

        if app in new_states:
            raise RuntimeError(f"app {app} already in input ModelState")

        _ids = cast(List[int], ids or [0])

        if not leader_id:
            leader_id = _ids[0]
            logger.debug(
                f"no leader_id provided: leader will be {app.name}/{leader_id}"
            )

        base_state = state_template or scenario.State()
        new_states[app] = {
            _id: base_state.replace(leader=(leader_id == _id)) for _id in _ids
        }
        new_model_state = model_state.replace(unit_states=new_states)

        # set this first so that queue_setup_sequence will use it
        self._model_state = new_model_state
        self.queue_setup_sequence(app)

        return new_model_state

    @property
    def _emitted_repr(self):
        """Utility to summarize the events that have been emitted by this Catan."""
        out = []
        for emitted in self._emitted:
            e = emitted.item
            remote_unit = (
                f"({e.event.relation.remote_app_name}/{e.event.relation_remote_unit_id})"
                if e.event.relation_remote_unit_id is not None
                else ""
            )

            out.append(
                f"{cast(App,e.app).name}/{e.unit_id} :: {e.event.path}{remote_unit}"
            )
        return out

    @property
    def _queue_repr(self):
        """Utility to summarize the current event queue."""
        out = []
        for e in self._event_queue:
            remote_unit = (
                f"({e.event.relation.remote_app_name}/{e.event.relation_remote_unit_id})"
                if e.event.relation_remote_unit_id is not None
                else ""
            )
            out.append(
                f"{cast(App, e.app).name}/{e.unit_id} :: {e.event.path}{remote_unit}"
            )
        return out

    def shuffle(self, respect_sequences: bool = True):
        """In-place-shuffle self._event_queue."""
        if respect_sequences:
            groups = defaultdict(list)

            for item in self._event_queue:
                if item.group:
                    groups[item.group].append(item)
                else:
                    # ungrouped elements go to -1
                    groups[-1].append(item)

            queue = []

            while groups:
                # pop the first element from a randomly selected ordered sequence
                target = random.choice(list(groups))
                sequence = groups[target]
                queue.append(sequence.pop(0))

                if not sequence:
                    del groups[target]

            self._event_queue = queue

        else:
            random.shuffle(self._event_queue)
