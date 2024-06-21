import dataclasses
import inspect
import logging
import random
import shlex
import subprocess
import sys
import tempfile
import typing
from collections import defaultdict
from contextlib import contextmanager
from functools import partial
from importlib import import_module
from itertools import chain, count
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import scenario  # pyright: ignore[reportMissingImports]
from ops import CharmBase
from scenario import consistency_checker  # pyright: ignore[reportMissingImports]
from scenario.runtime import UncaughtCharmError
from scenario.state import _CharmSpec, _DCBase  # pyright: ignore[reportMissingImports]

if typing.TYPE_CHECKING:
    from scenario.state import AnyRelation

logger = logging.getLogger("catan")
JUJU_VERSION = (3, 4, 1)


class CatanError(Exception):
    """Base of the Catan exception hierarchy."""


class InconsistentStateError(CatanError):
    """Raised when a user operation on Catan would result in an inconsistent state."""


class NotFoundError(CatanError):
    """Raised when a user operation on Catan attempts to manipulate an object that is not in the model state."""


class InvalidOperationError(CatanError):
    """Raised when a user operation on Catan would make the model state inconsistent."""


class ScenarioError(CatanError):
    """Raised when scenario raises an exception during event execution."""


class CheckFailed(CatanError):
    """Raised when a built-in check fails."""


def _import_charm_module(charm_root: Path):
    # if we're doing this from a charm repo, there'll be src/lib directories which might
    # conflict with any charm we're trying to import.
    for path in list(sys.path):
        if path.endswith("/src") or path.endswith("/lib"):
            sys.path.remove(path)

    # any modules that belong to THIS repo, we can safely ignore, unless this is the
    # charm we're working with right now.
    cwd = Path().resolve()
    testing_charm_root = cwd

    # assume we (the charm that's running the tests right now) are in a
    # `charm-root/tests/catan` or something dir;
    charm_root_found = True
    # walk up until we find a charmcraft file.
    while not (testing_charm_root / "charmcraft.yaml").exists():
        testing_charm_root = testing_charm_root.parent
        if not str(testing_charm_root).strip("/"):
            # reached root without finding charm root dir: where are we?
            charm_root_found = False
            break

    modules_to_unimport = []
    if charm_root_found and testing_charm_root != charm_root:
        # the charm that's running the tests is NOT the charm we're trying to load right now.
        testing_charm_root_str = str(testing_charm_root)
        for module_name, module in list(sys.modules.items()):
            # all import paths for this module; builtin modules have no __file__
            if not getattr(module, "__file__", None):
                continue
            # some modules have multiple import paths, make sure we nuke all of them
            if any(
                _path.startswith(testing_charm_root_str)
                for _path in getattr(module, "__path__", (inspect.getfile(module),))
            ):
                modules_to_unimport.append(module_name)

    for module in modules_to_unimport:
        sys.modules.pop(module)

    sources_roots = list(
        map(
            str,
            [
                charm_root / "src",
                charm_root / "lib",
            ],
        )
    )
    sys.path.extend(sources_roots)

    module = import_module("charm")
    return module


@dataclasses.dataclass(frozen=True)
class RunState:
    catan: "Catan"
    model_state: "ModelState"

    queue: List["_QueueItem"]
    last_item: "_HistoryItem"  # not just queueitem: we might also need the unit state
    next_item: Optional["_QueueItem"]
    history: List["_HistoryItem"]

    @property
    def n_processed_events(self):
        """Count the number of processed events."""
        return len(self.history)


@dataclasses.dataclass(frozen=True)
class App(_DCBase):
    """Application."""

    charm: _CharmSpec
    """Charm that this app deploys."""
    alias: Optional[str] = None
    """Name with which this app is deployed to juju."""

    @staticmethod
    def from_type(
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
        old_modules = sys.modules.copy()
        old_path = sys.path.copy()

        charm_root = Path(path)
        if not charm_root.exists():
            raise RuntimeError(
                f"{charm_root} not found: cannot import charm from path."
            )

        module = _import_charm_module(charm_root)

        if patches:
            for patch in patches:
                patch.__enter__()

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

        # undo imports
        sys.modules.clear()
        sys.modules.update(old_modules)
        sys.path.clear()
        sys.path.extend(old_path)

        return App(spec, alias=name or spec.meta["name"])

    @staticmethod
    def from_git(
        org: str,
        repo_name: str,
        name: Optional[str] = None,
        patches: Any = None,
        branch: Optional[str] = None,
    ):
        """Load app from a git repo."""
        # url = f"git@github.com:{org}/{repo_name}"
        url = f"http://github.com/{org}/{repo_name}"

        destination = tempfile.TemporaryDirectory()
        _branch = f" --branch {branch}" if branch else ""
        cmd = f"git clone {url} --depth 1{_branch} {destination.name}"

        try:
            subprocess.check_call(shlex.split(cmd))
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"failed cloning {url}@{branch if branch else 'main'} with {cmd!r}"
            ) from e

        charm_root = Path(destination.name)
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
        return f"<Binding {self.app.name}:{self.endpoint} ({self.relation_id})>"


@dataclasses.dataclass(frozen=True)
class Integration(_DCBase):
    """Juju integration."""

    binding1: Binding
    binding2: Binding

    @staticmethod
    def from_endpoints(app1: App, endpoint1: str, app2: App, endpoint2: str):
        """Construct an Integration object from its endpoints."""
        return Integration(Binding(app1, endpoint1), Binding(app2, endpoint2))

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

    def get_unit_state(self, app: App, unit: int):
        """Get a specific unit state."""
        return self.unit_states[app][unit]


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
            # ignore group in equality check
            # and self.group == other.group
        )

    def __repr__(self):
        remote_unit = ""
        if self.event.relation_remote_unit_id is not None:
            relation = self.event.relation
            if isinstance(relation, scenario.PeerRelation):
                remote_app_name = self.app.name
            else:
                remote_app_name = relation.remote_app_name
            remote_unit = f"({remote_app_name}/{self.event.relation_remote_unit_id})"

        return f"{cast(App, self.app).name}/{self.unit_id} :: {self.event.path}{remote_unit}"

    def __str__(self):
        return f"Q<{self.event.name}: {getattr(self.app, 'name')}/{self.unit_id} ({self.group or '~'})>"


@dataclasses.dataclass(frozen=True)
class _HistoryItem(_DCBase):
    item: _QueueItem
    state_out: scenario.State


class Catan:
    """Model-level scenario.State convergence tool.

    Settlers of Juju unite!
    Like scenario, but for multiple charms.
    """

    #############################
    # CATAN-WIDE CONFIG OPTIONS #
    #############################
    _auto_create_containers_on_deploy = True
    """Automatically add containers missing from the state template on deploy/add_unit."""
    _auto_fix_diverged_config_on_deploy = True
    """Automatically fix any divergence in config when adding a new unit."""
    _auto_create_peer_relations_on_deploy = True
    """Automatically add peer relations missing from the state template on deploy/add_unit."""
    _emit_pebble_ready_after_setup_phase = False
    """Toggle automatically firing pebble-ready at some point during the startup sequence."""

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

    @property
    def emitted(self) -> List[_HistoryItem]:
        """Inspect all events that have been emitted so far by this Catan."""
        return self._emitted

    @property
    def event_queue(self) -> List[_QueueItem]:
        """Inspect all events that have been queued on this Catan."""
        return self._event_queue

    def queue(
        self,
        event: Union[scenario.Event, str],
        app: Optional[App] = None,
        unit_id: Optional[int] = None,
    ):
        """Queue an event for emission on an app or a unit.

        This is rather low-level API, useful when you want to test a specific event that isn't
        part of any sequence in particular.

        Example usage:

        >>> c = Catan()
        >>> c.deploy(App.from_path("/path/to/my/charm/repo"), ids=[0, 1, 2])
        >>> c.queue("update-status")
        """
        self._queue(self._model_state, event, app, unit_id)

    def _queue(
        self,
        model_state: ModelState,
        event: Union[str, scenario.Event],
        app: Optional[App] = None,
        unit_id: Optional[int] = None,
    ) -> Tuple[_QueueItem, ...]:
        if isinstance(event, str):
            event = scenario.Event(event)

        qitem = _QueueItem(event, app, unit_id, group=self._current_group)
        expanded = tuple(self._expand_queue_item(model_state, qitem))
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

    @contextmanager
    def fixed_sequence(self):
        """Keep together any events queued within this context."""
        self._current_group = self._fixed_sequence_counter
        logger.debug(f"starting group sequence {self._fixed_sequence_counter}")

        yield

        self._fixed_sequence_counter += 1
        self._current_group = None
        logger.debug(f"exiting group sequence {self._fixed_sequence_counter}")

    def _get_next_queue_item(self, _pop: bool = True) -> Optional[_QueueItem]:
        if self._event_queue:
            if _pop:
                return self._event_queue.pop(0)
            else:
                return self._event_queue[0]

    def settle(
        self,
        steps: Optional[int] = None,
        on_event: Optional[Callable[[RunState], bool]] = None,
    ) -> ModelState:
        """Settle Catan.

        Params:
        ``steps``: execute at most this number of events from the queue and then return
        ``on_event``: hook that will be called back after each event emission, giving the caller the
            possibility to inspect the current state, the queue, and potentially even mutate the
            queue itself.
        """

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
            history_item = _HistoryItem(item, unit_state_out)
            self._emitted.append(history_item)

            ms_out_synced = self._model_reconcile(ms_out, app, unit_id)
            model_state = ms_out_synced

            if on_event and on_event(
                RunState(
                    catan=self,
                    model_state=model_state,
                    queue=self._event_queue,
                    last_item=history_item,
                    next_item=self._get_next_queue_item(_pop=False),
                    history=self._emitted,
                )
            ):
                break
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
            # it could be that we're processing an event on a unit which has already been removed
            # from the model_state (e.g. because we're nuking the app or scaling it down).
            try:
                state_in = self._dead_unit_states[app][unit_id]
                dead_unit = True
            except KeyError:
                raise NotFoundError(
                    f"{app}/{unit_id} not found in current model_state or dead_units_states."
                ) from e

        try:
            state_out = self._run_scenario(app, unit_id, state_in, event)
        except UncaughtCharmError as e:
            raise ScenarioError(
                f"Scenario failed emitting {event.name} on {app}/{unit_id}"
            ) from e

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

    def _model_reconcile(
        self, model_state_out: ModelState, app: App, unit_id: int
    ) -> ModelState:
        """Check what has changed between the model states and queue any generated events."""
        # fixme: no model_state_out mutation
        for i in model_state_out.integrations:
            # fixme: this mutates model-state-out in-place!
            self._reconcile_integration(model_state_out, i, app, unit_id)
        reconciled = self._reconcile_peers(model_state_out, app, unit_id)
        return reconciled

    @staticmethod
    def _find_relation(
        state: scenario.State, local_binding: Binding, remote_binding: Binding
    ):
        """Find a unique relation corresponding to this integration in the state.

        Raises an exception if multiple are found.
        """
        others = []
        found = None
        for rel in state.relations:
            if not isinstance(rel, scenario.Relation):
                continue

            if (
                rel.remote_app_name == remote_binding.app.name
                and rel.endpoint == local_binding.endpoint
            ) or (rel.endpoint == local_binding.endpoint):
                if found:
                    raise RuntimeError(
                        f"Multiple relations found matching {local_binding.endpoint} -> {remote_binding.endpoint}"
                    )
                found = rel
            else:
                others.append(rel)

        if not found:
            raise NotFoundError(
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
                remote_rel_from.remote_units_data
                != new_remote_units_data
            ) or (
                # app data changed
                remote_rel_from.remote_app_data
                != relation_from.local_app_data
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

        # fixme: no mutation!
        model_state_out.unit_states[binding_from.app] = new_states_from
        model_state_out.unit_states[binding_to.app] = new_states_to

    def _reconcile_peers(
        self,
        model_state_out: "ModelState",
        app: Optional["App"] = None,
        unit_id: Optional[int] = None,
        queue: bool = True,
    ) -> ModelState:
        """Sync all peer relations belonging to this app and queue any events on Catan."""

        def _replace_relation(state: scenario.State, relation: scenario.PeerRelation):
            return state.replace(
                relations=[
                    r for r in state.relations if r.relation_id != relation.relation_id
                ]
                + [relation]
            )

        def _sync_peer(endpoint: str, model_state: ModelState):
            try:
                app_unit_states = model_state.unit_states[app]
            except KeyError:
                # is the whole app gone?
                app_unit_states = self._dead_unit_states[app]

            peer_uids = [uid for uid in app_unit_states if uid != unit_id]

            new_app_unit_states = app_unit_states.copy()
            try:
                this_unit_state = new_app_unit_states[unit_id]  # read-only
            except KeyError:
                # is the unit gone?
                this_unit_state = self._dead_unit_states[app][unit_id]

            is_leader = this_unit_state.leader

            # there can only be one, luckily
            try:
                master_relation = this_unit_state.get_relations(endpoint)[0]
            except IndexError:
                # no peer in state. That's fine.
                return model_state

            for peer_uid in peer_uids:
                peer_relation = app_unit_states[peer_uid].get_relations(endpoint)[0]
                changes = False

                peers_data = peer_relation.peers_data
                # check for unit data changes
                if unit_id not in peers_data:
                    peers_data[unit_id] = {}

                if peer_relation.peers_data[unit_id] != master_relation.local_unit_data:
                    peers_data[unit_id] = master_relation.local_unit_data
                    peer_relation = peer_relation.replace(peers_data=peers_data)
                    changes = True

                # check for app data changes
                if (
                    is_leader
                    and peer_relation.local_app_data != master_relation.local_app_data
                ):
                    peer_relation = peer_relation.replace(
                        local_app_data=master_relation.local_app_data,
                        peers_data=peers_data,
                    )
                    changes = True

                if changes and queue:
                    new_app_unit_states[peer_uid] = _replace_relation(
                        new_app_unit_states[peer_uid], peer_relation
                    )
                    self._queue(model_state, peer_relation.changed_event, app, peer_uid)

            new_unit_states = model_state.unit_states.copy()
            new_unit_states[app] = new_app_unit_states
            return model_state.replace(unit_states=new_unit_states)

        ms_current = model_state_out
        for peer_endpoint in app.charm.meta.get("peers", []):
            ms_current = _sync_peer(peer_endpoint, ms_current)

        return ms_current

    def _reconcile_integration(
        self,
        model_state_out: "ModelState",
        integration: Integration,
        app: "App",
        unit_id: int,
        queue: bool = True,
    ):
        """Sync this integration's bindings and queue any events on Catan."""
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
        """Add an integration to the model.

        This method will NOT validate that the interface you're creating is valid.
        Caller's responsibility.
        """
        ms_out = self._model_state.replace(
            integrations=self._model_state.integrations + [integration]
        )
        for relation, app, other_app in zip(
            integration.relations, integration.apps, reversed(integration.apps)
        ):
            with self.fixed_sequence():
                self._queue(ms_out, relation.created_event, app)

                for remote_unit_id in ms_out.unit_states[other_app]:
                    self._queue(
                        ms_out,
                        relation.joined_event(remote_unit_id=remote_unit_id),
                        app,
                    )

                self._queue(ms_out, relation.changed_event, app)

        self._model_state = ms_out
        return ms_out

    def _get_interfaces(
        self, apps: Iterable[App]
    ) -> Tuple[Dict[str, List[Tuple[App, str]]], Dict[str, List[Tuple[App, str]]]]:
        # Get a mapping from all supported interfaces in the current model to the list of
        #  app:endpoint pairs that support them. Split by provider and requirer

        # mapping from apps to interfaces to endpoints
        providers = defaultdict(list)
        requirers = defaultdict(list)

        pool = apps or list(self._model_state.unit_states)

        for _app in pool:
            # mapping from interfaces to app, endpoint pairs
            for endpoint, ep_meta in _app.charm.meta.get("provides", {}).items():
                providers[ep_meta["interface"]].append((_app, endpoint))

            for endpoint, ep_meta in _app.charm.meta.get("requires", {}).items():
                requirers[ep_meta["interface"]].append((_app, endpoint))

        # interfaces that have both a requirer and a provider in the pool of apps under consideration
        # sorting is to make our lives easier in testing
        return providers, requirers

    def pebble_ready(
        self,
        app: Optional[App] = None,
        unit: Optional[int] = None,
        container_name: Optional[str] = None,
    ) -> ModelState:
        """Toggle container readiness in the model, an app or a specific unit.

        Defaults to all containers. Pass a container_name to target a specific container.
        """
        if container_name and not unit:
            raise RuntimeError("You need to provide `app` and `unit` as well.")
        if unit and not app:
            raise RuntimeError("You need to provide `app` as well.")

        def _connect(state: scenario.State, _app: App, _unit: int) -> scenario.State:
            # set container connectivity to True in this state (for a specific container
            # name, or all of them) and trigger the appropriate events

            if container_name:
                container = state.get_container(container_name)
                if container.can_connect:
                    logger.warning(f"container {container_name} are ready already")
                self._queue(
                    self._model_state, container.pebble_ready_event, _app, _unit
                )
                return state.with_can_connect(container_name, True)
            else:
                if already_on := list(
                    filter(lambda x: x.can_connect, state.containers)
                ):
                    logger.warning(
                        f"containers {[c.name for c in already_on]} are ready already"
                    )
                for c in state.containers:
                    self._queue(self._model_state, c.pebble_ready_event, _app, _unit)
                containers = [c.replace(can_connect=True) for c in state.containers]
                return state.replace(containers=containers)

        new_unit_states = {}
        for _app, unit_states in self._model_state.unit_states.items():
            _new_unit_states_for_app = unit_states.copy()
            if not app or _app.name == app.name:
                if unit:
                    # only replace for the one unit
                    _new_unit_states_for_app[unit] = _connect(
                        _new_unit_states_for_app[unit], _app, unit
                    )
                else:
                    # replace all of them
                    _new_unit_states_for_app = {
                        _unit: _connect(_new_unit_states_for_app[_unit], _app, _unit)
                        for _unit in unit_states
                    }
            new_unit_states[_app] = _new_unit_states_for_app

        ms = self._model_state.replace(unit_states=new_unit_states)
        self._model_state = ms
        return ms

    def integrate(
        self,
        app1: App,
        endpoint1: str,
        app2: App,
        endpoint2: str,
    ) -> ModelState:
        """Integrate two apps."""

        # check that they can be integrated.
        found = None
        providers, requirers = self._get_interfaces((app1, app2))
        for shared_interface in set(providers).intersection(set(requirers)):
            for prov_app, prov_ep in providers[shared_interface]:
                for req_app, req_ep in requirers[shared_interface]:
                    if {prov_ep, req_ep} == {endpoint1, endpoint2} and {
                        prov_app,
                        req_app,
                    } == {app1, app2}:
                        found = shared_interface
                        break

        if found:
            logger.debug(
                f"Found shared interface {found}: creating integration "
                f"{app1.name}:{endpoint1} --> {app2.name}:{endpoint2}"
            )

        else:
            raise InvalidOperationError(
                f"cannot relate {app1.name}:{endpoint1!r} --> {app2.name}:{endpoint2!r}. "
                f"Please check that the endpoint names are correct and the "
                f"interface they declare is the same."
            )

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
            local_units_ids = list(
                model_state.unit_states.get(app, self._dead_unit_states[app])
            )
            # do this rather than queuing for the whole app, so we can preserve the internal
            # ordering at the unit level: all that matters is that each unit sees 'broken' after 'departed',
            # but we don't quite care that units start seeing 'broken' only after all 'departed' have been fired.
            for unit_id in local_units_ids:
                with self.fixed_sequence():
                    for remote_unit_id in departing_units_ids:
                        self._queue(
                            model_state,
                            relation.departed_event(remote_unit_id=remote_unit_id),
                            app,
                            unit_id,
                        )
                    self._queue(model_state, relation.broken_event, app, unit_id)

        ms_out = model_state.replace(
            integrations=[i for i in model_state.integrations if i is not integration]
        )
        self._model_state = ms_out
        return ms_out

    def get_app(self, name: str) -> App:
        """Get an app by name."""
        return [a for a in self._model_state.unit_states if a.name == name][0]

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
            raise NotFoundError(f"Integration not found: {app1}:{endpoint} --> {app2}")

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
        if app not in self.model_state.unit_states:
            raise InvalidOperationError(
                f"app {app} not in model state: cannot queue action."
            )
        if unit is not None and unit not in self.model_state.unit_states[app]:
            raise InvalidOperationError(
                f"app {app}/{unit} not in model state: cannot queue action."
            )

        self._queue(self._model_state, cast(scenario.Action, action).event, app, unit)

    def queue_setup_sequence(self, app: App, unit: Optional[int] = None):
        """Queues setup phase event sequence for this app/unit."""

        model_state = self._model_state

        app_unit_states = model_state.unit_states[app]
        peer_ids = list(app_unit_states)

        with self.fixed_sequence():
            self._queue(model_state, "install", app, unit)
            # todo storage attached events

            # FIXME: peer relation ids need to be unified, because we don't have
            #  something like Integrations for peers. Perhaps self._peer_relation_ids[app][endpoint] ?
            # now queue all peer-relation events and create the peer
            # relations if they don't exist already
            if self._auto_create_peer_relations_on_deploy:
                for peer_id in peer_ids:
                    base_state = app_unit_states[peer_id]
                    other_units = [i for i in peer_ids if i != peer_id]
                    relations = self._add_peer_relations(
                        app, peer_id, other_units, base_state.relations
                    )
                    app_unit_states[peer_id] = base_state.replace(relations=relations)

            for _unit in app_unit_states:
                is_leader = app_unit_states[_unit].leader
                self._queue(
                    model_state,
                    "leader-elected" if is_leader else "leader-settings-changed",
                    app,
                    _unit,
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
        emit_pebble_ready: Optional[bool] = None,
    ):
        """Adds a unit to this application."""
        model_state = self._model_state
        new_states = model_state.unit_states.copy()

        peers = new_states[app]
        if unit in peers:
            raise InvalidOperationError(
                f"{app.name}/{unit} exists already in the model state"
            )

        if peers:
            if state.leader:
                # todo consider doing state.replace(leader=False) instead of raising
                raise InvalidOperationError("new units cannot join as leaders.")
        else:
            if not state.leader:
                logger.info(
                    f"new unit {unit} is the first unit of {app}: setting leader=True"
                )
                state = state.replace(leader=True)

        # add any containers
        if self._auto_create_containers_on_deploy:
            state = self._add_containers(state, app)

        # make sure the config is the same
        if peers:
            # only do this check if we have peers: if we're the first unit our config
            # is the one that matters
            leader_config = self._get_leader_state(app).config

            if state.config != leader_config:
                if self._auto_fix_diverged_config_on_deploy:
                    logger.debug(
                        f"new config for {app}/{unit} fixed to match the leader's."
                    )
                    state = state.replace(config=leader_config)
                else:
                    raise InconsistentStateError(
                        f"the config for {app}/{unit} should match "
                        f"that of the application leader."
                    )

        peers[unit] = state
        new_model_state = model_state.replace(unit_states=new_states)

        self._model_state = new_model_state
        # this also queues the peer relation events and adds any peer relations
        self.queue_setup_sequence(app, unit)

        if self._should_emit_pebble_ready_on_setup(emit_pebble_ready):
            self.pebble_ready(app, unit)

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

        # interfaces that have both a requirer and a provider in the pool of apps under consideration
        providers, requirers = self._get_interfaces(apps=app)

        integrations = []
        # sorting is to make our lives easier in testing
        for shared_interface in sorted(set(providers).intersection(set(requirers))):
            for prov_app, prov_ep in providers[shared_interface]:
                for req_app, req_ep in requirers[shared_interface]:
                    integration = Integration(
                        Binding(prov_app, prov_ep),
                        Binding(req_app, req_ep),
                    )
                    self._add_integration(integration)
                    integrations.append(integration)

        return integrations

    def _consistency_check(
        self, cc_output: consistency_checker.Results, operation: str
    ):
        errors, warnings = cc_output
        if errors:
            raise InconsistentStateError(
                f"Operation {operation!r} not allowed: it would result in an inconsistent state.",
                errors,
            )

        if warnings:
            logger.warning(
                f"Warnings found checking the consistency of the proposed state after operation "
                f"{operation!r}: {warnings}"
            )

    def configure(self, app: App, **_config):
        """Update the app configuration."""
        model_state = self._model_state
        new_states = model_state.unit_states.copy()
        leader_state = self._get_leader_state(app)
        if not leader_state:
            raise InvalidOperationError(f"{app} has no leader: cannot configure.")

        config = leader_state.config
        config.update(_config)

        new_unit_states = {}
        for _id, _unit_state in new_states[app].items():
            new_unit_state = _unit_state.replace(config=config)

            self._consistency_check(
                consistency_checker.check_config_consistency(
                    state=new_unit_state,
                    charm_spec=app.charm,
                    juju_version=JUJU_VERSION,
                ),
                operation=f"configure {app.name} {_config}",
            )

            new_unit_states[_id] = new_unit_state

        new_states[app] = new_unit_states
        new_model_state = model_state.replace(unit_states=new_states)

        self._queue(new_model_state, "config-changed", app)

        self._model_state = new_model_state
        return new_model_state

    def remove_app(self, app: App):
        """Remove an app."""
        model_state = self._model_state
        new_states = model_state.unit_states.copy()

        if app not in new_states:
            raise NotFoundError(f"app {app} not in ModelState")

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

    def scale(self, app: App, target: int) -> ModelState:
        """Scale an app up or down, by setting its unit count to `n`.

        If scaling up:
         - Any newly created units will have IDs increasing from the highest one currently in
           the model state.
         - The state of the newly created units will, too, be copied from the one with the highest ID.
        If scaling down:
         - Units with the lowest ID will be removed first (to avoid a flurry of leader re-elections,
           because if the leader is removed, the unit with the highest ID is elected by default).

        Params:
        `app`: app that you want to scale up or down.
        `target`: target number of units this app should have.
        """
        unit_states = self.model_state.unit_states[app]
        max_id = max(unit_states)
        current_count = len(unit_states)

        if target > current_count:
            logger.info(f"scaling up {app} from {current_count} to {target}")
            for i in range(target - current_count):
                self.add_unit(app, max_id + i + 1, state=unit_states[max_id].copy())

        elif target < current_count:
            logger.info(f"scaling down {app} from {current_count} to {target}")
            current_ms = self._model_state
            for i in range(current_count - target):
                min_id = min(current_ms.unit_states[app])
                current_ms = self.remove_unit(app, min_id)

        else:
            raise InvalidOperationError(f"app {app} already has scale {target}.")

        return self._model_state

    def remove_unit(self, app: App, unit: int, new_leader_id: Optional[int] = None):
        """Remove a unit.

        Params:
        `app`: app that you want to scale down.
        `unit`: unit that you want to vanquish.
        `new_leader_id`: ID of the unit that should be elected leader, if the unit you are
          removing happens to be the leader one. Leave it blank, and it shall be the unit with
          the smallest ID.
        """
        model_state = self._model_state
        new_states = model_state.unit_states.copy()

        if app not in new_states:
            raise NotFoundError(f"app {app} not in ModelState")

        unit_state = new_states[app].pop(unit)
        self._dead_unit_states[app][unit] = unit_state

        new_model_state = model_state.replace(unit_states=new_states)

        if unit_state.leader:
            # killing the leader!
            if new_states[app]:
                new_leader_id = new_leader_id or min(list(new_states[app]))
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

    def _add_peer_relations(
        self,
        app: App,
        unit_id: int,
        peer_ids: List[int],
        existing_relations: List["AnyRelation"] = None,
    ) -> Iterable["AnyRelation"]:
        existing_relations = existing_relations or []
        bound_peer_endpoints = [
            r.endpoint
            for r in existing_relations
            if isinstance(r, scenario.PeerRelation)
        ]

        peers = []
        for peer_endpoint, peer_meta in app.charm.meta.get("peers", {}).items():
            if peer_endpoint in bound_peer_endpoints:
                # user created this manually already, let's skip it
                continue

            peer = scenario.PeerRelation(
                endpoint=peer_endpoint,
                interface=peer_meta["interface"],
                # point to all peer ids except your own
                peers_data={u: {} for u in peer_ids if u != unit_id},
            )
            peers.append(peer)
            self.queue(peer.created_event, app, unit_id)
            # notify each peer that this unit is joining
            # notify this unit that each peer is joining
            for peer_id in peer_ids:
                self.queue(peer.joined_event(remote_unit_id=unit_id), app, peer_id)
                self.queue(peer.joined_event(remote_unit_id=peer_id), app, unit_id)
            self.queue(peer.changed_event, app)
        return existing_relations + peers

    def deploy(
        self,
        app: App,
        ids: Optional[Iterable[int]] = None,
        state_template: Optional[scenario.State] = None,
        leader_id: Optional[int] = None,
        emit_pebble_ready: bool = False,
    ) -> ModelState:
        """Deploy an app.

        `app`: The App to deploy
        `ids`: IDs of the units that should be created. If left None, a single unit of the app
          will be created, with ID 0.
        `state_template`: Blueprint of the scenario.State that should be assigned to all newly
          created units. If you need different states to be assigned to different units, you
          should use `deploy()` to deploy the leader, and `add_unit()` to add any follower units
          and assign to them the state you want.
        `leader_id`: ID of the unit that should be leader, selected amongst the `ids` you provided.
          Defaults to the first ID in the iterable you supplied.
        `emit_pebble_ready`: emit all pebble-ready events as part of the setup sequence.


        Example usage:
        >>> c = Catan()
        >>> c.deploy(
        ...    App.from_path("/path/to/my/charm/repo"),
        ...    ids=[0, 10, 24],  # create three units with id 0, 10 and 24
        ...    state_template=scenario.State(config={'foo': 'bar'}),
        ...    leader_id=10,  # unit 10 is leader
        ...    )
        """
        model_state = self._model_state
        new_states = model_state.unit_states.copy()

        if app in new_states:
            raise InvalidOperationError(f"app {app.name} already in input ModelState")

        _ids: List[int] = list(ids) if ids else [0]

        if not leader_id:
            leader_id = _ids[0]
            logger.debug(
                f"no leader_id provided: leader will be {app.name}/{leader_id}"
            )

        base_state = state_template or scenario.State()
        unit_states = {}

        for _id in _ids:
            # most charms will break if there's no container they should have
            if self._auto_create_containers_on_deploy:
                base_state = self._add_containers(base_state, app)

            unit_states[_id] = base_state.replace(leader=(leader_id == _id))

        new_states[app] = unit_states
        new_model_state = model_state.replace(unit_states=new_states)
        # set this first so that queue_setup_sequence will use it
        self._model_state = new_model_state
        # this will also add any missing peer relations to the states
        self.queue_setup_sequence(app)

        if self._should_emit_pebble_ready_on_setup(emit_pebble_ready):
            self.pebble_ready(app)

        return new_model_state

    @property
    def _emitted_repr(self):
        """Utility to summarize the events that have been emitted by this Catan."""
        return list(map(repr, (e.item for e in self._emitted)))

    @property
    def _queue_repr(self):
        """Utility to summarize the current event queue."""
        return list(map(repr, self._event_queue))

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

    def _should_emit_pebble_ready_on_setup(self, user_override: Optional[bool] = None):
        if user_override is None:
            return self._emit_pebble_ready_after_setup_phase
        return user_override

    def check_status(
        self,
        app,
        *unit: int,
        name: str,
        app_status: bool = False,
        message: Optional[str] = None,
        _raise: bool = True,
        model_state: Optional[ModelState] = None,
    ):
        """Utility to check the status of an app/unit in the current model state."""
        ms = model_state or self.model_state
        unit_states = ms.unit_states[app]
        unit_ids = unit or list(unit_states)

        failures = []

        for unit_id in unit_ids:
            state = unit_states[unit_id]
            target = getattr(state, "app_status" if app_status else "unit_status")
            if target.name != name:
                failures.append(target)
            elif message is not None and target.message != message:
                failures.append(target)

        result = not failures
        if _raise and not result:
            raise CheckFailed(failures)
        return result

    @staticmethod
    def _add_containers(state: scenario.State, app: App):
        existing_containers = set(c.name for c in state.containers)
        required_containers = set(app.charm.meta.get("containers", ()))
        missing_containers = required_containers.difference(existing_containers)
        logger.debug(f"adding missing containers to {app}: {missing_containers}")

        return state.replace(
            containers=state.containers
            + [
                scenario.Container(container_name)
                for container_name in missing_containers
            ]
        )

    def _get_leader_state(self, app: App) -> Optional[scenario.State]:
        leaders = [
            state
            for state in self._model_state.unit_states[app].values()
            if state.leader
        ]

        if not leaders:
            return None

        if len(leaders) > 1:
            raise InconsistentStateError(f"{app} has too many leaders")
        return leaders[0]
