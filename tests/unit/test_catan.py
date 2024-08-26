import ops
import pytest
from ops import Framework
from scenario import State

import catan


class AllObserver(ops.CharmBase):
    def __init__(self, framework: Framework):
        super().__init__(framework)
        for n, e in self.on.events().items():
            framework.observe(e, self._on_event)

    def _on_event(self, e: ops.EventBase):
        self._handle_event(e)

    # easier to monkeypatch later
    def _handle_event(self, e):
        pass


class FooCharm(AllObserver):
    META = {
        "name": "foo_app",
        "requires": {
            "a": {"interface": "a_interface"},
            "b": {"interface": "b_interface"},
        },
        "provides": {
            "c": {"interface": "c_interface"},
        },
        "peers": {"peers": {"interface": "foo-peers"}},
    }


class BarCharm(AllObserver):
    META = {
        "name": "bar_app",
        "requires": {
            "c": {"interface": "c_interface"},
        },
        "provides": {
            "a": {"interface": "a_interface"},
            "b": {"interface": "b_interface"},
        },
        "peers": {"poors": {"interface": "bar-peers"}},
    }


@pytest.fixture
def foo_app():
    return catan.App.from_type(FooCharm, meta=FooCharm.META)


@pytest.fixture
def bar_app():
    return catan.App.from_type(BarCharm, meta=BarCharm.META)


def test_deploy_1(foo_app):
    c = catan.Catan()
    c.deploy(foo_app, [0])

    assert c.model_state.unit_states[foo_app][0].replace(relations=[]) == State(
        leader=True
    )
    assert c._queue_repr == [
        "foo_app/0 :: install",
        "foo_app/0 :: peers_relation_created",
        "foo_app/0 :: peers_relation_changed",
        "foo_app/0 :: leader_elected",
        "foo_app/0 :: config_changed",
        "foo_app/0 :: start",
    ]


def test_deploy_12(foo_app, bar_app):
    c = catan.Catan()
    c.deploy(foo_app, [0])
    c.deploy(bar_app, [1, 4])

    # assert c.model_state.unit_states == {
    #     foo_app: {0: State(leader=True)},
    #     bar_app: {1: State(leader=True), 4: State()},
    # }
    assert c._queue_repr == [
        "foo_app/0 :: install",
        "foo_app/0 :: peers_relation_created",
        "foo_app/0 :: peers_relation_changed",
        "foo_app/0 :: leader_elected",
        "foo_app/0 :: config_changed",
        "foo_app/0 :: start",
        "bar_app/1 :: install",
        "bar_app/4 :: install",
        "bar_app/1 :: poors_relation_created",
        "bar_app/1 :: poors_relation_joined(bar_app/4)",
        "bar_app/1 :: poors_relation_changed",
        "bar_app/4 :: poors_relation_created",
        "bar_app/4 :: poors_relation_joined(bar_app/1)",
        "bar_app/4 :: poors_relation_changed",
        "bar_app/1 :: leader_elected",
        "bar_app/4 :: leader_settings_changed",
        "bar_app/1 :: config_changed",
        "bar_app/4 :: config_changed",
        "bar_app/1 :: start",
        "bar_app/4 :: start",
    ]


def test_integrate(foo_app, bar_app):
    from scenario import state

    state._next_relation_id_counter = 1
    c = catan.Catan()
    c.deploy(foo_app, [0])
    c.deploy(bar_app, [1, 4])

    c.clear_queue()
    c.integrate(foo_app, "a", bar_app, "a")

    assert c.model_state.integrations == [
        catan.Integration(
            catan.Binding(foo_app, "a", local_units_data={0: {}}, relation_id=4),
            catan.Binding(bar_app, "a", local_units_data={1: {}, 4: {}}, relation_id=5),
        )
    ]
    assert c._queue_repr == [
        "foo_app/0 :: a_relation_created",
        "foo_app/0 :: a_relation_joined(bar_app/1)",
        "foo_app/0 :: a_relation_joined(bar_app/4)",
        "foo_app/0 :: a_relation_changed",
        "bar_app/1 :: a_relation_created",
        "bar_app/4 :: a_relation_created",
        "bar_app/1 :: a_relation_joined(foo_app/0)",
        "bar_app/4 :: a_relation_joined(foo_app/0)",
        "bar_app/1 :: a_relation_changed",
        "bar_app/4 :: a_relation_changed",
    ]


def test_follower_talks_back(foo_app, bar_app):
    a_integration = catan.Integration(
        catan.Binding(foo_app, "a", local_units_data={0: {}}, relation_id=0),
        catan.Binding(bar_app, "a", local_units_data={1: {}, 4: {}}, relation_id=1),
    )
    ms = catan.ModelState(
        unit_states={
            foo_app: {0: State(leader=True)},
            bar_app: {1: State(leader=True), 4: State()},
        },
        integrations=[a_integration],
    )
    c = catan.Catan(ms)

    # let's make bar_app/4 say something in response to relation-changed
    def bar_on_event(self: ops.CharmBase, e: ops.EventBase):
        if e.handle.kind == "a_relation_changed" and not self.unit.is_leader():
            e.relation.data[self.unit]["doo"] = "boo"

    bar_app.charm.charm_type._handle_event = bar_on_event
    c.queue(a_integration.relations[1].changed_event, bar_app, 4)

    # execute a single event
    c.settle(steps=1)

    assert c._queue_repr == [
        "foo_app/0 :: a_relation_changed",
    ]


def test_leader_talks_back(foo_app, bar_app):
    a_integration = catan.Integration(
        catan.Binding(foo_app, "a", local_units_data={0: {}}, relation_id=0),
        catan.Binding(bar_app, "a", local_units_data={1: {}, 4: {}}, relation_id=1),
    )
    ms = catan.ModelState(
        unit_states={
            foo_app: {0: State(leader=True), 23: State()},
            bar_app: {1: State(leader=True), 4: State()},
        },
        integrations=[a_integration],
    )
    c = catan.Catan(ms)

    # let's make bar_app/1 say something in response to relation-changed,
    # and write it to the app databag
    def bar_on_event(self: ops.CharmBase, e: ops.EventBase):
        if e.handle.kind == "a_relation_changed" and not self.unit.is_leader():
            e.relation.data[self.app]["doo"] = "boo"

    bar_app.charm.charm_type._handle_event = bar_on_event
    c.queue(a_integration.relations[1].changed_event, bar_app, 1)

    # execute a single event
    c.settle(steps=1)

    assert c._queue_repr == [
        "foo_app/0 :: a_relation_changed",
        "foo_app/23 :: a_relation_changed",
        # apparently this is not expected in real juju!
        # "bar_app/4 :: a_relation_changed",
    ]


def test_peer_relation_syncing(foo_app):
    c = catan.Catan()

    def foo_on_event(self: ops.CharmBase, e: ops.EventBase):
        if e.handle.kind == "peers_relation_created":
            e.relation.data[self.unit]["doo"] = f"boo-{self.unit.name.split('/')[1]}"
            if self.unit.is_leader():
                e.relation.data[self.app]["woosh"] = "uwu"

    foo_app.charm.charm_type._handle_event = foo_on_event

    uids = [0, 1, 2]
    c.deploy(foo_app, ids=uids)

    for queued_peer_created_item in [
        qi for qi in c._event_queue if qi.event.name == "peers_relation_created"
    ]:
        rel_id_from_event = queued_peer_created_item.event.relation.relation_id
        rel_id_from_state = (
            c.model_state.unit_states[queued_peer_created_item.app][
                queued_peer_created_item.unit_id
            ]
            .get_relations("peers")[0]
            .relation_id
        )
        assert rel_id_from_event == rel_id_from_state

    ms_out = c.settle()
    for uid in uids:
        peer_relation = ms_out.unit_states[foo_app][uid].get_relations("peers")[0]
        assert peer_relation.local_app_data == {"woosh": "uwu"}
        for _other in uids:
            if _other != uid:
                assert peer_relation.peers_data[_other]["doo"] == f"boo-{_other}"

    assert c._emitted_repr == [
        "foo_app/0 :: install",
        "foo_app/1 :: install",
        "foo_app/2 :: install",
        "foo_app/0 :: peers_relation_created",
        "foo_app/0 :: peers_relation_joined(foo_app/1)",
        "foo_app/0 :: peers_relation_joined(foo_app/2)",
        "foo_app/0 :: peers_relation_changed",
        "foo_app/1 :: peers_relation_created",
        "foo_app/1 :: peers_relation_joined(foo_app/0)",
        "foo_app/1 :: peers_relation_joined(foo_app/2)",
        "foo_app/1 :: peers_relation_changed",
        "foo_app/2 :: peers_relation_created",
        "foo_app/2 :: peers_relation_joined(foo_app/0)",
        "foo_app/2 :: peers_relation_joined(foo_app/1)",
        "foo_app/2 :: peers_relation_changed",
        "foo_app/0 :: leader_elected",
        "foo_app/1 :: leader_settings_changed",
        "foo_app/2 :: leader_settings_changed",
        "foo_app/0 :: config_changed",
        "foo_app/1 :: config_changed",
        "foo_app/2 :: config_changed",
        "foo_app/0 :: start",
        "foo_app/1 :: start",
        "foo_app/2 :: start",
        "foo_app/0 :: peers_relation_changed",
        "foo_app/1 :: peers_relation_changed",
    ]
