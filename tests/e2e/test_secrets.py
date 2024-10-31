from typing import Callable

import pytest
from ops import CharmBase, Framework, EventBase, LifecycleEvent
from scenario import State

from catan import App, Catan, ModelState, Integration


def progcharm():
    """As in, programmable charm."""

    class Charm(CharmBase):
        logic: Callable[[EventBase], None]

        def __init__(self, framework: Framework):
            super().__init__(framework)
            for key, bound_event in self.on.events().items():
                if issubclass(bound_event.event_type, LifecycleEvent):
                    # skip collect-[app|unit]-status and [pre-]commit events.
                    continue
                framework.observe(bound_event, self._on_any_event)

        def _on_any_event(self, e):
            self.logic(e)

    return Charm


@pytest.fixture
def secret_requirer():
    pc = progcharm()

    def logic(self, _):
        foo = self.model.get_relation("foo")
        if foo and (secret_id := foo.data[foo.app].get("secret_id")):
            self.secret = self.model.get_secret(id=secret_id)
        else:
            self.secret = None

    pc.logic = logic

    return App.from_type(
        pc,
        meta={
            "name": "alter",
            "requires": {"foo": {"interface": "bar"}},
        },
    )


@pytest.fixture
def secret_owner():
    pc = progcharm()

    def logic(self, _):
        foo = self.model.get_relation("foo")
        if foo:
            secret = self.app.add_secret({"foo": "bar"})
            secret.grant(foo, unit=self.grant_to)
            self.secret = secret
            foo.data[self.app]["secret_id"] = secret.id
        else:
            self.secret = None

    pc.logic = logic
    pc.grant_to = None

    return App.from_type(
        pc,
        meta={
            "name": "diego",
            "provides": {"foo": {"interface": "bar"}},
        },
    )


@pytest.mark.parametrize("n_readers", (1, 3, 10))
def test_secret_changed(secret_owner, secret_requirer, n_readers):
    # GIVEN a relation over "foo"
    c = Catan(
        model_state=ModelState(
            {
                secret_owner: {0: State(leader=True)},
                secret_requirer: {
                    0: State(leader=True),
                    **{i: State() for i in range(1, n_readers)},
                },
            },
            integrations=[
                Integration.from_endpoints(secret_owner, "foo", secret_requirer, "foo")
            ],
        )
    )
    # WHEN a charm first creates a secret
    c.queue("update-status", secret_owner)
    c.settle()

    # THEN a secret-changed event is queued on the remote unit, as well as a relation-changed
    # (because of the secret ID being shared)
    assert c._emitted_repr == [
        "diego/0 :: update_status",
        *(f"alter/{i} :: foo_relation_changed" for i in range(0, n_readers)),
        *(f"alter/{i} :: secret_changed" for i in range(0, n_readers)),
        "diego/0 :: foo_relation_changed",
        *(f"alter/{i} :: foo_relation_changed" for i in range(0, n_readers)),
        *(f"alter/{i} :: secret_changed" for i in range(0, n_readers)),
    ]


@pytest.mark.parametrize("n_readers", (1, 3, 10))
def test_secret_changed_granted_unit(secret_owner, secret_requirer, n_readers):
    # GIVEN a relation over "foo"
    c = Catan(
        model_state=ModelState(
            {
                secret_owner: {0: State(leader=True)},
                secret_requirer: {
                    0: State(leader=True),
                    **{i: State() for i in range(1, n_readers)},
                },
            },
            integrations=[
                Integration.from_endpoints(secret_owner, "foo", secret_requirer, "foo")
            ],
        )
    )
    # GIVEN the secret owner will grant the secret to a specific remote unit only
    secret_owner.charm.charm_type.grant_to = n_readers - 1

    # WHEN a charm first creates a secret
    c.queue("update-status", secret_owner)
    c.settle()

    # THEN a secret-changed event is queued on the remote unit, as well as a relation-changed
    # (because of the secret ID being shared)
    assert c._emitted_repr == [
        "diego/0 :: update_status",
        *(f"alter/{i} :: foo_relation_changed" for i in range(0, n_readers)),
        *(f"alter/{i} :: secret_changed" for i in range(0, n_readers)),
    ]
