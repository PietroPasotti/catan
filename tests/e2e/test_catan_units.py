import random
from unittest.mock import patch

import pytest
from ops.pebble import Layer
from scenario import Container, Event, ExecOutput, Relation, State

from catan.catan import (
    App,
    Binding,
    Catan,
    InconsistentStateError,
    Integration,
    ModelState,
    _QueueItem,
)


@pytest.fixture
def tempo():
    tempo = App.from_path(
        "/home/pietro/canonical/tempo-k8s",
        patches=[patch("charm.KubernetesServicePatch")],
        name="tempo",
    )
    yield tempo


@pytest.fixture
def tempo_state():
    return State(
        leader=True,
        containers=[
            Container(
                name="tempo",
                can_connect=True,
                layers={
                    "foo": Layer(
                        {
                            "summary": "foo",
                            "description": "bar",
                            "services": {
                                "tempo": {
                                    "startup": "enabled",
                                    "current": "active",
                                    "name": "tempo",
                                },
                                "tempo-ready": {
                                    "startup": "disabled",
                                    "current": "active",
                                    "name": "tempo-ready",
                                },
                            },
                            "checks": {},
                        }
                    )
                },
            )
        ],
    )


@pytest.fixture
def traefik():
    # traefik = App.from_git("github.com/canonical/traefik-k8s", ...)
    traefik = App.from_path(
        "/home/pietro/canonical/traefik-k8s-operator",
        patches=[patch("charm.KubernetesServicePatch")],
        name="traefik",
    )
    yield traefik


@pytest.fixture
def traefik_state():
    return State(
        leader=True,
        config={
            # if we don't pass external_hostname, we have to mock
            # all sorts of lightkube calls
            "external_hostname": "0.0.0.0",
            # since we're passing a config, we have to provide all defaulted values
            "routing_mode": "path",
        },
        containers=[
            # unless the traefik service reports active, the
            # charm won't publish the ingress url.
            Container(
                name="traefik",
                can_connect=True,
                exec_mock={
                    (
                        "find",
                        "/opt/traefik/juju",
                        "-name",
                        "*.yaml",
                        "-delete",
                    ): ExecOutput(),
                    ("update-ca-certificates", "--fresh"): ExecOutput(),
                    ("/usr/bin/traefik", "version"): ExecOutput(stdout="0.1"),
                },
                layers={
                    "foo": Layer(
                        {
                            "summary": "foo",
                            "description": "bar",
                            "services": {
                                "traefik": {
                                    "startup": "enabled",
                                    "current": "active",
                                    "name": "traefik",
                                }
                            },
                            "checks": {},
                        }
                    )
                },
            )
        ],
    )


def test_fixed_sequence(tempo, tempo_state, traefik, traefik_state):
    c = Catan()

    with c.fixed_sequence():
        c._queue(c.model_state, "start", tempo, 0)
        c._queue(c.model_state, "install", tempo, 0)
        c._queue(c.model_state, "config-changed", tempo, 0)

    e1, e2, e3 = c._event_queue
    assert e1.group == e2.group == e3.group == 0
    assert [e._index for e in c._event_queue] == [0, 1, 2]


def test_fixed_sequence_multiple(tempo, tempo_state, traefik, traefik_state):
    c = Catan()

    with c.fixed_sequence():
        c._queue(c.model_state, "start", tempo, 0)
        c._queue(c.model_state, "install", tempo, 0)
        c._queue(c.model_state, "config-changed", tempo, 0)

    with c.fixed_sequence():
        c._queue(c.model_state, "foo", tempo, 0)
        c._queue(c.model_state, "bar", tempo, 0)
        c._queue(c.model_state, "baz", tempo, 0)

    e1, e2, e3, e4, e5, e6 = c._event_queue
    assert e1.group == e2.group == e3.group == 0
    assert e4.group == e5.group == e6.group == 1
    assert [e._index for e in c._event_queue] == [3, 4, 5, 6, 7, 8]


def test_event_queue_expansion(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            tempo: {
                1: tempo_state.replace(leader=True),
                3: tempo_state.replace(leader=False),
            },
            traefik: {0: traefik_state.replace(leader=True)},
        }
    )
    c = Catan(ms)
    qitem = _QueueItem(Event("update-status"), None, None, None)
    expanded = tuple(c._expand_queue_item(ms, qitem))

    assert len(expanded) == 3  # one update-status per unit
    assert set(x.event.name for x in expanded) == {
        "update_status"
    }  # all events the same
    assert set(x.app.name for x in expanded) == {
        tempo.name,
        traefik.name,
    }  # both apps present
    assert set(x.unit_id for x in expanded) == {1, 3, 0}  # all unit ids represented


@pytest.mark.parametrize("traefik_unit_id", (0, 4, 25))
def test_queue(tempo, tempo_state, traefik, traefik_state, traefik_unit_id):
    ms = ModelState(
        {
            tempo: {
                1: tempo_state.replace(leader=True),  # tempo/1
                2: tempo_state.replace(leader=False),  # tempo/2
            },
            traefik: {traefik_unit_id: traefik_state.replace(leader=True)},  # traefik/0
        },
        integrations=[
            Integration(
                Binding(tempo, "tracing"),
                Binding(traefik, "tracing"),
            )
        ],
    )
    c = Catan(ms)
    c.queue(Relation("tracing", remote_app_name="tempo").created_event, traefik)
    ms_out = c.settle()

    assert c._emitted_repr == [
        f"traefik/{traefik_unit_id} :: tracing_relation_created",
        # tempo notices traefik has published receiver requests
        "tempo/1 :: tracing_relation_changed",
        "tempo/2 :: tracing_relation_changed",
        # traefik notices tempo has published receiver urls
        f"traefik/{traefik_unit_id} :: tracing_relation_changed",
    ]

    traefik_state: State = ms_out.unit_states[traefik][traefik_unit_id]
    traefik_tracing_out = traefik_state.get_relations("tracing")[0]
    assert traefik_tracing_out.remote_app_data


def test_integrate(tempo, tempo_state, traefik, traefik_state):
    c = Catan(
        ModelState(
            {
                tempo: {
                    0: tempo_state.replace(leader=True),  # tempo/0
                    1: tempo_state.replace(leader=False),  # tempo/1
                },
                traefik: {0: traefik_state.replace(leader=True)},
            }
        )
    )

    c.integrate(tempo, "tracing", traefik, "tracing")
    ms_final = c.settle()

    assert c._emitted_repr == [
        "tempo/0 :: tracing_relation_created",
        "tempo/1 :: tracing_relation_created",
        "tempo/0 :: tracing_relation_joined(traefik/0)",
        "tempo/1 :: tracing_relation_joined(traefik/0)",
        "tempo/0 :: tracing_relation_changed",
        "tempo/1 :: tracing_relation_changed",
        "traefik/0 :: tracing_relation_created",
        "traefik/0 :: tracing_relation_joined(tempo/0)",
        "traefik/0 :: tracing_relation_joined(tempo/1)",
        "traefik/0 :: tracing_relation_changed",
        "tempo/0 :: tracing_relation_changed",
        "tempo/1 :: tracing_relation_changed",
        "traefik/0 :: tracing_relation_changed",
    ]
    traefik_tracing_out = ms_final.unit_states[traefik][0].get_relations("tracing")[0]
    assert traefik_tracing_out.remote_app_data


def test_disintegrate(tempo, tempo_state, traefik, traefik_state):
    tracing_tempo = Relation(
        "tracing",
    )
    tracing_traefik = Relation(
        "tracing",
    )

    integration = Integration(
        Binding(tempo, "tracing"),
        Binding(traefik, "tracing"),
    )
    ms = ModelState(
        {
            tempo: {
                0: tempo_state.replace(leader=True),
                1: tempo_state.replace(leader=False),
            },
            traefik: {0: traefik_state.replace(leader=True)},
        },
        integrations=[integration],
    )
    c = Catan(ms)
    c.disintegrate(integration)
    ms_final = c.settle()

    assert c._emitted_repr == [
        "tempo/0 :: tracing_relation_departed(traefik/0)",
        "tempo/0 :: tracing_relation_broken",
        "tempo/1 :: tracing_relation_departed(traefik/0)",
        "tempo/1 :: tracing_relation_broken",
        "traefik/0 :: tracing_relation_departed(tempo/0)",
        "traefik/0 :: tracing_relation_departed(tempo/1)",
        "traefik/0 :: tracing_relation_broken",
    ]
    assert not ms_final.unit_states[traefik][0].get_relations("tracing")
    assert not ms_final.integrations


def test_run_action(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            tempo: {
                0: tempo_state.replace(leader=True),
            },
            traefik: {
                1: traefik_state.replace(leader=True),
                3: traefik_state.replace(leader=False),
            },
        }
    )
    c = Catan(ms)

    c.run_action("show-proxied-endpoints", traefik, 1)

    c.settle()
    assert c._emitted_repr == ["traefik/1 :: show_proxied_endpoints_action"]


def test_deploy(traefik, traefik_state):
    ms = ModelState()
    c = Catan(ms)

    ms_trfk = c.deploy(traefik, ids=(6, 3), state_template=traefik_state)

    assert ms_trfk.unit_states[traefik] == {
        6: traefik_state.replace(leader=True),
        3: traefik_state.replace(leader=False),
    }

    c.settle()

    assert c._emitted_repr == [
        "traefik/6 :: install",
        "traefik/6 :: leader_elected",
        "traefik/6 :: config_changed",
        "traefik/6 :: start",
        "traefik/3 :: install",
        "traefik/3 :: leader_settings_changed",
        "traefik/3 :: config_changed",
        "traefik/3 :: start",
    ]


def test_add_unit(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            tempo: {
                0: tempo_state.replace(leader=True),
            },
        }
    )
    c = Catan(ms)

    c.deploy(traefik, ids=(1, 3), state_template=traefik_state)
    c.settle()

    new_traefik_unit_state = traefik_state.replace(leader=False)

    ms_traefik_scaled = c.add_unit(traefik, 42, state=new_traefik_unit_state)

    assert set(ms_traefik_scaled.unit_states[traefik]) == {1, 3, 42}
    assert ms_traefik_scaled.unit_states[traefik][42] == new_traefik_unit_state
    c.settle()

    assert c._emitted_repr == [
        "traefik/1 :: install",
        "traefik/1 :: leader_elected",
        "traefik/1 :: config_changed",
        "traefik/1 :: start",
        "traefik/3 :: install",
        "traefik/3 :: leader_settings_changed",
        "traefik/3 :: config_changed",
        "traefik/3 :: start",
        "traefik/42 :: install",
        "traefik/42 :: leader_settings_changed",
        "traefik/42 :: config_changed",
        "traefik/42 :: start",
    ]


def test_remove_unit(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            tempo: {
                0: tempo_state.replace(leader=True),
                1: tempo_state.replace(leader=False),
            },
        }
    )
    c = Catan(ms)

    c.remove_unit(tempo, 0)
    ms_out = c.settle()

    assert set(ms_out.unit_states[tempo]) == {1}
    assert ms_out.unit_states[tempo][1].leader

    c.settle()

    assert c._emitted_repr == [
        # tempo/1 becomes leader
        "tempo/1 :: leader_elected",
        # tempo/0 RIP
        "tempo/0 :: leader_settings_changed",
        "tempo/0 :: stop",
        "tempo/0 :: remove",
    ]


def test_remove_app(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            tempo: {
                0: tempo_state.replace(leader=True),
                1: tempo_state.replace(leader=False),
            },
        }
    )
    c = Catan(ms)

    c.remove_app(tempo)
    ms_out = c.settle()

    assert tempo not in ms_out.unit_states
    c.settle()

    assert c._emitted_repr == [
        # tempo/1 RIP
        "tempo/1 :: stop",
        "tempo/1 :: remove",
        # tempo/0 RIP, leader last
        "tempo/0 :: stop",
        "tempo/0 :: remove",
    ]


def test_remove_related_app(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            tempo: {
                0: tempo_state.replace(leader=True),
                1: tempo_state.replace(leader=False),
            },
            traefik: {0: traefik_state.replace(leader=True)},
        },
        integrations=[
            Integration(
                Binding(tempo, "tracing"),
                Binding(traefik, "tracing"),
            )
        ],
    )
    c = Catan(ms)

    c.remove_app(tempo)
    ms_out = c.settle()

    assert tempo not in ms_out.unit_states
    c.settle()

    assert c._emitted_repr == [
        "tempo/0 :: tracing_relation_departed(traefik/0)",
        "tempo/0 :: tracing_relation_broken",
        "tempo/1 :: tracing_relation_departed(traefik/0)",
        "tempo/1 :: tracing_relation_broken",
        "traefik/0 :: tracing_relation_departed(tempo/0)",
        "traefik/0 :: tracing_relation_departed(tempo/1)",
        "traefik/0 :: tracing_relation_broken",
        # tempo/1 RIP
        "tempo/1 :: stop",
        "tempo/1 :: remove",
        # tempo/0 RIP, leader last
        "tempo/0 :: stop",
        "tempo/0 :: remove",
    ]


def test_shuffle(tempo, tempo_state, traefik, traefik_state):
    c = Catan()
    c.deploy(traefik, ids=(1, 3), state_template=traefik_state)

    random.seed(123123123123123)

    c.shuffle()
    assert c._event_queue
    c.settle()

    # relative ordering of both setup sequences is maintained
    assert c._emitted_repr == [
        "traefik/1 :: install",
        "traefik/3 :: install",
        "traefik/1 :: leader_elected",
        "traefik/1 :: config_changed",
        "traefik/3 :: leader_settings_changed",
        "traefik/1 :: start",
        "traefik/3 :: config_changed",
        "traefik/3 :: start",
    ]


def test_shuffle_nonsequential(tempo, tempo_state, traefik, traefik_state):
    c = Catan()
    c.deploy(traefik, ids=(1, 3), state_template=traefik_state)

    random.seed(123123123123123)

    c.shuffle(respect_sequences=False)
    assert c._event_queue
    c.settle()

    # anything goes
    assert c._emitted_repr == [
        "traefik/3 :: config_changed",
        "traefik/3 :: leader_settings_changed",
        "traefik/1 :: start",
        "traefik/1 :: leader_elected",
        "traefik/3 :: start",
        "traefik/3 :: install",
        "traefik/1 :: config_changed",
        "traefik/1 :: install",
    ]


def test_config(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            traefik: {
                0: traefik_state.replace(leader=True),
                2: traefik_state.replace(leader=False),
            },
        }
    )
    c = Catan(ms)
    c.configure(traefik, external_hostname="foo.com")
    c.settle()

    assert c._emitted_repr == [
        "traefik/0 :: config_changed",
        "traefik/2 :: config_changed",
    ]


def test_config_bad_value(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            traefik: {
                0: traefik_state.replace(leader=True),
                2: traefik_state.replace(leader=False),
            },
        }
    )
    c = Catan(ms)
    with pytest.raises(InconsistentStateError):
        c.configure(traefik, gobble="dobble")


def test_imatrix_fill(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            tempo: {
                0: tempo_state.replace(leader=True),
                1: tempo_state.replace(leader=False),
            },
            traefik: {0: traefik_state.replace(leader=True)},
        }
    )
    c = Catan(ms)
    created = c.imatrix_fill()
    assert len(created) == 2

    ms_final = c.settle()

    assert c._emitted_repr == [
        "tempo/0 :: tracing_relation_created",
        "tempo/1 :: tracing_relation_created",
        "tempo/0 :: tracing_relation_joined(traefik/0)",
        "tempo/1 :: tracing_relation_joined(traefik/0)",
        "tempo/0 :: tracing_relation_changed",
        "tempo/1 :: tracing_relation_changed",
        "traefik/0 :: tracing_relation_created",
        "traefik/0 :: tracing_relation_joined(tempo/0)",
        "traefik/0 :: tracing_relation_joined(tempo/1)",
        "traefik/0 :: tracing_relation_changed",
        "traefik/0 :: traefik_route_relation_created",
        "traefik/0 :: traefik_route_relation_joined(tempo/0)",
        "traefik/0 :: traefik_route_relation_joined(tempo/1)",
        "traefik/0 :: traefik_route_relation_changed",
        "tempo/0 :: ingress_relation_created",
        "tempo/1 :: ingress_relation_created",
        "tempo/0 :: ingress_relation_joined(traefik/0)",
        "tempo/1 :: ingress_relation_joined(traefik/0)",
        "tempo/0 :: ingress_relation_changed",
        "tempo/1 :: ingress_relation_changed",
        "tempo/0 :: tracing_relation_changed",
        "tempo/1 :: tracing_relation_changed",
        "traefik/0 :: tracing_relation_changed",
        "traefik/0 :: traefik_route_relation_changed",
    ]
    assert len(ms_final.integrations) == 2
    ingress = c.get_integration(traefik, "traefik-route", tempo)
    tracing = c.get_integration(traefik, "tracing", tempo)

    tempo0_tracing_app_data = (
        ms_final.unit_states[tempo][0].get_relations("tracing")[0].local_app_data
    )
    tempo1_tracing_app_data = (
        ms_final.unit_states[tempo][1].get_relations("tracing")[0].local_app_data
    )
    assert tempo0_tracing_app_data == tempo1_tracing_app_data


def test_pebble_ready_all(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            tempo: {
                0: tempo_state.replace(leader=True).with_can_connect("tempo", False),
                1: tempo_state.replace(leader=False).with_can_connect("tempo", False),
            },
            traefik: {
                0: traefik_state.replace(leader=True).with_can_connect("traefik", False)
            },
        }
    )
    c = Catan(ms)
    ms_pebble_ready = c.pebble_ready()

    assert c._queue_repr == [
        "tempo/0 :: tempo_pebble_ready",
        "tempo/1 :: tempo_pebble_ready",
        "traefik/0 :: traefik_pebble_ready",
    ]

    # check we've connected all
    for states in ms_pebble_ready.unit_states.values():
        for state in states.values():
            for container in state.containers:
                assert container.can_connect


def test_pebble_ready_one(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            tempo: {
                0: tempo_state.replace(leader=True).with_can_connect("tempo", False),
                1: tempo_state.replace(leader=False).with_can_connect("tempo", False),
            },
            traefik: {
                0: traefik_state.replace(leader=True).with_can_connect("traefik", False)
            },
        }
    )
    c = Catan(ms)
    ms_pebble_ready = c.pebble_ready(tempo, 1, "tempo")

    assert c._queue_repr == [
        "tempo/1 :: tempo_pebble_ready",
    ]

    # check we've connected only tempo/1:tempo
    unit_states = ms_pebble_ready.unit_states
    assert not unit_states[tempo][0].get_container("tempo").can_connect
    assert unit_states[tempo][1].get_container("tempo").can_connect
    assert not unit_states[traefik][0].get_container("traefik").can_connect
