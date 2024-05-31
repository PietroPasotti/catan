from unittest.mock import patch

import pytest
from ops.pebble import Layer
from scenario import Container, ExecOutput, Relation, State, Event

from catan.catan import App, Binding, Catan, Integration, ModelState, _QueueItem


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
                    ): ExecOutput()
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
    tracing_tempo = Relation(
        "tracing",
    )
    tracing_traefik = Relation(
        "tracing",
    )
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
                Binding(tempo, tracing_tempo),
                Binding(traefik, tracing_traefik),
            )
        ],
    )
    c = Catan(ms)
    c.queue(tracing_traefik.created_event, traefik)
    ms_out = c.settle()

    assert c._emitted_repr == [
        f"traefik/{traefik_unit_id} :: tracing_relation_created",
        # tempo notices traefik has published receiver requests
        "tempo/1 :: tracing_relation_changed",
        "tempo/2 :: tracing_relation_changed",
        # traefik notices tempo has published receiver urls
        f"traefik/{traefik_unit_id} :: tracing_relation_changed",
    ]
    traefik_tracing_out = ms_out.unit_states[traefik][traefik_unit_id].get_relations(
        "tracing"
    )[0]
    assert traefik_tracing_out.remote_app_data


def test_integrate(tempo, tempo_state, traefik, traefik_state):
    c = Catan(
        ModelState(
            {
                tempo: {
                    0: tempo_state.replace(leader=True),
                    1: tempo_state.replace(leader=False),
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
        "tempo/0 :: tracing_relation_joined",
        "tempo/1 :: tracing_relation_joined",
        "tempo/0 :: tracing_relation_changed",
        "tempo/1 :: tracing_relation_changed",
        "traefik/0 :: tracing_relation_created",
        "traefik/0 :: tracing_relation_joined",
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
                Binding(tempo, tracing_tempo),
                Binding(traefik, tracing_traefik),
            )
        ],
    )
    c = Catan(ms)
    c.disintegrate(tempo, "tracing", traefik)
    ms_final = c.settle()

    assert c._emitted_repr == [
        "tempo/0 :: tracing_relation_departed",
        "tempo/1 :: tracing_relation_departed",
        "traefik/0 :: tracing_relation_departed",
        "tempo/0 :: tracing_relation_broken",
        "tempo/1 :: tracing_relation_broken",
        "traefik/0 :: tracing_relation_broken",
        "tempo/0 :: tracing_relation_departed",
        "tempo/1 :: tracing_relation_departed",
        "traefik/0 :: tracing_relation_departed",
        "tempo/0 :: tracing_relation_broken",
        "tempo/1 :: tracing_relation_broken",
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

    c.run_action("show-proxied-endpoints", traefik)

    c.settle()
    assert c._emitted_repr == [
        "traefik/1 :: show_proxied_endpoints_action",
        "traefik/3 :: show_proxied_endpoints_action",
    ]


def test_deploy(tempo, tempo_state, traefik, traefik_state):
    ms = ModelState(
        {
            tempo: {
                0: tempo_state.replace(leader=True),
            },
        }
    )
    c = Catan(ms)

    ms_trfk = c.deploy(traefik, ids=(1, 3), state_template=traefik_state)
    assert ms_trfk.unit_states[traefik] == {
        1: traefik_state.replace(leader=True),
        3: traefik_state.replace(leader=False),
    }

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


def test_shuffle(tempo, tempo_state, traefik, traefik_state):
    c = Catan()
    c.deploy(traefik, ids=(1, 3), state_template=traefik_state)
