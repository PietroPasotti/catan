from unittest.mock import patch

import pytest
from ops.pebble import Layer
from scenario import State, Container, ExecOutput, Relation

from catan.catan import Catan, App, ModelState, Integration, Binding


@pytest.fixture
def tempo():
    tempo = App.from_path(
        "/home/pietro/canonical/tempo-k8s",
        patches=[
            patch("charm.KubernetesServicePatch")
        ],
        name='tempo'
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
                                }
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
    traefik = App.from_path(
        "/home/pietro/canonical/traefik-k8s-operator",
        patches=[
            patch("charm.KubernetesServicePatch")
        ],
        name="traefik"
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

#
# def test_event_queue_expansion(tempo, tempo_state,
#                traefik, traefik_state):
#     ms = ModelState({
#         tempo: [
#             tempo_state.replace(leader=True),
#             tempo_state.replace(leader=False),
#         ],
#         traefik: [traefik_state.replace(leader=True)]
#     })
#     c = Catan()
#     c.queue("update-status")
#

def test_queue(tempo, tempo_state,
               traefik, traefik_state):
    tracing_tempo = Relation(
        "tracing",
    )
    tracing_traefik = Relation(
        "tracing",
    )
    ms = ModelState({
        tempo: [
            tempo_state.replace(leader=True),
            tempo_state.replace(leader=False),
        ],
        traefik: [traefik_state.replace(leader=True)]
    },
        integrations=[Integration(
            Binding(tempo, 'tracing', 'tracing', tracing_tempo),
            Binding(traefik, 'tracing', 'tracing', tracing_traefik),
        )]
    )
    c = Catan()

    c.queue(tracing_traefik.created_event, traefik)

    ms_out = c.settle(ms)
    emitted_repr = [(e[0].path, e[1]._true_name, e[2]) for e in c._emitted]
    assert emitted_repr == [
        ("tracing_relation_created", "traefik", 0),
        # tempo notices traefik has published receiver requests
        ("tracing_relation_changed", "tempo", 0),
        ("tracing_relation_changed", "tempo", 1),
        # traefik notices tempo has published receiver urls
        ("tracing_relation_changed", "traefik", 0),
    ]
    traefik_tracing_out = ms_out.unit_states[traefik][0].get_relations('tracing')[0]
    assert traefik_tracing_out.remote_app_data