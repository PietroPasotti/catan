from unittest.mock import patch

from catan import App


def test_clone_tempo():
    tempo = App.from_git(
        "canonical",
        "tempo-k8s-operator",
        branch="main",
        name="tempo",
        patches=[patch("charm.KubernetesServicePatch")],
    )
    assert tempo.charm.charm_type.__name__ == "TempoCharm"


def test_clone_traefik():
    traefik = App.from_git(
        "canonical",
        "traefik-k8s-operator",
        branch="main",
        name="traefik",
        patches=[patch("charm.KubernetesServicePatch")],
    )
    assert traefik.charm.charm_type.__name__ == "TraefikIngressCharm"
