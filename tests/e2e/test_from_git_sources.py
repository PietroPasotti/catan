from unittest.mock import patch

import pytest

from catan import App


@pytest.fixture
def tempo():
    tempo = App.from_git(
        "http://github.com/canonical/tempo-k8s",
        branch="main",
        name="tempo",
        patches=[patch("charm.KubernetesServicePatch")],
    )
    yield tempo


@pytest.fixture
def traefik():
    traefik = App.from_git(
        "http://github.com/canonical/traefik-k8s",
        branch="main",
        name="traefik",
        patches=[patch("charm.KubernetesServicePatch")],
    )
    yield traefik
