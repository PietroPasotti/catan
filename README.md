# Catan

Catan is a **charm relation interface integration testing sdk**.
Let's break it down:

- It's a testing library.
- It's testing framework-agnostic.
- It's built on top of [`ops-scenario`](https://github.com/canonical/ops-scenario).
- It's mainly meant for testing the integration between different charms as they communicate over relation data.

## Why not scenario?

Scenario operates at the level of the single charm unit execution: **one charm instance, one juju event** at a time. In order to practically verify the interaction between two charms, one has to continually manually play the role of the 'remote' unit and mock the relation data it would present or reply with in a given interaction.
This often means hardcoding that data in the tests, with predictable consequences.

Catan, by contrast, operates at the level of **multiple related units, one cloud admin action** at a time.
In scenario, you look at what happens if `nginx/0` receives a `foo-relation-departed` event while in state X. In Catan, given `nginx/0` is in state X and `mydb/1` is in state Y, you look at what happens if the cloud admin does `juju integrate nginx:foo mydb:bar`.


## Data structures

### ModelState

While the primary data structure you play with in Scenario is the `State` (which in the context of Catan we should probably refer to as "the Unit State"), in Catan the protagonist is the `ModelState`, which plays a similar role. The `ModelState` data structure encapsulates:

- The list of Apps that are present in the model.
- For each `App`:
    - The unit IDs and `scenario.State`s of each individual unit of the app.
- The list of `Integrations` present in the model.

### App

The `App` data structure encapsulates:
- App name, such as "nginx"
- Charm source and metadata (yes, a physical charm's source code)

### Integration

The `Integration` data structure encapsulates:
- Two Apps and the endpoints by which they are integrated.


# Using Catan

## Core concepts
A Catan test will generally consist of these three broad steps:

- **Arrange**: 
  - Set up the `ModelState` by declaring what `Apps` there are, how they are related, how many units of each there are and what `State` each unit is in.
- **Act**:
  - Declare a change of the `ModelState`, for example, by:
    - Adding/removing an integration
    - Running an action on one of the units
    - Triggering manually an event on one or more units
    - (TODO): causing a secret to expire
- **Assert**:
  - Verify that the `ModelState` you obtain back from Catan is what you expect it to be, for example:
    - Check that a unit is in a specific state
    - Check that a given event was fired on a unit
    - Check that a new secret revision was published by its owner, or viewed by its observer
    - Check that the relations involved in an `Integration` contain the data you expect

## Example

```python
from unittest.mock import patch
from scenario import State
from catan import Catan, ModelState, App


def test_integrate():
    # GIVEN: 
    # - the tempo and traefik applications
    tempo = App.from_path(
        "/path/to/tempo-k8s-operator/",
        patches=[
            patch("charm.KubernetesServicePatch")
        ])
    traefik = App.from_path(
        "/path/to/traefik-k8s-operator/",
        patches=[
            patch("charm.KubernetesServicePatch")
        ])
    
    ms = ModelState({
        tempo: {
            0: State(leader=True),
            1: State(),
        },
        traefik: {0: State(leader=True)}
    })

    c = Catan()
    # WHEN: 
    # - we simulate doing `juju relate tempo:tracing traefik:tracing`
    ms_out = c.integrate(ms, tempo, "tracing", traefik, "tracing")
    
    # we tell Catan to flush the event queue and keep running until it's empty
    ms_final = c.settle(ms_out)
    
    assert c._emitted_repr == [
        # this is the initial event sequence, programmed by juju
        'tempo/0 :: tracing_relation_created',
        'tempo/1 :: tracing_relation_created',
        'traefik/0 :: tracing_relation_created',
        'tempo/0 :: tracing_relation_joined',
        'tempo/1 :: tracing_relation_joined',
        'traefik/0 :: tracing_relation_joined',
        'tempo/0 :: tracing_relation_changed',
        'tempo/1 :: tracing_relation_changed',
        'traefik/0 :: tracing_relation_changed',
        
        # tempo notices traefik has made databag changes
        'tempo/0 :: tracing_relation_changed',
        'tempo/1 :: tracing_relation_changed',

        # traefik notices tempo has made databag changes
        'traefik/0 :: tracing_relation_changed',
        'traefik/0 :: tracing_relation_changed'
        
        # it could go on longer for multi-step interface protocols
    ]
    traefik_tracing_out = ms_final.unit_states[traefik][0].get_relations('tracing')[0]
    assert traefik_tracing_out.remote_app_data
```
