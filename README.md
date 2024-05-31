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
    - Adding/removing an app
    - Scaling up/down an app
    - Running an action on one of the units
    - Triggering manually an event on one or more units
    - (TODO): causing a secret to expire
  - Let `Catan.settle()` which means flush the event queue and keep doing that until it's empty (emitting an event might put more events in the queue!)
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
    
    c = Catan(ModelState({
        tempo: {
            0: State(leader=True),
            1: State(),
        },
        traefik: {0: State(leader=True)}
    }))
    # WHEN: 
    # - we simulate doing `juju relate tempo:tracing traefik:tracing`
    c.integrate(tempo, "tracing", traefik, "tracing")
    
    # we tell Catan to flush the event queue and keep running until it's empty
    # output is the model state in its final form
    ms: ModelState = c.settle()
    
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
    traefik_tracing_out = ms.unit_states[traefik][0].get_relations('tracing')[0]
    assert traefik_tracing_out.remote_app_data
```

# The event queue
Catan is all about managing an event queue and keeping the several scenario States in sync with one another every time a charm executes.

Much of the Catan API is about helping you to populate the event queue in a way that makes sense, while keeping the ModelState consistent with the history you're trying to tell.


## Getting started

You can instantiate `Catan` with an empty `ModelState`. This means that there are no apps and no integrations.

```python
import catan
c = catan.Catan()
```
Next you can mutate the model state.


## Deploying apps

```python
import catan
# you can inspect the return object to view what's in the `ModelState` at this point.
ms: catan.ModelState = c.deploy(catan.App.from_path("/path/to/charm", name="foo"), [0,1], ...)
```

this is going to add two units of `"foo"`: `foo/0` and `foo/1` to the `ModelState`, **and** queue a full [setup sequence](https://github.com/canonical/charm-events) for both:
- `*-storage-attached` (todo)
- `install`
- `leader-elected` on the leader unit, `leader-settings-changed` on the followers
- `config-changed`
- `start`

## Adding units

```python
import catan, scenario
app = catan.App.from_path("/path/to/charm", name="foo")
# this app has scale zero
ms = catan.ModelState(unit_states={app: {}})
catan.Catan(ms).add_unit(app, 3, state=scenario.State(leader=True))  # adds foo/3
```

this is going to add `"foo/3"` to the input `ModelState`, **and** queue a full setup sequence for that unit:


## Removing units

```python
import catan, scenario
app = catan.App.from_path("/path/to/charm", name="foo")
# this app has scale zero
ms = catan.ModelState(unit_states={app: {
  1: scenario.State(leader=False), # foo/1
  2: scenario.State(leader=True), # foo/2
}})
catan.Catan(ms).remove_unit(app, 1)  # kills foo/1
```

this is going to remove `"foo/1"` from the input `ModelState`, **and** queue a full teardown sequence for that unit:
- (todo) `storage-detached` for all storages
- `stop`
- `remove`

as well as a `leader-elected` on `foo/1`, since `foo/2` was the leader!

If the app had relations, we'd also see
- (todo) `relation-departed` for peer relations
- `relation-departed` + `relation-broken` for regular relations

and all remote units would also see a `relation-departed` for `foo/1`.

## Removing apps

```python
import catan, scenario
app = catan.App.from_path("/path/to/charm", name="foo")
# this app has scale zero
ms = catan.ModelState(unit_states={app: {
  1: scenario.State(leader=False), # foo/1
  2: scenario.State(leader=True), # foo/2
}})
catan.Catan(ms).remove_app(app)
```

this is going to remove `"foo/1"` and `"foo/2"` from the input `ModelState`, **and** queue a full teardown sequence for both units.

If the app had relations, we'd also see the expected departed/broken hooks.


## Adding integrations

You can define your input ModelState to already have an integration:

```python
from catan import Catan, ModelState, Integration, Binding
from scenario import State
c = Catan(
    ModelState(
        {
            app1: {0: State(leader=True)},
            app2: {0: State(leader=True)},
        },
      integrations=[
        Integration(
          Binding(app1, "tracing"),
          Binding(app2, "tracing"),
          )
      ]
    )
)
```

or you can add one and queue the corresponding events:

```python
from catan import Catan, ModelState
from scenario import State
c = Catan(
    ModelState(
        {
            app1: {0: State(leader=True)},
            app2: {0: State(leader=True)},
        }
    )
)

# juju relate app1:tracing app2:tracing
c.integrate(app1, "tracing", app2, "tracing")
```

This would queue:
- on all `app1` units:
  - `tracing-relation-created`
  - for all `app2` units:
    - `tracing-relation-joined` 
  - `tracing-relation-changed`
- on all `app2` units:
  - `tracing-relation-created`
  - for all `app1` units:
    - `tracing-relation-joined`
  - `tracing-relation-changed`

Typically, on `relation-changed` events, a charm can write data to their side of the relation. Catan will notice this and queue additional `relation-changed` events on the remote units.

So usually you'll see a back-and-forth of `relation-changed` events until the charms settle and stop reacting to one another's writes, depending on the protocol.


## Randomization
After you've populated the event queue, you can call `Catan.shuffle()` to randomize it in a way that still makes juju-sense. For example, a `start` event should not precede an `install` event.
`Catan.shuffle()` ensures that the event sequences can interleave with other sequences, while their internal relative ordering remains intact.

Events that are not part of a sequence can be shuffled around anywhere in the queue.

If you are manually queuing events, and you want to declare them as a sequence, you can use the `Catan.fixed_sequence` API: 
```python
import catan
c = catan.Catan()
with c.fixed_sequence():
  c.queue("update-status", app, 0)
  c.queue("stop", app, 0)
```

This will make sure that, if you do `c.shuffle()`, the stop event will always fire after update-status does.
