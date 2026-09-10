# EveDB

EveDB is a general-purpose database with built-in event sourcing for typed
entities. It stores entity changes as events, maintains current state, and
provides historical reads and replay through a small operations API.

Create an entity, submit new field values, and read the state you need. EveDB
handles event history, versions, snapshots, and reconstruction with minimal
configuration.

## Entities and events

An entity is a record with predefined, typed fields, following a relational
data model. Its schema determines which fields exist, their types, and whether
they accept null values.

An event contains new values for one or more fields. The engine applies those
assignments directly. Fields omitted from an event retain their previous values.
Assigning null clears a field's value where the schema permits it.

Events require no application-specific types or user-defined handlers. Their
meaning is expressed by the field assignments they contain. They record changes
to data; the application supplies any business context behind those changes.

## Operations

| Operation | Description |
| --- | --- |
| `create` | Create an entity with its initial state |
| `apply` | Apply field changes and record an event |
| `delete` | Remove an entity from active use while preserving its retained history |
| `get` | Read the current state of an entity |
| `get_at_version` | Read an entity's state at a specified version |
| `replay` | Reconstruct current state from the retained base state and event sequence |
| `replay_to_version` | Reconstruct state from the retained base through a specified version |

Creation establishes the entity's initial state. Subsequent events advance its
version. Deletion ends its active life while leaving historical states available
within the retained history.

Historical reads and replay do not roll back the entity's current version.

## Versions and transactions

Each entity has its own ordered sequence of versions. A version identifies a
specific point in that entity's history.

Transactions commit data changes and their corresponding events atomically.
Several changes to the same entity within a transaction remain separate,
ordered events with separate versions.

For example, a transaction beginning at entity version 7 can produce versions 8
and 9. Other transactions see the committed transition from 7 to 9. After
commit, historical reads can also retrieve version 8.

These intermediate versions describe the entity's history. They are not
independently committed snapshots of the entire database.

## Snapshots and replay

EveDB manages snapshots automatically. A snapshot associates entity state with
a version and accelerates access to historical or current state.

Replay reconstructs an entity by starting from its retained base state and
applying the subsequent events in order. It can run through the current version
or stop at a specified version.

The base state and event sequence determine the reconstructed result. Current
state and snapshots remain consistent with that history.

## History retention and archives

Local history can be limited to the latest N events per entity. When older
events are removed, their accumulated changes are preserved in a new base
state. Entity versions continue without resetting or renumbering.

For an entity at version 1000 retaining its latest 100 events:

```text
Base state:       version 900
Retained events:  versions 901 through 1000
Replay:           state at 900 + events 901 through 1000
```

Older events can be archived before removal from local storage. Archive
recovery is explicit. If a requested state cannot be reconstructed from
available local data, EveDB returns an error identifying the available version
range.

Compression preserves information. Retention determines how much history
remains available. Full local replay starts from the retained base, which may
already incorporate events no longer stored locally.

## Operation and maintenance

EveDB runs as a standalone database server with a data directory. The database
manages write-ahead logging, crash recovery, snapshots, compression, and
background maintenance.

Application developers work with entities and their history through the
operations API. They do not need to maintain a separate event store,
current-state projection, or snapshot process.
