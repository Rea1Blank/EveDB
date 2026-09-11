# Concurrent transactions

Use `SharedDatabase::open` once per directory and clone it for independent
in-process clients. `Database::into_shared` transfers an existing local owner.
`Database` remains available for exclusively borrowed local writes. A future
server will own the shared database and map network sessions to these handles;
there is no network transport yet.

```rust
use evedb_core::{IsolationLevel, SharedDatabase, TransactionOptions};

let database = SharedDatabase::open("data")?;
let connection = database.clone();
let mut transaction = connection.transaction_with_options(
    TransactionOptions::with_isolation(IsolationLevel::Snapshot),
)?;
// Stage create/apply/delete/schema/retention operations here.
// Other connections may read and stage their own transactions concurrently.
transaction.commit()?;
# Ok::<(), evedb_core::Error>(())
```

The runnable `concurrent` example demonstrates four clients starting write
transactions before any commits, while a reader holds an earlier snapshot:

```sh
cargo run --locked -p evedb-core --example concurrent -- .local/concurrent
```

## Isolation and conflicts

| Mode | Current behavior |
| --- | --- |
| Snapshot (default) | Pinned committed view plus own writes. Overlapping writers use first-committer-wins. Write skew is possible. |
| ReadCommitted | Reserved; returns `UnsupportedIsolation` before acquiring a transaction. |
| Serializable | Reserved; returns `UnsupportedIsolation`. No fallback to Snapshot. |
| Reader operations | Each call uses one committed view; `pin()` fixes the view across calls. |

Write sets include creates, updates, deletes, retention, and explicit acceleration
snapshots. Conflicts are based on the commit sequence of the latest write to a
key, not only its entity version. This detects concurrent creation of an absent
ID and changes that leave the version unchanged. IDs remain unavailable after
deletion. Catalog changes cause conservative catalog conflicts for older write
transactions, including changes that restore an earlier table name.

Validation, sequence assignment, WAL write/sync, publication, and conflict
bookkeeping run under one coordinator mutex. Transaction lifetime, reads, and
staging do not hold it. The assigned commit sequence can differ from transaction
start order; staged event metadata is stamped with the actual commit sequence.
Events already committed retain theirs. WAL recovery remains strictly ordered.

`Conflict` is a definite abort: start a fresh transaction and repeat its reads
and writes. `write` does not retry automatically because closures can perform
external side effects. Dropping a transaction discards its changes. Failed write
validation prevents subsequent commit. An empty Snapshot transaction returns its
captured sequence without appending a WAL record or validating read dependencies.

`CommitUnknown` is different: recovery may find the entire transaction or none
of it. All related connections, readers, and pending transactions then reject
data operations with `NeedsRecovery`. Drop all handles and snapshots, reopen,
and resolve the outcome before retrying. Durable request IDs/outcome queries
belong to the server protocol milestone and are not implemented yet.

## Lifetimes and remaining limits

Transactions retain their owner and pin their base view. Old readers remain
consistent across deletion, schema changes, retention, and compaction. The OS
directory lock survives until all connections, transactions, readers, and
snapshots are gone. Snapshots can retain history the latest view has pruned.

This is the concurrency foundation, not throughput or production qualification:

- Every commit still performs its own WAL synchronization. Maintenance runs
  synchronously under coordination; independent readers continue but commits
  wait behind it.
- Copy-on-write publication copies an overlay map; staging loads whole retained
  entities. Large histories and overlays still increase CPU/memory cost.
- Conflict metadata tracks changed keys. Explicit checkpoint/compact prunes
  revisions older than every active snapshot. Long-lived pins can retain this
  metadata and files; hard limits and automatic admission control are pending.
- Serializable read/predicate checks, ReadCommitted refresh, network sessions,
  cancellation, bounded queues, and group commit are sequenced in the roadmap.

Tests cover synchronized writers, disjoint and conflicting keys, catalog
conflicts, retention across compaction, abort/retry, commit sequence stamping,
WAL reopen, shared I/O failure, snapshot consistency, and owner lifetimes.
