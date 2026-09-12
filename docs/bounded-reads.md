# Bounded history reads

`Database`, `SharedDatabase`, `Reader`, and `ReadSnapshot` expose `history`.
The cursor reads a coherent snapshot, including events still in the overlay.
Checkpoint, retention, and compaction can proceed while that snapshot is pinned.

```rust,no_run
use evedb_core::{Database, HistoryOptions, Result};

fn read_history(db: &Database, table: u64, entity: u64) -> Result<()> {
    let mut history = db.history(table, entity, HistoryOptions {
        first_version: Some(100),
        last_version: Some(200),
        max_events: 64,
        max_bytes: 1024 * 1024,
        ..HistoryOptions::default()
    })?;
    while let Some(batch) = history.next_batch()? {
        for event in batch.iter() {
            println!("{}", event.version);
        }
    }
    Ok(())
}
```

Bounds are inclusive event versions. The retained base is a materialized state,
so the first available event is `base + 1`. Omitted bounds select all retained
events; an entity with no retained events yields no batches. Explicit unavailable
versions and reversed ranges are errors. A gap in stored history is corruption,
not a shorter successful result.

Each successful batch is nonempty and bounded by both event count and logical
decoded bytes. An event that cannot fit alone returns `LimitExceeded`; increase
the batch size explicitly if the database limits allow it. Routes are sought
again at the next version between batches; no decoded history is cached in the
cursor. Owned batches retain their memory reservations until dropped. Keeping
several batches can exhaust the shared read pool, even across different cursors.

Cloned `CancellationToken`s can cancel a read from another thread. Checks run
between records and before decoded allocation; an OS I/O call already running
is not forcibly interrupted. Each batch also has an operation deadline bounded
by snapshot expiry. Errors, observed cancellation, completion, and cursor drop
release the cursor's snapshot clone. An idle cursor remains subject to snapshot
expiry. After an error the cursor is terminal; subsequent calls return `None`.

## Memory admission

These budgets are per database. They count defined allocation costs, not process
RSS. Decoded fields carry a conservative 128-byte node/slot allowance plus
variable-value storage; owned input strings/vectors are charged by capacity.
Record size is checked from encoded field lengths before decoded allocation.

| Limit | Default | Scope |
| --- | --- | --- |
| `max_decoded_record_bytes` | 64 MiB | One decoded entity/event, including field allowances |
| `max_transaction_decoded_bytes` | 256 MiB | Cumulative admitted decoded transaction allocations |
| `max_resident_decoded_write_bytes` | 1 GiB | Transaction charges retained by staging, queues, and views |
| `max_read_memory_bytes` | 256 MiB | Active record decoding, history collection, owned batches |
| `max_scratch_bytes` | 128 MiB | Concurrent raw heap/frame and decompression buffers |
| `max_read_result_bytes` | 64 MiB | Logical size of an `events()` result |
| `max_history_batch_events` | 4096 | Database ceiling for one batch |
| `max_history_batch_bytes` | 64 MiB | Database ceiling for one batch |

Cursor defaults are 256 events and 1 MiB. Requested batch bounds are capped by
the database ceilings. Counters are available through `resource_usage` for
admission diagnostics; no external metrics infrastructure is added.

Decoded write admission accounts for copied current/base records, new operations,
events, and snapshots before they enter staging. Charges follow the existing
transaction reservations into published views. Checkpoint and rebind release
them only after their owners are gone. Charges are conservative and cumulative:
temporary or superseded allocations may remain charged until that reservation
retires. Recovery bypasses lowered admission limits for acknowledged WAL data;
new writes remain subject to the configured limits. New writes also reject
entity states exceeding the storage format's 16 MiB encoded-record limit.

The compatibility `events()` method now uses bounded batches and a result budget.
It reserves the remaining result allowance before decoding more events and uses
one deadline for the entire collection. Its returned `Vec<Event>` is owned by
the caller; the engine stops accounting it at return. The same ownership boundary
applies to ordinary owned entity results and caller-created clones. Page caches,
routing metadata, WAL encoding, and maintenance output buffers have their own
existing bounds. These pools are not a hard cap on all allocator overhead,
reconstruction intermediates, caller memory, or total process memory.
