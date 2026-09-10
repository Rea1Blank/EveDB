# Table storage

Status: implemented experimental local engine, written in Rust using the standard
library. File formats and APIs can change without a migration path. This is the
first storage implementation, not a production-qualified database server.

Sources checked on 2026-09-11. The comparison below describes the referenced
implementations; subsequent recommendations are EveDB design choices, not
claims that another engine implements this exact combination.

## Workload and logical table

The [product model](product.md) needs point reads of current entity state,
field updates, ordered per-entity history, historical reconstruction, and
atomic transactions that include both state and events. Start with row storage
for current state. Columnar analytics can be evaluated against a concrete
query workload later.

A logical table groups entities sharing a typed schema. It needs a stable
table identifier, schema versions, current records, retained base states,
events, optional acceleration snapshots, and indexes. A table therefore does
not have to correspond to a single physical data structure.

## What existing engines do

| Engine | Physical organization | Useful lesson for EveDB |
| --- | --- | --- |
| PostgreSQL heap | Tables and indexes have separate relation files, normally segmented at 1 GiB. Pages are usually 8 KiB and hold a slot directory and variable-length tuples. | Separate logical identity from physical storage; use pages and slot references. |
| SQLite | The main database file contains pages for table/index B-trees, a schema catalog, free space, and overflow data. Transaction recovery can require additional journal/WAL files. | Explicit byte formats, versioned headers, and a catalog that maps objects to storage roots. |
| MySQL InnoDB | A file-per-table tablespace contains a table and its indexes. Row data lives in the clustered index, normally keyed by the primary key. | Storing current rows directly in a key-ordered tree is a viable alternative to heap plus index. |
| RocksDB | A log-structured merge tree uses memory tables, persistent sorted files, a WAL, and compaction. | Sorted immutable runs are worth considering for history indexes; include background rewriting in the cost model. |
| KurrentDB | New records append to chunk files. A separate index maps a stream hash and event number to a logical log position. | Append history sequentially and index it by entity/version; logical stream ownership need not mean a file per entity. |

References:

- PostgreSQL: [file layout](https://www.postgresql.org/docs/18/storage-file-layout.html)
  and [page layout](https://www.postgresql.org/docs/18/storage-page-layout.html).
- SQLite: [database file format](https://www.sqlite.org/fileformat.html).
- InnoDB: [file-per-table tablespaces](https://dev.mysql.com/doc/refman/8.4/en/innodb-file-per-table-tablespaces.html)
  and [clustered/secondary indexes](https://dev.mysql.com/doc/refman/8.4/en/innodb-index-types.html).
- RocksDB: [engine overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview).
- KurrentDB: [chunk storage](https://docs.kurrent.io/server/v25.0/configuration/db-config)
  and [default index](https://docs.kurrent.io/server/v25.1/features/indexes/default).

## Implemented file layout

One process owns a database directory through an OS file lock. Numeric names
are 20-digit decimal IDs. Table names are resolved through the catalog, so a
rename keeps the table ID and physical identity.

```text
data/
  control                              format marker EVEDB001
  LOCK                                 exclusive OS lock
  catalog/
    <generation>.catalog               schemas, file sizes and CRC32C checksums
  wal/
    <generation>.wal                   checkpoint root, then transaction frames
  tables/
    <table-id>/<generation>/
      current.pages                    latest entity states and tombstones
      bases.pages                      retained reconstruction bases
      snapshots.pages                  optional acceleration states
      primary.index                    entity -> current/base locations
      history.index                    (entity, version) -> segment/offset
      snapshots.index                  (entity, version) -> snapshot location
      history/
        <segment-id>.events             sequential framed event records
```

A generation is immutable after publication. Current rows use an 8 KiB slotted
heap and a separate B+tree primary index. The tree is bulk-built bottom-up during
a checkpoint; transactional writes first enter the WAL and an in-memory overlay.
This version does not split or mutate tree pages in place. Linked leaves support
ordered range scans. Heap locators combine a page ID and slot ID and are scoped
to a generation; entity IDs remain independent of those locations.

Each heap/index page carries a version, identity, checkpoint sequence number,
and CRC32C. [The page format](storage-pages.md) specifies the 40-byte heap header,
slot directory, replacement, deletion, and compaction. The heap layer fragments
large records across checked overflow pages. Integers are little-endian; typed
fields use explicit field IDs and value tags. Null and an omitted assignment
have different encodings. Native Rust layouts are never written to disk.

Frames use a 32-byte header (magic, format, kind, sequence, payload length,
payload CRC32C, header CRC32C), followed by payload and an eight-byte `EVCOMMIT`
trailer. Event files are packed in entity/version order at checkpoint time and
rotate at the configured segment target. Event payloads use simple RLE only
when smaller, with an uncompressed fallback. WAL payloads are uncompressed.

## Logical records and operations

Creation establishes version zero and its full base state. Every `apply` and
`delete` advances the entity version. Each event stores the database transaction
sequence, schema version, operation, and field assignments. Several events in one
transaction keep separate entity versions and share the transaction sequence.

| API | Behavior |
| --- | --- |
| `Database::open[_with_options]` | Initialize an empty directory or lock, verify, and recover an existing database |
| `create_table`, `transaction` / `write` | Define tables; group schema and entity operations in an atomic transaction |
| `create`, `apply`, `delete` | Create state, append assignments, or append a deletion tombstone |
| `get` | Read current state through the overlay or primary index and heap, without replay |
| `get_at_version` | Seek to the closest eligible snapshot or retained base, then read only the required event range |
| `replay`, `replay_to_version` | Reconstruct from the retained base, deliberately bypassing acceleration snapshots |
| `events`, `retained_range` | Inspect retained events and available version bounds |
| `retain_last` | Advance the retained base atomically and keep the latest N events |
| `scan` | Visit active current entities in ID order, merging disk and overlay |
| `checkpoint` | Rewrite live data, publish a generation, and reclaim older files |

Tables support Bool, Int64, UInt64, finite Float64, UTF-8 Text, Bytes, and nullable
fields. Stable field IDs preserve event meaning. Schema changes currently allow
renaming fields and adding nullable fields; existing fields cannot be removed or
change type/nullability. Old schema versions remain in the catalog. Existing
entity states retain their own schema until changed; a later event fills new
nullable fields. Historical reads preserve the schema of the requested version.

Deleted entities disappear from `get` and `scan`. Their tombstones and retained
history remain readable, and their IDs cannot be reused. An out-of-range history
request returns `VersionUnavailable` with the first and last available versions.
An absent entity returns `NotFound` for history operations.

## Transactions and recovery

A transaction exclusively borrows the database handle, stages touched entities
and catalog changes, and validates each write before committing. A failed write
aborts the transaction; dropping a transaction discards it. The complete operation
batch is encoded into one checked WAL frame. `sync_all` must succeed before the
staged changes become visible or commit returns success. A mutex can serialize
access to one handle across threads; there are no concurrent MVCC readers yet.

On a WAL write/sync error, `CommitUnknown` means that reopening may find either
the complete transaction or no transaction. Data operations on that handle
return `NeedsRecovery` until it is dropped and reopened. A caller must resolve
this outcome before retrying a non-idempotent write. Catalog/sequence accessors
still expose the last published metadata; they do not establish a failed commit's
outcome. A checkpoint failure after publication starts also requires recovery.

Checkpointing proceeds as follows:

1. Reserve a new generation and write its heap, event, and index files.
2. Synchronize the files and write a checked catalog/manifest with file sizes
   and whole-file checksums.
3. Create a new WAL segment whose first frame contains that complete root;
   synchronize it before publishing the new generation in memory.
4. Retain the preceding usable checkpoint and WAL needed to replay from it.
   Reclaim older numeric generations only after publication.

The WAL's initial root frame is the publication record; the catalog file is a
separate copy. Recovery enumerates WAL roots, verifies checkpoint files, selects
the newest usable checkpoint, and replays subsequent complete transaction frames
in strict sequence. A damaged latest checkpoint can be rebuilt from the retained
preceding checkpoint and WAL. Incomplete checkpoint headers are unpublished
orphans. Only an incomplete transaction tail of the active WAL is truncated;
complete corrupt frames and incomplete sealed transaction tails produce errors.
Checksums detect damage; recovery requires an intact baseline and log.

The recovery WAL and entity history have different lifetimes. History is kept
according to retention, while WAL is retained according to checkpoint recovery.
Automatic retention/snapshot actions are explicit WAL operations, so reopening
with different options does not reinterpret already committed transactions.

Retaining N events advances the base to `current_version - N` when necessary.
Retaining zero events stores the current state as the new base. Versions never
reset. Logical removal is immediate, while physical reclamation needs checkpoints;
the preceding recovery generation can keep older history until another checkpoint.
Rewriting copies all still-needed records, including inactive entities, before
old shared segments can be deleted. This is not secure erasure of old bytes.

## Defaults, costs, and current limits

| Setting or bound | Value |
| --- | --- |
| Heap/index page size | 8192 bytes |
| Automatic checkpoint target | 8 MiB of active WAL, checked before the next transaction |
| History segment target | 8 MiB; one event may exceed the target |
| Automatic snapshot interval | Every 32 entity versions; zero disables it |
| Automatic retention | Disabled (`None`); retain all events |
| History compression | RLE with a raw fallback, enabled |
| Fields per schema | 1..=4096; IDs nonzero and unique |
| Table/field name length | 1..=255 UTF-8 bytes, without control characters |
| Encoded field collection | At most 8 MiB |
| Encoded record / frame payload | At most 16 MiB / 64 MiB |

Only successful transactions are acknowledged after WAL synchronization. Process
crash recovery is tested. Power-loss behavior is not qualified: directory
synchronization is implemented on Unix and currently a no-op on Windows, and the
filesystem/device must honor file synchronization. The tests do not emulate lost
OS caches, storage-controller caches, or arbitrary sector tearing. Failed first
initialization can leave a directory needing manual inspection before reuse.
Use disposable data while these guarantees and formats mature.

Checkpoints synchronously rewrite the whole database. Startup streams every
checkpoint file to verify its checksum. Point reads fetch index/heap pages on
demand, but there is no engine buffer pool or open-file cache. A changed entity's
whole retained history is loaded into memory; transactions stage all touched
entities, and recovery builds the overlay from the remaining WAL. The WAL size
target is not a hard memory bound. Index construction retains one separator per
leaf before building parent levels, so its memory also grows with index size.
`events` returns an allocated vector. Large histories and frequent checkpoints
can therefore be expensive despite indexed historical reads.

There is no server protocol, SQL/query planner, secondary field index, online
backup/archive format, background maintenance, table drop, or schema migration
with type changes in this slice. Page-size/clustered-tree/LSM alternatives and
production compression remain benchmark decisions. We have not established that
8 KiB or heap-plus-index is optimal. See [measurements](storage-benchmarks.md).

## Validation and examples

Run the lifecycle example on an unused directory:

```sh
cargo run --locked -p evedb-core --example lifecycle -- .local/lifecycle
cargo run --locked -p evedb-cli -- inspect .local/lifecycle
cargo run --locked -p evedb-cli -- checkpoint .local/lifecycle
cargo test --workspace --all-features --locked
```

Tests cover page checksums and mutation boundaries, overflow chains, multi-level
B+trees, truncated/corrupt frames, typed codecs, atomic cross-table operations,
schema history, snapshots, replay, retention, locks, checkpoint fallback, and
reclamation. A historical-read test damages an early event after opening and
confirms that a late snapshot read succeeds while explicit replay detects damage.

With the `fault-injection` feature, subprocess tests stop the writer before,
during, and after WAL writes/syncs, and at five checkpoint boundaries. Four
injected WAL I/O errors verify uncertain outcomes and handle poisoning. A process
exit after a complete write can recover that transaction even before sync,
because the operating system remains alive. This does not promise that such a
transaction survives power loss. The feature is disabled in default builds.
