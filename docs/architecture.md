# Architecture

## Workspace

EveDB uses a Cargo workspace with Rust edition 2024 and resolver 3. Package
version, license, toolchain requirements, and lint settings are shared at the
workspace root. Cargo.lock is committed. Packages are not published to crates.io.

The dependency direction is `evedb-cli -> evedb-core`. The core crate
must not depend on CLI code. Both crates use only the Rust standard library.

The executable provides initialization, catalog inspection, and checkpoint
commands. It uses the same public database API as Rust applications and the
lifecycle example. Inspection opens the database with its exclusive lock and
normal recovery; it is not an offline forensic reader.

## Local engine

The core modules have the following responsibilities:

| Module | Responsibility |
| --- | --- |
| `model` | Typed schemas, entities, events, version reconstruction, retention |
| `database` | Public API, transaction staging, WAL recovery, checkpoint publication |
| `snapshot` | Generation manifests, table readers/writers, indexed historical reads, compression |
| `storage/page` | Checked 8 KiB slotted-page format |
| `storage/heap` | Packed records and overflow chains |
| `storage/index` | Immutable B+tree build, point lookup, predecessor lookup, range scan |
| `storage/frame` | Bounded checksummed WAL/event/catalog frames |
| `codec`, `checksum`, `error` | Explicit byte encodings, CRC32C, shared errors |

One database handle owns the directory. Transactions exclusively borrow it,
stage changes, synchronize one complete WAL frame, then publish all changes
together. Current reads combine the recent in-memory overlay with immutable
checkpoint files. Historical reads seek through snapshot and event indexes;
explicit replay always starts at the retained base. Checkpoints rewrite live
data into a new generation and retain a recovery baseline before reclaiming
old files. There are no in-place tree updates, MVCC, or asynchronous workers.

The [storage design](storage.md) explains the borrowed database techniques,
actual file layout, synchronization protocol, and experimental limits. The
[benchmark notes](storage-benchmarks.md) record a reproducible first measurement.
Power-loss qualification and a hard memory budget remain open work.

## Growth

Keep closely related code in modules first. Add separate crates for storage,
protocols, or a server when their interfaces and dependency boundaries become
concrete. Keep benchmarks and integration tests beside the components they test.

The [product description](product.md) covers the entity model, operations API,
versioned history, replay, and standalone server experience.
