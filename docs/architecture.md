# Architecture

## Workspace

EveDB uses a Cargo workspace with Rust edition 2024 and resolver 3. Package
version, license, toolchain requirements, and lint settings are shared at the
workspace root. Cargo.lock is committed. Packages are not published to crates.io.

The dependency direction is `evedb-cli -> evedb-core`. The core crate
must not depend on CLI code. Both crates use only the Rust standard library.

The executable provides initialization, catalog inspection, checkpoint, and
compaction commands. It uses the same public database API as Rust applications and the
lifecycle example. Inspection opens the database with its exclusive lock and
normal recovery; it is not an offline forensic reader.

## Local engine

The core modules have the following responsibilities:

| Module | Responsibility |
| --- | --- |
| `model` | Typed schemas, entities, events, version reconstruction, retention |
| `database` | Public API, transaction staging, WAL recovery, checkpoint publication |
| `reader` | Immutable committed views, cloneable readers, snapshot pins and retention statistics |
| `shared` | Cloneable connections, isolation options, write conflicts and ordered commit coordination |
| `snapshot` | Generation manifests, slot addressing, occupancy counts, table readers/writers, indexed historical reads, compression |
| `storage/page` | Checked 8 KiB slotted-page format |
| `storage/pager` | Bounded cache of decoded pages and open checkpoint files |
| `storage/heap` | Packed records and overflow chains |
| `storage/index` | Immutable B+tree build, point lookup, predecessor lookup, range scan |
| `storage/frame` | Bounded checksummed WAL/event/catalog frames |
| `codec`, `checksum`, `error` | Explicit byte encodings, CRC32C, shared errors |

One database owner holds the directory. Local transactions borrow it exclusively;
shared connections stage independently against pinned views and validate write
dependencies under a commit coordinator. Transactions
stage changes into independent WAL frames. Shared commits can synchronize a
bounded group before coherent publication. Current reads combine the recent
in-memory overlay with immutable
checkpoint files. Historical reads seek through snapshot and event indexes;
explicit replay always starts at the retained base. A checkpoint writes only the
current states a transaction touched, keeps historical payloads independently,
and publishes a manifest addressing every referenced file. It
also collects generations that have lost density or exceed the generation budget,
rewriting their live entities. A recovery baseline is retained, and files no
manifest or live snapshot references are deleted. Cloneable readers acquire one
immutable view per operation; pinned snapshots keep their catalog, overlay, and
checkpoint files across operations. They retain the directory lock even after
the writer drops. Readers do not hold a publication lock during I/O or callbacks.
There are no in-place tree updates or background storage-maintenance workers:
collection runs on the writing thread. A weak-reference expiry worker releases
abandoned transaction and snapshot leases.

Published states share roots, catalogs, and individual overlay entities. Updating
a shared state currently copies its overlay map, not entity histories. This is
a throughput/memory limit scheduled for replacement in the production roadmap.
Pin statistics report unique referenced checkpoint bytes, not total memory or
bytes exclusively retained by old snapshots.

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
