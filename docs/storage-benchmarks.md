# Storage smoke benchmark

The executable example provides a reproducible first measurement, with
correctness assertions on reads, scans, and retention:

```sh
cargo run --release --locked -p evedb-core --example storage_bench -- .local/bench-10000 10000
```

Choose an unused directory for each run. The optional row count defaults to
10,000; the example leaves its database in place for inspection. Payloads are
1,024 repeated bytes plus a UInt64 field. Creation uses batches of 128 rows.
Defaults include an 8 MiB checkpoint target and snapshots every 32 versions.
The update phase applies 300 separate events to one entity in one transaction.
Historical reads request version 299; explicit replay reconstructs version 300.
Retention keeps ten events, then writes two checkpoints to release the older
recovery generation. Measurements include synchronous file I/O.

Every pull request runs the same example twice, once on its base commit and
once on its head, and receives one comment with the two columns and their
difference. Marks appear beyond ten percent. That comparison is one run per
commit on a shared GitHub runner against the numbers below, which come from a
quiet local machine; it indicates where to look, and it does not block a merge.

## Recorded runs

Three local Windows x64 runs, Rust `1.98.1-x86_64-pc-windows-gnu`, optimized
release build, no third-party crates. The first column is the original
measurement; the second is the same workload after checkpoint pages gained a
bounded cache, index nodes gained binary search, and ordered scans stopped
descending the tree twice per row; the third is after checkpoints became
incremental and started collecting generations. Each is a single run on one
machine, in a separate session.

| Operation | First | With the page cache | Incremental |
| --- | ---: | ---: | ---: |
| Create 10,000 entities, including automatic checkpoint work | 1,360.427 ms | 731.138 ms | 722.965 ms |
| Explicit checkpoint after creation | 9,070.136 ms | 874.448 ms | 196.621 ms |
| Reopen and verify checkpoint files | 77.751 ms | 82.975 ms | 79.878 ms |
| 1,000 deterministic pseudorandom current reads | 414.378 ms | 30.781 ms | 29.645 ms |
| Apply 300 events in one committed transaction | 9.508 ms | 9.984 ms | 9.773 ms |
| 100 indexed historical reads | 163.198 ms | 12.840 ms | 12.446 ms |
| 100 full replays from the retained base | 749.397 ms | 292.505 ms | 262.696 ms |
| Scan all 10,000 active entities | 4,327.973 ms | 58.621 ms | 33.967 ms |
| Retention and two full checkpoints | 22,934.397 ms | 1,809.103 ms | 88.079 ms |

Directory size was 47,632,445 bytes before retention and 47,565,864 after it in
the first two runs, and 24,270,891 before and 24,319,042 after in the third. The
cache changed how pages are read, not what is written; sharing files between
generations changed what is written. Roughly half the earlier figure was the
retained recovery generation holding a second full copy of records identical to
the published ones. Removing 290 small events from one entity still saves little
space beside the unchanged 10,000 current/base records, and now leaves them in
place until a collection rewrites that generation.

The explicit checkpoint after creation improved because it no longer copies the
records it just wrote; what remains is rebuilding the primary index over all
10,000 entities. Retention with two checkpoints improved the most, from seconds
to milliseconds, because those checkpoints touch one entity instead of the
database. Reads are unchanged, as expected: the primary index still names each
entity's generation directly, so a point read costs one descent regardless of how
many generations a manifest references.

Reopening did not improve, which is expected: startup still streams every
referenced file to verify its whole-file checksum, and that cost is proportional
to the database rather than to the number of files.

## Interpretation and next measurements

These are single-run totals with the operating-system cache enabled, without
cold-cache control, concurrent clients, percentiles, or a constrained process
memory budget. The persisted dataset exceeds the 8 MiB WAL target, but that target
does not bound engine memory. The run therefore does not establish out-of-memory
safety or performance for a database larger than physical RAM.

Snapshot lookup reduces reconstruction work in this workload. Caching decoded
pages removed the dominant cost of every read path, because the engine had been
reopening and revalidating a file for each lookup; the remaining per-read cost is
decoding a record into owned memory. Maintenance no longer scales with the whole
database, but it has not become free: a checkpoint still rebuilds the primary
index over every live entity, which is what the third column's 196 ms mostly is.

This benchmark also does not exercise what incremental publication costs over
time. It writes once, then touches one entity, so no generation ever loses enough
density to be collected and the manifest never approaches its generation budget.
A workload that repeatedly updates a changing subset would show the collector
working, the residue of superseded records between collections, and the cost of a
`compact` pass — none of which these numbers cover.

This run does not compare engines, page sizes, clustered trees, LSM layouts, or
compression algorithms. Future decisions need repeated cold/warm measurements,
larger-than-RAM data under an enforced memory budget, histories of different
lengths, compressible and random payloads, read/write mixtures, checkpoint write
amplification, and update-heavy workloads that drive collection. Power-loss tests are separate from this benchmark and from
the existing subprocess crash tests.
