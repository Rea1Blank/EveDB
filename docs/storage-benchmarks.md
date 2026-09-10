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

## Recorded run

One local Windows x64 run on 2026-09-11, Rust
`1.98.1-x86_64-pc-windows-gnu`, optimized release build, no third-party crates:

| Operation | Total elapsed |
| --- | ---: |
| Create 10,000 entities, including automatic checkpoint work | 1,360.427 ms |
| Explicit checkpoint after creation | 9,070.136 ms |
| Reopen and verify checkpoint files | 77.751 ms |
| 1,000 deterministic pseudorandom current reads | 414.378 ms |
| Apply 300 events in one committed transaction | 9.508 ms |
| 100 indexed historical reads | 163.198 ms |
| 100 full replays from the retained base | 749.397 ms |
| Scan all 10,000 active entities | 4,327.973 ms |
| Retention and two full checkpoints | 22,934.397 ms |

Directory size was 47,632,445 bytes before retention and 47,565,864 bytes after
retention and two checkpoints. This includes two recovery generations; it is not
the size of one logical snapshot. Removing 290 small events from one entity saves
little space beside the unchanged 10,000 current/base records.

## Interpretation and next measurements

These are single-run totals with the operating-system cache enabled, without
cold-cache control, concurrent clients, percentiles, or a constrained process
memory budget. The persisted dataset exceeds the 8 MiB WAL target, but that target
does not bound engine memory. The run therefore does not establish out-of-memory
safety or performance for a database larger than physical RAM.

Snapshot lookup reduces reconstruction work in this workload. Full-generation
rewriting is already a substantial maintenance cost, and the scan still performs
individual indexed row reads. Optimize checkpoint granularity and the scan access
path before claiming high write throughput or efficient large-table scans.

This run does not compare engines, page sizes, clustered trees, LSM layouts, or
compression algorithms. Future decisions need repeated cold/warm measurements,
larger-than-RAM data under an enforced memory budget, histories of different
lengths, compressible and random payloads, read/write mixtures, and checkpoint
write amplification. Power-loss tests are separate from this benchmark and from
the existing subprocess crash tests.
