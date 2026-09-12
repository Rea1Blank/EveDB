# Production roadmap

EveDB is intended to become a production database for concurrent clients and
sustained high request rates. Production readiness is a release gate, not a
description of the current experimental engine. This plan replaces the earlier
decision to defer multiple writers until benchmarks justify them.

## Review of the existing plans

Reviewed: README.md, docs/product.md, docs/architecture.md, docs/storage.md,
docs/storage-pages.md, docs/storage-benchmarks.md, and the original roadmap
against the implementation and its tests.

| Decision | Disposition and reason |
| --- | --- |
| Typed entities, history, retention, atomic cross-table writes | Keep: these define the product and its consistency boundary. |
| Immutable files, checksums, incremental collection | Keep and evolve; a buffer-pool/B+tree rewrite is not justified by current evidence. |
| Shared readers and retained snapshot generations | Required now; lifetimes include the directory lock, catalog, overlay, pager, and history files. |
| Multiple writers only if measurements justify them | Reject: independently staged writers are required now. Serialize commit ordering initially, not client transaction lifetimes. |
| Changing a mutable borrow later makes concurrency cheap | Reject: conflict detection, catalog changes, retention, publication, reclamation, and shared failures need contracts now. |
| Entity version alone suffices for conflicts | Reject: retention and explicit snapshots do not advance it. Serializable also needs read and predicate validation. |
| Snapshot reads are always serializable | Reject with concurrent writers: snapshot isolation allows write skew. Advertise the actual level. |
| Empty in-flight set as future MVCC machinery | Defer: private staging and ordered publication need a committed watermark. Add status tracking with a protocol that uses it. |
| Out-of-order WAL recovery is a local rule change | Reject: it needs a durable-prefix and hole protocol. Keep ordered recovery initially. |
| One fsync per transaction | Temporary only. Group commit is required before write-throughput qualification; acknowledge only after durable sync. |
| Run-routed indexes and background maintenance | Required throughput work; bound read amplification and maintenance debt. |
| No compatibility or production qualification promised | Valid current status, unacceptable final target. Define upgrades, restore, and qualification before release. |
| Standard-library-only implementation | Current fact, not a product constraint. Evaluate dependencies for TLS, protocols, runtime, and testing. |
| Distributed sharding, SQL planner, columnar analytics, advanced SSI | Defer until single-node gates pass. Preserve interfaces where cheap without claiming capabilities. |

## Transaction architecture

One process owns a directory. Cloneable in-process connections will map to
separate network sessions when the server is added. An idle transaction must not
lock out other clients. Transactions stage privately and read their own writes.
Commit validates dependencies, assigns the next sequence, writes a complete WAL
batch, synchronizes, and publishes one coherent view. Cross-table state and
events become visible together.

Use explicit transaction options: ReadCommitted, Snapshot, and Serializable.
Initially implement Snapshot and reject unsupported levels with a typed error,
without silently weakening the request. ReadCommitted will acquire a new
committed view per operation. Snapshot pins one view for the transaction.
Serializable requires read/absence/range dependencies or SSI, including mixed
isolation interactions; same-key write conflicts do not implement it.

Snapshot writers validate every touched entity with first-committer-wins,
including absent IDs, tombstones, retention, and acceleration snapshots.
Catalog changes conflict conservatively with transactions using an older
catalog. An aborted operation cannot commit. Conflicts mean retry the entire
transaction. Uncertain I/O outcomes require recovery and outcome resolution
before retry. A shared recovery-required state stops other connections too.

Snapshots own immutable read state, pin referenced files, and retain the OS
directory lock. Cleanup uses the union of current, recovery, and live snapshot
files. Report count, oldest sequence, and retained bytes. Initial copy-on-write
maps are a bridge, not the final memory/throughput design.

The commit coordinator is an internal boundary: group synchronization, parallel
preparation, and eventual pipelined publication preserve the API and recovery
rules. No network session owns its mutex across client calls. Background
checkpoints capture a committed frontier, write outside coordination, and
publish without losing newer changes. Reclamation must eventually account for
backup and replication consumers. Event retention and snapshot retention are
different policies.

## Implementation sequence

Each numbered step gets its own branch and PR with tests and documentation.
Merge only the reviewed head after required checks pass. The first delivery is
steps 1–3; subsequent steps are release requirements, not a claim that the first
delivery qualifies for production.

Steps 1–3 are implemented. See [concurrent transactions](docs/transactions.md)
for the API, guarantees and failure semantics. The authorized write-path series
(budgets, expiry, staging, shared overlay, group commit and independent history)
is implemented. External metrics/load qualification remain deferred; no production
throughput SLO has been qualified yet.

The completed authorized implementation series is detailed in
[write-path implementation](docs/write-path.md): budgets, deadlines, staging,
shared overlay, group commit, and independent history storage. External metrics
and the load harness are deferred to a separate workstream rather than gating
this series. Resource-accounting counters and correctness tests are still required.

| Step | Deliverable | Acceptance gate |
| --- | --- | --- |
| 1 | Product requirements and architectural contracts | Deferred capabilities and current limitations are explicit. |
| 2 | Cloneable readers, pinned snapshots, file/lock lifetimes | Consistent reads through writes, retention, compaction, and writer drop; later reclamation after snapshots drop. |
| 3 | Cloneable connections, independently staged writers, isolation options, conflicts | Synchronized clients stage simultaneously; disjoint writes commit; same/absent-key, catalog, and retention conflicts abort atomically; reopen preserves winners. |
| 4 | Bounded admission and concurrent workload harness | Bound active transactions, staging bytes, snapshot age/bytes, responses, and queue depth; overload/deadline errors; RPS and latency distributions. |
| 5 | Group commit and coordinator failure tests | Share sync; preserve acknowledged commits on restart; poison affected requests on faults; bound queue count/bytes/delay. |
| 6 | Current-state writes independent of history; scalable versioned overlays | Update cost independent of retained history; no copying all recent keys at snapshot/commit; bounded memory. |
| 7 | Per-run routing, background checkpoint/compaction, cache contention | Stable maintenance debt and p99 under churn, bounded amplification, restart-safe maintenance. |
| 8 | ReadCommitted, then Serializable in separate PRs | Isolation histories, phantom/absence/write-skew tests, DDL interactions, randomized model checks. |
| 9 | Server/protocol, split into transport/session/security PRs | Concurrent remote clients; bounds, cancellation, deadlines, transaction cleanup, auth, authorization, TLS, idempotency/outcome resolution, graceful drain, metrics. |
| 10 | Durability, compatibility, backup/restore, split by capability | Platform durability contract, storage fault matrix, crash/power-loss qualification, verified restore, upgrades, corruption diagnostics, disk-full handling. |
| 11 | Qualification and release | Sustained load/failure gates below pass on declared platforms; publish reproducible results and runbooks. |

## Performance and release gates

An RPS number without workload, hardware, durability, and latency is not a target.
Start with 1/8/32/128 clients, 1 KiB and 16 KiB random/compressible payloads,
100/0, 95/5, 50/50, and 0/100 read/write mixes, uniform/hot keys, single-operation
and cross-table batches, and short/long history. Include warm/cold and
larger-than-RAM datasets with an enforced memory budget. Record machine,
storage, filesystem, build, and options.

For the later qualification workstream, measure a baseline and commit a hardware-specific SLO
profile with numeric RPS, p95/p99 latency, error budget, and recovery-time targets.
Use open-loop arrivals as well as saturation tests; report queue time, conflicts,
retries, rejections, durable commits, and successful operations separately.
Single-run totals and batched event counts do not establish network RPS or
durable transactions per second.

Release evidence must include:

- No lost acknowledged commits, partial transactions, dirty reads, lost updates,
  or reclamation under live snapshots in tested schedules.
- WAL/publication/compaction/backup/restore fault coverage. Process termination
  is not power loss. Windows directory sync is currently a no-op and needs a
  platform-specific resolution before durability qualification.
- Bounded memory, descriptors, WAL, snapshot retention, queues, and compaction
  debt under sustained overload, with explicit backpressure and timeouts.
- At least 24 hours of mixed load and overload/recovery trials with maintenance
  enabled, meeting the recorded SLO profile without hidden retries.
- Verified restore and recovery time on production-sized data; documented
  upgrade/downgrade support, format rejection, and supported platforms.
- Metrics for latency, durable lag, conflicts, queues, memory, snapshots, cache,
  WAL, maintenance, recovery; actionable operating procedures.

Replication/HA is a separate release scope. The first qualified release may be
single-node with that availability limit documented; backup remains mandatory.
Existing unit and subprocess tests alone cannot qualify production readiness.

## Design references

These support the isolation/WAL distinctions; EveDB's architecture is our choice.

- [PostgreSQL isolation](https://www.postgresql.org/docs/18/transaction-iso.html): snapshot isolation and serialization anomalies.
- [SET TRANSACTION](https://www.postgresql.org/docs/18/sql-set-transaction.html): explicit levels and per-command snapshots.
- [RocksDB WAL performance](https://github.com/facebook/rocksdb/wiki/WAL-Performance): sharing synchronization.
- [RocksDB unordered writes](https://github.com/facebook/rocksdb/wiki/unordered_write): publication and snapshot protocols.

## Next maintenance series

See [maintenance and bounded reads](docs/maintenance-series.md) and issue #18.
Stage 1 implements background shared checkpoint/compaction with a frozen WAL
frontier, bounded job admission, and an uncheckpointed transaction-WAL limit.
Stage 2 implements packed, disjoint index partitions with direct root routing,
unchanged-partition reuse, slot translation, and bounded routing admission.
Stage 3 (bounded streaming reads) follows separately.
