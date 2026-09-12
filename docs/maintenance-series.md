# Maintenance and bounded reads

The next implementation series follows the accepted production priorities.
Each stage ships separately, with recovery and concurrency regression tests.
Metrics and throughput benchmarks remain deferred.

1. Background maintenance: freeze a committed frontier, rotate WAL, build files
   outside the commit coordinator, publish the frontier without losing later
   writes. Admit at most one maintenance job, bound uncheckpointed WAL growth,
   and preserve recovery and snapshot references during reclamation.
2. Incremental indexes: independently replace index partitions, reuse untouched
   partitions, and bound the number of files consulted for one lookup. Change
   the storage format explicitly when routing metadata changes.
3. Bounded reads: stream history by version range in bounded batches, support
   cancellation and deadlines, and account decoded data and temporary buffers
   separately from encoded mutation admission. Keep compatibility collectors
   subject to a result-size budget.

Validation must include writes during a paused checkpoint, crash recovery on
both sides of publication, old snapshots during reclamation, unchanged index
file reuse, version-range boundaries, cancellation, and budget release after
errors. Passing correctness checks does not establish an RPS guarantee.

## Stage 1 implementation

The shared owner admits one worker and captures a WAL frontier before building.
The rotated WAL starts with the previous root and accepts newer transactions;
a later root frame publishes the completed checkpoint. A persistent tail overlay
and its history-file owners survive publication. Explicit calls wait outside
commit coordination; background errors are reported by `wait_for_maintenance`.
The EVEDB004 marker rejects older directories without modification; migration
is not implemented. Local Database maintenance remains synchronous.

## Stage 2 implementation

EVEDB005 adds disjoint index routes, original-to-current primary slot maps, and
per-partition history dependencies. Primary partitions cover 1024 IDs; history
and snapshot partitions cover 1024 versions of one entity. Trees are packed into
one file per kind/table/new generation and addressed by root page. Untouched
partitions keep their original file and page; compaction packs them again.
`max_index_partitions` bounds metadata before extra trees are created. Recovery
and snapshot reclamation retain every routed index file and payload dependency.
Older format markers are rejected without migration or modification.

## Stage 3 implementation

The history cursor exposes inclusive event-version ranges, count/byte bounded
batches, cancellation, and operation/snapshot deadlines. Batch ownership carries
read-memory reservations, while raw/decompression scratch and decoded transaction
allocations have separate budgets. The compatibility collector is result bounded.
See [bounded reads](bounded-reads.md) for the API, defaults, ownership boundary,
recovery exemption, and the distinction between logical accounting and RSS.
