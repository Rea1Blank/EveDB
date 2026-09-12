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
