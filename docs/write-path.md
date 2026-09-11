# Write path implementation series

The authorized sequence is admission budgets, deadlines/expiry, staging without
repeated history copies, a structurally shared overlay, bounded group commit,
and independent history storage/checkpoints. Each stage has its own PR.
External metrics and the workload harness are separate follow-up work; functional
resource counters and correctness tests remain part of these changes.

## Admission budgets

`Options::limits` bounds active transactions, independent snapshot pins, charged
checkpoint bytes, operations per transaction, encoded transaction bytes, and
resident accepted mutation bytes. `resource_usage()` reports these exact counters.
All byte arithmetic is checked before accepting an operation or encoding its WAL
payload. Automatic retention/snapshot operations count toward the transaction.
Exceeding an operation limit aborts the transaction, including earlier staged writes.

Mutation reservations move with a transaction into committed views. A checkpoint
releases the current view's reservations; older pinned views keep their charges
until dropped. Snapshot clones share one permit. Checkpoint bytes are deliberately
charged per independent pin, so shared files can count more than once.

These are admission limits, not a bound on process RSS: caller-owned buffers,
decoded history, copy-on-write map nodes, WAL encoding scratch space, catalogs,
page cache and allocator overhead are not measured by encoded mutation bytes.
The following history/overlay stages remove major amplification paths. Queue
limits arrive with the actual group-commit queue, not as unused options.

Recovery must not discard acknowledged writes because an operator lowered limits.
It reconstructs and charges committed mutations even above the admission budget;
new transactions then fail until checkpointing releases enough charges. Recovery
memory itself is not capped by the admission limit.

## Deadlines and abandoned handles

`Options::timeouts` sets total transaction, idle transaction, read snapshot,
operation and commit-admission durations. Both local and shared transaction
options can request shorter total/idle limits. Zero/overflow durations fail;
durations use monotonic clocks and admission never restarts the total deadline.

A weak-reference expiry registry sweeps abandoned handles. Expiry clears the
private transaction state and its reservations/pins even while the caller keeps
the handle. Read snapshots retain their catalog/sequence metadata after expiry,
but reject data access. In-progress reads retain a registered file pin until they
finish, so expiry never deletes files underneath I/O. Scans check deadlines
between callbacks; the engine cannot preempt caller code or blocking OS I/O.

An expired handle returns `TransactionExpired`; an operation/admission deadline
returns `DeadlineExceeded`. Commit takes ownership of the staged data, checks its
deadline while waiting for coordination and immediately before the WAL boundary.
After writing begins it finishes synchronization/publication or returns an I/O
outcome error, even if the deadline passes. A timeout never pretends to undo WAL
bytes already written. The bounded commit queue stage replaces the temporary
timed coordinator acquisition mechanism.

## Transaction-owned staging

An entity is materialized at most once into a transaction's private staging map.
Subsequent operations mutate that private value directly. A failed operation
still aborts the whole transaction, so cloning its complete history before each
operation is unnecessary. Existing event payloads and acceleration snapshots keep
their allocations across subsequent writes. The current row may still be copied
for validation; initial loading of disk history is removed in the independent
history stage. The WAL and published format are unchanged in this stage.

## Structurally shared overlay

Published overlays and reservation ledgers use immutable balanced AVL nodes.
Pinning clones the root pointer; inserting/replacing a key copies only its search
path and balancing nodes. Older views retain their nodes and entity references.
Ordered range scans use a bounded traversal stack. Randomized model comparisons
check updates, all bound types, old roots and balance invariants; a clone-count
test guards against accidentally copying the entire map on publication.

Transactions share the catalog until a schema operation changes it. Ordinary
data transactions no longer clone every table definition at begin/commit. The
checkpoint format is unchanged; persistent here means immutable shared memory
versions, not an additional on-disk tree format.

## Bounded group commit

Shared connections use a FIFO queue bounded by request count and encoded mutation
bytes, including the group currently syncing. Full admission fails explicitly;
waiting requests can expire and immediately release their staging/pin reservations.
A caller leads one bounded group and hands coordination to another waiting caller.
There is no permanently running writer thread retaining an abandoned database.

`Options::group_commit` bounds transaction count and mutation bytes per flush.
One transaction larger than the group byte target runs alone. The default adds no
intentional collection delay; an optional delay is capped at one second. Local
exclusive transactions still flush individually. Each accepted transaction keeps
its own WAL frame and contiguous commit sequence; one synchronization covers the
group, followed by coherent publication. Validation includes overlapping writes
and catalog changes inside the group. Conflicts abort only the losing transaction.

Deadline checks apply before writing. A request claimed by the writer finishes its
cooperative deadline checks after any blocking checkpoint I/O. Once WAL writing
starts, all accepted members wait for synchronization/publication or receive
`CommitUnknown` on I/O failure. All existing views are then poisoned until reopen.
An unsynchronized group is not promised all-or-nothing recovery: complete frames
may recover independently, while partial frames never publish partial transactions.

Tests count actual WAL synchronizations internally, verify queue count/byte limits,
expiry and leadership handoff, check conflicting group members, and kill subprocesses
or inject I/O failures around group writes and synchronization. The internal test
counter is not a production metrics system.

## Independent history and lazy staging

`EVEDB003` separates current/base generation slots from immutable history file
references. The latest history and snapshot indexes address absolute generations;
the manifest records each historical generation's page sequence. Event locations
use a 16-bit segment and 48-bit offset. Older control formats are rejected without
rewriting existing data; a migration tool is not included in this series.

Ordinary writes load only primary/current/base records. They carry a lazy disk
history descriptor and share ordered trees of committed events and snapshots.
New events remain private until their final commit sequence is known, then move
into immutable shared nodes. Retention advances the base and removes tree prefixes;
old readers keep their roots. Historical reads materialize only requested data,
while `events()` necessarily returns an allocated vector of retained events.

A transaction staged across a checkpoint rebinds its already committed history to
the current checkpoint before publication. Write-conflict validation guarantees
the equivalent entity view. It drops now-durable in-memory prefixes, preserves
new events/explicit snapshots and keeps mutation charges during the transition.
Pinned readers continue to reference complete immutable manifests; reclamation
never depends on an unregistered historical file reference.

Ordinary checkpoints rebuild primary/history/snapshot indexes, reuse existing
payload files, and append new payloads. Current-state collection does not rewrite
historical payloads. Explicit compaction also rewrites history when needed;
repeated compaction reuses already compacted history to avoid duplicating it in
the recovery baseline. Fully unreferenced files disappear after recovery/pinned
views release them. Partially live historical segments need explicit compaction.
`limits.max_history_files` bounds published event/snapshot payload files (4096 by
default); exceeding it fails checkpoint publication without losing WAL commits.
Compaction/retention or a deliberate configuration change can restore progress.

Tests verify that updating an entity with 100 disk events opens only three current
read files, committed payload pointers remain shared across writes, a small update
writes only its new historical bytes, current generations can disappear while
history survives, stale writers/readers survive compaction, retention reclaims
released segments, format rejection preserves user files, and the file budget
fails before publication. Prefix trimming/floor queries are checked against an
ordered model with tree balance and old-root invariants.

Remaining costs are explicit: synchronous maintenance, full index rebuilds,
whole-file startup verification, caller-owned results and decoded-record memory.
These changes remove major write amplification paths; they do not establish a
production RPS claim. External metrics, load qualification, background maintenance,
network serving, additional isolation levels, backup/upgrades and platform
power-loss qualification remain the subsequent roadmap work.

Conflict revisions are pruned after automatic checkpoints as well as explicit
checkpoint/compaction calls, after durable commit bookkeeping is installed. The
oldest live snapshot remains the conservative pruning boundary. A regression test
holds a stale writer through automatic checkpoints, verifies its conflict, then
checks that revisions stop accumulating after the writer releases its pin.
