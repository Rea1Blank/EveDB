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
