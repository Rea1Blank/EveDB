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
