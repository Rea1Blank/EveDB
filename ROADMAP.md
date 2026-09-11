# Roadmap

This file records where the engine stands, the architectural decisions taken so
far, and what each of them costs to reverse. It is a direction and a set of
commitments, not a schedule or a promise of dates.

The guiding constraint is stated once here, because most decisions below follow
from it: EveDB should not have to be redesigned to serve many concurrent
clients. Work that is cheap now and expensive later is done now. Work that can
be added without changing stored formats or user-visible interfaces is deferred
until a measured need justifies it.

## Where we are

A single-owner local storage engine written in Rust against the standard
library. One handle owns a data directory through an OS file lock. Transactions
exclusively borrow that handle, stage their changes privately, synchronize one
complete WAL frame, and publish. Reads combine an in-memory overlay of recent
changes with immutable checkpoint files. Checkpoints write only the entities a
transaction touched, leave the rest in the generations that already hold them,
and collect generations that have lost density.

There is no server, no network protocol, and no concurrency: every read and
write runs on one thread through one exclusive handle. The
[storage document](docs/storage.md) describes the implemented design and its
limits; [architecture](docs/architecture.md) describes the module boundaries.

## Decisions

### Cross-entity transactions stay

A transaction may span entities and tables, and commits them atomically. The
alternative — narrowing a transaction to one entity — would let the database be
partitioned into independent single-writer shards, which is the cheaper route to
parallel writes. It was considered and rejected: it moves a real limitation onto
every application that needs a consistency boundary wider than one entity.

The cost of keeping it is that parallel writes have to be earned the way
PostgreSQL earns them, through concurrent insertion into one ordered log rather
than through partitioning.

Reversing this decision later is possible but expensive for users, not for us:
it removes a guarantee applications will have been written against.

### Concurrency arrives in two steps, readers first

Many readers with one writer comes first; concurrent writers come later, if
measurements justify them. This matches how SQLite in WAL mode and LMDB are
built, and it is the step that turns read throughput from one core into as many
cores as the machine has.

Deferring concurrent writers is safe because the transition relaxes interfaces
rather than tightening them. `Transaction` currently borrows the database
mutably; moving to a shared borrow later does not break calling code.

### Visibility goes through one predicate

Every read decides what it can see by calling one function on its snapshot,
never by comparing sequence numbers directly. Today that function answers
whether a sequence number is at or below the snapshot's watermark. When several
writers commit out of order, it will also consult the set of transactions still
in flight, the way PostgreSQL consults its snapshot's in-progress list.

This is the clearest example of cheap now and expensive later. Funnelling
visibility through one predicate costs an indirection today. Retrofitting it
means revisiting every read path.

The WAL already serves as the commit record: a frame exists only for a
transaction that committed, so no separate status log is needed.

### Reads live on their own handle

The read API belongs to a cloneable handle that is safe to send between threads,
not to the writer. What that handle does internally is free to stay simple, and
at first it will be. What matters is that application code is written against a
type that already admits concurrent use.

Keeping reads on the writing handle would be the one genuine trap in this
design: every consumer would have to change when concurrency arrives.

### Storage evolves toward run-routed indexes

Published generations are immutable and a generation behaves like a sorted run;
the collector that rewrites sparse generations behaves like a compactor. The
remaining step is to route reads to per-generation indexes instead of rebuilding
one global primary index at every checkpoint, and to move compaction into the
background.

The alternative is a mutable B+tree over a buffer pool with page-level logging,
which is what PostgreSQL and InnoDB use. It has better read amplification and
would be a rewrite of the storage layer, including page latching and torn-page
protection. The current design reaches a similar outcome by evolution, so it is
preferred until a measurement says otherwise.

Nothing in the published format blocks this: history and snapshot indexes are
already per generation, and only the primary index is global.

## Next

Readers that do not corner us. Four items, in one piece of work:

1. A reader handle that is cloneable and safe to send between threads, carrying
   the whole read API.
2. Read state behind shared ownership — manifest, catalog and overlay reached
   through a snapshot the reader holds, rather than borrowed from the writer.
3. One visibility predicate, with a watermark in the snapshot from the start and
   the in-flight set left as an empty placeholder.
4. Retention of generations referenced by live snapshots. This is not future
   work: a reader holding a manifest must not have its files deleted underneath
   it, and on Windows an open file cannot be deleted at all. Reclamation already
   works from the set of files live manifests reference, so this adds a term to
   that set.

Two levels of read isolation follow from the same mechanism, and the level is
expressed by the handle rather than by a parameter. A reader takes the newest
published snapshot for the duration of each call, which keeps a single operation
internally consistent. Pinning a reader fixes its snapshot across calls, which
gives repeatable, phantom-free reads — a serializable read-only transaction,
since the write history is serial. A pinned snapshot also holds generations on
disk, so that cost must be visible in the engine's reported statistics.

## Deferred, and why that is safe

| Deferred | Why it stays cheap to add |
| --- | --- |
| Writer threads | Moving `Transaction` from a mutable to a shared borrow relaxes an interface; calling code is unaffected |
| Group commit | Frame format is unchanged; only who calls `sync_all`, and when, changes |
| Conflict detection | The unit already exists — an entity's version — so a check at commit time suffices, with no predicate locks |
| Recovery of out-of-order frames | Each frame carries its own sequence number, so the rule changes from "strictly the next" to "the complete prefix" without touching bytes |
| Sharding the page cache | Entirely inside the pager: no stored format, no public interface |
| Per-generation index routing and background compaction | Immutable generations and a working collector are already in place |

## Known ceilings

These are the reasons the engine cannot serve a large workload today. They are
listed so that no benchmark is mistaken for a verdict on the design, and so the
order of future work stays honest.

1. Applying one event loads the entity's whole retained history into memory. The
   cost of a write grows with the length of that history, while a writer needs
   only the current state and version.
2. A checkpoint rebuilds the primary index over every live entity. Checkpoint
   frequency grows with the write rate and this cost grows with the database, so
   the two multiply.
3. Every transaction synchronizes its own WAL frame, which bounds commits to
   what one synchronization stream can do.
4. Reads run on one thread, and the page cache serializes every page access
   through one lock.

Item 4 is what the next piece of work addresses. Items 1 through 3 are bounded,
local changes that do not require a redesign.

## Not promised

Parity with a mature general-purpose database. The work above removes
architectural ceilings, which makes a comparison meaningful; it does not supply
a query planner, statistics, parallel plans, or decades of tuning.

Stable on-disk formats, a migration path between them, or production
qualification. Power-loss behaviour in particular is not qualified.

Partitioning the database into independent shards, which was considered and
rejected above.
