// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    Database, Error, Event, ReadSnapshot, Reader, Result, SharedDatabase, TableId, deadline,
    error::corrupt,
    memory::Accounted,
    resources::{MemoryKind, MemoryReservation, check},
};
use std::{
    cell::{Cell, RefCell},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// Cloneable cooperative cancellation; cancelling any clone cancels all its reads.
#[derive(Clone, Default, Debug)]
pub struct CancellationToken(Arc<AtomicBool>);
impl CancellationToken {
    /// Signals cancellation. It does not interrupt an OS I/O call already in progress.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    /// Reports whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
    fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }
}

/// Inclusive event-version range and per-batch limits. The retained base has no event.
#[derive(Clone, Debug)]
pub struct HistoryOptions {
    /// First event version, or the first retained event when omitted.
    pub first_version: Option<u64>,
    /// Last event version, or the snapshot's current version when omitted.
    pub last_version: Option<u64>,
    /// Maximum events returned per batch, also capped by the database limit.
    pub max_events: usize,
    /// Logical decoded bytes per batch, also capped by the database limit.
    pub max_bytes: usize,
    /// Cooperative cancellation shared with other callers.
    pub cancellation: CancellationToken,
}
impl Default for HistoryOptions {
    fn default() -> Self {
        Self {
            first_version: None,
            last_version: None,
            max_events: 256,
            max_bytes: 1024 * 1024,
            cancellation: CancellationToken::default(),
        }
    }
}

/// An owned history batch. Its decoded-memory reservation lasts until it is dropped.
/// Cloning individual events creates caller-managed allocations outside this budget.
pub struct HistoryBatch {
    events: Vec<Accounted<Event>>,
    _slots: MemoryReservation,
    bytes: usize,
}
impl HistoryBatch {
    /// Events in increasing version order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Event> {
        self.events.iter().map(|event| &event.value)
    }
    /// Number of events in this batch.
    pub fn len(&self) -> usize {
        self.events.len()
    }
    /// Whether the batch is empty. Successful cursor batches are always nonempty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
    /// Logical decoded bytes reserved for this batch, including its event slots.
    pub fn memory_bytes(&self) -> usize {
        self.bytes
    }
}

/// Resumable history over one pinned view. It retains no decoded events between calls.
/// Completion, cancellation, errors, or dropping the cursor release its snapshot clone.
pub struct HistoryCursor {
    deadline: Option<std::time::Instant>,
    snapshot: Option<ReadSnapshot>,
    table: TableId,
    id: u64,
    next: Option<u64>,
    last: u64,
    options: HistoryOptions,
}
impl HistoryCursor {
    /// Reads the next bounded batch. After an error or completion, returns None.
    pub fn next_batch(&mut self) -> Result<Option<HistoryBatch>> {
        let result = self.read_batch();
        if result.is_err() || self.next.is_none() {
            self.snapshot = None;
            self.next = None;
        }
        result
    }
    fn read_batch(&mut self) -> Result<Option<HistoryBatch>> {
        let Some(first) = self.next else {
            return Ok(None);
        };
        self.options.cancellation.check()?;
        let snapshot = self
            .snapshot
            .as_ref()
            .ok_or_else(|| corrupt("cursor lost its snapshot"))?;
        let (batch, next) = snapshot.with_state(|state, end| {
            let end = self.deadline.map_or(end, |deadline| deadline.min(end));
            deadline::check(Some(end))?;
            let resources = &state.resources;
            let total = Cell::new(0usize);
            let expected = Cell::new(Some(first));
            let slots = RefCell::new(resources.memory(MemoryKind::Read, 0, false)?);
            let mut events = Vec::new();
            let slot_bytes = std::mem::size_of::<Accounted<Event>>();
            let complete = state.visit_events(
                self.table,
                self.id,
                first,
                self.last,
                &mut || {
                    self.options.cancellation.check()?;
                    deadline::check(Some(end))
                },
                &mut |size| {
                    self.options.cancellation.check()?;
                    deadline::check(Some(end))?;
                    if size
                        .checked_add(slot_bytes)
                        .and_then(|size| total.get().checked_add(size))
                        .is_none_or(|size| size > self.options.max_bytes)
                    {
                        if total.get() == 0 {
                            return Err(Error::LimitExceeded {
                                resource: "history batch bytes",
                                limit: self.options.max_bytes,
                            });
                        }
                        return Ok(false);
                    }
                    slots.borrow_mut().grow(slot_bytes, false)?;
                    Ok(true)
                },
                &mut |event| {
                    if Some(event.version) != expected.get() {
                        return Err(corrupt("gap in cursor history"));
                    }
                    expected.set(if event.version == self.last {
                        None
                    } else {
                        event.version.checked_add(1)
                    });
                    total.set(total.get() + event.bytes() + slot_bytes);
                    events
                        .try_reserve_exact(1)
                        .map_err(|_| Error::Invalid("cannot allocate history batch".into()))?;
                    events.push(event);
                    Ok(events.len() < self.options.max_events)
                },
            )?;
            self.options.cancellation.check()?;
            if complete && expected.get().is_some() {
                return Err(corrupt("truncated cursor history"));
            }
            if events.is_empty() {
                return Err(corrupt("empty unfinished history batch"));
            }
            Ok((
                HistoryBatch {
                    events,
                    _slots: slots.into_inner(),
                    bytes: total.get(),
                },
                expected.get(),
            ))
        })?;
        self.next = next;
        Ok(Some(batch))
    }
}

impl ReadSnapshot {
    /// Opens an event cursor on this snapshot. Explicit unavailable versions are rejected.
    pub fn history(
        &self,
        table: TableId,
        id: u64,
        mut options: HistoryOptions,
    ) -> Result<HistoryCursor> {
        options.cancellation.check()?;
        if options.max_events == 0 || options.max_bytes == 0 {
            return Err(Error::Invalid(
                "history batch limits must be positive".into(),
            ));
        }
        let (base, current) = self.retained_range(table, id)?;
        let state = self.state()?;
        options.max_events = options
            .max_events
            .min(state.state.resources.limits.max_history_batch_events);
        options.max_bytes = options
            .max_bytes
            .min(state.state.resources.limits.max_history_batch_bytes);
        let available = base.checked_add(1).filter(|first| *first <= current);
        for requested in [options.first_version, options.last_version]
            .into_iter()
            .flatten()
        {
            if available.is_none_or(|first| requested < first) || requested > current {
                return Err(Error::VersionUnavailable {
                    requested,
                    first: base.saturating_add(1),
                    last: current,
                });
            }
        }
        let first = options.first_version.or(available);
        let last = options.last_version.unwrap_or(current);
        if first.is_some_and(|first| first > last) {
            return Err(Error::Invalid("history range is reversed".into()));
        }
        Ok(HistoryCursor {
            deadline: None,
            snapshot: first.map(|_| self.clone()),
            table,
            id,
            next: first,
            last,
            options,
        })
    }
}
impl Reader {
    /// Opens a bounded cursor on a newly pinned committed snapshot.
    pub fn history(
        &self,
        table: TableId,
        id: u64,
        options: HistoryOptions,
    ) -> Result<HistoryCursor> {
        self.pin()?.history(table, id, options)
    }
}
impl Database {
    /// Opens a bounded cursor on a newly pinned committed snapshot.
    pub fn history(
        &self,
        table: TableId,
        id: u64,
        options: HistoryOptions,
    ) -> Result<HistoryCursor> {
        self.reader().history(table, id, options)
    }
}
impl SharedDatabase {
    /// Opens a bounded cursor on a newly pinned committed snapshot.
    pub fn history(
        &self,
        table: TableId,
        id: u64,
        options: HistoryOptions,
    ) -> Result<HistoryCursor> {
        self.reader().history(table, id, options)
    }
}

pub(crate) fn collect(snapshot: &ReadSnapshot, table: TableId, id: u64) -> Result<Vec<Event>> {
    snapshot.with_state(|state, end| {
        let resources = &state.resources;
        let mut cursor = snapshot.history(
            table,
            id,
            HistoryOptions {
                max_bytes: resources.limits.max_history_batch_bytes,
                ..HistoryOptions::default()
            },
        )?;
        cursor.deadline = Some(end);
        let mut batches = Vec::new();
        let mut bytes = 0usize;
        let mut count = 0usize;
        while cursor.next.is_some() {
            deadline::check(Some(end))?;
            let remaining = resources.limits.max_read_result_bytes.saturating_sub(bytes);
            if remaining == 0 {
                return Err(Error::LimitExceeded {
                    resource: "read result bytes",
                    limit: resources.limits.max_read_result_bytes,
                });
            }
            cursor.options.max_bytes = remaining.min(resources.limits.max_history_batch_bytes);
            let batch = match cursor.next_batch() {
                Err(Error::LimitExceeded {
                    resource: "history batch bytes",
                    ..
                }) if remaining <= resources.limits.max_history_batch_bytes => {
                    return Err(Error::LimitExceeded {
                        resource: "read result bytes",
                        limit: resources.limits.max_read_result_bytes,
                    });
                }
                other => other?,
            };
            let Some(batch) = batch else {
                break;
            };
            check(
                "read result bytes",
                bytes,
                batch.memory_bytes(),
                resources.limits.max_read_result_bytes,
            )?;
            bytes += batch.memory_bytes();
            count += batch.len();
            batches.push(batch);
        }
        let _output = resources.memory(
            MemoryKind::Read,
            count
                .checked_mul(
                    std::mem::size_of::<Event>() + std::mem::size_of::<Option<MemoryReservation>>(),
                )
                .ok_or_else(|| corrupt("read result size overflow"))?,
            false,
        )?;
        let mut result = Vec::new();
        result
            .try_reserve_exact(count)
            .map_err(|_| Error::Invalid("cannot allocate history result".into()))?;
        let mut ownership = Vec::new();
        ownership
            .try_reserve_exact(count)
            .map_err(|_| Error::Invalid("cannot allocate result ownership".into()))?;
        for batch in batches {
            for Accounted { value, memory } in batch.events {
                ownership.push(memory);
                result.push(value);
            }
        }
        Ok(result)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataType, Field, Schema, Value, test_support::TempDir};

    #[test]
    fn an_expired_operation_stops_before_loading_or_retaining_a_batch() {
        let dir = TempDir::new();
        let mut db = Database::open(&dir.0).unwrap();
        let table = db
            .create_table(
                "items",
                Schema::new(vec![Field {
                    id: 1,
                    name: "n".into(),
                    data_type: DataType::UInt64,
                    nullable: false,
                }])
                .unwrap(),
            )
            .unwrap();
        db.create(table, 1, [(1, Value::UInt64(0))].into()).unwrap();
        db.apply(table, 1, [(1, Value::UInt64(1))].into()).unwrap();
        let mut cursor = db.history(table, 1, HistoryOptions::default()).unwrap();
        cursor.deadline = Some(std::time::Instant::now());
        assert!(matches!(cursor.next_batch(), Err(Error::DeadlineExceeded)));
        assert_eq!(db.resource_usage().snapshots, 0);
        assert_eq!(db.resource_usage().read_memory_bytes, 0);
        assert_eq!(db.resource_usage().scratch_bytes, 0);
    }
}
