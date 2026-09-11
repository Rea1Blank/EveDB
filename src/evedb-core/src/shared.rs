// SPDX-License-Identifier: AGPL-3.0-only

//! Concurrent connections and the ordered commit coordinator.

#[cfg(test)]
#[path = "shared_tests.rs"]
mod group_tests;

use crate::{
    Database, Entity, Error, Fields, Options, ReadSnapshot, Reader, Result, Schema, SnapshotStats,
    TableId, Transaction,
    commit_queue::{Queue, Ticket},
    database::Prepared,
};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};

/// Isolation requested for a transaction. Unsupported levels are rejected.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum IsolationLevel {
    /// A new committed view per operation. Reserved; currently unsupported.
    ReadCommitted,
    /// One view plus own writes; first committer wins on overlapping writes.
    /// This prevents lost updates but permits write skew.
    #[default]
    Snapshot,
    /// Equivalent to a serial execution. Reserved; currently unsupported.
    Serializable,
}

#[cfg(test)]
mod deadline_tests {
    use super::*;
    use crate::{DataType, Field, Value, test_support::TempDir};
    use std::time::Duration;

    #[test]
    fn waiting_commit_expires_without_writing_or_leaking_a_permit() {
        let dir = TempDir::new();
        let db = SharedDatabase::open(&dir.0).unwrap();
        let table = db
            .create_table(
                "items",
                Schema::new(vec![Field {
                    id: 1,
                    name: "value".into(),
                    data_type: DataType::UInt64,
                    nullable: false,
                }])
                .unwrap(),
            )
            .unwrap();
        let requested = TransactionOptions {
            timeout: Some(Duration::from_millis(40)),
            ..TransactionOptions::default()
        };
        let mut tx = db.transaction_with_options(requested).unwrap();
        tx.create(table, 1, [(1, Value::UInt64(1))].into()).unwrap();
        let guard = db.coordinator.lock().unwrap();
        assert!(matches!(tx.commit(), Err(Error::DeadlineExceeded)));
        assert_eq!(db.resource_usage().transactions, 0);
        drop(guard);
        assert!(db.get(table, 1).unwrap().is_none());
    }
}

/// Per-transaction policy, independent of storage/maintenance options.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransactionOptions {
    /// Requested isolation. Defaults to Snapshot.
    pub isolation: IsolationLevel,
    /// Optional shorter total lifetime than the database default.
    pub timeout: Option<std::time::Duration>,
    /// Optional shorter idle lifetime than the database default.
    pub idle_timeout: Option<std::time::Duration>,
}
impl TransactionOptions {
    /// Requests an explicit isolation level.
    pub fn with_isolation(isolation: IsolationLevel) -> Self {
        Self {
            isolation,
            ..Self::default()
        }
    }
}

struct Coordinator {
    database: Database,
    // Retention and explicit snapshots count as writes even without a new entity version.
    revisions: BTreeMap<(TableId, u64), u64>,
    catalog_revision: u64,
}

/// Cloneable in-process database connection. Transactions stage independently.
///
/// Readers and staging do not acquire the commit mutex. Only validation, WAL
/// synchronization, publication, and synchronous maintenance use it. Network
/// transport and background maintenance remain
/// separate roadmap work. Use one owner per directory and clone its connections.
#[derive(Clone)]
pub struct SharedDatabase {
    coordinator: Arc<Mutex<Coordinator>>,
    queue: Arc<Queue>,
    reader: Reader,
    options: Arc<Options>,
}
impl SharedDatabase {
    /// Opens a concurrent database with default storage options.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Database::open(path)?.into_shared())
    }
    /// Opens a concurrent database with explicit storage options.
    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        Ok(Database::open_with_options(path, options)?.into_shared())
    }
    pub(crate) fn from_database(database: Database) -> Self {
        Self {
            queue: Arc::new(Queue::default()),
            reader: database.reader(),
            options: Arc::new(database.options()),
            coordinator: Arc::new(Mutex::new(Coordinator {
                database,
                revisions: BTreeMap::new(),
                catalog_revision: 0,
            })),
        }
    }
    /// Starts an independent Snapshot transaction. Dropping it aborts its writes.
    pub fn transaction(&self) -> Result<Transaction<'static>> {
        self.transaction_with_options(TransactionOptions::default())
    }
    /// Starts a transaction with explicit isolation; never substitutes a weaker mode.
    pub fn transaction_with_options(
        &self,
        options: TransactionOptions,
    ) -> Result<Transaction<'static>> {
        if options.isolation != IsolationLevel::Snapshot {
            return Err(Error::UnsupportedIsolation(options.isolation));
        }
        let pin = self.reader.pin()?;
        Transaction::shared(self.clone(), pin, (*self.options).clone(), options)
    }
    /// Runs a transaction without automatic retries. The closure may have side effects.
    pub fn write<T>(
        &self,
        operation: impl FnOnce(&mut Transaction<'static>) -> Result<T>,
    ) -> Result<T> {
        let mut tx = self.transaction()?;
        let result = operation(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }
    /// Returns current admission counters.
    pub fn resource_usage(&self) -> crate::ResourceUsage {
        self.reader.resource_usage()
    }
    /// Returns an independent reader exposing the full read API.
    pub fn reader(&self) -> Reader {
        self.reader.clone()
    }
    /// Pins a committed view across multiple read operations.
    pub fn read_snapshot(&self) -> Result<ReadSnapshot> {
        self.reader.pin()
    }
    /// Returns a current entity from one committed view.
    pub fn get(&self, table: TableId, id: u64) -> Result<Option<Entity>> {
        self.reader.get(table, id)
    }
    /// Returns the last published commit sequence.
    pub fn sequence(&self) -> Result<u64> {
        self.reader.sequence()
    }
    /// Reports resources retained by live snapshots, including write transactions.
    pub fn snapshot_stats(&self) -> SnapshotStats {
        self.reader.snapshot_stats()
    }
    /// Creates a table in its own transaction.
    pub fn create_table(&self, name: &str, schema: Schema) -> Result<TableId> {
        self.write(|tx| tx.create_table(name, schema))
    }
    /// Creates an entity in its own transaction.
    pub fn create(&self, table: TableId, id: u64, fields: Fields) -> Result<()> {
        self.write(|tx| tx.create(table, id, fields))
    }
    /// Applies assignments in their own transaction.
    pub fn apply(&self, table: TableId, id: u64, fields: Fields) -> Result<()> {
        self.write(|tx| tx.apply(table, id, fields))
    }
    /// Deletes an entity in its own transaction.
    pub fn delete(&self, table: TableId, id: u64) -> Result<()> {
        self.write(|tx| tx.delete(table, id))
    }
    /// Runs synchronous checkpoint work while independent readers continue.
    pub fn checkpoint(&self) -> Result<()> {
        let mut coordinator = self.coordinator.lock().map_err(|_| Error::NeedsRecovery)?;
        coordinator.database.checkpoint()?;
        Self::prune_revisions(&mut coordinator);
        Ok(())
    }
    /// Compacts the database while preserving every live snapshot's files.
    pub fn compact(&self) -> Result<()> {
        let mut coordinator = self.coordinator.lock().map_err(|_| Error::NeedsRecovery)?;
        coordinator.database.compact()?;
        Self::prune_revisions(&mut coordinator);
        Ok(())
    }
    fn prune_revisions(coordinator: &mut Coordinator) {
        let oldest = coordinator
            .database
            .snapshot_stats()
            .oldest_sequence
            .unwrap_or_else(|| coordinator.database.sequence());
        coordinator
            .revisions
            .retain(|_, sequence| *sequence > oldest);
    }
    pub(crate) fn commit(&self, batch: Prepared) -> Result<u64> {
        self.reader.ready()?;
        if batch.is_empty() {
            return Ok(batch.base_sequence);
        }
        let end = batch.deadline;
        crate::deadline::check(end)?;
        let ticket = self.queue.submit(batch, &self.options.limits)?;
        loop {
            let mut state = self.queue.state.lock().map_err(|_| Error::NeedsRecovery)?;
            if let Some(result) = ticket.lock().expect("commit request").result.take() {
                return result;
            }
            if Queue::expired(end) && Queue::cancel(&mut state, &ticket) {
                self.queue.changed.notify_all();
                continue;
            }
            if !state.running {
                state.running = true;
                drop(state);
                self.run_group(end);
            } else {
                let wait = end.map_or(std::time::Duration::from_millis(10), |end| {
                    end.saturating_duration_since(std::time::Instant::now())
                        .min(std::time::Duration::from_millis(10))
                });
                let wait = wait.max(std::time::Duration::from_millis(1));
                drop(
                    self.queue
                        .changed
                        .wait_timeout(state, wait)
                        .map_err(|_| Error::NeedsRecovery)?,
                );
            }
        }
    }
    fn run_group(&self, end: Option<std::time::Instant>) {
        let mut leadership = Leadership {
            queue: &self.queue,
            tickets: Vec::new(),
            armed: true,
        };
        let mut coordinator = loop {
            if Queue::expired(end) {
                return;
            }
            match self.coordinator.try_lock() {
                Ok(guard) => break guard,
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    let mut state = self.queue.state.lock().expect("commit queue");
                    let claimed = Queue::claim(&mut state, &self.options.group_commit);
                    leadership
                        .tickets
                        .extend(claimed.into_iter().map(|(ticket, _)| ticket));
                    return;
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    std::thread::sleep(std::time::Duration::from_millis(1))
                }
            }
        };
        let gather_end = std::time::Instant::now() + self.options.group_commit.delay;
        let gather_end = end.map_or(gather_end, |end| end.min(gather_end));
        let mut state = self.queue.state.lock().expect("commit queue");
        while state.waiting.len() < self.options.group_commit.max_transactions
            && std::time::Instant::now() < gather_end
        {
            state = self
                .queue
                .changed
                .wait_timeout(
                    state,
                    gather_end.saturating_duration_since(std::time::Instant::now()),
                )
                .expect("commit queue")
                .0;
        }
        let claimed = Queue::claim(&mut state, &self.options.group_commit);
        drop(state);
        leadership
            .tickets
            .extend(claimed.iter().map(|(ticket, _)| ticket.clone()));
        let mut accepted = Vec::new();
        let mut metadata = Vec::new();
        let mut completed = Vec::new();
        let mut group_keys = BTreeMap::new();
        let mut catalog_revision = coordinator.catalog_revision;
        for (ticket, batch) in claimed {
            let validation = (|| -> Result<()> {
                coordinator.database.ready()?;
                crate::deadline::check(batch.deadline)?;
                if catalog_revision > batch.base_sequence {
                    return Err(Error::Conflict {
                        table: None,
                        entity: None,
                    });
                }
                for key in batch.keys() {
                    if group_keys
                        .get(&key)
                        .or_else(|| coordinator.revisions.get(&key))
                        .is_some_and(|lsn| *lsn > batch.base_sequence)
                    {
                        return Err(Error::Conflict {
                            table: Some(key.0),
                            entity: Some(key.1),
                        });
                    }
                }
                Ok(())
            })();
            if let Err(error) = validation {
                completed.push((ticket, Err(error)));
                continue;
            }
            let keys: Vec<_> = batch.keys().collect();
            let changes_catalog = batch.changes_catalog();
            // Only comparison with the begin sequence matters during validation.
            // Actual revision sequences are installed after durable publication.
            for key in &keys {
                group_keys.insert(*key, u64::MAX);
            }
            if changes_catalog {
                catalog_revision = u64::MAX;
            }
            metadata.push((ticket, keys, changes_catalog));
            accepted.push(batch);
        }
        let results = Prepared::commit_group(accepted, &mut coordinator.database);
        for ((ticket, keys, catalog), result) in metadata.into_iter().zip(results) {
            if let Ok(sequence) = &result {
                for key in keys {
                    coordinator.revisions.insert(key, *sequence);
                }
                if catalog {
                    coordinator.catalog_revision = *sequence;
                }
            }
            completed.push((ticket, result));
        }
        drop(coordinator);
        leadership.tickets.clear();
        leadership.armed = false;
        self.queue.finish(completed);
    }
}
// Hand leadership back even when coordination fails or unwinds. Claimed callers
// must never wait forever after a panic in the writer.
struct Leadership<'a> {
    queue: &'a Queue,
    tickets: Vec<Ticket>,
    armed: bool,
}
impl Drop for Leadership<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.queue.finish(
            self.tickets
                .drain(..)
                .map(|ticket| (ticket, Err(Error::NeedsRecovery)))
                .collect(),
        );
    }
}
