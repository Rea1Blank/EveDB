// SPDX-License-Identifier: AGPL-3.0-only

//! Concurrent connections and the ordered commit coordinator.

use crate::{
    Database, Entity, Error, Fields, Options, ReadSnapshot, Reader, Result, Schema, SnapshotStats,
    TableId, Transaction, database::Prepared,
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

/// Per-transaction policy, independent of storage/maintenance options.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransactionOptions {
    /// Requested isolation. Defaults to Snapshot.
    pub isolation: IsolationLevel,
}
impl TransactionOptions {
    /// Requests an explicit isolation level.
    pub fn with_isolation(isolation: IsolationLevel) -> Self {
        Self { isolation }
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
/// transport, group commit, bounded admission, and background maintenance remain
/// separate roadmap work. Use one owner per directory and clone its connections.
#[derive(Clone)]
pub struct SharedDatabase {
    coordinator: Arc<Mutex<Coordinator>>,
    reader: Reader,
    options: Options,
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
            reader: database.reader(),
            options: database.options(),
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
        Ok(Transaction::shared(self.clone(), pin, self.options.clone()))
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
    pub(crate) fn commit(&self, batch: Prepared, _pin: ReadSnapshot) -> Result<u64> {
        let mut coordinator = self.coordinator.lock().map_err(|_| Error::NeedsRecovery)?;
        coordinator.database.ready()?;
        if batch.is_empty() {
            return Ok(batch.base_sequence);
        }
        if coordinator.catalog_revision > batch.base_sequence {
            return Err(Error::Conflict {
                table: None,
                entity: None,
            });
        }
        let keys: Vec<_> = batch.keys().collect();
        for key in &keys {
            if coordinator
                .revisions
                .get(key)
                .is_some_and(|lsn| *lsn > batch.base_sequence)
            {
                return Err(Error::Conflict {
                    table: Some(key.0),
                    entity: Some(key.1),
                });
            }
        }
        let changes_catalog = batch.changes_catalog();
        let sequence = batch.commit(&mut coordinator.database)?;
        // The mutex covers both publication and dependency bookkeeping: another
        // commit can never validate against a view whose revisions are missing.
        for key in keys {
            coordinator.revisions.insert(key, sequence);
        }
        if changes_catalog {
            coordinator.catalog_revision = sequence;
        }
        Ok(sequence)
    }
}
