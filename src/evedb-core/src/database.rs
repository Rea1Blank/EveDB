// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    Entity, Error, Event, EventKind, Fields, Result, Schema, Table, TableId,
    codec::{Decoder, Encoder},
    error::corrupt,
    model::*,
    snapshot::{self, Root, TableWriter},
    storage::{frame, index},
};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// Configuration for checkpointing and automatic history maintenance.
#[derive(Clone, Debug)]
pub struct Options {
    /// Start a checkpoint before the next transaction after this WAL size.
    pub checkpoint_bytes: u64,
    /// Target size of a history segment; one large event may exceed it.
    pub history_segment_bytes: u64,
    /// Create a snapshot every N entity versions; zero disables automatic snapshots.
    pub snapshot_interval: u64,
    /// Automatically retain at most N events per entity; None keeps all events.
    pub retain_events: Option<usize>,
    /// Use RLE for history records only when it reduces their size.
    pub compress_history: bool,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            checkpoint_bytes: 8 * 1024 * 1024,
            history_segment_bytes: 8 * 1024 * 1024,
            snapshot_interval: 32,
            retain_events: None,
            compress_history: true,
        }
    }
}

/// A single-owner local database that synchronizes the WAL before acknowledging writes.
///
/// A transaction exclusively borrows this handle. To share it between threads,
/// place it behind a mutex. An OS file lock excludes other handles/processes.
/// Checkpointed records and indexes are read from disk on demand; recently
/// changed entities reside in memory until the next checkpoint.
/// Power-loss durability depends on filesystem synchronization guarantees;
/// directory synchronization is currently implemented only on Unix.
pub struct Database {
    directory: PathBuf,
    _lock: File,
    wal: File,
    root: Root,
    catalog: BTreeMap<TableId, Table>,
    overlay: BTreeMap<(TableId, u64), EntityData>,
    lsn: u64,
    next_generation: u64,
    options: Options,
    poisoned: bool,
}
impl Database {
    /// Opens or initializes a database using default maintenance options.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, Options::default())
    }

    /// Opens a database, verifies checkpoint files, and replays committed WAL frames.
    ///
    /// An incomplete final WAL frame is truncated. Complete corrupt frames cause
    /// an error. If a checkpoint is damaged, the retained preceding checkpoint
    /// and WAL are used to reconstruct the committed state.
    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        if options.checkpoint_bytes < 4096 || options.history_segment_bytes < 4096 {
            return Err(Error::Invalid(
                "checkpoint and segment targets must be at least 4096 bytes".into(),
            ));
        }
        let directory = path.as_ref().to_owned();
        fs::create_dir_all(&directory)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("LOCK"))?;
        lock.try_lock().map_err(|_| Error::Locked)?;
        if !directory.join("control").exists() {
            initialize(&directory)?;
        }
        if fs::read(directory.join("control"))? != b"EVEDB001" {
            return Err(corrupt("unsupported database control format"));
        }
        let segments = numbered_files(&directory.join("wal"), "wal")?;
        if segments.is_empty() {
            return Err(corrupt("missing recovery WAL"));
        }
        let mut roots = Vec::new();
        for &(number, ref path) in &segments {
            let mut file = File::open(path)?;
            match frame::read(&mut file)? {
                Some(frame) if frame.kind == 1 => {
                    let root = Root::decode(&frame.payload)?;
                    if root.generation != number || root.lsn != frame.lsn {
                        return Err(corrupt("checkpoint/WAL identity mismatch"));
                    }
                    roots.push(root);
                }
                None => {}
                _ => return Err(corrupt("WAL segment lacks its checkpoint header")),
            }
        }
        let mut selected = None;
        let mut last_error = None;
        for root in roots.iter().rev() {
            match root.verify(&directory) {
                Ok(()) => {
                    selected = Some(root.clone());
                    break;
                }
                Err(e) => last_error = Some(e),
            }
        }
        let root = selected
            .ok_or_else(|| last_error.unwrap_or_else(|| corrupt("no complete checkpoint")))?;
        let active_generation = roots
            .last()
            .ok_or_else(|| corrupt("missing active WAL"))?
            .generation;
        let wal_path = directory
            .join("wal")
            .join(format!("{active_generation:020}.wal"));
        let wal = OpenOptions::new().read(true).write(true).open(wal_path)?;
        let next_generation = numbered_files(&directory.join("catalog"), "catalog")?
            .last()
            .map_or(1, |(n, _)| n.saturating_add(1))
            .max(segments.last().unwrap().0.saturating_add(1));
        let mut db = Self {
            directory,
            _lock: lock,
            wal,
            catalog: root.catalog.clone(),
            lsn: root.lsn,
            root,
            overlay: BTreeMap::new(),
            next_generation,
            options,
            poisoned: false,
        };
        let mut active_end = 0;
        for (number, path) in segments {
            if number < db.root.generation || number > active_generation {
                continue;
            }
            let mut file = File::open(path)?;
            let Some(header) = frame::read(&mut file)? else {
                continue;
            };
            if header.kind != 1 {
                return Err(corrupt("invalid WAL header"));
            }
            let mut complete_end = file.stream_position()?;
            while let Some(record) = frame::read(&mut file)? {
                if record.kind != 2 {
                    return Err(corrupt("unexpected record in transaction WAL"));
                }
                if record.lsn > db.root.lsn {
                    if db.lsn.checked_add(1) != Some(record.lsn) {
                        return Err(corrupt("gap or duplicate in transaction sequence"));
                    }
                    let mut tx = Transaction::new(&mut db);
                    for op in decode_ops(&record.payload)? {
                        tx.run(op)
                            .map_err(|e| corrupt(format!("invalid WAL operation: {e}")))?;
                    }
                    tx.publish(record.lsn);
                }
                complete_end = file.stream_position()?;
            }
            if number != active_generation && complete_end != file.metadata()?.len() {
                return Err(corrupt("incomplete frame in a sealed WAL segment"));
            }
            if number == active_generation {
                active_end = complete_end;
            }
        }
        if db.wal.metadata()?.len() != active_end {
            db.wal.set_len(active_end)?;
            db.wal.sync_all()?;
        }
        db.wal.seek(SeekFrom::End(0))?;
        Ok(db)
    }

    fn ready(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::NeedsRecovery)
        } else {
            Ok(())
        }
    }
    /// Returns the last committed transaction sequence number.
    pub fn sequence(&self) -> u64 {
        self.lsn
    }
    /// Lists table definitions in stable ID order.
    pub fn tables(&self) -> impl Iterator<Item = &Table> {
        self.catalog.values()
    }
    /// Resolves a table by its current name.
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.catalog.values().find(|t| t.name == name)
    }
    /// Starts an isolated write transaction. Dropping it discards staged operations.
    pub fn transaction(&mut self) -> Result<Transaction<'_>> {
        self.ready()?;
        if self.wal.metadata()?.len() >= self.options.checkpoint_bytes && self.lsn > self.root.lsn {
            self.checkpoint()?;
        }
        Ok(Transaction::new(self))
    }
    /// Runs and commits a group of operations, discarding all of them on error.
    pub fn write<T>(
        &mut self,
        operation: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let mut tx = self.transaction()?;
        let result = operation(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }
    /// Creates a table in its own transaction.
    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<TableId> {
        self.write(|tx| tx.create_table(name, schema))
    }
    /// Creates an entity in its own transaction.
    pub fn create(&mut self, table: TableId, id: u64, fields: Fields) -> Result<()> {
        self.write(|tx| tx.create(table, id, fields))
    }
    /// Applies field assignments in their own transaction.
    pub fn apply(&mut self, table: TableId, id: u64, fields: Fields) -> Result<()> {
        self.write(|tx| tx.apply(table, id, fields))
    }
    /// Deletes an entity while preserving its retained history.
    pub fn delete(&mut self, table: TableId, id: u64) -> Result<()> {
        self.write(|tx| tx.delete(table, id))
    }
    /// Returns a live current entity without replaying its events.
    pub fn get(&self, table: TableId, id: u64) -> Result<Option<Entity>> {
        self.ready()?;
        self.definition(table)?;
        let current = match self.overlay.get(&(table, id)) {
            Some(data) => Some(data.current.clone()),
            None => self.root.current(&self.directory, table, id)?,
        };
        Ok(current.filter(|entity| !entity.deleted))
    }
    /// Reconstructs a retained version, using a snapshot when available.
    pub fn get_at_version(&self, table: TableId, id: u64, version: u64) -> Result<Entity> {
        self.ready()?;
        let definition = self.definition(table)?;
        if let Some(data) = self.overlay.get(&(table, id)) {
            return data.at(definition, version, true);
        }
        self.root.at(&self.directory, table, id, version, true)
    }
    /// Replays retained history from its base through the current version.
    pub fn replay(&self, table: TableId, id: u64) -> Result<Entity> {
        self.ready()?;
        let definition = self.definition(table)?;
        if let Some(data) = self.overlay.get(&(table, id)) {
            return data.at(definition, data.current.version, false);
        }
        let current = self
            .root
            .current(&self.directory, table, id)?
            .ok_or_else(|| Error::NotFound(format!("entity {id}")))?;
        self.root
            .at(&self.directory, table, id, current.version, false)
    }
    /// Replays retained history from its base through a specific version.
    pub fn replay_to_version(&self, table: TableId, id: u64, version: u64) -> Result<Entity> {
        self.ready()?;
        let definition = self.definition(table)?;
        if let Some(data) = self.overlay.get(&(table, id)) {
            return data.at(definition, version, false);
        }
        self.root.at(&self.directory, table, id, version, false)
    }
    /// Returns retained events in entity-version order.
    pub fn events(&self, table: TableId, id: u64) -> Result<Vec<Event>> {
        self.ready()?;
        Ok(self
            .load(table, id)?
            .ok_or_else(|| Error::NotFound(format!("entity {id}")))?
            .events)
    }
    /// Returns the inclusive range of reconstructible entity versions.
    pub fn retained_range(&self, table: TableId, id: u64) -> Result<(u64, u64)> {
        self.ready()?;
        let data = self
            .load(table, id)?
            .ok_or_else(|| Error::NotFound(format!("entity {id}")))?;
        Ok((data.base.version, data.current.version))
    }
    /// Retains the latest N events and atomically advances the retained base.
    pub fn retain_last(&mut self, table: TableId, id: u64, count: usize) -> Result<()> {
        self.write(|tx| tx.retain_last(table, id, count))
    }
    /// Visits live entities in ID order. Records are loaded one at a time.
    pub fn scan(&self, table: TableId, mut visit: impl FnMut(Entity) -> Result<()>) -> Result<()> {
        self.ready()?;
        self.definition(table)?;
        self.visit_ids(table, |id| {
            if let Some(entity) = self.get(table, id)? {
                visit(entity)?;
            }
            Ok(())
        })
    }
    fn definition(&self, id: TableId) -> Result<&Table> {
        self.catalog
            .get(&id)
            .ok_or_else(|| Error::NotFound(format!("table {id}")))
    }
    fn load(&self, table: TableId, id: u64) -> Result<Option<EntityData>> {
        self.definition(table)?;
        if let Some(data) = self.overlay.get(&(table, id)) {
            return Ok(Some(data.clone()));
        }
        self.root.entity(&self.directory, table, id)
    }
    fn visit_ids(&self, table: TableId, mut visit: impl FnMut(u64) -> Result<()>) -> Result<()> {
        let disk: Box<dyn Iterator<Item = Result<u64>>> = if self.root.catalog.contains_key(&table)
        {
            Box::new(
                index::scan(
                    &snapshot::file_path(&self.directory, table, self.root.generation, 3, 0),
                    self.root.lsn,
                    [0, 0],
                    [u64::MAX, u64::MAX],
                )?
                .map(|entry| entry.map(|(key, _)| key[0])),
            )
        } else {
            Box::new(std::iter::empty())
        };
        let mut disk = disk.peekable();
        let mut recent = self
            .overlay
            .range((table, 0)..=(table, u64::MAX))
            .map(|(&(t, id), _)| {
                debug_assert_eq!(t, table);
                id
            })
            .peekable();
        loop {
            let old = match disk.peek() {
                Some(Ok(id)) => Some(*id),
                Some(Err(_)) => return Err(disk.next().unwrap().unwrap_err()),
                None => None,
            };
            let new = recent.peek().copied();
            let id = match (old, new) {
                (None, None) => break,
                (Some(a), Some(b)) => a.min(b),
                (Some(a), None) => a,
                (None, Some(b)) => b,
            };
            if old == Some(id) {
                disk.next();
            }
            if new == Some(id) {
                recent.next();
            }
            visit(id)?;
        }
        Ok(())
    }

    /// Rewrites live data into a new immutable generation and publishes it durably.
    ///
    /// Keeps the preceding checkpoint and its WAL for recovery. Only older,
    /// engine-owned numeric generations are reclaimed. Checkpointing is synchronous.
    pub fn checkpoint(&mut self) -> Result<()> {
        self.ready()?;
        let generation = self.next_generation;
        self.next_generation = generation
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("generation exhausted".into()))?;
        let catalog_path = self
            .directory
            .join("catalog")
            .join(format!("{generation:020}.catalog"));
        let mut catalog_file = File::create_new(catalog_path)?;
        let mut root = Root {
            generation,
            lsn: self.lsn,
            catalog: self.catalog.clone(),
            files: Vec::new(),
        };
        for &table in self.catalog.keys() {
            let mut writer = TableWriter::create(
                &self.directory,
                table,
                generation,
                self.lsn,
                self.options.history_segment_bytes,
                self.options.compress_history,
            )?;
            self.visit_ids(table, |id| {
                let data = self
                    .load(table, id)?
                    .ok_or_else(|| corrupt("entity vanished during checkpoint"))?;
                writer.append(id, &data)
            })?;
            root.files.extend(writer.finish()?);
            fault("checkpoint-files");
        }
        let payload = root.encode();
        frame::append(&mut catalog_file, 4, self.lsn, &payload)?;
        catalog_file.sync_all()?;
        snapshot::sync_directory(&self.directory.join("catalog"))?;
        fault("checkpoint-catalog");
        let path = self
            .directory
            .join("wal")
            .join(format!("{generation:020}.wal"));
        let mut wal = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let encoded = frame::encode(1, self.lsn, &payload)?;
        // From this point a failed publication could be recovered as complete.
        // Refuse further writes to the previous WAL until recovery resolves it.
        self.poisoned = true;
        wal.write_all(&encoded[..encoded.len() / 2])?;
        fault("checkpoint-wal-partial");
        wal.write_all(&encoded[encoded.len() / 2..])?;
        wal.sync_all()?;
        snapshot::sync_directory(&self.directory.join("wal"))?;
        fault("checkpoint-published");
        let previous = self.root.generation;
        self.root = root;
        self.wal = wal;
        self.overlay.clear();
        self.poisoned = false;
        // Publication succeeded: cleanup errors do not invalidate acknowledged commits.
        self.cleanup(previous, generation)?;
        fault("checkpoint-cleanup");
        Ok(())
    }
    fn cleanup(&self, previous: u64, current: u64) -> Result<()> {
        for (number, path) in numbered_files(&self.directory.join("wal"), "wal")? {
            if number < previous {
                fs::remove_file(path)?;
            }
        }
        for (number, path) in numbered_files(&self.directory.join("catalog"), "catalog")? {
            if number != previous && number != current {
                fs::remove_file(path)?;
            }
        }
        for table in fs::read_dir(self.directory.join("tables"))? {
            let table = table?;
            if !table.file_type()?.is_dir()
                || parse_number(&table.file_name().to_string_lossy()).is_none()
            {
                continue;
            }
            for entry in fs::read_dir(table.path())? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                if let Some(number) = parse_number(&entry.file_name().to_string_lossy())
                    && number != previous
                    && number != current
                {
                    fs::remove_dir_all(entry.path())?;
                }
            }
            snapshot::sync_directory(&table.path())?;
        }
        snapshot::sync_directory(&self.directory.join("wal"))?;
        snapshot::sync_directory(&self.directory.join("catalog"))?;
        Ok(())
    }
}

/// A staged transaction. Any failed write operation prevents its commit.
pub struct Transaction<'a> {
    db: &'a mut Database,
    catalog: BTreeMap<TableId, Table>,
    staged: BTreeMap<(TableId, u64), EntityData>,
    operations: Vec<Operation>,
    failed: bool,
}
impl<'a> Transaction<'a> {
    fn new(db: &'a mut Database) -> Self {
        Self {
            catalog: db.catalog.clone(),
            db,
            staged: BTreeMap::new(),
            operations: Vec::new(),
            failed: false,
        }
    }
    /// Creates a table. Schema version must be one.
    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<TableId> {
        let id = self
            .catalog
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("table IDs exhausted".into()))?;
        self.run(Operation::Table(Table {
            id,
            name: name.into(),
            schemas: vec![schema],
        }))?;
        Ok(id)
    }
    /// Renames a table while keeping its ID and physical storage identity.
    pub fn rename_table(&mut self, id: TableId, name: &str) -> Result<()> {
        let mut table = self.table_for_write(id)?;
        table.name = name.into();
        self.run(Operation::Table(table))
    }
    /// Adds a schema version. Existing fields retain IDs, types, and nullability.
    /// Fields may be renamed; newly introduced fields must accept null.
    pub fn alter_table(&mut self, id: TableId, fields: Vec<crate::Field>) -> Result<()> {
        let mut table = self.table_for_write(id)?;
        let version = table
            .schema()
            .version
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("schema versions exhausted".into()))?;
        table.schemas.push(Schema { version, fields });
        self.run(Operation::Table(table))
    }
    /// Creates an entity at version zero. Deleted identifiers cannot be reused.
    pub fn create(&mut self, table: TableId, id: u64, fields: Fields) -> Result<()> {
        self.run(Operation::Create(table, id, fields))
    }
    /// Applies assignments, producing a separate version even within one transaction.
    pub fn apply(&mut self, table: TableId, id: u64, fields: Fields) -> Result<()> {
        self.run(Operation::Apply(table, id, fields))?;
        self.maintain(table, id)
    }
    /// Ends an entity's active life with a versioned deletion event.
    pub fn delete(&mut self, table: TableId, id: u64) -> Result<()> {
        self.run(Operation::Delete(table, id))?;
        self.maintain(table, id)
    }
    /// Moves the retained base forward and keeps at most N subsequent events.
    pub fn retain_last(&mut self, table: TableId, id: u64, count: usize) -> Result<()> {
        self.run(Operation::Retain(table, id, count as u64))
    }
    /// Saves an explicit acceleration snapshot of the staged current state.
    pub fn snapshot(&mut self, table: TableId, id: u64) -> Result<()> {
        self.run(Operation::Snapshot(table, id))
    }
    /// Reads current state including this transaction's own changes.
    pub fn get(&self, table: TableId, id: u64) -> Result<Option<Entity>> {
        self.table(table)?;
        if let Some(data) = self.staged.get(&(table, id)) {
            return Ok((!data.current.deleted).then(|| data.current.clone()));
        }
        if !self.db.catalog.contains_key(&table) {
            return Ok(None);
        }
        self.db.get(table, id)
    }
    fn table(&self, id: TableId) -> Result<&Table> {
        self.catalog
            .get(&id)
            .ok_or_else(|| Error::NotFound(format!("table {id}")))
    }
    fn table_for_write(&mut self, id: TableId) -> Result<Table> {
        match self.table(id).cloned() {
            Ok(table) => Ok(table),
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }
    fn maintain(&mut self, table: TableId, id: u64) -> Result<()> {
        let interval = self.db.options.snapshot_interval;
        if interval != 0
            && self.staged[&(table, id)]
                .current
                .version
                .is_multiple_of(interval)
        {
            self.snapshot(table, id)?;
        }
        if let Some(count) = self.db.options.retain_events {
            self.retain_last(table, id, count)?;
        }
        Ok(())
    }
    fn run(&mut self, operation: Operation) -> Result<()> {
        if self.failed {
            return Err(Error::Invalid(
                "transaction was aborted by an earlier operation".into(),
            ));
        }
        let result = self.execute(&operation);
        if result.is_err() {
            self.failed = true;
        } else {
            self.operations.push(operation);
        }
        result
    }
    fn execute(&mut self, operation: &Operation) -> Result<()> {
        if let Operation::Table(table) = operation {
            validate_table_change(&self.catalog, table)?;
            self.catalog.insert(table.id, table.clone());
            return Ok(());
        }
        let (table_id, id) = operation.entity_key().unwrap();
        let table = self.table(table_id)?.clone();
        let existing = if let Some(data) = self.staged.get(&(table_id, id)) {
            Some(data.clone())
        } else if self.db.catalog.contains_key(&table_id) {
            self.db.load(table_id, id)?
        } else {
            None
        };
        let data = if let Operation::Create(_, _, fields) = operation {
            if existing.is_some() {
                return Err(Error::AlreadyExists(format!("entity {id}")));
            }
            let mut fields = fields.clone();
            table.schema().normalize(&mut fields, true)?;
            let current = Entity {
                id,
                version: 0,
                schema_version: table.schema().version,
                deleted: false,
                fields,
            };
            EntityData {
                base: current.clone(),
                current,
                events: Vec::new(),
                snapshots: BTreeMap::new(),
            }
        } else {
            let mut data = existing.ok_or_else(|| Error::NotFound(format!("entity {id}")))?;
            match operation {
                Operation::Apply(..) | Operation::Delete(..) => {
                    let fields = if let Operation::Apply(_, _, fields) = operation {
                        if fields.is_empty() {
                            return Err(Error::Invalid("an apply needs at least one field".into()));
                        }
                        let mut fields = fields.clone();
                        table.schema().normalize(&mut fields, false)?;
                        fields
                    } else {
                        Fields::new()
                    };
                    if data.current.deleted {
                        return Err(Error::Invalid(format!("entity {id} is deleted")));
                    }
                    let event =
                        Event {
                            version: data.current.version.checked_add(1).ok_or_else(|| {
                                Error::Invalid("entity versions exhausted".into())
                            })?,
                            transaction: self.db.lsn.checked_add(1).ok_or_else(|| {
                                Error::Invalid("transaction sequence exhausted".into())
                            })?,
                            schema_version: table.schema().version,
                            kind: if matches!(operation, Operation::Delete(..)) {
                                EventKind::Delete
                            } else {
                                EventKind::Apply
                            },
                            fields,
                        };
                    apply_event(&mut data.current, &event, &table)?;
                    data.events.push(event);
                }
                Operation::Retain(_, _, count) => data.retain(
                    &table,
                    usize::try_from(*count)
                        .map_err(|_| Error::Invalid("retention count overflow".into()))?,
                )?,
                Operation::Snapshot(..) => {
                    data.snapshots
                        .insert(data.current.version, data.current.clone());
                }
                _ => unreachable!(),
            }
            data
        };
        self.staged.insert((table_id, id), data);
        Ok(())
    }
    /// Flushes one atomic WAL transaction and publishes all staged changes.
    ///
    /// On an I/O error during writing or synchronization, the outcome is unknown:
    /// this database handle refuses further operations until it is reopened.
    pub fn commit(self) -> Result<u64> {
        if self.failed {
            return Err(Error::Invalid(
                "cannot commit an aborted transaction".into(),
            ));
        }
        if self.operations.is_empty() {
            return Ok(self.db.lsn);
        }
        let lsn = self
            .db
            .lsn
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("transaction sequence exhausted".into()))?;
        let encoded = frame::encode(2, lsn, &encode_ops(&self.operations))?;
        fault("wal-before-write");
        let result = (|| -> std::io::Result<()> {
            io_fault("before-write")?;
            self.db.wal.write_all(&encoded[..encoded.len() / 2])?;
            io_fault("partial-write")?;
            fault("wal-partial");
            self.db.wal.write_all(&encoded[encoded.len() / 2..])?;
            io_fault("before-sync")?;
            fault("wal-written");
            self.db.wal.sync_all()?;
            io_fault("after-sync")?;
            fault("wal-synced");
            Ok(())
        })();
        if let Err(e) = result {
            self.db.poisoned = true;
            return Err(Error::CommitUnknown(e));
        }
        self.publish(lsn);
        Ok(lsn)
    }
    fn publish(self, lsn: u64) {
        self.db.catalog = self.catalog;
        self.db.overlay.extend(self.staged);
        self.db.lsn = lsn;
    }
}

#[derive(Clone)]
enum Operation {
    Table(Table),
    Create(u64, u64, Fields),
    Apply(u64, u64, Fields),
    Delete(u64, u64),
    Retain(u64, u64, u64),
    Snapshot(u64, u64),
}
impl Operation {
    fn entity_key(&self) -> Option<(u64, u64)> {
        match self {
            Self::Table(_) => None,
            Self::Create(t, id, _)
            | Self::Apply(t, id, _)
            | Self::Delete(t, id)
            | Self::Retain(t, id, _)
            | Self::Snapshot(t, id) => Some((*t, *id)),
        }
    }
}
fn encode_ops(operations: &[Operation]) -> Vec<u8> {
    let mut e = Encoder::default();
    e.u64(operations.len() as u64);
    for op in operations {
        match op {
            Operation::Table(table) => {
                e.u8(0);
                encode_table(table, &mut e);
            }
            Operation::Create(t, id, fields) | Operation::Apply(t, id, fields) => {
                e.u8(if matches!(op, Operation::Create(..)) {
                    1
                } else {
                    2
                });
                e.u64(*t);
                e.u64(*id);
                encode_fields(fields, &mut e);
            }
            Operation::Delete(t, id) => {
                e.u8(3);
                e.u64(*t);
                e.u64(*id);
            }
            Operation::Retain(t, id, n) => {
                e.u8(4);
                e.u64(*t);
                e.u64(*id);
                e.u64(*n);
            }
            Operation::Snapshot(t, id) => {
                e.u8(5);
                e.u64(*t);
                e.u64(*id);
            }
        }
    }
    e.0
}
fn decode_ops(bytes: &[u8]) -> Result<Vec<Operation>> {
    let mut d = Decoder::new(bytes);
    let count = d.count(1)?;
    let mut ops = Vec::new();
    if count == 0 {
        return Err(corrupt("empty WAL transaction"));
    }
    for _ in 0..count {
        ops.push(match d.u8()? {
            0 => Operation::Table(decode_table(&mut d)?),
            1 => Operation::Create(d.u64()?, d.u64()?, decode_fields(&mut d)?),
            2 => Operation::Apply(d.u64()?, d.u64()?, decode_fields(&mut d)?),
            3 => Operation::Delete(d.u64()?, d.u64()?),
            4 => Operation::Retain(d.u64()?, d.u64()?, d.u64()?),
            5 => Operation::Snapshot(d.u64()?, d.u64()?),
            _ => return Err(corrupt("unknown WAL operation")),
        });
    }
    d.finish()?;
    Ok(ops)
}
fn validate_table_change(catalog: &BTreeMap<u64, Table>, table: &Table) -> Result<()> {
    validate_name(&table.name)?;
    if catalog
        .values()
        .any(|old| old.id != table.id && old.name == table.name)
    {
        return Err(Error::AlreadyExists(table.name.clone()));
    }
    if table.schemas.is_empty() {
        return Err(Error::Invalid("table needs a schema".into()));
    }
    for schema in &table.schemas {
        schema.validate()?;
    }
    if let Some(old) = catalog.get(&table.id) {
        if table.schemas == old.schemas {
            return Ok(());
        }
        if table.schemas.len() != old.schemas.len() + 1
            || !table.schemas.starts_with(&old.schemas)
            || table.schema().version != old.schema().version + 1
        {
            return Err(Error::Invalid("invalid schema evolution".into()));
        }
        let latest = table.schema();
        for field in &old.schema().fields {
            if !latest.fields.iter().any(|new| {
                new.id == field.id
                    && new.data_type == field.data_type
                    && new.nullable == field.nullable
            }) {
                return Err(Error::Invalid(
                    "existing field IDs, types and nullability must be preserved".into(),
                ));
            }
        }
        for field in &latest.fields {
            if !old.schema().fields.iter().any(|old| old.id == field.id) && !field.nullable {
                return Err(Error::Invalid("new fields must be nullable".into()));
            }
        }
    } else if table.id != catalog.keys().next_back().copied().unwrap_or(0) + 1
        || table.schemas.len() != 1
        || table.schema().version != 1
    {
        return Err(Error::Invalid(
            "new table requires the next ID and schema version one".into(),
        ));
    }
    Ok(())
}
fn initialize(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        if entry?.file_name() != "LOCK" {
            return Err(Error::Invalid(
                "refusing to initialize a nonempty data directory".into(),
            ));
        }
    }
    for name in ["wal", "catalog", "tables"] {
        fs::create_dir(directory.join(name))?;
    }
    let root = Root::empty();
    let payload = root.encode();
    let mut catalog = File::create_new(directory.join("catalog/00000000000000000000.catalog"))?;
    frame::append(&mut catalog, 4, 0, &payload)?;
    catalog.sync_all()?;
    let mut wal = File::create_new(directory.join("wal/00000000000000000000.wal"))?;
    frame::append(&mut wal, 1, 0, &payload)?;
    wal.sync_all()?;
    let mut control = File::create_new(directory.join("control"))?;
    control.write_all(b"EVEDB001")?;
    control.sync_all()?;
    snapshot::sync_directory(&directory.join("catalog"))?;
    snapshot::sync_directory(&directory.join("wal"))?;
    snapshot::sync_directory(directory)?;
    if let Some(parent) = directory.parent().filter(|p| !p.as_os_str().is_empty()) {
        snapshot::sync_directory(parent)?;
    }
    Ok(())
}
fn parse_number(value: &str) -> Option<u64> {
    (value.len() == 20 && value.bytes().all(|b| b.is_ascii_digit()))
        .then(|| value.parse().ok())
        .flatten()
}
fn numbered_files(directory: &Path, extension: &str) -> Result<Vec<(u64, PathBuf)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some(extension)
            && let Some(n) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(parse_number)
        {
            files.push((n, path));
        }
    }
    files.sort_by_key(|(n, _)| *n);
    Ok(files)
}
fn fault(name: &str) {
    #[cfg(feature = "fault-injection")]
    if std::env::var("EVEDB_FAILPOINT").ok().as_deref() == Some(name) {
        std::process::exit(91);
    }
    #[cfg(not(feature = "fault-injection"))]
    let _ = name;
}

fn io_fault(name: &str) -> std::io::Result<()> {
    #[cfg(feature = "fault-injection")]
    if std::env::var("EVEDB_IO_ERROR").ok().as_deref() == Some(name) {
        return Err(std::io::Error::other(format!(
            "injected I/O error at {name}"
        )));
    }
    #[cfg(not(feature = "fault-injection"))]
    let _ = name;
    Ok(())
}
