// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    Entity, Error, Event, Result, Table, TableId,
    deadline::{self, Lease},
    error::corrupt,
    model::EntityData,
    ordered_map::OrderedMap,
    resources::{Permit, Reservation, Resources},
    snapshot::Root,
    storage::{
        index,
        pager::{FileId, Pager},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    sync::{
        Arc, Mutex, RwLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Clone)]
pub(crate) struct ReadState {
    pub root: Arc<Root>,
    pub catalog: Arc<BTreeMap<TableId, Table>>,
    pub overlay: OrderedMap<(TableId, u64), Arc<EntityData>>,
    pub lsn: u64,
    pub resources: Arc<Resources>,
    pub charges: OrderedMap<u64, Arc<Reservation>>,
    pub pager: Arc<Pager>,
    pub _lock: Arc<DirectoryLock>,
    pub poisoned: Arc<AtomicBool>,
}
// Closing one descriptor is insufficient on Unix if a concurrently spawned
// process still holds an inherited copy before exec. Release the lock when the
// last engine owner (including its pinned readers) goes away.
pub(crate) struct DirectoryLock(pub File);
impl Drop for DirectoryLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}
impl ReadState {
    pub(crate) fn ready(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(Error::NeedsRecovery)
        } else {
            Ok(())
        }
    }
    /// Returns a live current entity without replaying its events.
    pub fn get(&self, table: TableId, id: u64) -> Result<Option<Entity>> {
        self.ready()?;
        self.definition(table)?;
        let current = match self.overlay.get(&(table, id)) {
            Some(data) => Some(data.current.clone()),
            None => self.root.current(&self.pager, table, id)?,
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
        self.root.at(&self.pager, table, id, version, true)
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
            .current(&self.pager, table, id)?
            .ok_or_else(|| Error::NotFound(format!("entity {id}")))?;
        self.root.at(&self.pager, table, id, current.version, false)
    }
    /// Replays retained history from its base through a specific version.
    pub fn replay_to_version(&self, table: TableId, id: u64, version: u64) -> Result<Entity> {
        self.ready()?;
        let definition = self.definition(table)?;
        if let Some(data) = self.overlay.get(&(table, id)) {
            return data.at(definition, version, false);
        }
        self.root.at(&self.pager, table, id, version, false)
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
    /// Visits live entities in ID order. Records are loaded one at a time.
    ///
    /// The scan reads each record through the primary-index entry it already
    /// holds, so visiting an entity costs one heap read rather than a second
    /// descent through the tree.
    pub fn scan(&self, table: TableId, mut visit: impl FnMut(Entity) -> Result<()>) -> Result<()> {
        self.ready()?;
        self.definition(table)?;
        self.visit_entries(table, |id, locator| {
            let entity = match self.overlay.get(&(table, id)) {
                Some(data) => data.current.clone(),
                None => {
                    let locator = locator.ok_or_else(|| corrupt("scanned entity has no record"))?;
                    self.root.state(&self.pager, table, 0, locator[0], id)?
                }
            };
            if !entity.deleted {
                visit(entity)?;
            }
            Ok(())
        })
    }
    pub(crate) fn definition(&self, id: TableId) -> Result<&Table> {
        self.catalog
            .get(&id)
            .ok_or_else(|| Error::NotFound(format!("table {id}")))
    }
    pub(crate) fn load(&self, table: TableId, id: u64) -> Result<Option<EntityData>> {
        self.definition(table)?;
        if let Some(data) = self.overlay.get(&(table, id)) {
            return Ok(Some((**data).clone()));
        }
        self.root.entity(&self.pager, table, id)
    }
    /// Visits every live identifier of a table in order, merging disk and overlay.
    ///
    /// The visitor also receives the primary-index entry of an entity that has
    /// a checkpointed record, so a caller that only needs the current state can
    /// read it directly instead of descending the tree a second time. `None`
    /// marks an entity that exists only in the overlay.
    pub(crate) fn visit_entries(
        &self,
        table: TableId,
        mut visit: impl FnMut(u64, Option<index::Value>) -> Result<()>,
    ) -> Result<()> {
        let disk: Box<dyn Iterator<Item = Result<(u64, index::Value)>>> =
            if self.root.catalog.contains_key(&table) {
                Box::new(
                    index::scan(
                        &self.pager,
                        self.root.file(table, 3),
                        self.root.lsn,
                        [0, 0],
                        [u64::MAX, u64::MAX],
                    )?
                    .map(|entry| entry.map(|(key, value)| (key[0], value))),
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
                Some(Ok((id, value))) => Some((*id, *value)),
                Some(Err(_)) => return Err(disk.next().unwrap().unwrap_err()),
                None => None,
            };
            let new = recent.peek().copied();
            let id = match (old.map(|(id, _)| id), new) {
                (None, None) => break,
                (Some(a), Some(b)) => a.min(b),
                (Some(a), None) => a,
                (None, Some(b)) => b,
            };
            let locator = old.filter(|&(old, _)| old == id).map(|(_, value)| value);
            if locator.is_some() {
                disk.next();
            }
            if new == Some(id) {
                recent.next();
            }
            visit(id, locator)?;
        }
        Ok(())
    }
}

pub(crate) struct SnapshotData {
    _permit: Permit,
    pub state: Arc<ReadState>,
}
pub(crate) struct ReaderShared {
    pub current: RwLock<Arc<ReadState>>,
    pins: Mutex<Vec<Weak<SnapshotData>>>,
}
impl ReaderShared {
    pub fn new(state: Arc<ReadState>) -> Self {
        Self {
            current: RwLock::new(state),
            pins: Mutex::new(Vec::new()),
        }
    }
    pub fn publish(&self, state: Arc<ReadState>) {
        *self.current.write().expect("published read state") = state;
    }
    pub fn pinned_files(&self) -> BTreeSet<FileId> {
        let mut pins = self.pins.lock().expect("snapshot pins");
        let mut files = BTreeSet::new();
        pins.retain(|weak| {
            if let Some(pin) = weak.upgrade() {
                files.extend(pin.state.root.file_ids());
                true
            } else {
                false
            }
        });
        files
    }
    pub fn stats(&self) -> SnapshotStats {
        let mut pins = self.pins.lock().expect("snapshot pins");
        let mut result = SnapshotStats::default();
        let mut files = BTreeMap::new();
        pins.retain(|weak| {
            if let Some(pin) = weak.upgrade() {
                result.active += 1;
                result.oldest_sequence = Some(
                    result
                        .oldest_sequence
                        .map_or(pin.state.lsn, |n| n.min(pin.state.lsn)),
                );
                files.extend(pin.state.root.file_sizes());
                true
            } else {
                false
            }
        });
        result.referenced_bytes = files.values().sum();
        result
    }
}

/// Snapshot retention costs. Clones of a snapshot count as one pin.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotStats {
    /// Independently pinned snapshots, including transactions using them.
    pub active: usize,
    /// Oldest pinned committed sequence, if any.
    pub oldest_sequence: Option<u64>,
    /// Unique checkpoint bytes referenced by pins, including files still current.
    /// This excludes overlays and is not an estimate of total process memory.
    pub referenced_bytes: u64,
}

/// Cloneable connection for reads. Each operation acquires a committed snapshot.
/// Use [`pin`](Self::pin) for a repeatable view across operations.
#[derive(Clone)]
pub struct Reader {
    pub(crate) shared: Arc<ReaderShared>,
}
impl Reader {
    pub(crate) fn ready(&self) -> Result<()> {
        self.shared
            .current
            .read()
            .map_err(|_| Error::NeedsRecovery)?
            .ready()
    }
    /// Returns current admission counters without acquiring a snapshot pin.
    pub fn resource_usage(&self) -> crate::ResourceUsage {
        self.shared
            .current
            .read()
            .expect("published state")
            .resources
            .usage()
    }
    /// Pins a coherent catalog, current state, and retained history.
    /// The snapshot keeps checkpoint files and the directory lock alive.
    pub fn pin(&self) -> Result<ReadSnapshot> {
        let state = self
            .shared
            .current
            .read()
            .map_err(|_| Error::NeedsRecovery)?;
        state.ready()?;
        let bytes = state
            .root
            .file_sizes()
            .try_fold(0usize, |sum, (_, size)| {
                sum.checked_add(usize::try_from(size).unwrap_or(usize::MAX))
            })
            .ok_or(Error::LimitExceeded {
                resource: "pinned checkpoint bytes",
                limit: state.resources.limits.max_pinned_bytes,
            })?;
        let permit = state.resources.snapshot(bytes)?;
        let data = Arc::new(SnapshotData {
            _permit: permit,
            state: state.clone(),
        });
        let mut pins = self.shared.pins.lock().map_err(|_| Error::NeedsRecovery)?;
        pins.retain(|pin| pin.strong_count() != 0);
        pins.push(Arc::downgrade(&data));
        let catalog = state.catalog.clone();
        let sequence = state.lsn;
        let pin = Lease::new(
            data,
            &state.resources.reaper,
            Some(state.resources.timeouts.snapshot),
            None,
            None,
        )?;
        Ok(ReadSnapshot {
            pin,
            catalog,
            sequence,
        })
    }
    /// Returns the last published sequence.
    pub fn sequence(&self) -> Result<u64> {
        Ok(self.pin()?.sequence())
    }
    /// Returns owned table definitions from one committed view.
    pub fn tables(&self) -> Result<Vec<Table>> {
        Ok(self.pin()?.tables().cloned().collect())
    }
    /// Resolves a name from one committed view.
    pub fn table(&self, name: &str) -> Result<Option<Table>> {
        Ok(self.pin()?.table(name).cloned())
    }
    /// Reports checkpoint retention by active snapshots.
    pub fn snapshot_stats(&self) -> SnapshotStats {
        self.shared.stats()
    }
    /// See [`Database::get`](crate::Database::get); uses one committed view per call.
    pub fn get(&self, table: TableId, id: u64) -> Result<Option<Entity>> {
        self.pin()?.get(table, id)
    }
    /// See [`Database::get_at_version`](crate::Database::get_at_version); uses one committed view per call.
    pub fn get_at_version(&self, table: TableId, id: u64, version: u64) -> Result<Entity> {
        self.pin()?.get_at_version(table, id, version)
    }
    /// See [`Database::replay`](crate::Database::replay); uses one committed view per call.
    pub fn replay(&self, table: TableId, id: u64) -> Result<Entity> {
        self.pin()?.replay(table, id)
    }
    /// See [`Database::replay_to_version`](crate::Database::replay_to_version); uses one committed view per call.
    pub fn replay_to_version(&self, table: TableId, id: u64, version: u64) -> Result<Entity> {
        self.pin()?.replay_to_version(table, id, version)
    }
    /// See [`Database::events`](crate::Database::events); uses one committed view per call.
    pub fn events(&self, table: TableId, id: u64) -> Result<Vec<Event>> {
        self.pin()?.events(table, id)
    }
    /// See [`Database::retained_range`](crate::Database::retained_range); uses one committed view per call.
    pub fn retained_range(&self, table: TableId, id: u64) -> Result<(u64, u64)> {
        self.pin()?.retained_range(table, id)
    }
    /// See [`Database::scan`](crate::Database::scan); uses one committed view per call.
    pub fn scan(&self, table: TableId, visit: impl FnMut(Entity) -> Result<()>) -> Result<()> {
        self.pin()?.scan(table, visit)
    }
}

/// A repeatable committed view, including catalog and retained history.
/// Snapshot isolation does not promise serializability with concurrent writers.
#[derive(Clone)]
pub struct ReadSnapshot {
    pin: Arc<Lease<Arc<SnapshotData>>>,
    catalog: Arc<BTreeMap<TableId, Table>>,
    sequence: u64,
}
impl ReadSnapshot {
    pub(crate) fn state(&self) -> Result<Arc<SnapshotData>> {
        self.pin.with(|data| Ok(data.clone()))
    }
    fn with_state<R>(
        &self,
        operation: impl FnOnce(&ReadState, std::time::Instant) -> Result<R>,
    ) -> Result<R> {
        let data = self.state()?;
        let end = deadline::deadline(data.state.resources.timeouts.operation)?;
        let end = self.pin.deadline.map_or(end, |expiry| end.min(expiry));
        deadline::check(Some(end))?;
        let result = operation(&data.state, end);
        deadline::check(Some(end))?;
        result
    }
    /// Committed sequence captured by this snapshot.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Table definitions as they existed at snapshot acquisition.
    pub fn tables(&self) -> impl Iterator<Item = &Table> {
        self.catalog.values()
    }
    /// Resolves a table using the pinned catalog.
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.catalog.values().find(|table| table.name == name)
    }
    /// See [`Database::get`](crate::Database::get); uses this pinned view.
    pub fn get(&self, table: TableId, id: u64) -> Result<Option<Entity>> {
        self.with_state(|state, _| state.get(table, id))
    }
    /// See [`Database::get_at_version`](crate::Database::get_at_version); uses this pinned view.
    pub fn get_at_version(&self, table: TableId, id: u64, version: u64) -> Result<Entity> {
        self.with_state(|state, _| state.get_at_version(table, id, version))
    }
    /// See [`Database::replay`](crate::Database::replay); uses this pinned view.
    pub fn replay(&self, table: TableId, id: u64) -> Result<Entity> {
        self.with_state(|state, _| state.replay(table, id))
    }
    /// See [`Database::replay_to_version`](crate::Database::replay_to_version); uses this pinned view.
    pub fn replay_to_version(&self, table: TableId, id: u64, version: u64) -> Result<Entity> {
        self.with_state(|state, _| state.replay_to_version(table, id, version))
    }
    /// See [`Database::events`](crate::Database::events); uses this pinned view.
    pub fn events(&self, table: TableId, id: u64) -> Result<Vec<Event>> {
        self.with_state(|state, _| state.events(table, id))
    }
    /// See [`Database::retained_range`](crate::Database::retained_range); uses this pinned view.
    pub fn retained_range(&self, table: TableId, id: u64) -> Result<(u64, u64)> {
        self.with_state(|state, _| state.retained_range(table, id))
    }
    /// See [`Database::scan`](crate::Database::scan); uses this pinned view.
    pub fn scan(&self, table: TableId, visit: impl FnMut(Entity) -> Result<()>) -> Result<()> {
        self.with_state(|state, end| {
            let mut visit = visit;
            state.scan(table, |entity| {
                deadline::check(Some(end))?;
                visit(entity)
            })
        })
    }
}
