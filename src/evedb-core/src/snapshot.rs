// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    Entity, Error, Result, Table, TableId,
    checksum::Checksum,
    codec::{Decoder, Encoder, MAX_RECORD},
    error::corrupt,
    model::*,
    storage::{
        frame,
        heap::{self, HeapWriter},
        index::{self},
        pager::{FileId, Pager, table_path},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Seek},
    path::{Path, PathBuf},
    sync::Arc,
};

/// One published generation and the checkpoint sequence stamped on its pages.
///
/// A manifest addresses generations by their position in [`Root::generations`],
/// so a stored locator carries one byte instead of a generation number.
#[derive(Clone, Copy)]
pub(crate) struct GenerationInfo {
    pub generation: u64,
    pub lsn: u64,
}
#[derive(Clone)]
pub(crate) struct FileInfo {
    pub(crate) generation: u64,
    pub(crate) table: u64,
    pub(crate) kind: u8,
    pub(crate) segment: u64,
    pub(crate) size: u64,
    pub(crate) crc: u32,
}
impl FileInfo {
    pub fn id(&self) -> FileId {
        FileId::new(self.table, self.generation, self.kind, self.segment)
    }
}
/// How many entities one table left in one generation, and how many died since.
///
/// Current state/base form a unit, so superseding them makes exactly one entity
/// of its former generation unreachable. Counting that at publication keeps the
/// collector's decisions free of any scan.
#[derive(Clone, Copy, Default)]
pub(crate) struct Occupancy {
    pub entities: u64,
    pub dead: u64,
}
impl Occupancy {
    /// The share of written entities still reachable, or 1.0 for an empty generation.
    pub fn live_ratio(&self) -> f64 {
        if self.entities == 0 {
            return 1.0;
        }
        (self.entities - self.dead) as f64 / self.entities as f64
    }
}
#[derive(Clone)]
pub(crate) struct Root {
    pub generation: u64,
    pub lsn: u64,
    pub catalog: BTreeMap<TableId, Table>,
    /// Referenced generations in ascending order; a locator's slot indexes this.
    pub generations: Vec<GenerationInfo>,
    pub files: Vec<FileInfo>,
    pub partitions: BTreeMap<crate::partitions::Route, crate::partitions::Partition>,
    pub history_generations: BTreeMap<u64, u64>,
    pub history_compacted: bool,
    /// Live and superseded entity counts per table and generation slot.
    pub occupancy: BTreeMap<(TableId, u8), Occupancy>,
}
impl Root {
    pub fn file_sizes(&self) -> impl Iterator<Item = (FileId, u64)> + '_ {
        self.files.iter().map(|file| {
            (
                FileId::new(file.table, file.generation, file.kind, file.segment),
                file.size,
            )
        })
    }
    pub fn empty() -> Self {
        Self {
            generation: 0,
            lsn: 0,
            catalog: BTreeMap::new(),
            generations: vec![GenerationInfo {
                generation: 0,
                lsn: 0,
            }],
            files: Vec::new(),
            partitions: BTreeMap::new(),
            history_generations: BTreeMap::new(),
            history_compacted: true,
            occupancy: BTreeMap::new(),
        }
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut e = Encoder::default();
        e.u64(self.generation);
        e.u64(self.lsn);
        e.u64(self.catalog.len() as u64);
        for table in self.catalog.values() {
            encode_table(table, &mut e);
        }
        e.u64(self.generations.len() as u64);
        for entry in &self.generations {
            e.u64(entry.generation);
            e.u64(entry.lsn);
        }
        e.u64(self.partitions.len() as u64);
        for (&(table, kind, lower), part) in &self.partitions {
            e.u64(table);
            e.u8(kind);
            e.u64(lower[0]);
            e.u64(lower[1]);
            e.u64(part.file.generation);
            e.u64(part.file.segment);
            e.u64(part.lsn);
            e.u64(part.base);
            for value in part.first.into_iter().chain(part.last) {
                e.u64(value);
            }
            e.u64(part.slots.len() as u64);
            for (&old, &new) in &part.slots {
                e.u8(old);
                e.u8(new);
            }
            e.u64(part.dependencies.len() as u64);
            for file in &part.dependencies {
                e.u64(file.table);
                e.u64(file.generation);
                e.u8(file.kind);
                e.u64(file.segment);
            }
        }
        e.u8(u8::from(self.history_compacted));
        e.u64(self.history_generations.len() as u64);
        for (&generation, &lsn) in &self.history_generations {
            e.u64(generation);
            e.u64(lsn);
        }
        e.u64(self.files.len() as u64);
        for file in &self.files {
            e.u64(file.generation);
            e.u64(file.table);
            e.u8(file.kind);
            e.u64(file.segment);
            e.u64(file.size);
            e.u32(file.crc);
        }
        e.u64(self.occupancy.len() as u64);
        for (&(table, slot), value) in &self.occupancy {
            e.u64(table);
            e.u8(slot);
            e.u64(value.entities);
            e.u64(value.dead);
        }
        e.0
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut d = Decoder::new(bytes);
        let generation = d.u64()?;
        let lsn = d.u64()?;
        let n = d.count(25)?;
        let mut catalog = BTreeMap::new();
        let mut names = BTreeSet::new();
        for _ in 0..n {
            let table = decode_table(&mut d)?;
            if !names.insert(table.name.clone()) || catalog.insert(table.id, table).is_some() {
                return Err(corrupt("duplicate table"));
            }
        }
        let n = d.count(16)?;
        let mut generations: Vec<GenerationInfo> = Vec::new();
        for _ in 0..n {
            let entry = GenerationInfo {
                generation: d.u64()?,
                lsn: d.u64()?,
            };
            if generations
                .last()
                .is_some_and(|previous| previous.generation >= entry.generation)
            {
                return Err(corrupt("unordered generation list"));
            }
            generations.push(entry);
        }
        if generations.len() > heap::MAX_SLOTS
            || generations.last().map(|entry| entry.generation) != Some(generation)
        {
            return Err(corrupt("invalid generation list"));
        }
        let n = d.count(105)?;
        let mut partitions = BTreeMap::new();
        let mut index_files = BTreeSet::new();
        let mut index_roots = BTreeSet::new();
        for _ in 0..n {
            let table = d.u64()?;
            let kind = d.u8()?;
            let lower = [d.u64()?, d.u64()?];
            let file = FileId::new(table, d.u64()?, kind, d.u64()?);
            let sequence = d.u64()?;
            let base = d.u64()?;
            let first = [d.u64()?, d.u64()?];
            let last = [d.u64()?, d.u64()?];
            let count = d.count(2)?;
            let mut slots = BTreeMap::new();
            for _ in 0..count {
                let old = d.u8()?;
                let new = d.u8()?;
                if usize::from(new) >= generations.len() || slots.insert(old, new).is_some() {
                    return Err(corrupt("invalid partition slot mapping"));
                }
            }
            let count = d.count(25)?;
            let mut dependencies = BTreeSet::new();
            for _ in 0..count {
                let dependency = FileId::new(d.u64()?, d.u64()?, d.u8()?, d.u64()?);
                if dependency.table != table
                    || !matches!(dependency.kind, 2 | 6)
                    || !dependencies.insert(dependency)
                {
                    return Err(corrupt("invalid partition dependency"));
                }
            }
            if !catalog.contains_key(&table)
                || !(3..=5).contains(&kind)
                || file.generation > generation
                || sequence > lsn
                || first > last
                || crate::partitions::bucket(kind, first) != lower
                || crate::partitions::bucket(kind, last) != lower
                || (kind != 3 && !slots.is_empty())
                || (kind == 3 && slots.is_empty())
                || !index_roots.insert((file, base))
            {
                return Err(corrupt("invalid index partition"));
            }
            index_files.insert(file);
            let part = crate::partitions::Partition {
                base,
                file,
                lsn: sequence,
                first,
                last,
                slots,
                dependencies,
            };
            if partitions.insert((table, kind, lower), part).is_some() {
                return Err(corrupt("duplicate index partition route"));
            }
        }
        let history_compacted = match d.u8()? {
            0 => false,
            1 => true,
            _ => return Err(corrupt("invalid history compaction flag")),
        };
        let n = d.count(16)?;
        let mut history_generations = BTreeMap::new();
        for _ in 0..n {
            let at = d.u64()?;
            let sequence = d.u64()?;
            if at > generation
                || sequence > lsn
                || history_generations.insert(at, sequence).is_some()
            {
                return Err(corrupt("invalid history generation"));
            }
        }
        let n = d.count(37)?;
        let mut files = Vec::new();
        let mut seen = BTreeSet::new();
        for _ in 0..n {
            let file = FileInfo {
                generation: d.u64()?,
                table: d.u64()?,
                kind: d.u8()?,
                segment: d.u64()?,
                size: d.u64()?,
                crc: d.u32()?,
            };
            if !catalog.contains_key(&file.table)
                || file.kind > 6
                || (file.kind < 3 && file.segment != 0)
                || (if matches!(file.kind, 2 | 6) {
                    !history_generations.contains_key(&file.generation)
                } else if matches!(file.kind, 3..=5) {
                    !index_files.contains(&file.id())
                } else {
                    !generations
                        .iter()
                        .any(|entry| entry.generation == file.generation)
                })
                || !seen.insert((file.table, file.kind, file.generation, file.segment))
            {
                return Err(corrupt("invalid checkpoint file reference"));
            }
            files.push(file);
        }
        for part in partitions.values() {
            if !files.iter().any(|info| {
                info.id() == part.file && part.base < info.size / crate::storage::PAGE_SIZE as u64
            }) {
                return Err(corrupt("partition root outside index file"));
            }
            for file in std::iter::once(&part.file).chain(&part.dependencies) {
                if !seen.contains(&(file.table, file.kind, file.generation, file.segment)) {
                    return Err(corrupt("incomplete partition manifest"));
                }
            }
        }
        let n = d.count(25)?;
        let mut occupancy = BTreeMap::new();
        for _ in 0..n {
            let table = d.u64()?;
            let slot = d.u8()?;
            let value = Occupancy {
                entities: d.u64()?,
                dead: d.u64()?,
            };
            if !catalog.contains_key(&table)
                || usize::from(slot) >= generations.len()
                || value.dead > value.entities
                || occupancy.insert((table, slot), value).is_some()
            {
                return Err(corrupt("invalid generation occupancy"));
            }
        }
        d.finish()?;
        Ok(Self {
            generation,
            lsn,
            catalog,
            generations,
            files,
            partitions,
            history_generations,
            history_compacted,
            occupancy,
        })
    }
    /// Resolves the generation slot a stored locator refers to.
    fn at_slot(&self, slot: u8) -> Result<GenerationInfo> {
        self.generations
            .get(usize::from(slot))
            .copied()
            .ok_or_else(|| corrupt("locator names an absent generation"))
    }
    /// Locates a file of the generation a locator points into.
    fn slotted(&self, table: TableId, kind: u8, slot: u8, segment: u64) -> Result<(FileId, u64)> {
        let entry = self.at_slot(slot)?;
        Ok((
            FileId::new(table, entry.generation, kind, segment),
            entry.lsn,
        ))
    }
    /// Lists the physical identity of every file this manifest references.
    pub(crate) fn file_ids(&self) -> impl Iterator<Item = FileId> + '_ {
        self.files
            .iter()
            .map(|file| FileId::new(file.table, file.generation, file.kind, file.segment))
    }
    /// Sums the occupancy of one generation slot across every table.
    pub(crate) fn totals(&self, slot: u8) -> Occupancy {
        let mut total = Occupancy::default();
        for (&(_, at), value) in &self.occupancy {
            if at == slot {
                total.entities += value.entities;
                total.dead += value.dead;
            }
        }
        total
    }
    /// Builds the generation list of the next manifest and the slot translation.
    ///
    /// Surviving generations keep their order, so translating an old slot is a
    /// lookup; an absorbed generation translates to `None` and its entities are
    /// rewritten.
    pub(crate) fn rebuild_slots(
        &self,
        absorb: &BTreeSet<u64>,
        generation: u64,
        lsn: u64,
    ) -> Result<(Vec<GenerationInfo>, [Option<u8>; heap::MAX_SLOTS])> {
        let mut slots = [None; heap::MAX_SLOTS];
        let mut generations = Vec::new();
        for (old, entry) in self.generations.iter().enumerate() {
            if !absorb.contains(&entry.generation) {
                slots[old] = Some(generations.len() as u8);
                generations.push(*entry);
            }
        }
        generations.push(GenerationInfo { generation, lsn });
        if generations.len() > heap::MAX_SLOTS {
            return Err(Error::Invalid(
                "a manifest cannot reference more generations".into(),
            ));
        }
        Ok((generations, slots))
    }
    /// Copies the file references that survive into the next manifest.
    ///
    /// Indexes are rebuilt for the new manifest. Historical payload references
    /// are collected separately while those indexes are written.
    pub(crate) fn kept_files(&self, absorb: &BTreeSet<u64>) -> Vec<FileInfo> {
        self.files
            .iter()
            .filter(|file| matches!(file.kind, 0 | 1) && !absorb.contains(&file.generation))
            .cloned()
            .collect()
    }
    /// Moves the occupancy of surviving generations onto their new slots.
    pub(crate) fn kept_occupancy(
        &self,
        slots: &[Option<u8>; heap::MAX_SLOTS],
    ) -> BTreeMap<(TableId, u8), Occupancy> {
        self.occupancy
            .iter()
            .filter_map(|(&(table, slot), value)| {
                Some(((table, slots[usize::from(slot)]?), *value))
            })
            .collect()
    }
    pub fn verify(&self, pager: &Pager) -> Result<()> {
        for file in &self.files {
            let (size, crc) = digest(&pager.path(FileId::new(
                file.table,
                file.generation,
                file.kind,
                file.segment,
            )))?;
            if size != file.size || crc != file.crc {
                return Err(corrupt("checkpoint file checksum mismatch"));
            }
        }
        Ok(())
    }
    /// Reads the current state through the primary index, or `None` if absent.
    pub fn current(&self, pager: &Pager, table: TableId, id: u64) -> Result<Option<Entity>> {
        let Some((_, value)) = self.locate(pager, table, id)? else {
            return Ok(None);
        };
        self.state(pager, table, 0, value[0], id).map(Some)
    }
    /// Reads the primary-index entry of an entity and checks its generation slot.
    ///
    /// Current state and base share a generation slot. Historical payloads are
    /// addressed independently through the newest historical indexes.
    fn locate(&self, pager: &Pager, table: TableId, id: u64) -> Result<Option<(u8, index::Value)>> {
        if !self.catalog.contains_key(&table) {
            return Ok(None);
        }
        let Some(value) = self.lookup_index(pager, table, 3, [id, 0])? else {
            return Ok(None);
        };
        Ok(Some((entity_slot(value)?, value)))
    }
    /// Reads one stored entity state and checks that it belongs to `id`.
    pub(crate) fn state(
        &self,
        pager: &Pager,
        table: TableId,
        kind: u8,
        locator: u64,
        id: u64,
    ) -> Result<Entity> {
        let (slot, location) = heap::split(locator);
        let (file, lsn) = self.slotted(table, kind, slot, 0)?;
        let entity = decode_entity(&heap::read(pager, file, location, lsn)?)?;
        if entity.id != id {
            return Err(corrupt("index references another entity"));
        }
        Ok(entity)
    }
    /// Reads one framed event at a known segment location.
    fn event_at(
        &self,
        pager: &Pager,
        table: TableId,
        location: index::Value,
    ) -> Result<(u64, Event, u64)> {
        if !self.history_generations.contains_key(&location[0]) {
            return Err(corrupt("absent history generation"));
        }
        let (segment, offset) = event_split(location[1]);
        let id = FileId::new(table, location[0], 6, segment);
        let file = pager.file(id)?;
        let record = frame::read_at(&file.file, offset, file.len)?
            .ok_or_else(|| corrupt("truncated indexed event"))?;
        if record.kind != 3 {
            return Err(corrupt("invalid history frame kind"));
        }
        let (entity_id, payload) = unpack(&record.payload)?;
        Ok((entity_id, decode_event(&payload)?, record.lsn))
    }
    pub fn at(
        &self,
        pager: &Pager,
        table: TableId,
        id: u64,
        version: u64,
        snapshots: bool,
    ) -> Result<Entity> {
        let (_, value) = self
            .locate(pager, table, id)?
            .ok_or_else(|| Error::NotFound(format!("entity {id}")))?;
        let current = self.state(pager, table, 0, value[0], id)?;
        let state = self.state(pager, table, 1, value[1], id)?;
        if state.version > current.version {
            return Err(corrupt("invalid state reference"));
        }
        if version < state.version || version > current.version {
            return Err(Error::VersionUnavailable {
                requested: version,
                first: state.version,
                last: current.version,
            });
        }
        if snapshots && version == current.version {
            return Ok(current);
        }
        self.advance(pager, table, id, state, version, snapshots)
    }
    pub fn advance(
        &self,
        pager: &Pager,
        table: TableId,
        id: u64,
        mut state: Entity,
        version: u64,
        snapshots: bool,
    ) -> Result<Entity> {
        let definition = self
            .catalog
            .get(&table)
            .ok_or_else(|| Error::NotFound(format!("table {table}")))?;
        if snapshots
            && let Some((key, location)) = self.floor_index(pager, table, 5, [id, version])?
            && key[0] == id
            && key[1] >= state.version
        {
            state = self.history_state(pager, table, location, id)?;
            if state.version != key[1] {
                return Err(corrupt("invalid snapshot identity"));
            }
        }
        if state.version == version {
            return Ok(state);
        }
        for entry in self.scan_index(pager, table, 4, [id, state.version + 1], [id, version])? {
            let (key, location) = entry?;
            let (entity_id, event, lsn) = self.event_at(pager, table, location)?;
            if entity_id != id
                || event.version != key[1]
                || event.transaction != lsn
                || event.transaction > self.lsn
            {
                return Err(corrupt("invalid indexed event identity"));
            }
            apply_event(&mut state, &event, definition)?;
        }
        if state.version != version {
            return Err(corrupt("gap in indexed history"));
        }
        Ok(state)
    }

    pub fn history_state(
        &self,
        pager: &Pager,
        table: TableId,
        location: index::Value,
        id: u64,
    ) -> Result<Entity> {
        let lsn = self
            .history_generations
            .get(&location[0])
            .ok_or_else(|| corrupt("absent snapshot generation"))?;
        let entity = decode_entity(&heap::read(
            pager,
            FileId::new(table, location[0], 2, 0),
            location[1],
            *lsn,
        )?)?;
        if entity.id != id {
            return Err(corrupt("snapshot references another entity"));
        }
        Ok(entity)
    }
    pub fn events(
        &self,
        pager: &Pager,
        table: TableId,
        id: u64,
        first: u64,
        last: u64,
    ) -> Result<Vec<Event>> {
        let mut events = Vec::new();
        if first > last {
            return Ok(events);
        }
        for item in self.scan_index(pager, table, 4, [id, first], [id, last])? {
            let (key, location) = item?;
            let (entity_id, event, lsn) = self.event_at(pager, table, location)?;
            if entity_id != id
                || event.version != key[1]
                || event.transaction != lsn
                || event.transaction > self.lsn
            {
                return Err(corrupt("history index references another event"));
            }
            events.push(event);
        }
        Ok(events)
    }
    pub fn entity(
        self: &Arc<Self>,
        pager: &Arc<Pager>,
        table: TableId,
        id: u64,
    ) -> Result<Option<EntityData>> {
        let Some((_, value)) = self.locate(pager, table, id)? else {
            return Ok(None);
        };
        let current = self.state(pager, table, 0, value[0], id)?;
        let base = self.state(pager, table, 1, value[1], id)?;
        if base.version > current.version {
            return Err(corrupt("invalid current/base reference"));
        }
        Ok(Some(EntityData {
            charges: crate::ordered_map::OrderedMap::new(),
            disk: Some(Arc::new(crate::history::DiskHistory {
                root: self.clone(),
                pager: pager.clone(),
                table,
                through: current.version,
            })),
            current,
            base,
            events: Vec::new(),
            committed: crate::ordered_map::OrderedMap::new(),
            snapshots: crate::ordered_map::OrderedMap::new(),
        }))
    }
}
/// Reads the generation slot shared by the locators of a primary-index entry.
pub(crate) fn entity_slot(value: index::Value) -> Result<u8> {
    let (slot, _) = heap::split(value[0]);
    let (base, _) = heap::split(value[1]);
    if slot != base {
        return Err(corrupt(
            "entity state and base disagree on their generation",
        ));
    }
    Ok(slot)
}
/// Rewrites the generation slot of a primary-index entry for a new manifest.
pub(crate) fn remap(value: index::Value, slot: u8) -> index::Value {
    let (_, current) = heap::split(value[0]);
    let (_, base) = heap::split(value[1]);
    [heap::locate(slot, current), heap::locate(slot, base)]
}
pub(crate) fn digest(path: &Path) -> Result<(u64, u32)> {
    let mut file = File::open(path)?;
    let mut crc = Checksum::new();
    let mut size = 0;
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        crc.update(&buffer[..n]);
        size += n as u64;
    }
    Ok((size, crc.finish()))
}
pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Simple optional RLE; incompressible payloads remain verbatim.
fn pack(id: u64, bytes: &[u8], compress: bool) -> Vec<u8> {
    let mut encoded = Vec::new();
    if compress {
        let mut offset = 0;
        while offset < bytes.len() {
            let value = bytes[offset];
            let mut count = 1;
            while count < 255 && offset + count < bytes.len() && bytes[offset + count] == value {
                count += 1;
            }
            encoded.push(count as u8);
            encoded.push(value);
            offset += count;
        }
    }
    let use_rle = compress && encoded.len() < bytes.len();
    let mut result = id.to_le_bytes().to_vec();
    result.push(u8::from(use_rle));
    result.extend((bytes.len() as u32).to_le_bytes());
    result.extend(if use_rle { &encoded } else { bytes });
    result
}
fn unpack(bytes: &[u8]) -> Result<(u64, Vec<u8>)> {
    if bytes.len() < 13 {
        return Err(corrupt("short history record"));
    }
    let id = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    let size = u32::from_le_bytes(bytes[9..13].try_into().unwrap()) as usize;
    if size > MAX_RECORD {
        return Err(corrupt("oversized history record"));
    }
    let mut result = Vec::with_capacity(size);
    match bytes[8] {
        0 => result.extend(&bytes[13..]),
        1 => {
            let (chunks, remainder) = bytes[13..].as_chunks::<2>();
            if !remainder.is_empty() {
                return Err(corrupt("invalid RLE payload"));
            }
            for chunk in chunks {
                if chunk[0] == 0 || result.len() + chunk[0] as usize > size {
                    return Err(corrupt("RLE length overflow"));
                }
                result.resize(result.len() + chunk[0] as usize, chunk[1]);
            }
        }
        _ => return Err(corrupt("unsupported history compression")),
    }
    if result.len() != size {
        return Err(corrupt("history length mismatch"));
    }
    Ok((id, result))
}

pub(crate) struct TableOutput {
    pub files: Vec<FileInfo>,
    pub entities: u64,
    pub history_generations: BTreeMap<u64, u64>,
    pub partitions: BTreeMap<crate::partitions::Route, crate::partitions::Partition>,
}
pub(crate) struct TableWriter {
    current: HeapWriter,
    bases: HeapWriter,
    snapshots: HeapWriter,
    indexes: crate::partitions::Indexes,
    history: File,
    segment: u64,
    segment_limit: u64,
    compress: bool,
    directory: PathBuf,
    table: u64,
    generation: u64,
    slot: u8,
    entities: u64,
    reused: BTreeMap<FileId, FileInfo>,
    history_generations: BTreeMap<u64, u64>,
    rewrite_history: bool,
    rewrite_indexes: bool,
}
impl TableWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        directory: &Path,
        table: u64,
        generation: u64,
        slot: u8,
        lsn: u64,
        limit: u64,
        compress: bool,
        rewrite_history: bool,
        max_index_partitions: usize,
        rewrite_indexes: bool,
    ) -> Result<Self> {
        let dir = table_path(directory, table, generation);
        fs::create_dir_all(dir.join("history"))?;
        fs::create_dir_all(dir.join("indexes"))?;
        let path = |kind| FileId::new(table, generation, kind, 0).path(directory);
        Ok(Self {
            current: HeapWriter::create(&path(0), lsn)?,
            bases: HeapWriter::create(&path(1), lsn)?,
            snapshots: HeapWriter::create(&path(2), lsn)?,
            indexes: crate::partitions::Indexes::new(
                directory,
                table,
                generation,
                lsn,
                max_index_partitions,
            ),
            history: File::create_new(path(6))?,
            segment: 0,
            segment_limit: limit,
            compress,
            directory: directory.to_owned(),
            table,
            generation,
            slot,
            entities: 0,
            reused: BTreeMap::new(),
            history_generations: [(generation, lsn)].into(),
            rewrite_history,
            rewrite_indexes,
        })
    }
    /// Records an entity that already lives in a kept generation.
    ///
    /// Only the primary-index entry is rewritten, with its locators remapped to
    /// this manifest's generation slots. No record is copied.
    pub fn carry(
        &mut self,
        id: u64,
        value: index::Value,
        root: &Root,
        pager: &Pager,
    ) -> Result<()> {
        self.indexes.append(3, [id, 0], value, None)?;
        self.copy_history(root, pager, id, 0, u64::MAX, None)
    }
    fn remember(&mut self, root: &Root, file: FileId) -> Result<()> {
        if let std::collections::btree_map::Entry::Vacant(entry) = self.reused.entry(file) {
            let info = root
                .files
                .iter()
                .find(|info| {
                    FileId::new(info.table, info.generation, info.kind, info.segment) == file
                })
                .ok_or_else(|| corrupt("history references an unlisted file"))?;
            entry.insert(info.clone());
            self.history_generations.insert(
                file.generation,
                *root
                    .history_generations
                    .get(&file.generation)
                    .ok_or_else(|| corrupt("absent history sequence"))?,
            );
        }
        Ok(())
    }
    pub fn carry_partition(
        &mut self,
        root: &Root,
        route: crate::partitions::Route,
        slots: &[Option<u8>; heap::MAX_SLOTS],
    ) -> Result<()> {
        let mut part = root.partitions[&route].clone();
        for slot in part.slots.values_mut() {
            *slot =
                slots[usize::from(*slot)].ok_or_else(|| corrupt("reused collected partition"))?;
        }
        for &file in &part.dependencies {
            self.remember(root, file)?;
        }
        self.indexes.reuse(route, part, root)
    }
    pub fn carry_partition_history(
        &mut self,
        root: &Root,
        lower: u64,
        upper: u64,
        slots: &[Option<u8>; heap::MAX_SLOTS],
    ) -> Result<()> {
        for kind in 4..=5 {
            for (&route, _) in root
                .partitions
                .range((self.table, kind, [lower, 0])..=(self.table, kind, [upper, u64::MAX]))
            {
                self.carry_partition(root, route, slots)?;
            }
        }
        Ok(())
    }
    fn copy_history(
        &mut self,
        root: &Root,
        pager: &Pager,
        id: u64,
        base: u64,
        last: u64,
        overrides: Option<&EntityData>,
    ) -> Result<()> {
        for kind in [5, 4] {
            let first = if kind == 4 {
                if base >= last {
                    continue;
                }
                base + 1
            } else {
                base
            };
            let low = crate::partitions::bucket(kind, [id, first]);
            let high = crate::partitions::bucket(kind, [id, last]);
            for (&route, part) in root
                .partitions
                .range((self.table, kind, low)..=(self.table, kind, high))
            {
                let upper = crate::partitions::end(kind, route.2)[1];
                let modified = overrides.is_some_and(|data| {
                    if kind == 5 {
                        data.snapshots.range(route.2[1]..=upper).next().is_some()
                    } else {
                        data.committed.range(route.2[1]..=upper).next().is_some()
                            || data
                                .events
                                .iter()
                                .any(|event| (route.2[1]..=upper).contains(&event.version))
                    }
                });
                if !self.rewrite_history
                    && !self.rewrite_indexes
                    && !modified
                    && first <= part.first[1]
                    && last >= part.last[1]
                {
                    for &file in &part.dependencies {
                        self.remember(root, file)?;
                    }
                    self.indexes.reuse(route, part.clone(), root)?;
                    continue;
                }
                for entry in index::scan_at(
                    pager,
                    part.file,
                    part.lsn,
                    part.base,
                    [id, first],
                    [id, last],
                )? {
                    let (key, location) = entry?;
                    if kind == 5 {
                        if overrides.is_some_and(|data| data.snapshots.contains_key(&key[1])) {
                            continue;
                        }
                        if self.rewrite_history {
                            let state = root.history_state(pager, self.table, location, id)?;
                            self.append_snapshot(id, &state)?;
                        } else {
                            let file = FileId::new(self.table, location[0], 2, 0);
                            self.remember(root, file)?;
                            self.indexes.append(5, key, location, Some(file))?;
                        }
                    } else if self.rewrite_history {
                        let (entity_id, event, lsn) = root.event_at(pager, self.table, location)?;
                        if entity_id != id || event.version != key[1] || event.transaction != lsn {
                            return Err(corrupt("invalid collected event"));
                        }
                        self.append_event(id, &event)?;
                    } else {
                        let file =
                            FileId::new(self.table, location[0], 6, event_split(location[1]).0);
                        self.remember(root, file)?;
                        self.indexes.append(4, key, location, Some(file))?;
                    }
                }
            }
        }
        Ok(())
    }
    pub fn append(&mut self, id: u64, data: &EntityData) -> Result<()> {
        let current = heap::locate(
            self.slot,
            self.current.append(&encode_entity(&data.current))?,
        );
        let base = heap::locate(self.slot, self.bases.append(&encode_entity(&data.base))?);
        self.indexes.append(3, [id, 0], [current, base], None)?;
        self.entities += 1;
        if let Some(disk) = &data.disk
            && data.base.version <= disk.through
        {
            self.copy_history(
                &disk.root,
                &disk.pager,
                id,
                data.base.version,
                disk.through,
                Some(data),
            )?;
        }
        for (_, state) in data.snapshots.range(data.base.version..) {
            self.append_snapshot(id, state)?;
        }
        for (_, event) in data.committed.range((
            std::ops::Bound::Excluded(data.base.version),
            std::ops::Bound::Unbounded,
        )) {
            self.append_event(id, event)?;
        }
        for event in &data.events {
            self.append_event(id, event)?;
        }
        Ok(())
    }
    fn append_snapshot(&mut self, id: u64, state: &Entity) -> Result<()> {
        let location = self.snapshots.append(&encode_entity(state))?;
        self.indexes.append(
            5,
            [id, state.version],
            [self.generation, location],
            Some(FileId::new(self.table, self.generation, 2, 0)),
        )
    }
    fn append_event(&mut self, id: u64, event: &Event) -> Result<()> {
        let payload = pack(id, &encode_event(event), self.compress);
        let offset = self.history.stream_position()?;
        if offset != 0 && offset + payload.len() as u64 + 40 > self.segment_limit {
            self.history.sync_all()?;
            self.segment = self
                .segment
                .checked_add(1)
                .ok_or_else(|| corrupt("history segment exhausted"))?;
            self.history = File::create_new(
                FileId::new(self.table, self.generation, 6, self.segment).path(&self.directory),
            )?;
        }
        let offset = self.history.stream_position()?;
        let location = event_location(self.segment, offset)?;
        frame::append(&mut self.history, 3, event.transaction, &payload)?;
        self.indexes.append(
            4,
            [id, event.version],
            [self.generation, location],
            Some(FileId::new(self.table, self.generation, 6, self.segment)),
        )
    }
    pub fn finish(self) -> Result<TableOutput> {
        let entities = self.entities;
        self.current.finish()?;
        self.bases.finish()?;
        self.snapshots.finish()?;
        let (index_files, partitions) = self.indexes.finish()?;
        self.history.sync_all()?;
        let mut files: Vec<_> = self.reused.into_values().collect();
        files.extend(index_files);
        let kinds = (0..3)
            .map(|kind| (kind, 0))
            .chain((0..=self.segment).map(|segment| (6, segment)));
        for (kind, segment) in kinds {
            let (size, crc) = digest(
                &FileId::new(self.table, self.generation, kind, segment).path(&self.directory),
            )?;
            files.push(FileInfo {
                generation: self.generation,
                table: self.table,
                kind,
                segment,
                size,
                crc,
            });
        }
        let dir = table_path(&self.directory, self.table, self.generation);
        sync_directory(&dir.join("history"))?;
        sync_directory(&dir.join("indexes"))?;
        sync_directory(&dir)?;
        sync_directory(dir.parent().unwrap())?;
        sync_directory(&self.directory.join("tables"))?;
        Ok(TableOutput {
            files,
            entities,
            history_generations: self.history_generations,
            partitions,
        })
    }
}

fn event_location(segment: u64, offset: u64) -> Result<u64> {
    if segment > u16::MAX as u64 || offset >= (1u64 << 48) {
        return Err(Error::Invalid("history location exhausted".into()));
    }
    Ok((segment << 48) | offset)
}
fn event_split(location: u64) -> (u64, u64) {
    (location >> 48, location & ((1u64 << 48) - 1))
}
