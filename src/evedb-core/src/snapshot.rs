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
        index::{self, IndexWriter},
        pager::{FileId, Pager, table_path},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Seek},
    path::{Path, PathBuf},
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
    generation: u64,
    table: u64,
    kind: u8,
    segment: u64,
    size: u64,
    crc: u32,
}
/// How many entities one table left in one generation, and how many died since.
///
/// An entity is written as a unit, so superseding it makes exactly one entity
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
    /// Live and superseded entity counts per table and generation slot.
    pub occupancy: BTreeMap<(TableId, u8), Occupancy>,
}
impl Root {
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
                || (file.kind != 6 && file.segment != 0)
                || !generations
                    .iter()
                    .any(|entry| entry.generation == file.generation)
                || (file.kind == 3 && file.generation != generation)
                || !seen.insert((file.table, file.kind, file.generation, file.segment))
            {
                return Err(corrupt("invalid checkpoint file reference"));
            }
            files.push(file);
        }
        for id in catalog.keys() {
            if !seen.contains(&(*id, 3, generation, 0)) {
                return Err(corrupt("incomplete checkpoint manifest"));
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
    /// Locates the primary index, which always belongs to the newest generation.
    pub(crate) fn file(&self, table: TableId, kind: u8) -> FileId {
        FileId::new(table, self.generation, kind, 0)
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
    /// A primary index describes one manifest's whole key space, so the next
    /// checkpoint always writes its own and never inherits one.
    pub(crate) fn kept_files(&self, absorb: &BTreeSet<u64>) -> Vec<FileInfo> {
        self.files
            .iter()
            .filter(|file| file.kind != 3 && !absorb.contains(&file.generation))
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
    /// An entity is written as a unit, so its current state, base, snapshots and
    /// events all live in the generation its locators name.
    fn locate(&self, pager: &Pager, table: TableId, id: u64) -> Result<Option<(u8, index::Value)>> {
        if !self.catalog.contains_key(&table) {
            return Ok(None);
        }
        let Some(value) = index::lookup(pager, self.file(table, 3), self.lsn, [id, 0])? else {
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
        slot: u8,
        location: index::Value,
    ) -> Result<(u64, Event, u64)> {
        let (id, _) = self.slotted(table, 6, slot, location[0])?;
        let file = pager.file(id)?;
        let record = frame::read_at(&file.file, location[1], file.len)?
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
        let definition = self
            .catalog
            .get(&table)
            .ok_or_else(|| Error::NotFound(format!("entity {id}")))?;
        let (slot, value) = self
            .locate(pager, table, id)?
            .ok_or_else(|| Error::NotFound(format!("entity {id}")))?;
        let current = self.state(pager, table, 0, value[0], id)?;
        let mut state = self.state(pager, table, 1, value[1], id)?;
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
        if snapshots {
            if version == current.version {
                return Ok(current);
            }
            let (file, lsn) = self.slotted(table, 5, slot, 0)?;
            if let Some((key, location)) = index::floor(pager, file, lsn, [id, version])?
                && key[0] == id
                && key[1] >= state.version
            {
                state = self.state(pager, table, 2, location[0], id)?;
                if state.version != key[1] {
                    return Err(corrupt("invalid snapshot identity"));
                }
            }
        }
        if state.version == version {
            return Ok(state);
        }
        let (file, lsn) = self.slotted(table, 4, slot, 0)?;
        for entry in index::scan(pager, file, lsn, [id, state.version + 1], [id, version])? {
            let (key, location) = entry?;
            let (entity_id, event, lsn) = self.event_at(pager, table, slot, location)?;
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

    pub fn entity(&self, pager: &Pager, table: TableId, id: u64) -> Result<Option<EntityData>> {
        let Some((slot, value)) = self.locate(pager, table, id)? else {
            return Ok(None);
        };
        let current = self.state(pager, table, 0, value[0], id)?;
        let base = self.state(pager, table, 1, value[1], id)?;
        if base.version > current.version {
            return Err(corrupt("invalid current/base reference"));
        }
        let mut events = Vec::new();
        let (file, lsn) = self.slotted(table, 4, slot, 0)?;
        for item in index::scan(pager, file, lsn, [id, 0], [id, u64::MAX])? {
            let (key, location) = item?;
            let (entity_id, event, lsn) = self.event_at(pager, table, slot, location)?;
            if entity_id != id
                || event.version != key[1]
                || event.transaction != lsn
                || event.transaction > self.lsn
            {
                return Err(corrupt("history index references another event"));
            }
            events.push(event);
        }
        let mut snapshots = BTreeMap::new();
        let (file, lsn) = self.slotted(table, 5, slot, 0)?;
        for item in index::scan(pager, file, lsn, [id, 0], [id, u64::MAX])? {
            let (key, location) = item?;
            let state = self.state(pager, table, 2, location[0], id)?;
            if state.version != key[1]
                || state.version < base.version
                || state.version > current.version
            {
                return Err(corrupt("invalid snapshot reference"));
            }
            snapshots.insert(key[1], state);
        }
        let data = EntityData {
            current,
            base,
            events,
            snapshots,
        };
        if data.at(&self.catalog[&table], data.current.version, false)? != data.current {
            return Err(corrupt("current state does not match its history"));
        }
        Ok(Some(data))
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

pub(crate) struct TableWriter {
    current: HeapWriter,
    bases: HeapWriter,
    snapshots: HeapWriter,
    primary: IndexWriter,
    history_index: IndexWriter,
    snapshot_index: IndexWriter,
    history: File,
    segment: u64,
    segment_limit: u64,
    compress: bool,
    directory: PathBuf,
    table: u64,
    generation: u64,
    slot: u8,
    entities: u64,
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
    ) -> Result<Self> {
        let dir = table_path(directory, table, generation);
        fs::create_dir_all(dir.join("history"))?;
        let path = |kind| FileId::new(table, generation, kind, 0).path(directory);
        Ok(Self {
            current: HeapWriter::create(&path(0), lsn)?,
            bases: HeapWriter::create(&path(1), lsn)?,
            snapshots: HeapWriter::create(&path(2), lsn)?,
            primary: IndexWriter::create(&path(3), lsn)?,
            history_index: IndexWriter::create(&path(4), lsn)?,
            snapshot_index: IndexWriter::create(&path(5), lsn)?,
            history: File::create_new(path(6))?,
            segment: 0,
            segment_limit: limit,
            compress,
            directory: directory.to_owned(),
            table,
            generation,
            slot,
            entities: 0,
        })
    }
    /// Records an entity that already lives in a kept generation.
    ///
    /// Only the primary-index entry is rewritten, with its locators remapped to
    /// this manifest's generation slots. No record is copied.
    pub fn carry(&mut self, id: u64, value: index::Value) -> Result<()> {
        self.primary.append([id, 0], value)
    }
    /// Writes one whole entity into this generation.
    pub fn append(&mut self, id: u64, data: &EntityData) -> Result<()> {
        let slot = self.slot;
        let current = heap::locate(slot, self.current.append(&encode_entity(&data.current))?);
        let base = heap::locate(slot, self.bases.append(&encode_entity(&data.base))?);
        self.primary.append([id, 0], [current, base])?;
        self.entities += 1;
        for (&version, state) in &data.snapshots {
            let location = heap::locate(slot, self.snapshots.append(&encode_entity(state))?);
            self.snapshot_index.append([id, version], [location, 0])?;
        }
        for event in &data.events {
            let payload = pack(id, &encode_event(event), self.compress);
            let offset = self.history.stream_position()?;
            if offset != 0 && offset + payload.len() as u64 + 40 > self.segment_limit {
                self.history.sync_all()?;
                self.segment += 1;
                self.history = File::create_new(
                    FileId::new(self.table, self.generation, 6, self.segment).path(&self.directory),
                )?;
            }
            let offset = frame::append(&mut self.history, 3, event.transaction, &payload)?;
            self.history_index
                .append([id, event.version], [self.segment, offset])?;
        }
        Ok(())
    }
    pub fn finish(self) -> Result<(Vec<FileInfo>, u64)> {
        let entities = self.entities;
        self.current.finish()?;
        self.bases.finish()?;
        self.snapshots.finish()?;
        self.primary.finish()?;
        self.history_index.finish()?;
        self.snapshot_index.finish()?;
        self.history.sync_all()?;
        let mut files = Vec::new();
        let kinds = (0..6)
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
        sync_directory(&dir)?;
        sync_directory(dir.parent().unwrap())?;
        sync_directory(&self.directory.join("tables"))?;
        Ok((files, entities))
    }
}
