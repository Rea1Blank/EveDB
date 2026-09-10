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
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

#[derive(Clone)]
pub(crate) struct FileInfo {
    table: u64,
    kind: u8,
    segment: u64,
    size: u64,
    crc: u32,
}
#[derive(Clone)]
pub(crate) struct Root {
    pub generation: u64,
    pub lsn: u64,
    pub catalog: BTreeMap<TableId, Table>,
    pub files: Vec<FileInfo>,
}
impl Root {
    pub fn empty() -> Self {
        Self {
            generation: 0,
            lsn: 0,
            catalog: BTreeMap::new(),
            files: Vec::new(),
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
        e.u64(self.files.len() as u64);
        for file in &self.files {
            e.u64(file.table);
            e.u8(file.kind);
            e.u64(file.segment);
            e.u64(file.size);
            e.u32(file.crc);
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
        let n = d.count(29)?;
        let mut files = Vec::new();
        let mut seen = BTreeSet::new();
        for _ in 0..n {
            let file = FileInfo {
                table: d.u64()?,
                kind: d.u8()?,
                segment: d.u64()?,
                size: d.u64()?,
                crc: d.u32()?,
            };
            if !catalog.contains_key(&file.table)
                || file.kind > 6
                || (file.kind != 6 && file.segment != 0)
                || !seen.insert((file.table, file.kind, file.segment))
            {
                return Err(corrupt("invalid checkpoint file reference"));
            }
            files.push(file);
        }
        for id in catalog.keys() {
            for kind in 0..6 {
                if !seen.contains(&(*id, kind, 0)) {
                    return Err(corrupt("incomplete checkpoint manifest"));
                }
            }
        }
        d.finish()?;
        Ok(Self {
            generation,
            lsn,
            catalog,
            files,
        })
    }
    pub fn verify(&self, directory: &Path) -> Result<()> {
        for file in &self.files {
            let (size, crc) = digest(&file_path(
                directory,
                file.table,
                self.generation,
                file.kind,
                file.segment,
            ))?;
            if size != file.size || crc != file.crc {
                return Err(corrupt("checkpoint file checksum mismatch"));
            }
        }
        Ok(())
    }
    pub fn current(&self, directory: &Path, table: TableId, id: u64) -> Result<Option<Entity>> {
        if !self.catalog.contains_key(&table) {
            return Ok(None);
        }
        let Some(value) = index::lookup(
            &file_path(directory, table, self.generation, 3, 0),
            self.lsn,
            [id, 0],
        )?
        else {
            return Ok(None);
        };
        let entity = decode_entity(&heap::read(
            &file_path(directory, table, self.generation, 0, 0),
            value[0],
            self.lsn,
        )?)?;
        if entity.id != id {
            return Err(corrupt("primary index references another entity"));
        }
        Ok(Some(entity))
    }
    pub fn at(
        &self,
        directory: &Path,
        table: TableId,
        id: u64,
        version: u64,
        snapshots: bool,
    ) -> Result<Entity> {
        let definition = self
            .catalog
            .get(&table)
            .ok_or_else(|| Error::NotFound(format!("entity {id}")))?;
        let value = index::lookup(
            &file_path(directory, table, self.generation, 3, 0),
            self.lsn,
            [id, 0],
        )?
        .ok_or_else(|| Error::NotFound(format!("entity {id}")))?;
        let current = decode_entity(&heap::read(
            &file_path(directory, table, self.generation, 0, 0),
            value[0],
            self.lsn,
        )?)?;
        let mut state = decode_entity(&heap::read(
            &file_path(directory, table, self.generation, 1, 0),
            value[1],
            self.lsn,
        )?)?;
        if current.id != id || state.id != id || state.version > current.version {
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
            if let Some((key, location)) = index::floor(
                &file_path(directory, table, self.generation, 5, 0),
                self.lsn,
                [id, version],
            )? && key[0] == id
                && key[1] >= state.version
            {
                state = decode_entity(&heap::read(
                    &file_path(directory, table, self.generation, 2, 0),
                    location[0],
                    self.lsn,
                )?)?;
                if state.id != id || state.version != key[1] {
                    return Err(corrupt("invalid snapshot identity"));
                }
            }
        }
        if state.version == version {
            return Ok(state);
        }
        let mut segment: Option<(u64, File)> = None;
        for entry in index::scan(
            &file_path(directory, table, self.generation, 4, 0),
            self.lsn,
            [id, state.version + 1],
            [id, version],
        )? {
            let (key, location) = entry?;
            if segment.as_ref().map(|(n, _)| *n) != Some(location[0]) {
                segment = Some((
                    location[0],
                    File::open(file_path(directory, table, self.generation, 6, location[0]))?,
                ));
            }
            let file = &mut segment.as_mut().unwrap().1;
            file.seek(SeekFrom::Start(location[1]))?;
            let record = frame::read(file)?.ok_or_else(|| corrupt("truncated indexed event"))?;
            let (entity_id, payload) = unpack(&record.payload)?;
            let event = decode_event(&payload)?;
            if record.kind != 3
                || entity_id != id
                || event.version != key[1]
                || event.transaction != record.lsn
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

    pub fn entity(&self, directory: &Path, table: TableId, id: u64) -> Result<Option<EntityData>> {
        if !self.catalog.contains_key(&table) {
            return Ok(None);
        }
        let Some(value) = index::lookup(
            &file_path(directory, table, self.generation, 3, 0),
            self.lsn,
            [id, 0],
        )?
        else {
            return Ok(None);
        };
        let current = decode_entity(&heap::read(
            &file_path(directory, table, self.generation, 0, 0),
            value[0],
            self.lsn,
        )?)?;
        let base = decode_entity(&heap::read(
            &file_path(directory, table, self.generation, 1, 0),
            value[1],
            self.lsn,
        )?)?;
        if current.id != id || base.id != id || base.version > current.version {
            return Err(corrupt("invalid current/base reference"));
        }
        let mut events = Vec::new();
        for item in index::scan(
            &file_path(directory, table, self.generation, 4, 0),
            self.lsn,
            [id, 0],
            [id, u64::MAX],
        )? {
            let (key, location) = item?;
            let mut file =
                File::open(file_path(directory, table, self.generation, 6, location[0]))?;
            file.seek(SeekFrom::Start(location[1]))?;
            let record =
                frame::read(&mut file)?.ok_or_else(|| corrupt("truncated history frame"))?;
            if record.kind != 3 {
                return Err(corrupt("invalid history frame kind"));
            }
            let (entity_id, payload) = unpack(&record.payload)?;
            let event = decode_event(&payload)?;
            if entity_id != id
                || event.version != key[1]
                || event.transaction != record.lsn
                || event.transaction > self.lsn
            {
                return Err(corrupt("history index references another event"));
            }
            events.push(event);
        }
        let mut snapshots = BTreeMap::new();
        for item in index::scan(
            &file_path(directory, table, self.generation, 5, 0),
            self.lsn,
            [id, 0],
            [id, u64::MAX],
        )? {
            let (key, location) = item?;
            let state = decode_entity(&heap::read(
                &file_path(directory, table, self.generation, 2, 0),
                location[0],
                self.lsn,
            )?)?;
            if state.id != id
                || state.version != key[1]
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
pub(crate) fn table_path(root: &Path, table: u64, generation: u64) -> PathBuf {
    root.join("tables")
        .join(format!("{table:020}"))
        .join(format!("{generation:020}"))
}
pub(crate) fn file_path(
    root: &Path,
    table: u64,
    generation: u64,
    kind: u8,
    segment: u64,
) -> PathBuf {
    let dir = table_path(root, table, generation);
    match kind {
        0 => dir.join("current.pages"),
        1 => dir.join("bases.pages"),
        2 => dir.join("snapshots.pages"),
        3 => dir.join("primary.index"),
        4 => dir.join("history.index"),
        5 => dir.join("snapshots.index"),
        6 => dir.join("history").join(format!("{segment:020}.events")),
        _ => unreachable!("validated file kind"),
    }
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
}
impl TableWriter {
    pub fn create(
        directory: &Path,
        table: u64,
        generation: u64,
        lsn: u64,
        limit: u64,
        compress: bool,
    ) -> Result<Self> {
        let dir = table_path(directory, table, generation);
        fs::create_dir_all(dir.join("history"))?;
        let path = |kind| file_path(directory, table, generation, kind, 0);
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
        })
    }
    pub fn append(&mut self, id: u64, data: &EntityData) -> Result<()> {
        let current = self.current.append(&encode_entity(&data.current))?;
        let base = self.bases.append(&encode_entity(&data.base))?;
        self.primary.append([id, 0], [current, base])?;
        for (&version, state) in &data.snapshots {
            let location = self.snapshots.append(&encode_entity(state))?;
            self.snapshot_index.append([id, version], [location, 0])?;
        }
        for event in &data.events {
            let payload = pack(id, &encode_event(event), self.compress);
            let offset = self.history.stream_position()?;
            if offset != 0 && offset + payload.len() as u64 + 40 > self.segment_limit {
                self.history.sync_all()?;
                self.segment += 1;
                self.history = File::create_new(file_path(
                    &self.directory,
                    self.table,
                    self.generation,
                    6,
                    self.segment,
                ))?;
            }
            let offset = frame::append(&mut self.history, 3, event.transaction, &payload)?;
            self.history_index
                .append([id, event.version], [self.segment, offset])?;
        }
        Ok(())
    }
    pub fn finish(self) -> Result<Vec<FileInfo>> {
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
            let (size, crc) = digest(&file_path(
                &self.directory,
                self.table,
                self.generation,
                kind,
                segment,
            ))?;
            files.push(FileInfo {
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
        Ok(files)
    }
}
