// SPDX-License-Identifier: AGPL-3.0-only

//! Disjoint immutable index partitions. A point lookup visits exactly one tree.

use crate::{
    Error, Result,
    error::corrupt,
    snapshot::{FileInfo, Root, digest, entity_slot, remap},
    storage::{
        index::{self, IndexWriter, Key, Value},
        pager::{FileId, Pager},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Seek,
    path::{Path, PathBuf},
};

pub(crate) const WIDTH: u64 = 1024;
pub(crate) type Route = (u64, u8, Key);
type Scan<'a> = Box<dyn Iterator<Item = Result<(Key, Value)>> + 'a>;
pub(crate) fn bucket(kind: u8, key: Key) -> Key {
    if kind == 3 {
        [key[0] / WIDTH * WIDTH, 0]
    } else {
        [key[0], key[1] / WIDTH * WIDTH]
    }
}
pub(crate) fn end(kind: u8, lower: Key) -> Key {
    if kind == 3 {
        [lower[0].saturating_add(WIDTH - 1), u64::MAX]
    } else {
        [lower[0], lower[1].saturating_add(WIDTH - 1)]
    }
}
#[derive(Clone)]
pub(crate) struct Partition {
    pub file: FileId,
    pub lsn: u64,
    pub base: u64,
    pub first: Key,
    pub last: Key,
    /// Original encoded primary slot -> this manifest's slot.
    pub slots: BTreeMap<u8, u8>,
    pub dependencies: BTreeSet<FileId>,
}
impl Partition {
    fn translate(&self, value: Value) -> Result<Value> {
        if self.file.kind != 3 {
            return Ok(value);
        }
        let slot = entity_slot(value)?;
        Ok(remap(
            value,
            *self
                .slots
                .get(&slot)
                .ok_or_else(|| corrupt("unmapped primary partition slot"))?,
        ))
    }
}
impl Root {
    pub fn lookup_index(
        &self,
        pager: &Pager,
        table: u64,
        kind: u8,
        key: Key,
    ) -> Result<Option<Value>> {
        let Some(part) = self.partitions.get(&(table, kind, bucket(kind, key))) else {
            return Ok(None);
        };
        index::lookup_at(pager, part.file, part.lsn, part.base, key)?
            .map(|value| part.translate(value))
            .transpose()
    }
    pub fn floor_index(
        &self,
        pager: &Pager,
        table: u64,
        kind: u8,
        key: Key,
    ) -> Result<Option<(Key, Value)>> {
        let mut candidates = self
            .partitions
            .range((table, kind, [0, 0])..=(table, kind, bucket(kind, key)))
            .rev();
        for (_, part) in &mut candidates {
            if let Some((key, value)) = index::floor_at(pager, part.file, part.lsn, part.base, key)?
            {
                return Ok(Some((key, part.translate(value)?)));
            }
        }
        Ok(None)
    }
    pub fn scan_index<'a>(
        &'a self,
        pager: &'a Pager,
        table: u64,
        kind: u8,
        lower: Key,
        upper: Key,
    ) -> Result<Scan<'a>> {
        if lower > upper {
            return Ok(Box::new(std::iter::empty()));
        }
        let parts = self
            .partitions
            .range((table, kind, bucket(kind, lower))..=(table, kind, bucket(kind, upper)));
        Ok(Box::new(parts.flat_map(move |(_, part)| {
            let iter: Box<dyn Iterator<Item = Result<(Key, Value)>> + 'a> =
                match index::scan_at(pager, part.file, part.lsn, part.base, lower, upper) {
                    Ok(cursor) => Box::new(cursor.map(|item| {
                        item.and_then(|(key, value)| Ok((key, part.translate(value)?)))
                    })),
                    Err(error) => Box::new(std::iter::once(Err(error))),
                };
            iter
        })))
    }
}

struct Pending {
    writer: IndexWriter,
    route: Route,
    part: Partition,
}
pub(crate) struct Indexes {
    limit: usize,
    directory: PathBuf,
    table: u64,
    generation: u64,
    lsn: u64,
    handles: [Option<File>; 3],
    next: [u64; 3],
    pending: [Option<Pending>; 3],
    pub parts: BTreeMap<Route, Partition>,
    pub files: BTreeMap<FileId, FileInfo>,
}
impl Indexes {
    pub fn new(directory: &Path, table: u64, generation: u64, lsn: u64, limit: usize) -> Self {
        Self {
            limit,
            directory: directory.to_owned(),
            table,
            generation,
            lsn,
            handles: [None, None, None],
            next: [0; 3],
            pending: [None, None, None],
            parts: BTreeMap::new(),
            files: BTreeMap::new(),
        }
    }
    fn admit(&self) -> Result<()> {
        if self.parts.len() + self.pending.iter().filter(|part| part.is_some()).count()
            >= self.limit
        {
            return Err(Error::LimitExceeded {
                resource: "index partitions",
                limit: self.limit,
            });
        }
        Ok(())
    }
    fn flush(&mut self, at: usize) -> Result<()> {
        let Some(pending) = self.pending[at].take() else {
            return Ok(());
        };
        let mut file = pending.writer.finish_part()?;
        self.next[at] = file.stream_position()? / crate::storage::PAGE_SIZE as u64;
        self.handles[at] = Some(file);
        if self.parts.insert(pending.route, pending.part).is_some() {
            return Err(corrupt("duplicate built partition"));
        }
        Ok(())
    }
    pub fn append(
        &mut self,
        kind: u8,
        key: Key,
        value: Value,
        dependency: Option<FileId>,
    ) -> Result<()> {
        let at = usize::from(kind - 3);
        let route = (self.table, kind, bucket(kind, key));
        if self.pending[at].as_ref().is_none_or(|p| p.route != route) {
            self.flush(at)?;
            self.admit()?;
            if self.parts.contains_key(&route) {
                return Err(corrupt("append to sealed index partition"));
            }
            let file = FileId::new(self.table, self.generation, kind, 0);
            let handle = match self.handles[at].take() {
                Some(file) => file,
                None => File::create_new(file.path(&self.directory))?,
            };
            let writer = IndexWriter::from_file(handle, self.lsn, self.next[at])?;
            self.pending[at] = Some(Pending {
                writer,
                route,
                part: Partition {
                    file,
                    lsn: self.lsn,
                    base: self.next[at],
                    first: key,
                    last: key,
                    slots: BTreeMap::new(),
                    dependencies: BTreeSet::new(),
                },
            });
        }
        let pending = self.pending[at].as_mut().unwrap();
        pending.writer.append(key, value)?;
        pending.part.last = key;
        if kind == 3 {
            let slot = entity_slot(value)?;
            pending.part.slots.insert(slot, slot);
        }
        if let Some(file) = dependency {
            pending.part.dependencies.insert(file);
        }
        Ok(())
    }
    pub fn reuse(&mut self, route: Route, part: Partition, root: &Root) -> Result<()> {
        let at = usize::from(route.1 - 3);
        self.flush(at)?;
        self.admit()?;
        let info = root
            .files
            .iter()
            .find(|info| info.id() == part.file)
            .ok_or_else(|| corrupt("unlisted partition"))?;
        self.files.insert(info.id(), info.clone());
        if self.parts.insert(route, part).is_some() {
            return Err(corrupt("duplicate reused partition"));
        }
        Ok(())
    }
    pub fn finish(mut self) -> Result<(Vec<FileInfo>, BTreeMap<Route, Partition>)> {
        for at in 0..3 {
            self.flush(at)?;
        }
        for (at, handle) in self.handles.into_iter().enumerate() {
            if let Some(handle) = handle {
                handle.sync_all()?;
                let file = FileId::new(self.table, self.generation, at as u8 + 3, 0);
                let (size, crc) = digest(&file.path(&self.directory))?;
                self.files.insert(
                    file,
                    FileInfo {
                        generation: self.generation,
                        table: self.table,
                        kind: file.kind,
                        segment: 0,
                        size,
                        crc,
                    },
                );
            }
        }
        Ok((self.files.into_values().collect(), self.parts))
    }
}
