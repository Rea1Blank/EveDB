// SPDX-License-Identifier: AGPL-3.0-only

use crate::{model::EntityData, ordered_map::OrderedMap, snapshot::Root, storage::pager::FileId};
use std::{ops::RangeBounds, sync::Arc};

type Key = (u64, u64);

/// Persistent overlay with independently counted history-file owners.
#[derive(Clone)]
pub(crate) struct Overlay {
    entries: OrderedMap<Key, Arc<EntityData>>,
    roots: OrderedMap<u64, (Arc<Root>, usize, usize)>,
    pub bytes: usize,
}
impl Overlay {
    pub fn new() -> Self {
        Self {
            entries: OrderedMap::new(),
            roots: OrderedMap::new(),
            bytes: 0,
        }
    }
    pub fn get(&self, key: &Key) -> Option<&Arc<EntityData>> {
        self.entries.get(key)
    }
    pub fn contains_key(&self, key: &Key) -> bool {
        self.entries.contains_key(key)
    }
    pub fn range(
        &self,
        bounds: impl RangeBounds<Key>,
    ) -> crate::ordered_map::Range<'_, Key, Arc<EntityData>> {
        self.entries.range(bounds)
    }
    pub fn insert(&mut self, key: Key, value: Arc<EntityData>) {
        let previous = self
            .entries
            .get(&key)
            .and_then(|data| data.disk.as_ref())
            .map(|disk| disk.root.generation);
        let next = value.disk.as_ref().map(|disk| disk.root.generation);
        if previous == next {
            self.entries.insert(key, value);
            return;
        }
        if let Some(disk) = self.entries.get(&key).and_then(|old| old.disk.as_ref()) {
            let generation = disk.root.generation;
            let (root, count, bytes) = self.roots.get(&generation).unwrap().clone();
            if count == 1 {
                self.roots.remove(&generation);
                self.bytes -= bytes;
            } else {
                self.roots.insert(generation, (root, count - 1, bytes));
            }
        }
        if let Some(disk) = &value.disk {
            let generation = disk.root.generation;
            let (count, bytes) = self.roots.get(&generation).map_or_else(
                || {
                    (
                        0,
                        disk.root
                            .file_sizes()
                            .map(|(_, size)| usize::try_from(size).unwrap_or(usize::MAX))
                            .fold(0usize, usize::saturating_add),
                    )
                },
                |(_, count, bytes)| (*count, *bytes),
            );
            if count == 0 {
                self.bytes = self.bytes.saturating_add(bytes);
            }
            self.roots
                .insert(generation, (disk.root.clone(), count + 1, bytes));
        }
        self.entries.insert(key, value);
    }
    pub fn file_sizes(&self) -> impl Iterator<Item = (FileId, u64)> + '_ {
        self.roots
            .range(..)
            .flat_map(|(_, (root, _, _))| root.file_sizes())
    }
}
impl Extend<(Key, Arc<EntityData>)> for Overlay {
    fn extend<T: IntoIterator<Item = (Key, Arc<EntityData>)>>(&mut self, values: T) {
        for (key, value) in values {
            self.insert(key, value);
        }
    }
}
