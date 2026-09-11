// SPDX-License-Identifier: AGPL-3.0-only

//! Shared reader for the immutable files of a published checkpoint.
//!
//! Checkpoint pages never change after publication, so a cached page needs no
//! invalidation while its generation is referenced. Decoding and checksum
//! verification therefore happen once per cached page instead of once per read.
//! Reclaiming a generation drops its pages through [`Pager::forget`].

use super::PAGE_SIZE;
use crate::{Result, error::corrupt};
use std::{
    any::Any,
    collections::{HashMap, VecDeque},
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

/// The default page cache budget in bytes.
pub(crate) const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;
/// The default number of checkpoint files kept open.
pub(crate) const DEFAULT_OPEN_FILES: usize = 256;

/// The physical identity of one checkpoint file.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub(crate) struct FileId {
    pub table: u64,
    pub generation: u64,
    pub kind: u8,
    pub segment: u64,
}
impl FileId {
    pub fn new(table: u64, generation: u64, kind: u8, segment: u64) -> Self {
        Self {
            table,
            generation,
            kind,
            segment,
        }
    }
    /// Resolves this file inside a database directory.
    pub fn path(&self, root: &Path) -> PathBuf {
        let dir = table_path(root, self.table, self.generation);
        match self.kind {
            0 => dir.join("current.pages"),
            1 => dir.join("bases.pages"),
            2 => dir.join("snapshots.pages"),
            3 => dir.join("primary.index"),
            4 => dir.join("history.index"),
            5 => dir.join("snapshots.index"),
            6 => dir
                .join("history")
                .join(format!("{:020}.events", self.segment)),
            _ => unreachable!("validated file kind"),
        }
    }
}

/// Resolves the directory holding one table generation.
pub(crate) fn table_path(root: &Path, table: u64, generation: u64) -> PathBuf {
    root.join("tables")
        .join(format!("{table:020}"))
        .join(format!("{generation:020}"))
}

/// An open checkpoint file and the length it had when it was opened.
///
/// Published files never change length, so the size is read once.
pub(crate) struct CachedFile {
    pub file: File,
    pub len: u64,
}

/// Reads `buffer.len()` bytes at an absolute offset without moving a shared cursor.
///
/// Positional reads let several readers share one descriptor.
pub(crate) fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::read_exact_at(file, buffer, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < buffer.len() {
            match file.seek_read(&mut buffer[done..], offset + done as u64) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer",
                    ));
                }
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut handle = file;
        handle.seek(SeekFrom::Start(offset))?;
        handle.read_exact(buffer)
    }
}

/// Page cache and open-file statistics since the database was opened.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PagerStats {
    pub hits: u64,
    pub misses: u64,
    pub cached_pages: usize,
    pub open_files: usize,
}

/// A bounded cache of decoded pages and open descriptors over immutable files.
pub(crate) struct Pager {
    directory: PathBuf,
    files: Mutex<FileCache>,
    pages: Mutex<PageCache>,
}
impl Pager {
    pub fn new(directory: PathBuf, cache_bytes: usize, max_open_files: usize) -> Self {
        Self {
            directory,
            files: Mutex::new(FileCache {
                open: HashMap::new(),
                order: VecDeque::new(),
                limit: max_open_files.max(4),
            }),
            pages: Mutex::new(PageCache {
                index: HashMap::new(),
                slots: Vec::new(),
                hand: 0,
                limit: (cache_bytes / PAGE_SIZE).max(8),
                hits: 0,
                misses: 0,
            }),
        }
    }
    pub fn path(&self, id: FileId) -> PathBuf {
        id.path(&self.directory)
    }

    /// Returns a shared descriptor, opening and caching it on the first use.
    pub fn file(&self, id: FileId) -> Result<Arc<CachedFile>> {
        let mut cache = self.files.lock().expect("page cache mutex");
        if let Some(file) = cache.open.get(&id) {
            return Ok(file.clone());
        }
        let file = File::open(self.path(id))?;
        let len = file.metadata()?.len();
        let handle = Arc::new(CachedFile { file, len });
        cache.insert(id, handle.clone());
        Ok(handle)
    }

    /// Returns a decoded page, reading and validating it only on a cache miss.
    ///
    /// The decoder must fully validate the page; callers still check the
    /// identity fields that depend on the caller's expectations, such as the
    /// checkpoint sequence number.
    pub fn page<T, F>(&self, id: FileId, number: u64, decode: F) -> Result<Arc<T>>
    where
        T: Any + Send + Sync,
        F: FnOnce(&[u8; PAGE_SIZE]) -> Result<T>,
    {
        let key = (id, number);
        if let Some(cached) = self.pages.lock().expect("page cache mutex").get(key) {
            return cached
                .downcast::<T>()
                .map_err(|_| corrupt("page cache type mismatch"));
        }
        let file = self.file(id)?;
        let offset = number
            .checked_mul(PAGE_SIZE as u64)
            .ok_or_else(|| corrupt("page offset overflow"))?;
        let mut bytes = [0; PAGE_SIZE];
        read_exact_at(&file.file, &mut bytes, offset)?;
        let value = Arc::new(decode(&bytes)?);
        self.pages
            .lock()
            .expect("page cache mutex")
            .insert(key, value.clone());
        Ok(value)
    }

    /// Drops every cached page and descriptor of the matching files.
    ///
    /// Call this after reclaiming a generation so its identifiers can never
    /// resolve to stale bytes if they are somehow reused.
    pub fn forget(&self, matches: impl Fn(FileId) -> bool) {
        self.pages
            .lock()
            .expect("page cache mutex")
            .retain(|id| !matches(id));
        let mut files = self.files.lock().expect("page cache mutex");
        let FileCache { open, order, .. } = &mut *files;
        open.retain(|&id, _| !matches(id));
        order.retain(|id| open.contains_key(id));
    }

    #[cfg(test)]
    pub fn stats(&self) -> PagerStats {
        let pages = self.pages.lock().expect("page cache mutex");
        let files = self.files.lock().expect("page cache mutex");
        PagerStats {
            hits: pages.hits,
            misses: pages.misses,
            cached_pages: pages.index.len(),
            open_files: files.open.len(),
        }
    }
}

struct FileCache {
    open: HashMap<FileId, Arc<CachedFile>>,
    order: VecDeque<FileId>,
    limit: usize,
}
impl FileCache {
    fn insert(&mut self, id: FileId, file: Arc<CachedFile>) {
        while self.order.len() >= self.limit {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            self.open.remove(&evicted);
        }
        self.open.insert(id, file);
        self.order.push_back(id);
    }
}

struct Slot {
    key: (FileId, u64),
    value: Arc<dyn Any + Send + Sync>,
    referenced: bool,
}

/// A clock replacement cache: eviction sweeps slots and clears reference bits.
struct PageCache {
    index: HashMap<(FileId, u64), usize>,
    slots: Vec<Option<Slot>>,
    hand: usize,
    limit: usize,
    hits: u64,
    misses: u64,
}
impl PageCache {
    fn get(&mut self, key: (FileId, u64)) -> Option<Arc<dyn Any + Send + Sync>> {
        let Some(&position) = self.index.get(&key) else {
            self.misses += 1;
            return None;
        };
        let slot = self.slots[position].as_mut().expect("indexed slot");
        slot.referenced = true;
        self.hits += 1;
        Some(slot.value.clone())
    }
    fn insert(&mut self, key: (FileId, u64), value: Arc<dyn Any + Send + Sync>) {
        if self.index.contains_key(&key) {
            return;
        }
        let slot = Slot {
            key,
            value,
            referenced: true,
        };
        if self.slots.len() < self.limit {
            self.index.insert(key, self.slots.len());
            self.slots.push(Some(slot));
            return;
        }
        let position = self.evict();
        if let Some(previous) = self.slots[position].take() {
            self.index.remove(&previous.key);
        }
        self.index.insert(key, position);
        self.slots[position] = Some(slot);
    }
    /// Returns a slot to reuse, clearing reference bits along the way.
    fn evict(&mut self) -> usize {
        for _ in 0..self.slots.len() * 2 {
            let position = self.hand;
            self.hand = (self.hand + 1) % self.slots.len();
            match &mut self.slots[position] {
                None => return position,
                Some(slot) if !slot.referenced => return position,
                Some(slot) => slot.referenced = false,
            }
        }
        self.hand
    }
    fn retain(&mut self, keep: impl Fn(FileId) -> bool) {
        for slot in &mut self.slots {
            if slot.as_ref().is_some_and(|s| !keep(s.key.0))
                && let Some(removed) = slot.take()
            {
                self.index.remove(&removed.key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;
    use std::io::Write;

    fn page_bytes(fill: u8) -> [u8; PAGE_SIZE] {
        [fill; PAGE_SIZE]
    }

    #[test]
    fn cached_pages_are_decoded_once_and_evicted_under_a_budget() {
        let dir = TempDir::new();
        let id = FileId::new(1, 1, 3, 0);
        let path = id.path(&dir.0);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = File::create_new(&path).unwrap();
        for n in 0..16u8 {
            file.write_all(&page_bytes(n)).unwrap();
        }
        file.sync_all().unwrap();

        // A budget of eight slots, so sixteen distinct pages must evict.
        let pager = Pager::new(dir.0.clone(), 8 * PAGE_SIZE, 4);
        let decode = |bytes: &[u8; PAGE_SIZE]| Ok(bytes[0]);
        assert_eq!(*pager.page(id, 0, decode).unwrap(), 0);
        assert_eq!(*pager.page(id, 0, decode).unwrap(), 0);
        assert_eq!(pager.stats().hits, 1);
        assert_eq!(pager.stats().misses, 1);

        for n in 0..16 {
            assert_eq!(*pager.page(id, n, decode).unwrap(), n as u8);
        }
        assert!(pager.stats().cached_pages <= 8);

        // A decoder failure is reported and nothing is cached for that page.
        let cached = pager.stats().cached_pages;
        assert!(
            pager
                .page(id, 3, |_| Err::<u8, _>(corrupt("rejected")))
                .is_err()
        );
        assert!(pager.stats().cached_pages <= cached);

        // Reading past the end of the file is an I/O error, not a silent zero page.
        assert!(pager.page(id, 16, decode).is_err());

        pager.forget(|other| other.generation == 1);
        assert_eq!(pager.stats().cached_pages, 0);
        assert_eq!(pager.stats().open_files, 0);
    }

    #[test]
    fn a_mismatched_cached_type_is_rejected_rather_than_reinterpreted() {
        let dir = TempDir::new();
        let id = FileId::new(2, 1, 0, 0);
        let path = id.path(&dir.0);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        File::create_new(&path)
            .unwrap()
            .write_all(&page_bytes(7))
            .unwrap();
        let pager = Pager::new(dir.0.clone(), DEFAULT_CACHE_BYTES, DEFAULT_OPEN_FILES);
        assert_eq!(*pager.page(id, 0, |b| Ok(b[0])).unwrap(), 7);
        assert!(pager.page(id, 0, |b| Ok(u64::from(b[0]))).is_err());
    }
}
