// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    PAGE_SIZE,
    pager::{FileId, Pager},
};
use crate::{Result, checksum::crc32c, error::corrupt};
use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
};

pub(crate) type Key = [u64; 2];
pub(crate) type Value = [u64; 2];
const HEADER: usize = 40;
const LEAF_WIDTH: usize = 32;
const BRANCH_WIDTH: usize = 24;
const LEAF_CAPACITY: usize = (PAGE_SIZE - HEADER) / LEAF_WIDTH;
const BRANCH_CAPACITY: usize = (PAGE_SIZE - HEADER) / BRANCH_WIDTH;

fn put(page: &mut [u8], at: usize, value: u64) {
    page[at..at + 8].copy_from_slice(&value.to_le_bytes());
}
fn get(page: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(page[at..at + 8].try_into().unwrap())
}
fn page(kind: u16, id: u64, lsn: u64, count: usize, next: u64) -> [u8; PAGE_SIZE] {
    let mut bytes = [0; PAGE_SIZE];
    bytes[..4].copy_from_slice(b"EVIX");
    bytes[4..6].copy_from_slice(&1_u16.to_le_bytes());
    bytes[6..8].copy_from_slice(&kind.to_le_bytes());
    put(&mut bytes, 8, id);
    put(&mut bytes, 16, lsn);
    bytes[24..26].copy_from_slice(&(count as u16).to_le_bytes());
    put(&mut bytes, 32, next);
    bytes
}
fn write(file: &mut File, mut bytes: [u8; PAGE_SIZE]) -> Result<()> {
    let crc = crc32c(&bytes);
    bytes[28..32].copy_from_slice(&crc.to_le_bytes());
    file.write_all(&bytes)?;
    Ok(())
}

/// Bulk-built immutable B+tree. Leaf pages are linked for ordered scans.
pub(crate) struct IndexWriter {
    file: File,
    lsn: u64,
    next: u64,
    count: u64,
    leaf: Vec<(Key, Value)>,
    level: Vec<(Key, u64)>,
    previous: Option<Key>,
}
impl IndexWriter {
    pub fn create(path: &Path, lsn: u64) -> Result<Self> {
        let mut file = File::create_new(path)?;
        file.write_all(&[0; PAGE_SIZE])?;
        Ok(Self {
            file,
            lsn,
            next: 1,
            count: 0,
            leaf: Vec::new(),
            level: Vec::new(),
            previous: None,
        })
    }
    pub fn append(&mut self, key: Key, value: Value) -> Result<()> {
        if self.previous.is_some_and(|previous| previous >= key) {
            return Err(corrupt("index keys are not strictly ordered"));
        }
        if self.leaf.len() == LEAF_CAPACITY {
            self.flush_leaf(self.next + 1)?;
        }
        self.leaf.push((key, value));
        self.previous = Some(key);
        self.count += 1;
        Ok(())
    }
    fn flush_leaf(&mut self, next: u64) -> Result<()> {
        if self.leaf.is_empty() {
            return Ok(());
        }
        let mut bytes = page(1, self.next, self.lsn, self.leaf.len(), next);
        for (i, (key, value)) in self.leaf.iter().enumerate() {
            let at = HEADER + i * LEAF_WIDTH;
            put(&mut bytes, at, key[0]);
            put(&mut bytes, at + 8, key[1]);
            put(&mut bytes, at + 16, value[0]);
            put(&mut bytes, at + 24, value[1]);
        }
        write(&mut self.file, bytes)?;
        self.level.push((self.leaf[0].0, self.next));
        self.next += 1;
        self.leaf.clear();
        Ok(())
    }
    pub fn finish(mut self) -> Result<()> {
        self.flush_leaf(0)?;
        while self.level.len() > 1 {
            let mut parents = Vec::new();
            for children in self.level.chunks(BRANCH_CAPACITY) {
                let mut bytes = page(2, self.next, self.lsn, children.len(), 0);
                for (i, (key, id)) in children.iter().enumerate() {
                    let at = HEADER + i * BRANCH_WIDTH;
                    put(&mut bytes, at, key[0]);
                    put(&mut bytes, at + 8, key[1]);
                    put(&mut bytes, at + 16, *id);
                }
                write(&mut self.file, bytes)?;
                parents.push((children[0].0, self.next));
                self.next += 1;
            }
            self.level = parents;
        }
        let root = self.level.first().map_or(0, |item| item.1);
        let mut meta = page(0, 0, self.lsn, 0, root);
        put(&mut meta, 40, self.count);
        self.file.seek(SeekFrom::Start(0))?;
        write(&mut self.file, meta)?;
        self.file.sync_all()?;
        Ok(())
    }
}

/// One decoded and fully validated B+tree page.
///
/// Structural validation happens when the page enters the cache, so repeated
/// lookups against a hot node cost only a binary search.
pub(crate) struct Node {
    bytes: [u8; PAGE_SIZE],
}
impl Node {
    /// Validates a page against the identity the caller expects to have read.
    ///
    /// The checkpoint sequence number is deliberately not checked here: it
    /// depends on which generation the caller is reading, while a cached page
    /// is shared. Callers check it through [`Node::lsn`].
    fn decode(bytes: &[u8; PAGE_SIZE], expected_id: u64) -> Result<Self> {
        let mut bytes = *bytes;
        let expected = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
        bytes[28..32].fill(0);
        if &bytes[..4] != b"EVIX"
            || bytes[4..6] != 1_u16.to_le_bytes()
            || get(&bytes, 8) != expected_id
            || bytes[26..28] != [0; 2]
            || crc32c(&bytes) != expected
        {
            return Err(corrupt("invalid index page header or checksum"));
        }
        let node = Self { bytes };
        let count = node.count();
        let width = match node.kind() {
            0 if expected_id == 0 && count == 0 => return Ok(node),
            1 => LEAF_WIDTH,
            2 => BRANCH_WIDTH,
            _ => return Err(corrupt("invalid index node kind")),
        };
        if count == 0 || count > (PAGE_SIZE - HEADER) / width {
            return Err(corrupt("invalid index node count"));
        }
        let mut previous = None;
        for i in 0..count {
            let key = node.key(i);
            if previous.is_some_and(|p| p >= key) {
                return Err(corrupt("unordered index node"));
            }
            // Children are written before their parent, so a descent that only
            // ever moves to a smaller page identifier cannot cycle.
            if node.kind() == 2 && (node.child(i) == 0 || node.child(i) >= expected_id) {
                return Err(corrupt("invalid index child"));
            }
            previous = Some(key);
        }
        if node.kind() == 1 && node.next() != 0 && node.next() <= expected_id {
            return Err(corrupt("cyclic index leaf chain"));
        }
        Ok(node)
    }
    fn kind(&self) -> u16 {
        u16::from_le_bytes(self.bytes[6..8].try_into().unwrap())
    }
    fn count(&self) -> usize {
        u16::from_le_bytes(self.bytes[24..26].try_into().unwrap()) as usize
    }
    fn lsn(&self) -> u64 {
        get(&self.bytes, 16)
    }
    /// The next leaf in the scan chain, or the root pointer on a metadata page.
    fn next(&self) -> u64 {
        get(&self.bytes, 32)
    }
    fn width(&self) -> usize {
        if self.kind() == 1 {
            LEAF_WIDTH
        } else {
            BRANCH_WIDTH
        }
    }
    fn key(&self, index: usize) -> Key {
        let at = HEADER + index * self.width();
        [get(&self.bytes, at), get(&self.bytes, at + 8)]
    }
    fn value(&self, index: usize) -> Value {
        let at = HEADER + index * LEAF_WIDTH;
        [get(&self.bytes, at + 16), get(&self.bytes, at + 24)]
    }
    fn child(&self, index: usize) -> u64 {
        get(&self.bytes, HEADER + index * BRANCH_WIDTH + 16)
    }
    /// Returns the first entry whose key is greater than or equal to `key`.
    fn lower_bound(&self, key: Key) -> usize {
        let (mut low, mut high) = (0, self.count());
        while low < high {
            let middle = low + (high - low) / 2;
            if self.key(middle) < key {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }
    /// Returns the last entry whose key is less than or equal to `key`.
    fn floor_index(&self, key: Key) -> Option<usize> {
        let index = self.lower_bound(key);
        if index < self.count() && self.key(index) == key {
            return Some(index);
        }
        index.checked_sub(1)
    }
}

fn node(pager: &Pager, file: FileId, id: u64, lsn: u64) -> Result<Arc<Node>> {
    let node = pager.page(file, id, |bytes| Node::decode(bytes, id))?;
    if node.lsn() != lsn {
        return Err(corrupt("invalid index page header or checksum"));
    }
    Ok(node)
}

/// Walks to the leaf that would contain `key`.
///
/// Branch separators are the first key of their subtree, so the chosen child
/// always covers the predecessor of `key` as well as `key` itself.
fn descend(pager: &Pager, file: FileId, lsn: u64, key: Key) -> Result<Option<Arc<Node>>> {
    if !pager.file(file)?.len.is_multiple_of(PAGE_SIZE as u64) {
        return Err(corrupt("truncated index file"));
    }
    let meta = node(pager, file, 0, lsn)?;
    let mut id = meta.next();
    while id != 0 {
        let current = node(pager, file, id, lsn)?;
        match current.kind() {
            1 => return Ok(Some(current)),
            2 => id = current.child(current.floor_index(key).unwrap_or(0)),
            _ => return Err(corrupt("index root points to metadata")),
        }
    }
    Ok(None)
}

pub(crate) fn lookup(pager: &Pager, file: FileId, lsn: u64, key: Key) -> Result<Option<Value>> {
    let Some(leaf) = descend(pager, file, lsn, key)? else {
        return Ok(None);
    };
    let index = leaf.lower_bound(key);
    Ok((index < leaf.count() && leaf.key(index) == key).then(|| leaf.value(index)))
}
pub(crate) fn floor(
    pager: &Pager,
    file: FileId,
    lsn: u64,
    key: Key,
) -> Result<Option<(Key, Value)>> {
    let Some(leaf) = descend(pager, file, lsn, key)? else {
        return Ok(None);
    };
    Ok(leaf
        .floor_index(key)
        .map(|index| (leaf.key(index), leaf.value(index))))
}
pub(crate) fn scan(
    pager: &Pager,
    file: FileId,
    lsn: u64,
    lower: Key,
    upper: Key,
) -> Result<IndexCursor<'_>> {
    let leaf = descend(pager, file, lsn, lower)?;
    let position = leaf.as_ref().map_or(0, |leaf| leaf.lower_bound(lower));
    Ok(IndexCursor {
        pager,
        file,
        lsn,
        upper,
        leaf,
        position,
        failed: false,
    })
}
pub(crate) struct IndexCursor<'a> {
    pager: &'a Pager,
    file: FileId,
    lsn: u64,
    upper: Key,
    leaf: Option<Arc<Node>>,
    position: usize,
    failed: bool,
}
impl Iterator for IndexCursor<'_> {
    type Item = Result<(Key, Value)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            let leaf = self.leaf.clone()?;
            if self.position == leaf.count() {
                let next = leaf.next();
                if next == 0 {
                    self.leaf = None;
                    return None;
                }
                match node(self.pager, self.file, next, self.lsn) {
                    Ok(page) if page.kind() == 1 => {
                        self.leaf = Some(page);
                        self.position = 0;
                        continue;
                    }
                    Ok(_) => {
                        self.failed = true;
                        return Some(Err(corrupt("leaf chain points to a branch")));
                    }
                    Err(e) => {
                        self.failed = true;
                        return Some(Err(e));
                    }
                }
            }
            let key = leaf.key(self.position);
            if key > self.upper {
                self.leaf = None;
                return None;
            }
            let value = leaf.value(self.position);
            self.position += 1;
            return Some(Ok((key, value)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        storage::pager::{DEFAULT_CACHE_BYTES, DEFAULT_OPEN_FILES},
        test_support::TempDir,
    };

    /// Builds an index at the location `file` resolves to inside `dir`.
    fn writer(dir: &TempDir, file: FileId, lsn: u64) -> IndexWriter {
        let path = file.path(&dir.0);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        IndexWriter::create(&path, lsn).unwrap()
    }
    fn pager(dir: &TempDir) -> Pager {
        Pager::new(dir.0.clone(), DEFAULT_CACHE_BYTES, DEFAULT_OPEN_FILES)
    }

    #[test]
    fn three_level_tree_supports_point_floor_and_range_queries() {
        let dir = TempDir::new();
        let file = FileId::new(1, 1, 3, 0);
        let mut index = writer(&dir, file, 77);
        for n in 0..100_000 {
            index.append([n / 1000, n % 1000], [n, n * 2]).unwrap();
        }
        assert!(index.append([99, 999], [0, 0]).is_err());
        index.finish().unwrap();
        let pager = pager(&dir);
        for n in [0, 253, 254, 338 * 254, 90_123, 99_999] {
            assert_eq!(
                lookup(&pager, file, 77, [n / 1000, n % 1000]).unwrap(),
                Some([n, n * 2])
            );
        }
        assert_eq!(lookup(&pager, file, 77, [100, 0]).unwrap(), None);
        assert_eq!(
            floor(&pager, file, 77, [1, 1001]).unwrap(),
            Some(([1, 999], [1999, 3998]))
        );
        let range: Vec<_> = scan(&pager, file, 77, [3, 990], [4, 10])
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(range.len(), 21);
        assert_eq!(range.first().unwrap().0, [3, 990]);
        assert_eq!(range.last().unwrap().0, [4, 10]);
        assert!(lookup(&pager, file, 78, [0, 0]).is_err());
    }

    #[test]
    fn empty_tree_and_nonexistent_floor_are_supported() {
        let dir = TempDir::new();
        let empty = FileId::new(1, 1, 3, 0);
        writer(&dir, empty, 0).finish().unwrap();
        let pager = pager(&dir);
        assert_eq!(lookup(&pager, empty, 0, [0, 0]).unwrap(), None);
        assert_eq!(floor(&pager, empty, 0, [0, 0]).unwrap(), None);
        let single = FileId::new(1, 1, 4, 0);
        let mut index = writer(&dir, single, 0);
        index.append([5, 2], [8, 9]).unwrap();
        index.finish().unwrap();
        assert_eq!(floor(&pager, single, 0, [5, 1]).unwrap(), None);
    }

    /// The predecessor of a key may be the last entry of a preceding leaf.
    ///
    /// Branch separators carry the first key of a subtree, so the descent must
    /// land on the leaf that owns the predecessor rather than the next leaf.
    #[test]
    fn floor_crosses_leaf_boundaries() {
        let dir = TempDir::new();
        let file = FileId::new(2, 1, 5, 0);
        let mut index = writer(&dir, file, 3);
        // Even keys only, across several leaves, so odd probes fall between them.
        for n in 0..5_000u64 {
            index.append([0, n * 2], [n, 0]).unwrap();
        }
        index.finish().unwrap();
        let pager = pager(&dir);
        for n in 0..5_000u64 {
            assert_eq!(
                floor(&pager, file, 3, [0, n * 2 + 1]).unwrap(),
                Some(([0, n * 2], [n, 0])),
                "probe between entries {n} and {}",
                n + 1
            );
        }
        // A probe below every key still has no predecessor.
        assert_eq!(
            floor(&pager, file, 3, [0, 0]).unwrap(),
            Some(([0, 0], [0, 0]))
        );
        let first = FileId::new(2, 1, 4, 0);
        let mut index = writer(&dir, first, 3);
        for n in 1..5_000u64 {
            index.append([0, n * 2], [n, 0]).unwrap();
        }
        index.finish().unwrap();
        assert_eq!(floor(&pager, first, 3, [0, 1]).unwrap(), None);
    }

    /// A cached node must still be rejected when read for another generation.
    #[test]
    fn a_hot_node_is_still_checked_against_the_expected_sequence() {
        let dir = TempDir::new();
        let file = FileId::new(3, 1, 3, 0);
        let mut index = writer(&dir, file, 42);
        index.append([1, 1], [7, 7]).unwrap();
        index.finish().unwrap();
        let pager = pager(&dir);
        assert_eq!(lookup(&pager, file, 42, [1, 1]).unwrap(), Some([7, 7]));
        assert!(lookup(&pager, file, 43, [1, 1]).is_err());
        assert_eq!(lookup(&pager, file, 42, [1, 1]).unwrap(), Some([7, 7]));
    }
}
