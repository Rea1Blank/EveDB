// SPDX-License-Identifier: AGPL-3.0-only

use super::PAGE_SIZE;
use crate::{Result, checksum::crc32c, error::corrupt};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

pub(crate) type Key = [u64; 2];
pub(crate) type Value = [u64; 2];
const HEADER: usize = 40;
const LEAF_CAPACITY: usize = (PAGE_SIZE - HEADER) / 32;
const BRANCH_CAPACITY: usize = (PAGE_SIZE - HEADER) / 24;

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
            let at = HEADER + i * 32;
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
                    let at = HEADER + i * 24;
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

fn read(file: &mut File, id: u64, lsn: u64) -> Result<[u8; PAGE_SIZE]> {
    let offset = id
        .checked_mul(PAGE_SIZE as u64)
        .ok_or_else(|| corrupt("index page offset overflow"))?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0; PAGE_SIZE];
    file.read_exact(&mut bytes)?;
    let expected = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
    bytes[28..32].fill(0);
    if &bytes[..4] != b"EVIX"
        || bytes[4..6] != 1_u16.to_le_bytes()
        || get(&bytes, 8) != id
        || get(&bytes, 16) != lsn
        || bytes[26..28] != [0; 2]
        || crc32c(&bytes) != expected
    {
        return Err(corrupt("invalid index page header or checksum"));
    }
    let kind = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
    let count = u16::from_le_bytes(bytes[24..26].try_into().unwrap()) as usize;
    let width = match kind {
        0 if id == 0 && count == 0 => return Ok(bytes),
        1 => 32,
        2 => 24,
        _ => return Err(corrupt("invalid index node kind")),
    };
    if count == 0 || count > (PAGE_SIZE - HEADER) / width {
        return Err(corrupt("invalid index node count"));
    }
    let mut previous = None;
    for i in 0..count {
        let at = HEADER + i * width;
        let key = [get(&bytes, at), get(&bytes, at + 8)];
        if previous.is_some_and(|p| p >= key) {
            return Err(corrupt("unordered index node"));
        }
        if kind == 2 && (get(&bytes, at + 16) == 0 || get(&bytes, at + 16) >= id) {
            return Err(corrupt("invalid index child"));
        }
        previous = Some(key);
    }
    if kind == 1 && get(&bytes, 32) != 0 && get(&bytes, 32) <= id {
        return Err(corrupt("cyclic index leaf chain"));
    }
    Ok(bytes)
}

pub(crate) fn lookup(path: &Path, lsn: u64, key: Key) -> Result<Option<Value>> {
    let mut cursor = scan(path, lsn, key, key)?;
    cursor
        .next()
        .transpose()
        .map(|entry| entry.map(|(_, value)| value))
}
pub(crate) fn floor(path: &Path, lsn: u64, key: Key) -> Result<Option<(Key, Value)>> {
    let cursor = scan(path, lsn, key, key)?;
    let Some(bytes) = cursor.leaf.as_ref() else {
        return Ok(None);
    };
    let count = u16::from_le_bytes(bytes[24..26].try_into().unwrap()) as usize;
    let mut result = None;
    for i in 0..count {
        let at = HEADER + i * 32;
        let candidate = [get(bytes, at), get(bytes, at + 8)];
        if candidate > key {
            break;
        }
        result = Some((candidate, [get(bytes, at + 16), get(bytes, at + 24)]));
    }
    Ok(result)
}
pub(crate) fn scan(path: &Path, lsn: u64, lower: Key, upper: Key) -> Result<IndexCursor> {
    let mut file = File::open(path)?;
    if !file.metadata()?.len().is_multiple_of(PAGE_SIZE as u64) {
        return Err(corrupt("truncated index file"));
    }
    let meta = read(&mut file, 0, lsn)?;
    let mut id = get(&meta, 32);
    let mut leaf = None;
    while id != 0 {
        let bytes = read(&mut file, id, lsn)?;
        if bytes[6..8] == 1_u16.to_le_bytes() {
            leaf = Some(bytes);
            break;
        }
        if bytes[6..8] != 2_u16.to_le_bytes() {
            return Err(corrupt("index root points to metadata"));
        }
        let count = u16::from_le_bytes(bytes[24..26].try_into().unwrap()) as usize;
        let mut chosen = 0;
        for i in 0..count {
            let at = HEADER + i * 24;
            if [get(&bytes, at), get(&bytes, at + 8)] <= lower {
                chosen = i;
            } else {
                break;
            }
        }
        id = get(&bytes, HEADER + chosen * 24 + 16);
    }
    Ok(IndexCursor {
        file,
        lsn,
        lower,
        upper,
        leaf,
        position: 0,
        failed: false,
    })
}
pub(crate) struct IndexCursor {
    file: File,
    lsn: u64,
    lower: Key,
    upper: Key,
    leaf: Option<[u8; PAGE_SIZE]>,
    position: usize,
    failed: bool,
}
impl Iterator for IndexCursor {
    type Item = Result<(Key, Value)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            let bytes = self.leaf.as_ref()?;
            let count = u16::from_le_bytes(bytes[24..26].try_into().unwrap()) as usize;
            if self.position == count {
                let next = get(bytes, 32);
                if next == 0 {
                    self.leaf = None;
                    return None;
                }
                match read(&mut self.file, next, self.lsn) {
                    Ok(page) if page[6..8] == 1_u16.to_le_bytes() => {
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
            let at = HEADER + self.position * 32;
            self.position += 1;
            let key = [get(bytes, at), get(bytes, at + 8)];
            if key < self.lower {
                continue;
            }
            if key > self.upper {
                self.leaf = None;
                return None;
            }
            self.lower = key;
            return Some(Ok((key, [get(bytes, at + 16), get(bytes, at + 24)])));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;

    #[test]
    fn three_level_tree_supports_point_floor_and_range_queries() {
        let dir = TempDir::new();
        let path = dir.0.join("tree.index");
        let mut writer = IndexWriter::create(&path, 77).unwrap();
        for n in 0..100_000 {
            writer.append([n / 1000, n % 1000], [n, n * 2]).unwrap();
        }
        assert!(writer.append([99, 999], [0, 0]).is_err());
        writer.finish().unwrap();
        for n in [0, 253, 254, 338 * 254, 90_123, 99_999] {
            assert_eq!(
                lookup(&path, 77, [n / 1000, n % 1000]).unwrap(),
                Some([n, n * 2])
            );
        }
        assert_eq!(lookup(&path, 77, [100, 0]).unwrap(), None);
        assert_eq!(
            floor(&path, 77, [1, 1001]).unwrap(),
            Some(([1, 999], [1999, 3998]))
        );
        let range: Vec<_> = scan(&path, 77, [3, 990], [4, 10])
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(range.len(), 21);
        assert_eq!(range.first().unwrap().0, [3, 990]);
        assert_eq!(range.last().unwrap().0, [4, 10]);
        assert!(lookup(&path, 78, [0, 0]).is_err());
    }

    #[test]
    fn empty_tree_and_nonexistent_floor_are_supported() {
        let dir = TempDir::new();
        let path = dir.0.join("empty.index");
        IndexWriter::create(&path, 0).unwrap().finish().unwrap();
        assert_eq!(lookup(&path, 0, [0, 0]).unwrap(), None);
        assert_eq!(floor(&path, 0, [0, 0]).unwrap(), None);
        let path = dir.0.join("single.index");
        let mut writer = IndexWriter::create(&path, 0).unwrap();
        writer.append([5, 2], [8, 9]).unwrap();
        writer.finish().unwrap();
        assert_eq!(floor(&path, 0, [5, 1]).unwrap(), None);
    }
}
