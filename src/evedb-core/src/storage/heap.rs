// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    MAX_RECORD_SIZE, PAGE_SIZE, SlottedPage,
    pager::{FileId, Pager},
};
use crate::{Error, Result, codec::MAX_RECORD, error::corrupt};
use std::{fs::File, io::Write, path::Path, sync::Arc};

/// The number of generation slots one manifest can address.
pub(crate) const MAX_SLOTS: usize = 256;
/// Pages addressable by one heap file, leaving the top byte to the slot.
const PAGE_LIMIT: u64 = 1 << 40;

/// Stamps the generation slot of a manifest onto an in-file location.
pub(crate) fn locate(slot: u8, location: u64) -> u64 {
    (u64::from(slot) << 56) | location
}
/// Splits a stored locator into its generation slot and in-file location.
pub(crate) fn split(locator: u64) -> (u8, u64) {
    ((locator >> 56) as u8, locator & ((PAGE_LIMIT << 16) - 1))
}

pub(crate) struct HeapWriter {
    file: File,
    page: SlottedPage,
    lsn: u64,
}
impl HeapWriter {
    pub fn create(path: &Path, lsn: u64) -> Result<Self> {
        let mut page = SlottedPage::new(0);
        page.set_lsn(lsn);
        Ok(Self {
            file: File::create_new(path)?,
            page,
            lsn,
        })
    }
    fn flush(&mut self) -> Result<()> {
        if self.page.record_count() == 0 {
            return Ok(());
        }
        self.file.write_all(self.page.as_bytes())?;
        self.page = SlottedPage::new(self.page.page_id() + 1);
        self.page.set_lsn(self.lsn);
        Ok(())
    }
    pub fn append(&mut self, bytes: &[u8]) -> Result<u64> {
        if bytes.len() > MAX_RECORD {
            return Err(Error::Invalid("record exceeds 16 MiB".into()));
        }
        if self.page.page_id() >= PAGE_LIMIT {
            return Err(Error::Invalid(
                "heap file exceeds its addressable size".into(),
            ));
        }
        if bytes.len() < MAX_RECORD_SIZE {
            let mut record = Vec::with_capacity(bytes.len() + 1);
            record.push(0);
            record.extend(bytes);
            if self.page.free_space() < record.len() + 4 {
                self.flush()?;
            }
            let slot = self.page.insert(&record)?;
            return Ok((self.page.page_id() << 16) | u64::from(slot));
        }
        self.flush()?;
        let start = self.page.page_id() << 16;
        let count = bytes.len().div_ceil(MAX_RECORD_SIZE - 13);
        for (index, chunk) in bytes.chunks(MAX_RECORD_SIZE - 13).enumerate() {
            let mut record = vec![1];
            record.extend((bytes.len() as u32).to_le_bytes());
            record.extend((index as u32).to_le_bytes());
            record.extend((count as u32).to_le_bytes());
            record.extend(chunk);
            self.page.insert(&record)?;
            self.flush()?;
        }
        Ok(start)
    }
    pub fn finish(mut self) -> Result<()> {
        self.flush()?;
        self.file.sync_all()?;
        Ok(())
    }
}

pub(crate) fn read(pager: &Pager, file: FileId, location: u64, lsn: u64) -> Result<Vec<u8>> {
    let page_id = location >> 16;
    let page = read_page(pager, file, page_id, lsn)?;
    let record = page
        .get(location as u16)
        .ok_or_else(|| corrupt("absent heap slot"))?;
    match record.first() {
        Some(0) => Ok(record[1..].to_vec()),
        Some(1) if record.len() >= 13 => {
            let total = u32::from_le_bytes(record[1..5].try_into().unwrap()) as usize;
            let count = u32::from_le_bytes(record[9..13].try_into().unwrap()) as usize;
            if !(MAX_RECORD_SIZE..=MAX_RECORD).contains(&total)
                || count != total.div_ceil(MAX_RECORD_SIZE - 13)
                || location as u16 != 0
            {
                return Err(corrupt("invalid overflow chain"));
            }
            let header: [u8; 12] = record[1..13].try_into().unwrap();
            let mut bytes = Vec::with_capacity(total);
            for index in 0..count {
                let chunk_page = read_page(pager, file, page_id + index as u64, lsn)?;
                let chunk = chunk_page
                    .get(0)
                    .ok_or_else(|| corrupt("missing overflow record"))?;
                if chunk.len() < 13
                    || chunk[0] != 1
                    || chunk[1..5] != header[..4]
                    || chunk[9..13] != header[8..]
                    || u32::from_le_bytes(chunk[5..9].try_into().unwrap()) as usize != index
                {
                    return Err(corrupt("invalid overflow fragment"));
                }
                bytes.extend(&chunk[13..]);
                if bytes.len() > total {
                    return Err(corrupt("overflow chain is too long"));
                }
            }
            if bytes.len() != total {
                return Err(corrupt("truncated overflow chain"));
            }
            Ok(bytes)
        }
        _ => Err(corrupt("unknown heap record kind")),
    }
}
fn read_page(pager: &Pager, file: FileId, id: u64, lsn: u64) -> Result<Arc<SlottedPage>> {
    let page = pager.page(file, id, |bytes: &[u8; PAGE_SIZE]| {
        let page = SlottedPage::from_bytes(bytes)?;
        if page.page_id() != id {
            return Err(corrupt("misdirected heap page"));
        }
        Ok(page)
    })?;
    if page.lsn() != lsn {
        return Err(corrupt("misdirected heap page"));
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        storage::pager::{DEFAULT_CACHE_BYTES, DEFAULT_OPEN_FILES},
        test_support::TempDir,
    };

    #[test]
    fn inline_and_overflow_boundaries_roundtrip_with_identity_checks() {
        let dir = TempDir::new();
        let file = FileId::new(1, 1, 0, 0);
        let path = file.path(&dir.0);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut writer = HeapWriter::create(&path, 19).unwrap();
        let mut records = Vec::new();
        for size in [
            0,
            1,
            MAX_RECORD_SIZE - 1,
            MAX_RECORD_SIZE,
            MAX_RECORD_SIZE + 1,
            300_000,
        ] {
            let bytes: Vec<_> = (0..size).map(|n| (n % 251) as u8).collect();
            let location = writer.append(&bytes).unwrap();
            records.push((location, bytes));
        }
        writer.finish().unwrap();
        let pager = Pager::new(dir.0.clone(), DEFAULT_CACHE_BYTES, DEFAULT_OPEN_FILES);
        for (location, bytes) in records {
            assert_eq!(read(&pager, file, location, 19).unwrap(), bytes);
        }
        assert!(read(&pager, file, 0, 20).is_err());
        assert!(read(&pager, file, u64::MAX, 19).is_err());
    }
}
