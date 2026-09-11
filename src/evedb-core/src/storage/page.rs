// SPDX-License-Identifier: AGPL-3.0-only

use std::{error::Error, fmt};

/// The experimental page size in bytes (8 KiB).
pub const PAGE_SIZE: usize = 8192;

const HEADER_SIZE: usize = 40;
const SLOT_SIZE: usize = 4;
const MAGIC: &[u8; 4] = b"EVPG";
const FORMAT_VERSION: u16 = 2;
const COUNT_OFFSET: usize = 16;
const LOWER_OFFSET: usize = 18;
const UPPER_OFFSET: usize = 20;

/// The largest record that fits on an otherwise empty page.
pub const MAX_RECORD_SIZE: usize = PAGE_SIZE - HEADER_SIZE - SLOT_SIZE;

/// A failure to decode a page or insert a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageError {
    /// The input is not exactly one page long.
    InvalidSize,
    /// The magic, page size, or reserved header bytes are invalid.
    InvalidHeader,
    /// The page uses an unsupported format version.
    UnsupportedVersion(u16),
    /// The slot directory or record boundaries are inconsistent.
    InvalidLayout,
    /// The record cannot fit even on an empty page.
    RecordTooLarge,
    /// The page has insufficient space for the record and its slot.
    Full,
    /// The checksum does not match the page bytes.
    ChecksumMismatch,
}

impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSize => write!(f, "expected exactly {PAGE_SIZE} page bytes"),
            Self::InvalidHeader => f.write_str("invalid page header"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported page format version {version}")
            }
            Self::InvalidLayout => f.write_str("invalid slot directory or record boundaries"),
            Self::RecordTooLarge => write!(f, "record exceeds {MAX_RECORD_SIZE} bytes"),
            Self::Full => f.write_str("insufficient space for the record and its slot"),
            Self::ChecksumMismatch => f.write_str("page checksum mismatch"),
        }
    }
}

impl Error for PageError {}

/// An experimental fixed-size page containing opaque variable-length records.
///
/// The slot directory grows forward; record bytes grow backward. Slots are
/// zero-based and remain valid as more records are inserted into this page.
/// Empty records are supported and still consume a slot. Integers use explicit
/// little-endian encoding, independent of Rust's memory layout.
///
/// Format 2 includes a CRC32C checksum and a transaction sequence number.
/// The pager publishes these pages in immutable checkpoint generations.
#[derive(Clone)]
pub struct SlottedPage {
    bytes: [u8; PAGE_SIZE],
}

impl SlottedPage {
    /// Creates an empty page with a caller-assigned identifier.
    ///
    /// The caller is responsible for uniqueness within the owning table file.
    pub fn new(page_id: u64) -> Self {
        let mut page = Self {
            bytes: [0; PAGE_SIZE],
        };
        page.bytes[..4].copy_from_slice(MAGIC);
        page.write_u16(4, FORMAT_VERSION);
        page.write_u16(6, PAGE_SIZE as u16);
        page.bytes[8..16].copy_from_slice(&page_id.to_le_bytes());
        page.write_u16(LOWER_OFFSET, HEADER_SIZE as u16);
        page.write_u16(UPPER_OFFSET, PAGE_SIZE as u16);
        page.refresh_checksum();
        page
    }

    /// Decodes one page, checking its header and every slot before exposing data.
    ///
    /// Format 2 requires tightly packed records in reverse slot order. Gaps,
    /// overlapping records, and pointers into the header or free space fail
    /// validation. A CRC32C checksum covers the header, directory, and payload.
    ///
    /// # Errors
    ///
    /// Returns an error for a wrong length, unsupported version, invalid header,
    /// or inconsistent record layout.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PageError> {
        let bytes: [u8; PAGE_SIZE] = bytes.try_into().map_err(|_| PageError::InvalidSize)?;
        let page = Self { bytes };
        if &page.bytes[..4] != MAGIC {
            return Err(PageError::InvalidHeader);
        }
        let version = page.read_u16(4);
        if version != FORMAT_VERSION {
            return Err(PageError::UnsupportedVersion(version));
        }
        if usize::from(page.read_u16(6)) != PAGE_SIZE
            || page.read_u16(22) != 0
            || page.bytes[36..40] != [0; 4]
        {
            return Err(PageError::InvalidHeader);
        }

        let lower = usize::from(page.read_u16(LOWER_OFFSET));
        let upper = usize::from(page.read_u16(UPPER_OFFSET));
        let directory_end = HEADER_SIZE + usize::from(page.record_count()) * SLOT_SIZE;
        if lower != directory_end || lower > upper || upper > PAGE_SIZE {
            return Err(PageError::InvalidLayout);
        }

        let mut expected_end = PAGE_SIZE;
        for slot in 0..page.record_count() {
            let (offset, length) = page.record_location(slot);
            if offset == 0 && length == 0 {
                continue;
            }
            if offset < upper || offset + length != expected_end {
                return Err(PageError::InvalidLayout);
            }
            expected_end = offset;
        }
        if expected_end != upper {
            return Err(PageError::InvalidLayout);
        }
        let expected_crc = u32::from_le_bytes(page.bytes[32..36].try_into().unwrap());
        if expected_crc != page.computed_checksum() {
            return Err(PageError::ChecksumMismatch);
        }
        Ok(page)
    }

    /// Returns the exact page representation, including its header.
    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.bytes
    }

    /// Returns the identifier assigned when the page was created.
    pub fn page_id(&self) -> u64 {
        u64::from_le_bytes(self.bytes[8..16].try_into().expect("fixed header width"))
    }

    /// Returns the transaction sequence number stored in this page.
    pub fn lsn(&self) -> u64 {
        u64::from_le_bytes(self.bytes[24..32].try_into().unwrap())
    }

    /// Sets the transaction sequence number and refreshes the checksum.
    pub fn set_lsn(&mut self, lsn: u64) {
        self.bytes[24..32].copy_from_slice(&lsn.to_le_bytes());
        self.refresh_checksum();
    }

    /// Returns allocated slot count, including empty records and removed slots.
    pub fn record_count(&self) -> u16 {
        self.read_u16(COUNT_OFFSET)
    }

    /// Returns unallocated bytes; each new record also needs a four-byte slot.
    pub fn free_space(&self) -> usize {
        usize::from(self.read_u16(UPPER_OFFSET) - self.read_u16(LOWER_OFFSET))
    }

    /// Inserts a record and returns its zero-based slot within this page.
    ///
    /// # Errors
    ///
    /// Returns [`PageError::RecordTooLarge`] if the record exceeds an empty
    /// page's capacity, or [`PageError::Full`] if this page lacks space.
    /// On either error, all page bytes remain unchanged.
    pub fn insert(&mut self, record: &[u8]) -> Result<u16, PageError> {
        if record.len() > MAX_RECORD_SIZE {
            return Err(PageError::RecordTooLarge);
        }
        if record.len() + SLOT_SIZE > self.free_space() {
            return Err(PageError::Full);
        }

        let slot = self.record_count();
        let lower = usize::from(self.read_u16(LOWER_OFFSET));
        let upper = usize::from(self.read_u16(UPPER_OFFSET));
        let offset = upper - record.len();
        self.bytes[offset..upper].copy_from_slice(record);
        self.write_u16(lower, offset as u16);
        self.write_u16(lower + 2, record.len() as u16);
        self.write_u16(COUNT_OFFSET, slot + 1);
        self.write_u16(LOWER_OFFSET, (lower + SLOT_SIZE) as u16);
        self.write_u16(UPPER_OFFSET, offset as u16);
        self.refresh_checksum();
        Ok(slot)
    }

    /// Reads a record by its zero-based slot, or returns `None` if it is absent.
    pub fn get(&self, slot: u16) -> Option<&[u8]> {
        if slot >= self.record_count() {
            return None;
        }
        let (offset, length) = self.record_location(slot);
        if offset == 0 {
            return None;
        }
        Some(&self.bytes[offset..offset + length])
    }

    /// Replaces a live record without changing its slot. Failure preserves the page.
    pub fn replace(&mut self, slot: u16, record: &[u8]) -> Result<(), PageError> {
        if self.get(slot).is_none() {
            return Err(PageError::InvalidLayout);
        }
        if record.len() > MAX_RECORD_SIZE {
            return Err(PageError::RecordTooLarge);
        }
        let mut rebuilt = Self::new(self.page_id());
        rebuilt.set_lsn(self.lsn());
        for index in 0..self.record_count() {
            rebuilt.append_optional(if index == slot {
                Some(record)
            } else {
                self.get(index)
            })?;
        }
        *self = rebuilt;
        Ok(())
    }

    /// Removes a live record and compacts its bytes, keeping other slots stable.
    /// Returns false if the slot is already absent. Removed slot IDs are not reused.
    pub fn remove(&mut self, slot: u16) -> bool {
        if self.get(slot).is_none() {
            return false;
        }
        let mut rebuilt = Self::new(self.page_id());
        rebuilt.set_lsn(self.lsn());
        for index in 0..self.record_count() {
            rebuilt
                .append_optional(if index == slot { None } else { self.get(index) })
                .expect("removing a record cannot increase page size");
        }
        *self = rebuilt;
        true
    }

    fn append_optional(&mut self, record: Option<&[u8]>) -> Result<(), PageError> {
        if let Some(record) = record {
            self.insert(record)?;
        } else {
            if self.free_space() < SLOT_SIZE {
                return Err(PageError::Full);
            }
            let lower = usize::from(self.read_u16(LOWER_OFFSET));
            self.write_u16(lower, 0);
            self.write_u16(lower + 2, 0);
            self.write_u16(LOWER_OFFSET, (lower + SLOT_SIZE) as u16);
            self.write_u16(COUNT_OFFSET, self.record_count() + 1);
            self.refresh_checksum();
        }
        Ok(())
    }

    /// Computes the checksum the page should carry, treating its field as zero.
    ///
    /// The field is skipped in place rather than by copying the page, so
    /// verifying a page costs no allocation and no 8 KiB memcpy.
    fn computed_checksum(&self) -> u32 {
        let mut crc = crate::checksum::Checksum::new();
        crc.update(&self.bytes[..32]);
        crc.update(&[0; 4]);
        crc.update(&self.bytes[36..]);
        crc.finish()
    }

    fn refresh_checksum(&mut self) {
        let crc = self.computed_checksum();
        self.bytes[32..36].copy_from_slice(&crc.to_le_bytes());
    }

    fn record_location(&self, slot: u16) -> (usize, usize) {
        let entry = HEADER_SIZE + usize::from(slot) * SLOT_SIZE;
        (
            usize::from(self.read_u16(entry)),
            usize::from(self.read_u16(entry + 2)),
        )
    }

    fn read_u16(&self, offset: usize) -> u16 {
        u16::from_le_bytes([self.bytes[offset], self.bytes[offset + 1]])
    }

    fn write_u16(&mut self, offset: usize, value: u16) {
        self.bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }
}
