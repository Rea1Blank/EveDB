// SPDX-License-Identifier: AGPL-3.0-only

use super::pager::read_exact_at;
use crate::{Error, Result, checksum::crc32c, codec::MAX_FRAME, error::corrupt};
use std::{
    fs::File,
    io::{Read, Seek, Write},
};

pub(crate) struct Frame {
    pub kind: u16,
    pub lsn: u64,
    pub payload: Vec<u8>,
}

pub(crate) fn encode(kind: u16, lsn: u64, payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() > MAX_FRAME {
        return Err(Error::Invalid("transaction exceeds 64 MiB".into()));
    }
    let mut header = [0; 32];
    header[..4].copy_from_slice(b"EVFR");
    header[4..6].copy_from_slice(&1_u16.to_le_bytes());
    header[6..8].copy_from_slice(&kind.to_le_bytes());
    header[8..16].copy_from_slice(&lsn.to_le_bytes());
    header[16..24].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    header[24..28].copy_from_slice(&crc32c(payload).to_le_bytes());
    let crc = crc32c(&header[..28]);
    header[28..32].copy_from_slice(&crc.to_le_bytes());
    let mut bytes = header.to_vec();
    bytes.extend(payload);
    bytes.extend(b"EVCOMMIT");
    Ok(bytes)
}
pub(crate) fn append(file: &mut File, kind: u16, lsn: u64, payload: &[u8]) -> Result<u64> {
    let offset = file.stream_position()?;
    file.write_all(&encode(kind, lsn, payload)?)?;
    Ok(offset)
}

/// Validates a frame header and returns its declared payload length.
fn header_length(header: &[u8; 32]) -> Result<u64> {
    if &header[..4] != b"EVFR"
        || header[4..6] != 1_u16.to_le_bytes()
        || crc32c(&header[..28]) != u32::from_le_bytes(header[28..32].try_into().unwrap())
    {
        return Err(corrupt("invalid frame header or checksum"));
    }
    let length = u64::from_le_bytes(header[16..24].try_into().unwrap());
    if length > MAX_FRAME as u64 {
        return Err(corrupt("oversized frame"));
    }
    Ok(length)
}

/// Assembles a frame from a validated header, its payload, and its trailer.
fn assemble(header: &[u8; 32], payload: Vec<u8>, trailer: &[u8; 8]) -> Result<Frame> {
    if trailer != b"EVCOMMIT"
        || crc32c(&payload) != u32::from_le_bytes(header[24..28].try_into().unwrap())
    {
        return Err(corrupt("invalid committed frame payload or trailer"));
    }
    Ok(Frame {
        kind: u16::from_le_bytes(header[6..8].try_into().unwrap()),
        lsn: u64::from_le_bytes(header[8..16].try_into().unwrap()),
        payload,
    })
}

/// None means an empty or incomplete tail. The caller retains the last complete offset.
/// Complete frames with bad checksums are errors, never silently discarded.
pub(crate) fn read(file: &mut File) -> Result<Option<Frame>> {
    let position = file.stream_position()?;
    let remaining = file.metadata()?.len().saturating_sub(position);
    if remaining < 32 {
        return Ok(None);
    }
    let mut header = [0; 32];
    file.read_exact(&mut header)?;
    let length = header_length(&header)?;
    if remaining < 40 + length {
        return Ok(None);
    }
    let mut payload = vec![0; length as usize];
    file.read_exact(&mut payload)?;
    let mut trailer = [0; 8];
    file.read_exact(&mut trailer)?;
    assemble(&header, payload, &trailer).map(Some)
}

/// Reads one frame at an absolute offset without moving a shared cursor.
///
/// `end` is the length of the published file, so a truncated tail is reported
/// as `None` exactly as it is for a sequential read.
pub(crate) fn read_at(file: &File, offset: u64, end: u64) -> Result<Option<Frame>> {
    let remaining = end.saturating_sub(offset);
    if remaining < 32 {
        return Ok(None);
    }
    let mut header = [0; 32];
    read_exact_at(file, &mut header, offset)?;
    let length = header_length(&header)?;
    if remaining < 40 + length {
        return Ok(None);
    }
    let mut rest = vec![0; length as usize + 8];
    read_exact_at(file, &mut rest, offset + 32)?;
    let trailer: [u8; 8] = rest[length as usize..].try_into().unwrap();
    rest.truncate(length as usize);
    assemble(&header, rest, &trailer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;
    use std::io::SeekFrom;

    #[test]
    fn every_incomplete_prefix_is_distinguished_from_a_committed_frame() {
        let dir = TempDir::new();
        let path = dir.0.join("frames");
        let bytes = encode(2, 123, b"one complete atomic transaction").unwrap();
        let mut file = File::create_new(path).unwrap();
        for cut in 0..bytes.len() {
            file.set_len(0).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&bytes[..cut]).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            assert!(read(&mut file).unwrap().is_none(), "accepted cut at {cut}");
        }
        file.set_len(0).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&bytes).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let record = read(&mut file).unwrap().unwrap();
        assert_eq!(record.kind, 2);
        assert_eq!(record.lsn, 123);
        assert_eq!(record.payload, b"one complete atomic transaction");
        assert!(read(&mut file).unwrap().is_none());
        for offset in [0, 20, 28, 35, bytes.len() - 1] {
            let mut damaged = bytes.clone();
            damaged[offset] ^= 1;
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(&damaged).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            assert!(read(&mut file).is_err());
        }
    }
}
