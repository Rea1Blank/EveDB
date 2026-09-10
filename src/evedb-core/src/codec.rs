// SPDX-License-Identifier: AGPL-3.0-only

use crate::{Result, error::corrupt};

pub(crate) const MAX_RECORD: usize = 16 * 1024 * 1024;
pub(crate) const MAX_FRAME: usize = 64 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct Encoder(pub Vec<u8>);
impl Encoder {
    pub fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    pub fn u32(&mut self, v: u32) {
        self.0.extend(v.to_le_bytes());
    }
    pub fn u64(&mut self, v: u64) {
        self.0.extend(v.to_le_bytes());
    }
    pub fn bytes(&mut self, v: &[u8]) {
        self.u64(v.len() as u64);
        self.0.extend(v);
    }
    pub fn string(&mut self, v: &str) {
        self.bytes(v.as_bytes());
    }
}

pub(crate) struct Decoder<'a> {
    data: &'a [u8],
    offset: usize,
}
impl<'a> Decoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }
    pub fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or_else(|| corrupt("length overflow"))?;
        let bytes = self
            .data
            .get(self.offset..end)
            .ok_or_else(|| corrupt("truncated record"))?;
        self.offset = end;
        Ok(bytes)
    }
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn count(&mut self, minimum_bytes: usize) -> Result<usize> {
        let n = usize::try_from(self.u64()?).map_err(|_| corrupt("length overflow"))?;
        if n > self.remaining() / minimum_bytes.max(1) {
            return Err(corrupt("invalid item count"));
        }
        Ok(n)
    }
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.count(1)?;
        self.take(n)
    }
    pub fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| corrupt("invalid UTF-8"))
    }
    pub fn remaining(&self) -> usize {
        self.data.len() - self.offset
    }
    pub fn finish(self) -> Result<()> {
        if self.remaining() != 0 {
            return Err(corrupt("unexpected trailing bytes"));
        }
        Ok(())
    }
}
