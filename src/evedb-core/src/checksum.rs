// SPDX-License-Identifier: AGPL-3.0-only

const TABLE: [u32; 256] = {
    let mut table = [0; 256];
    let mut i = 0;
    while i < 256 {
        let mut value = i as u32;
        let mut bit = 0;
        while bit < 8 {
            value = (value >> 1) ^ (0x82f6_3b78_u32 & (0_u32.wrapping_sub(value & 1)));
            bit += 1;
        }
        table[i] = value;
        i += 1;
    }
    table
};

pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = Checksum::new();
    crc.update(bytes);
    crc.finish()
}

pub(crate) struct Checksum(u32);
impl Checksum {
    pub fn new() -> Self {
        Self(!0)
    }
    pub fn update(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = TABLE[((self.0 ^ u32::from(byte)) & 255) as usize] ^ (self.0 >> 8);
        }
    }
    pub fn finish(self) -> u32 {
        !self.0
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn castagnoli_check_vector() {
        assert_eq!(super::crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(super::crc32c(b""), 0);
    }
}
