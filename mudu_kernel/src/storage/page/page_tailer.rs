use crate::wal::lsn::LSN;
use byteorder::{ByteOrder, LittleEndian};
use mudu::common::crc::crc32;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;

pub const PAGE_TAILER_SIZE: usize = 8;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PageTailer {
    lsn: LSN,
    checksum: u32,
}

impl PageTailer {
    pub fn new(lsn: LSN, checksum: u32) -> Self {
        Self { lsn, checksum }
    }

    pub fn decode(input: &[u8]) -> RS<Self> {
        if input.len() < PAGE_TAILER_SIZE {
            return Err(m_error!(
                EC::DecodeErr,
                format!(
                    "page tailer requires {} bytes, got {}",
                    PAGE_TAILER_SIZE,
                    input.len()
                )
            ));
        }

        Ok(Self {
            lsn: LittleEndian::read_u32(&input[0..4]) as LSN,
            checksum: LittleEndian::read_u32(&input[4..8]),
        })
    }

    pub fn encode(&self, out: &mut [u8]) -> RS<()> {
        if out.len() < PAGE_TAILER_SIZE {
            return Err(m_error!(
                EC::EncodeErr,
                format!(
                    "page tailer encode requires {} bytes, got {}",
                    PAGE_TAILER_SIZE,
                    out.len()
                )
            ));
        }

        LittleEndian::write_u32(&mut out[0..4], self.lsn);
        LittleEndian::write_u32(&mut out[4..8], self.checksum);
        Ok(())
    }

    /// Computes the page CRC32 over the full page except for the trailing tailer.
    /// The covered range is `[0, page.len() - PAGE_TAILER_SIZE)`.
    pub fn checksum_for_page(page: &[u8]) -> RS<u32> {
        if page.len() < PAGE_TAILER_SIZE {
            return Err(m_error!(
                EC::EncodeErr,
                format!(
                    "page checksum requires at least {} bytes, got {}",
                    PAGE_TAILER_SIZE,
                    page.len()
                )
            ));
        }

        Ok(crc32(&page[..page.len() - PAGE_TAILER_SIZE]))
    }

    pub fn refresh_checksum(&mut self, page: &[u8]) -> RS<()> {
        self.checksum = Self::checksum_for_page(page)?;
        Ok(())
    }

    pub fn validate_checksum(&self, page: &[u8]) -> RS<()> {
        let actual = Self::checksum_for_page(page)?;
        if self.checksum != actual {
            return Err(m_error!(
                EC::DecodeErr,
                format!(
                    "page checksum mismatch: stored={}, actual={}",
                    self.checksum, actual
                )
            ));
        }
        Ok(())
    }

    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    pub fn checksum(&self) -> u32 {
        self.checksum
    }

    pub fn set_lsn(&mut self, lsn: LSN) {
        self.lsn = lsn;
    }

    pub fn set_checksum(&mut self, checksum: u32) {
        self.checksum = checksum;
    }
}

#[cfg(test)]
mod tests {
    use super::{PageTailer, PAGE_TAILER_SIZE};

    #[test]
    fn page_tailer_encodes_to_fixed_8_bytes() {
        let tailer = PageTailer::default();
        let mut encoded = [0u8; PAGE_TAILER_SIZE];
        tailer.encode(&mut encoded).unwrap();
        assert_eq!(encoded.len(), PAGE_TAILER_SIZE);
    }

    #[test]
    fn page_tailer_roundtrip() {
        let tailer = PageTailer::new(17, 29);
        let mut encoded = [0u8; PAGE_TAILER_SIZE];
        tailer.encode(&mut encoded).unwrap();
        let decoded = PageTailer::decode(&encoded).unwrap();
        assert_eq!(decoded, tailer);
    }

    #[test]
    fn page_tailer_checksum_covers_page_except_tailer() {
        let mut page = [0u8; 32];
        page[..24].copy_from_slice(&[1u8; 24]);
        page[24..].copy_from_slice(&[9u8; 8]);

        let checksum = PageTailer::checksum_for_page(&page).unwrap();
        page[24..].copy_from_slice(&[7u8; 8]);
        assert_eq!(checksum, PageTailer::checksum_for_page(&page).unwrap());
    }
}
