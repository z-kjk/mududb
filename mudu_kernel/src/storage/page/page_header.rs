use crate::storage::page::PageId;
use crate::wal::lsn::LSN;
use byteorder::{ByteOrder, LittleEndian};
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;

pub const PAGE_HEADER_SIZE: usize = 128;
pub const NONE_PAGE_ID: PageId = PageId::MAX;
/// Magic value for the ASCII tag `PAGE`.
/// As a numeric constant it is `0x5041_4745` (`'P' 'A' 'G' 'E'`).
/// Because the header is encoded in little-endian order, the on-page byte
/// sequence is `45 47 41 50`, which corresponds to `E G A P` if read byte by byte.
pub const PAGE_HEADER_MAGIC: u32 = 0x5041_4745;
const PAGE_HEADER_FIELD_COUNT: usize = 10;
const PAGE_HEADER_FIXED_SIZE: usize = PAGE_HEADER_FIELD_COUNT * std::mem::size_of::<u32>();
const PAGE_HEADER_RESERVED_SIZE: usize = PAGE_HEADER_SIZE - PAGE_HEADER_FIXED_SIZE;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageHeader {
    magic: u32,
    page_id: PageId,
    prev_page: PageId,
    next_page: PageId,
    lsn: LSN,
    flags: u32,
    record_count: u32,
    first_free_offset: u32,
    free_bytes: u32,
    last_record_offset: u32,
    reserved: [u8; PAGE_HEADER_RESERVED_SIZE],
}

impl PageHeader {
    pub fn new(page_id: PageId) -> Self {
        Self {
            magic: PAGE_HEADER_MAGIC,
            page_id,
            prev_page: NONE_PAGE_ID,
            next_page: NONE_PAGE_ID,
            lsn: 0,
            flags: 0,
            record_count: 0,
            first_free_offset: PAGE_HEADER_SIZE as u32,
            free_bytes: 0,
            last_record_offset: 0,
            reserved: [0; PAGE_HEADER_RESERVED_SIZE],
        }
    }

    pub fn decode(input: &[u8]) -> RS<Self> {
        if input.len() < PAGE_HEADER_SIZE {
            return Err(m_error!(
                EC::DecodeErr,
                format!(
                    "page header requires {} bytes, got {}",
                    PAGE_HEADER_SIZE,
                    input.len()
                )
            ));
        }

        let mut offset = 0usize;
        let read_u32 = |input: &[u8], offset: &mut usize| -> u32 {
            let value = LittleEndian::read_u32(&input[*offset..*offset + 4]);
            *offset += 4;
            value
        };

        let magic = read_u32(input, &mut offset);
        let page_id = read_u32(input, &mut offset);
        let prev_page = read_u32(input, &mut offset);
        let next_page = read_u32(input, &mut offset);
        let lsn = read_u32(input, &mut offset) as LSN;
        let flags = read_u32(input, &mut offset);
        let record_count = read_u32(input, &mut offset);
        let first_free_offset = read_u32(input, &mut offset);
        let free_bytes = read_u32(input, &mut offset);
        let last_record_offset = read_u32(input, &mut offset);
        let mut reserved = [0u8; PAGE_HEADER_RESERVED_SIZE];
        reserved.copy_from_slice(&input[offset..offset + PAGE_HEADER_RESERVED_SIZE]);

        Ok(Self {
            magic,
            page_id,
            prev_page,
            next_page,
            lsn,
            flags,
            record_count,
            first_free_offset,
            free_bytes,
            last_record_offset,
            reserved,
        })
    }

    pub fn encode(&self, out: &mut [u8]) -> RS<()> {
        if out.len() < PAGE_HEADER_SIZE {
            return Err(m_error!(
                EC::EncodeErr,
                format!(
                    "page header encode requires {} bytes, got {}",
                    PAGE_HEADER_SIZE,
                    out.len()
                )
            ));
        }

        let mut offset = 0usize;

        let write_u32 = |out: &mut [u8], offset: &mut usize, value: u32| {
            LittleEndian::write_u32(&mut out[*offset..*offset + 4], value);
            *offset += 4;
        };

        write_u32(out, &mut offset, self.magic);
        write_u32(out, &mut offset, self.page_id);
        write_u32(out, &mut offset, self.prev_page);
        write_u32(out, &mut offset, self.next_page);
        write_u32(out, &mut offset, self.lsn);
        write_u32(out, &mut offset, self.flags);
        write_u32(out, &mut offset, self.record_count);
        write_u32(out, &mut offset, self.first_free_offset);
        write_u32(out, &mut offset, self.free_bytes);
        write_u32(out, &mut offset, self.last_record_offset);
        out[offset..offset + PAGE_HEADER_RESERVED_SIZE].copy_from_slice(&self.reserved);
        Ok(())
    }

    pub fn magic(&self) -> u32 {
        self.magic
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn prev_page(&self) -> PageId {
        self.prev_page
    }

    pub fn next_page(&self) -> PageId {
        self.next_page
    }

    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    pub fn flags(&self) -> u32 {
        self.flags
    }

    pub fn record_count(&self) -> u32 {
        self.record_count
    }

    pub fn first_free_offset(&self) -> u32 {
        self.first_free_offset
    }

    pub fn free_bytes(&self) -> u32 {
        self.free_bytes
    }

    pub fn last_record_offset(&self) -> u32 {
        self.last_record_offset
    }

    pub fn reserved(&self) -> &[u8; PAGE_HEADER_RESERVED_SIZE] {
        &self.reserved
    }

    pub fn set_prev_page(&mut self, prev_page: PageId) {
        self.prev_page = prev_page;
    }

    pub fn set_next_page(&mut self, next_page: PageId) {
        self.next_page = next_page;
    }

    pub fn set_lsn(&mut self, lsn: LSN) {
        self.lsn = lsn;
    }

    pub fn set_flags(&mut self, flags: u32) {
        self.flags = flags;
    }

    pub fn set_record_count(&mut self, record_count: u32) {
        self.record_count = record_count;
    }

    pub fn set_first_free_offset(&mut self, first_free_offset: u32) {
        self.first_free_offset = first_free_offset;
    }

    pub fn set_free_bytes(&mut self, free_bytes: u32) {
        self.free_bytes = free_bytes;
    }

    pub fn set_last_record_offset(&mut self, last_record_offset: u32) {
        self.last_record_offset = last_record_offset;
    }

    pub fn reserved_mut(&mut self) -> &mut [u8; PAGE_HEADER_RESERVED_SIZE] {
        &mut self.reserved
    }
}

impl Default for PageHeader {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::{PageHeader, PAGE_HEADER_MAGIC, PAGE_HEADER_SIZE};

    #[test]
    fn page_header_encodes_to_fixed_128_bytes() {
        let header = PageHeader::default();
        let mut encoded = [0u8; PAGE_HEADER_SIZE];
        header.encode(&mut encoded).unwrap();
        assert_eq!(encoded.len(), PAGE_HEADER_SIZE);
        assert_eq!(header.magic(), PAGE_HEADER_MAGIC);
    }

    #[test]
    fn page_header_roundtrip() {
        let mut header = PageHeader::new(7);
        header.set_prev_page(5);
        header.set_next_page(9);
        header.set_lsn(11);
        header.set_flags(17);
        header.set_record_count(19);
        header.set_first_free_offset(23);
        header.set_free_bytes(29);
        header.set_last_record_offset(31);
        header.reserved_mut()[0] = 37;

        let mut encoded = [0u8; PAGE_HEADER_SIZE];
        header.encode(&mut encoded).unwrap();
        let decoded = PageHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, header);
    }
}
