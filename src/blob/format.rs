use std::{fs::File, io, mem::offset_of, os::unix::fs::FileExt};

use bytemuck::{Pod, Zeroable};
use chrono::{DateTime, Utc};
use uuid::Uuid;

// "VBSF" (Vorst Blob Segment File)
pub const SEGMENT_MAGIC: u32 = 0x56425346;

// "VBSO" (Vorst Blob Segment Object)
pub const OBJECT_MAGIC: u32 = 0x5642534F;

pub const SEGMENT_HEADER_SIZE: usize = size_of::<SegmentHeader>();
pub const OBJECT_HEADER_SIZE: usize = size_of::<ObjectHeader>();

pub const FLAG_NONE: u16 = 0x0000;
pub const FLAG_TOMBSTONE: u16 = 0x0001;
pub const FLAG_CORRUPTED: u16 = 0x0002;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct SegmentHeader {
    magic: u32,
    version: u32,
    capacity: u64,
    created_at: i64,
    sealed_at: i64,
    entries_count: u32,
    segment_id: [u8; 16],
    _padding: [u8; 4],
}

impl SegmentHeader {
    pub fn new(id: Uuid, count: u32, capacity: u64, created_at: DateTime<Utc>) -> Self {
        Self {
            magic: SEGMENT_MAGIC.to_le(),
            version: 1u32.to_le(),
            capacity: capacity.to_le(),
            created_at: created_at.timestamp().to_le(),
            sealed_at: 0,
            entries_count: count.to_le(),
            segment_id: id.into_bytes(),
            _padding: [0u8; 4],
        }
    }

    pub fn magic(&self) -> u32 {
        u32::from_le(self.magic)
    }

    pub fn capacity(&self) -> u64 {
        u64::from_le(self.capacity)
    }

    pub fn created_at(&self) -> io::Result<DateTime<Utc>> {
        let ts = i64::from_le(self.created_at);
        DateTime::from_timestamp_secs(ts).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid segment segment header")
        })
    }

    pub fn sealed_at(&self) -> i64 {
        i64::from_le(self.sealed_at)
    }

    pub fn entries_count(&self) -> u32 {
        u32::from_le(self.entries_count)
    }

    pub fn segment_id(&self) -> Uuid {
        Uuid::from_bytes(self.segment_id)
    }

    pub fn validate(&self, id: Uuid) -> io::Result<()> {
        if self.magic() != SEGMENT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid segment header magic number",
            ));
        }

        if self.segment_id() != id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment header UUID mismatch",
            ));
        }

        Ok(())
    }

    pub fn write_sealed_at(file: &File, dt: DateTime<Utc>) -> io::Result<()> {
        let offset = offset_of!(Self, sealed_at) as u64;
        let ts = dt.timestamp();
        file.write_all_at(&ts.to_le_bytes(), offset)
    }

    pub fn write_entries_count(file: &File, count: u32) -> io::Result<()> {
        let offset = offset_of!(Self, entries_count) as u64;
        file.write_all_at(&count.to_le_bytes(), offset)
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct ObjectHeader {
    magic: u32,
    version: u16,
    flags: u16,
    data_len: u32,
    checksum: u32,
    object_id: u64,
}

impl ObjectHeader {
    pub fn new(id: u64, len: u32, flags: u16, checksum: u32) -> Self {
        Self {
            magic: OBJECT_MAGIC.to_le(),
            version: 1u16.to_le(),
            flags: flags.to_le(),
            data_len: len.to_le(),
            checksum: checksum.to_le(),
            object_id: id.to_le(),
        }
    }

    pub fn magic(&self) -> u32 {
        u32::from_le(self.magic)
    }

    pub fn flags(&self) -> u16 {
        u16::from_le(self.flags)
    }

    pub fn object_id(&self) -> u64 {
        u64::from_le(self.object_id)
    }

    pub fn data_len(&self) -> u32 {
        u32::from_le(self.data_len)
    }

    pub fn checksum(&self) -> u32 {
        u32::from_le(self.checksum)
    }

    pub fn object_size(&self) -> u64 {
        OBJECT_HEADER_SIZE as u64 + self.data_len() as u64
    }
}

#[inline(always)]
pub const fn align_to_page(size: u64, page_size: u64) -> u64 {
    // NOTE: Current page alignment is hardcoded to the provided `page_size`. In
    // production, we should query `libc::sysconf(_SC_PAGESIZE)` or use `O_DIRECT`
    // requirements (usually 512 or 4096 bytes) to ensure we are actually hitting
    // the disk's physical sector boundaries for maximum throughput.

    debug_assert!(page_size.is_power_of_two());
    let page_mask = page_size - 1;
    (size + page_mask) & !page_mask
}
