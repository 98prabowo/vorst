use std::{fs::File, mem::offset_of, os::unix::fs::FileExt};

use bytemuck::{Pod, Zeroable};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    PAGE_SIZE,
    blob::{
        error::{Error, Result},
        utils::AlignedBuffer,
    },
};

// "VBSF" (Vorst Blob Segment Format)
pub const SEGMENT_MAGIC: u32 = 0x56425346;

// "VBSO" (Vorst Blob Segment Object)
pub const OBJECT_MAGIC: u32 = 0x5642534F;

pub const SEGMENT_HEADER_SIZE: usize = size_of::<SegmentHeader>();
pub const OBJECT_HEADER_SIZE: usize = size_of::<ObjectHeader>();

pub const SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
pub const OBJECT_SIZE: u64 = 4 * 1024 * 1024;
pub const DATA_SIZE: u64 = OBJECT_SIZE - PAGE_SIZE;

pub const FLAG_NONE: u16 = 0x0000;
pub const FLAG_TOMBSTONE: u16 = 0x0001;
pub const FLAG_CORRUPTED: u16 = 0x0002;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct SegmentHeader {
    magic: u32,
    version: u32,
    segment_size: u64,
    created_at: i64,
    sealed_at: i64,
    entries_count: u32,
    segment_id: [u8; 16],
    _padding: [u8; 4],
}

impl SegmentHeader {
    pub fn new(id: Uuid, count: u32, segment_size: u64, created_at: DateTime<Utc>) -> Self {
        Self {
            magic: SEGMENT_MAGIC.to_le(),
            version: 1u32.to_le(),
            segment_size: segment_size.to_le(),
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

    pub fn version(&self) -> u32 {
        u32::from_le(self.version)
    }

    pub fn segment_size(&self) -> u64 {
        u64::from_le(self.segment_size)
    }

    pub fn created_at(&self) -> Result<DateTime<Utc>> {
        let ts = i64::from_le(self.created_at);
        DateTime::from_timestamp_secs(ts).ok_or(Error::InvalidTimestamp)
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

    pub fn validate(&self, id: Uuid) -> Result<()> {
        if self.magic() != SEGMENT_MAGIC {
            return Err(Error::InvalidMagic {
                expected: SEGMENT_MAGIC,
                found: self.magic(),
            });
        }

        if self.version() > 1 {
            return Err(Error::Internal("unsupported segment version".to_string()));
        }

        if self.segment_id() != id {
            return Err(Error::IdMismatch {
                expected: id,
                found: self.segment_id(),
            });
        }

        Ok(())
    }

    pub fn header(file: &File, segment_id: Uuid) -> Result<Self> {
        let mut buf = AlignedBuffer::<{ size_of::<Self>() }>::default();
        file.read_exact_at(&mut *buf, 0)?;

        let header: &Self = bytemuck::from_bytes(&*buf);
        header.validate(segment_id)?;

        Ok(*header)
    }

    pub fn write_sealed_at(file: &File, dt: DateTime<Utc>) -> Result<()> {
        let offset = offset_of!(Self, sealed_at) as u64;
        let ts = dt.timestamp();
        file.write_all_at(&ts.to_le_bytes(), offset)?;
        Ok(())
    }

    pub fn write_entries_count(file: &File, count: u32) -> Result<()> {
        let offset = offset_of!(Self, entries_count) as u64;
        file.write_all_at(&count.to_le_bytes(), offset)?;
        Ok(())
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
    file_id: [u8; 16],
    object_id: [u8; 16],
}

impl ObjectHeader {
    pub fn new(file_id: Uuid, object_id: Uuid, len: u32, flags: u16, checksum: u32) -> Self {
        Self {
            magic: OBJECT_MAGIC.to_le(),
            version: 1u16.to_le(),
            flags: flags.to_le(),
            data_len: len.to_le(),
            checksum: checksum.to_le(),
            file_id: file_id.into_bytes(),
            object_id: object_id.into_bytes(),
        }
    }

    pub fn magic(&self) -> u32 {
        u32::from_le(self.magic)
    }

    pub fn version(&self) -> u16 {
        u16::from_le(self.version)
    }

    pub fn flags(&self) -> u16 {
        u16::from_le(self.flags)
    }

    pub fn file_id(&self) -> Uuid {
        Uuid::from_bytes(self.file_id)
    }

    pub fn object_id(&self) -> Uuid {
        Uuid::from_bytes(self.object_id)
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

    pub fn header(file: &File, segment_id: Uuid, offset: u64, file_capacity: u64) -> Result<Self> {
        if !offset.is_multiple_of(PAGE_SIZE) {
            return Err(Error::Internal(format!("unaligned offset: {offset}")));
        }

        let mut buf = AlignedBuffer::<{ size_of::<Self>() }>::default();
        file.read_exact_at(&mut *buf, offset)?;

        let header: &Self = bytemuck::from_bytes(&*buf);

        if header.magic() != OBJECT_MAGIC {
            return Err(Error::InvalidMagic {
                expected: OBJECT_MAGIC,
                found: header.magic(),
            });
        }

        if header.version() > 1 {
            return Err(Error::Internal("unsupported object version".to_string()));
        }

        if offset + header.object_size() > file_capacity {
            return Err(Error::OutOfBounds {
                id: segment_id,
                offset,
                size: header.object_size(),
            });
        }

        Ok(*header)
    }
}

const _: () = assert!(std::mem::size_of::<SegmentHeader>() == SEGMENT_HEADER_SIZE);
const _: () = assert!(std::mem::size_of::<ObjectHeader>() == OBJECT_HEADER_SIZE);
const _: () = assert!(std::mem::align_of::<SegmentHeader>() <= PAGE_SIZE as usize);
