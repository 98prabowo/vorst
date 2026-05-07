use std::{fs::File, io, mem::offset_of, os::unix::fs::FileExt};

use bytemuck::{Pod, Zeroable};
use chrono::{DateTime, Utc};
use uuid::Uuid;

pub const BLOB_MAGIC: u32 = 0x56424c42; // "VBLB"
pub const OBJECT_MAGIC: u32 = 0x564F424A; // "VOBJ"
pub const OBJECT_HEADER_SIZE: u64 = size_of::<ObjectHeader>() as u64;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct FileHeader {
    magic: u32,
    version: u32,
    threshold: u64,
    created_at: i64,
    sealed_at: i64,
    entries_count: u32,
    blob_id: [u8; 16],
    _padding: [u8; 4],
}

impl FileHeader {
    pub fn new(id: Uuid, count: u32, threshold: u64, created_at: DateTime<Utc>) -> Self {
        Self {
            magic: BLOB_MAGIC.to_le(),
            version: 1u32.to_le(),
            threshold: threshold.to_le(),
            created_at: created_at.timestamp().to_le(),
            sealed_at: 0,
            entries_count: count.to_le(),
            blob_id: id.into_bytes(),
            _padding: [0u8; 4],
        }
    }

    pub fn magic(&self) -> u32 {
        u32::from_le(self.magic)
    }

    pub fn threshold(&self) -> u64 {
        u64::from_le(self.threshold)
    }

    pub fn created_at(&self) -> io::Result<DateTime<Utc>> {
        let ts = i64::from_le(self.created_at);
        DateTime::from_timestamp_secs(ts)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid blob file header"))
    }

    pub fn sealed_at(&self) -> i64 {
        i64::from_le(self.sealed_at)
    }

    pub fn entries_count(&self) -> u32 {
        u32::from_le(self.entries_count)
    }

    pub fn blob_id(&self) -> Uuid {
        Uuid::from_bytes(self.blob_id)
    }

    pub fn validate(&self, id: Uuid) -> io::Result<()> {
        if self.magic() != BLOB_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid file header magic number",
            ));
        }

        if self.blob_id() != id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file header UUID mismatch",
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
        OBJECT_HEADER_SIZE + self.data_len() as u64
    }
}
