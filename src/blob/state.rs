use std::fs::File;

use crate::blob::{OBJECT_SIZE, utils::AlignedBuffer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentStatus {
    Active,
    Sealed,
    Compacted,
}

pub trait SegmentState {
    fn status(&self) -> SegmentStatus;
    fn total_bytes(&self) -> u64;
}

pub trait ImmutableSegment: SegmentState + Default {}

pub struct Active {
    pub segment_size: u64,
    pub entries_count: u32,
    pub write_cursor: u64,
    pub file: File,
    pub io_buffer: Box<AlignedBuffer<{ OBJECT_SIZE as usize }>>,
}

#[derive(Default)]
pub struct Sealed {
    pub total_bytes: u64,
    pub entry_count: u32,
}

#[derive(Default)]
pub struct Compacted {
    pub total_bytes: u64,
}

impl SegmentState for Active {
    fn status(&self) -> SegmentStatus {
        SegmentStatus::Active
    }

    fn total_bytes(&self) -> u64 {
        self.write_cursor
    }
}

impl SegmentState for Sealed {
    fn status(&self) -> SegmentStatus {
        SegmentStatus::Sealed
    }

    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

impl SegmentState for Compacted {
    fn status(&self) -> SegmentStatus {
        SegmentStatus::Compacted
    }

    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

impl ImmutableSegment for Sealed {}
impl ImmutableSegment for Compacted {}
