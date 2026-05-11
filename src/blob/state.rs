use std::fs::File;

use crate::blob::{OBJECT_SIZE, types::CompactionMap, utils::AlignedBuffer};

pub trait SegmentState {}
pub trait ImmutableSegment: SegmentState + Default {}

pub struct Active {
    pub segment_size: u64,
    pub entries_count: u32,
    pub write_cursor: u64,
    pub file: File,
    pub io_buffer: Box<AlignedBuffer<{ OBJECT_SIZE as usize }>>,
}

#[derive(Default)]
pub struct Sealed;

#[derive(Default)]
pub struct Compacted {
    pub mappings: Vec<CompactionMap>,
}

impl SegmentState for Active {}
impl SegmentState for Sealed {}
impl SegmentState for Compacted {}

impl ImmutableSegment for Sealed {}
impl ImmutableSegment for Compacted {}
