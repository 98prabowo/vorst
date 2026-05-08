use crate::blob::types::CompactionMap;

pub trait SegmentState: Default {}
pub trait ImmutableSegment: SegmentState {}

#[derive(Default)]
pub struct Active {
    pub capacity: u64,
    pub entries_count: u32,
    pub write_cursor: u64,
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
