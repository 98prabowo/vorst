use crate::blob::CompactionMap;

pub trait BlobState: Default {}
pub trait ImmutableBlob: BlobState {}

#[derive(Default)]
pub struct Active {
    pub threshold: u64,
    pub entries_count: u32,
    pub write_cursor: u64,
}

#[derive(Default)]
pub struct Sealed;

#[derive(Default)]
pub struct Compacted {
    pub mappings: Vec<CompactionMap>,
}

impl BlobState for Active {}
impl BlobState for Sealed {}
impl BlobState for Compacted {}

impl ImmutableBlob for Sealed {}
impl ImmutableBlob for Compacted {}
