use std::path::PathBuf;

use uuid::Uuid;

use crate::blob::{Blob, Compacted};

pub struct ObjectOffset {
    pub object_id: u64,
    pub offset: u64,
    pub flags: u16,
}

pub struct CompactionMap {
    pub object_id: u64,
    pub old_blob_id: Uuid,
    pub old_offset: u64,
    pub new_offset: u64,
}

pub struct CompactedBlob {
    pub new_blob: Blob<Compacted>,
    pub removed_blob_ids: Vec<Uuid>,
    pub removed_paths: Vec<PathBuf>,
}
