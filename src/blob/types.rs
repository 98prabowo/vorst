use std::path::PathBuf;

use chrono::Duration;
use uuid::Uuid;

use crate::blob::{segment::Segment, state::Compacted};

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
    pub new_blob: Segment<Compacted>,
    pub removed_blob_ids: Vec<Uuid>,
    pub removed_paths: Vec<PathBuf>,
}

pub struct CompactionPlan {
    pub candidates: Vec<Uuid>,
}

pub struct CompactionPolicy {
    pub max_sealed_files: usize,
    pub max_sealed_bytes: u64,
    pub tombstone_threshold: f32,
    pub min_interval: Duration,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            max_sealed_files: 10,
            max_sealed_bytes: 20 * 1024 * 1024 * 1024,
            tombstone_threshold: 0.3,
            min_interval: Duration::seconds(3600),
        }
    }
}
