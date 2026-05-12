use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::blob::state::SegmentStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectLocation {
    pub segment_id: Uuid,
    pub object_offset: ObjectOffset,
    pub length: u32,
    pub checksum: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectOffset {
    pub object_id: Uuid,
    pub offset: u64,
    pub flags: u16,
}

pub struct SegmentStats {
    pub id: Uuid,
    pub total_bytes: u64,
    pub live_bytes: u64,
    pub entry_count: u32,
    pub status: SegmentStatus,
    pub created_at: DateTime<Utc>,
}

impl SegmentStats {
    pub fn fragmentation(&self) -> f32 {
        if self.total_bytes == 0 {
            return 0.0;
        }
        self.total_bytes.saturating_sub(self.live_bytes) as f32 / self.total_bytes as f32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestedObject {
    pub offset: u64,
    pub length: u32,
    pub checksum: u32,
}
