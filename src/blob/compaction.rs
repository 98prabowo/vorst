use uuid::Uuid;

#[derive(Default)]
pub struct CompactionPlan {
    pub promotion: Vec<Uuid>,
    pub merges: Vec<Vec<Uuid>>,
    pub deferred: Vec<Uuid>,
}

impl CompactionPlan {
    pub fn is_empty(&self) -> bool {
        self.promotion.is_empty() && self.merges.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactedObject {
    pub object_id: Uuid,
    pub offset_new: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedSegment {
    pub segment_id: Uuid,
    pub objects: Vec<CompactedObject>,
    pub removed_segments: Vec<Uuid>,
}
