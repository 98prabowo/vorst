use uuid::Uuid;

use crate::{
    blob::{BlobStorage, CompactionPlan, SegmentStatus},
    config::{CompactionPolicy, StorageConfig},
    error::Result,
    metadata::MetadataStorage,
};

pub struct StorageCoordinator {
    metadata: MetadataStorage,
    blob: BlobStorage,
    blob_segment_size: u64,
    blob_data_size: u64,
    compaction_policy: CompactionPolicy,
}

impl StorageCoordinator {
    pub fn open(config: StorageConfig) -> Result<Self> {
        let metadata = MetadataStorage::open(&config.metadata.path)?;

        let blob = BlobStorage::open(
            &config.blob.base_dir,
            config.blob.segment_size,
            config.file_cache.shard_count,
            config.file_cache.shard_capacity,
        )?;

        Ok(Self {
            metadata,
            blob,
            blob_segment_size: config.blob.segment_size,
            blob_data_size: config.blob.data_size,
            compaction_policy: config.compaction_policy,
        })
    }

    pub fn put_file(&mut self, file_id: Uuid, mut data: &[u8]) -> Result<()> {
        let mut chunks = Vec::new();

        while !data.is_empty() {
            let chunk_len = std::cmp::min(data.len() as u64, self.blob_data_size) as usize;
            let chunk_data = &data[..chunk_len];

            let object_id = Uuid::now_v7();
            let location = self.blob.put(file_id, object_id, chunk_data)?;

            data = &data[chunk_len..];
            chunks.push(location);
        }

        self.metadata
            .record_multi_chunk_ingestion(file_id, chunks)?;

        Ok(())
    }

    pub fn get_file(&self, file_id: Uuid) -> Result<Vec<u8>> {
        let object_ids = self.metadata.get_file_object_ids(file_id)?;

        if object_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut file_data = Vec::new();

        for object_id in object_ids {
            let location = self.metadata.get_object_location(object_id)?;

            let chunk_data = self
                .blob
                .get(&location.segment_id, location.object_offset.offset)?;

            file_data.extend(chunk_data);
        }

        Ok(file_data)
    }

    pub fn delete_file(&mut self, file_id: Uuid) -> Result<()> {
        let _object_ids = self.metadata.delete_file(file_id)?;
        self.blob.delete(file_id)?;
        Ok(())
    }
}

impl StorageCoordinator {
    pub fn prepare_compaction(&self) -> Result<CompactionPlan> {
        let mut plan = CompactionPlan::default();

        let stats = self.blob.get_all_stats(|segment_id, object_id, offset| {
            self.metadata
                .check_liveliness(segment_id, object_id, offset)
                .unwrap_or(false)
        })?;

        let mut candidates = Vec::new();

        for stat in stats {
            if stat.fragmentation() <= self.compaction_policy.promotion_fragmentation_max
                && stat.status == SegmentStatus::Sealed
            {
                plan.promotion.push(stat.id);
                continue;
            }

            if stat.fragmentation() > self.compaction_policy.merge_fragmentation_min {
                candidates.push(stat);
                continue;
            }

            plan.deferred.push(stat.id);
        }

        candidates.sort_by_key(|c| c.live_bytes);

        let mut merge_batch = Vec::new();
        let mut merge_batch_size = 0;

        let batch_size_threshold =
            (self.blob_segment_size as f32 * self.compaction_policy.segment_fill_ratio) as u64;

        for stat in candidates {
            if merge_batch_size + stat.live_bytes > self.blob_segment_size
                && merge_batch_size >= batch_size_threshold
                && !merge_batch.is_empty()
            {
                plan.merges.push(std::mem::take(&mut merge_batch));
                merge_batch_size = 0;
            }

            merge_batch_size += stat.live_bytes;
            merge_batch.push(stat.id);
        }

        if !merge_batch.is_empty() {
            if merge_batch_size >= batch_size_threshold || merge_batch.len() > 1 {
                plan.merges.push(merge_batch);
            } else {
                plan.deferred.extend(merge_batch);
            }
        }

        Ok(plan)
    }

    pub fn run_compaction(&mut self, plan: CompactionPlan) -> Result<()> {
        let compacted_segments =
            self.blob
                .execute_compaction(plan, |segment_id, object_id, offset| {
                    self.metadata
                        .check_liveliness(segment_id, object_id, offset)
                        .unwrap_or(false)
                })?;

        for result in compacted_segments {
            let mut updates = Vec::new();

            for object in result.objects {
                let mut location = self.metadata.get_object_location(object.object_id)?;
                location.segment_id = result.segment_id;
                location.object_offset.offset = object.offset_new;
                updates.push((object.object_id, location));
            }

            self.metadata.update_locations(updates)?;
        }

        Ok(())
    }
}
