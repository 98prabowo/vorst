use chrono::Duration;

use crate::blob::{DATA_SIZE, OBJECT_SIZE, SEGMENT_SIZE};

pub struct StorageConfig {
    pub chunk_size: u64,
    pub blob: BlobStorageConfig,
    pub metadata: MetadataStorageConfig,
    pub file_cache: FileCacheConfig,
    pub compaction_policy: CompactionPolicy,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            chunk_size: OBJECT_SIZE,
            blob: BlobStorageConfig::default(),
            metadata: MetadataStorageConfig::default(),
            file_cache: FileCacheConfig::default(),
            compaction_policy: CompactionPolicy::default(),
        }
    }
}

pub struct FileCacheConfig {
    pub shard_count: u32,
    pub shard_capacity: u32,
}

impl Default for FileCacheConfig {
    fn default() -> Self {
        let max_fd_open = rlimit::getrlimit(rlimit::Resource::NOFILE)
            .map(|(soft, _hard)| (soft as u32) * 8 / 10)
            .unwrap_or(512);

        let shard_count = 16;
        let shard_capacity = max_fd_open / shard_count;

        Self {
            shard_count,
            shard_capacity,
        }
    }
}

pub struct BlobStorageConfig {
    pub base_dir: String,
    pub segment_size: u64,
    pub data_size: u64,
}

impl Default for BlobStorageConfig {
    fn default() -> Self {
        Self {
            base_dir: "./data/blob".to_string(),
            segment_size: SEGMENT_SIZE,
            data_size: DATA_SIZE,
        }
    }
}

pub struct MetadataStorageConfig {
    pub path: String,
}

impl Default for MetadataStorageConfig {
    fn default() -> Self {
        Self {
            path: "./data/metadata/vorst.redb".to_string(),
        }
    }
}

pub struct CompactionPolicy {
    pub promotion_fragmentation_max: f32,
    pub merge_fragmentation_min: f32,
    pub segment_fill_ratio: f32,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            promotion_fragmentation_max: 0.02,
            merge_fragmentation_min: 0.0,
            segment_fill_ratio: 0.85,
        }
    }
}
