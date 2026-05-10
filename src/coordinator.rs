use std::io;

use uuid::Uuid;

use crate::{
    blob::{BlobStorage, CompactionPolicy},
    metadata::MetadataStorage,
};

pub struct StorageConfig {
    chunk_size: u64,
    metadata_path: String,
    blob_base_dir: String,
    blob_capacity: u64,
    file_pool_count: u32,
    file_pool_capacity: u32,
}

impl Default for StorageConfig {
    fn default() -> Self {
        let max_fd_open = rlimit::getrlimit(rlimit::Resource::NOFILE)
            .map(|(soft, _hard)| (soft as u32) * 8 / 10)
            .unwrap_or(512);

        let file_pool_count = 16;
        let file_pool_capacity = max_fd_open / file_pool_count;

        Self {
            chunk_size: 4 * 1024 * 1024,
            metadata_path: "./data/metadata/vorst.redb".to_string(),
            blob_base_dir: "./data/blob".to_string(),
            blob_capacity: 64 * 1024 * 1024,
            file_pool_count,
            file_pool_capacity,
        }
    }
}

pub struct StorageCoordinator {
    metadata: MetadataStorage,
    blob: BlobStorage,
    chunk_size: u64,
}

impl StorageCoordinator {
    pub fn open(config: StorageConfig, page_size: u64) -> anyhow::Result<Self> {
        let metadata = MetadataStorage::open(&config.metadata_path)
            .map_err(|e| anyhow::anyhow!("Failed to open metadata: {}", e))?;

        let blob = BlobStorage::open(
            &config.blob_base_dir,
            config.blob_capacity,
            page_size,
            config.file_pool_count,
            config.file_pool_capacity,
            CompactionPolicy::default(),
        )
        .map_err(|e| anyhow::anyhow!("Failed to open blob storage: {}", e))?;

        Ok(Self {
            metadata,
            blob,
            chunk_size: config.chunk_size,
        })
    }

    pub fn put_file(&mut self, file_id: Uuid, mut data: &[u8]) -> anyhow::Result<()> {
        let mut chunks = Vec::new();

        while !data.is_empty() {
            let current_chunk_len = std::cmp::min(data.len() as u64, self.chunk_size) as usize;
            let chunk_data = &data[..current_chunk_len];

            let object_id = Uuid::now_v7();
            let location = self.blob.put(object_id, chunk_data)?;

            data = &data[current_chunk_len..];
            chunks.push(location);
        }

        self.metadata
            .record_multi_chunk_ingestion(file_id, chunks)?;

        Ok(())
    }

    pub fn get_file(&self, file_id: Uuid) -> anyhow::Result<Vec<u8>> {
        let object_ids = self.metadata.get_file_object_ids(file_id)?;

        if object_ids.is_empty() {
            return Ok(vec![]);
        }

        let mut file_data = Vec::new();

        for object_id in object_ids {
            let location = self
                .metadata
                .get_object_location(object_id)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "chunk metadata missing"))?;

            let chunk_data = self
                .blob
                .get(&location.segment_id, location.object_offset.offset)?;

            file_data.extend(chunk_data);
        }

        Ok(file_data)
    }

    pub fn delete_file(&mut self, file_id: Uuid) -> anyhow::Result<()> {
        let object_ids = self.metadata.delete_file(file_id)?;

        for object_id in object_ids {
            self.blob.delete(object_id)?;
        }

        Ok(())
    }
}
