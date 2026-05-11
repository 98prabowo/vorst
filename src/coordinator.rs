use uuid::Uuid;

use crate::{
    blob::{BlobStorage, CompactionPolicy, DATA_SIZE, OBJECT_SIZE, SEGMENT_SIZE},
    error::Result,
    metadata::MetadataStorage,
};

pub struct StorageConfig {
    chunk_size: u64,
    metadata_path: String,
    blob_base_dir: String,
    blob_segment_size: u64,
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
            chunk_size: OBJECT_SIZE,
            metadata_path: "./data/metadata/vorst.redb".to_string(),
            blob_base_dir: "./data/blob".to_string(),
            blob_segment_size: SEGMENT_SIZE,
            file_pool_count,
            file_pool_capacity,
        }
    }
}

pub struct StorageCoordinator {
    metadata: MetadataStorage,
    blob: BlobStorage,
    data_size_max: u64,
}

impl StorageCoordinator {
    pub fn open(config: StorageConfig) -> Result<Self> {
        let metadata = MetadataStorage::open(&config.metadata_path)?;

        let blob = BlobStorage::open(
            &config.blob_base_dir,
            config.blob_segment_size,
            config.file_pool_count,
            config.file_pool_capacity,
            CompactionPolicy::default(),
        )?;

        Ok(Self {
            metadata,
            blob,
            data_size_max: DATA_SIZE,
        })
    }

    pub fn put_file(&mut self, file_id: Uuid, mut data: &[u8]) -> Result<()> {
        let mut chunks = Vec::new();

        while !data.is_empty() {
            let chunk_len = std::cmp::min(data.len() as u64, self.data_size_max) as usize;
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
