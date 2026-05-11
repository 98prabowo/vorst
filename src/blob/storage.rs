use std::{
    any::TypeId,
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use uuid::Uuid;

use crate::blob::{
    // compaction::BlobCompactable,
    cache::FileCachePool,
    error::{Error, Result},
    format::{FLAG_NONE, FLAG_TOMBSTONE, OBJECT_HEADER_SIZE, ObjectHeader},
    segment::Segment,
    state::{Active, Compacted, ImmutableSegment, Sealed},
    types::{
        CompactedSegment, CompactionPlan, CompactionPolicy, IngestedObject, ObjectLocation,
        ObjectOffset,
    },
    utils::align_to_page,
};

const ACTIVE_DIR: &str = "ingestion";
const SEALED_DIR: &str = "sealed";
const COMPACTED_DIR: &str = "compacted";

const SEGMENT_PREFIX: &str = "segment-";
const SEGMENT_EXTENSION: &str = ".blob";

pub struct BlobStorage {
    layout: StorageLayout,
    active: Option<Segment<Active>>,
    sealed: HashMap<Uuid, Segment<Sealed>>,
    compacted: HashMap<Uuid, Segment<Compacted>>,
    file_cache: FileCachePool,
    capacity: u64,
    page_size: u64,
    compaction_policy: CompactionPolicy,
}

// MARK: - Open

impl BlobStorage {
    pub fn open<P>(
        base_dir: P,
        capacity: u64,
        page_size: u64,
        file_pool_count: u32,
        file_pool_capacity: u32,
        compaction_policy: CompactionPolicy,
    ) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let layout = StorageLayout::new(base_dir);
        layout.initialize()?;

        let file_cache = FileCachePool::new(file_pool_count as usize, file_pool_capacity as usize)?;

        let compacted = Self::hydrate_descriptors::<Compacted>(&layout, page_size)?;
        let mut sealed = Self::hydrate_descriptors::<Sealed>(&layout, page_size)?;

        let mut active_paths = Self::scan_segments(layout.active_dir(), "tmp")?;
        active_paths.sort_by_key(|p| Self::path_id(p).unwrap_or_default());

        let mut active = None;

        if let Some(last_path) = active_paths.pop() {
            let id = Self::path_id(&last_path)?;
            let segment = Segment::<Active>::open(id, last_path, page_size)?;
            active = Some(segment);

            for zombie_path in active_paths {
                let id = Self::path_id(&zombie_path)?;
                let zombie = Segment::<Active>::open(id, zombie_path, page_size)?;
                let mut sealed_segment = zombie.seal()?;

                let sealed_path = layout.path_for(id).sealed().build();
                let shard = layout.path_for(id).sealed().shard_dir();
                fs::create_dir_all(shard)?;
                fs::rename(&sealed_segment.path, &sealed_path)?;

                sealed_segment.path = sealed_path;
                sealed.insert(id, sealed_segment);
            }
        }

        Ok(Self {
            layout,
            active,
            sealed,
            compacted,
            file_cache,
            capacity,
            page_size,
            compaction_policy,
        })
    }

    fn hydrate_descriptors<S: ImmutableSegment + 'static>(
        layout: &StorageLayout,
        page_size: u64,
    ) -> Result<HashMap<Uuid, Segment<S>>> {
        let dir = if TypeId::of::<S>() == TypeId::of::<Compacted>() {
            layout.compacted_dir()
        } else {
            layout.sealed_dir()
        };

        let paths = Self::scan_segments(dir, "blob")?;
        let mut map = HashMap::with_capacity(paths.len());

        for path in paths {
            let id = Self::path_id(&path)?;
            let segment = Segment::<S>::open_readonly(id, path, page_size)?;
            map.insert(id, segment);
        }

        Ok(map)
    }

    fn scan_segments<P>(root: P, extension: &str) -> Result<Vec<PathBuf>>
    where
        P: AsRef<Path>,
    {
        let mut paths = Vec::new();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                for sub_entry in fs::read_dir(path)? {
                    let sub_path = sub_entry?.path();
                    if sub_path.extension().is_some_and(|ext| ext == extension) {
                        paths.push(sub_path);
                    }
                }
            } else {
                if path.extension().is_some_and(|ext| ext == extension) {
                    paths.push(path);
                }
            }
        }
        Ok(paths)
    }

    fn path_id<P>(path: P) -> Result<Uuid>
    where
        P: AsRef<Path>,
    {
        let file_name = path
            .as_ref()
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| Error::Internal("invalid filename".to_string()))?;

        let id_str = file_name
            .strip_prefix(SEGMENT_PREFIX)
            .and_then(|s| s.split('.').next())
            .ok_or_else(|| Error::Internal(format!("format mismatch: {}", file_name)))?;

        id_str
            .parse::<Uuid>()
            .map_err(|_| Error::Internal(format!("invalid UUID: {}", id_str)))
    }
}

// MARK: - Put

impl BlobStorage {
    pub fn put(&mut self, file_id: Uuid, object_id: Uuid, data: &[u8]) -> Result<ObjectLocation> {
        let total_needed = align_to_page(
            OBJECT_HEADER_SIZE as u64 + data.len() as u64,
            self.page_size,
        );
        if total_needed > self.capacity {
            return Err(Error::StorageFull {
                needed: total_needed - self.capacity,
            });
        }

        if self.active.is_none() {
            let id = Uuid::now_v7();
            let path = self.layout.path_for(id).active().build();
            self.active = Some(Segment::<Active>::new(
                id,
                path,
                self.capacity,
                self.page_size,
            )?);
        }

        let active = self
            .active
            .as_mut()
            .ok_or_else(|| Error::Internal("active segment lost at ingestion".to_string()))?;

        match active.ingest(file_id, object_id, data) {
            Ok(IngestedObject {
                offset,
                length,
                checksum,
            }) => {
                let object_location = ObjectLocation {
                    segment_id: active.id(),
                    object_offset: ObjectOffset {
                        object_id,
                        offset,
                        flags: FLAG_NONE,
                    },
                    length,
                    checksum,
                };

                Ok(object_location)
            }
            Err(Error::StorageFull { .. }) => {
                self.rotate_and_put(file_id, object_id, data, FLAG_NONE)
            }
            Err(e) => Err(e),
        }
    }

    fn rotate_and_put(
        &mut self,
        file_id: Uuid,
        object_id: Uuid,
        data: &[u8],
        flags: u16,
    ) -> Result<ObjectLocation> {
        let old_active = self
            .active
            .take()
            .ok_or_else(|| Error::Internal("active segment lost at rotation".to_string()))?;

        let old_id = old_active.id();
        let sealed_path = self.layout.path_for(old_id).sealed().build();
        let sealed_shard = self.layout.path_for(old_id).sealed().shard_dir();

        if !sealed_shard.exists() {
            fs::create_dir_all(sealed_shard)?;
        }

        let mut sealed = old_active.seal()?;

        fs::rename(&sealed.path, &sealed_path)?;
        sealed.path = sealed_path;

        self.sealed.insert(sealed.id(), sealed);

        let new_id = Uuid::now_v7();
        let new_path = self.layout.path_for(new_id).active().build();

        let mut next_active =
            Segment::<Active>::new(new_id, new_path, self.capacity, self.page_size)?;

        let IngestedObject {
            offset,
            length,
            checksum,
        } = next_active.ingest(file_id, object_id, data)?;

        let object_location = ObjectLocation {
            segment_id: new_id,
            object_offset: ObjectOffset {
                object_id,
                offset,
                flags,
            },
            length,
            checksum,
        };

        self.active = Some(next_active);

        Ok(object_location)
    }
}

// MARK: - Get

impl BlobStorage {
    pub fn get(&self, segment_id: &Uuid, offset: u64) -> Result<Vec<u8>> {
        if let Some(active) = self.active.as_ref()
            && active.id() == *segment_id
        {
            let (header, data) = active.read_object_at(&active.state.file, offset)?;
            return self.process_header_result(header, data);
        }

        if let Some(segment) = self.sealed.get(segment_id) {
            let cached_file = self.file_cache.get(*segment_id, &segment.path)?;
            let (header, data) = segment.read_object_at(&cached_file, offset)?;
            return self.process_header_result(header, data);
        }

        if let Some(segment) = self.compacted.get(segment_id) {
            let cached_file = self.file_cache.get(*segment_id, &segment.path)?;
            let (header, data) = segment.read_object_at(&cached_file, offset)?;
            return self.process_header_result(header, data);
        }

        Err(Error::Internal("segment not found".to_string()))
    }

    fn process_header_result(&self, header: ObjectHeader, data: Vec<u8>) -> Result<Vec<u8>> {
        if (header.flags() & FLAG_TOMBSTONE) != 0 {
            return Err(Error::ObjectDeleted {
                id: header.object_id(),
            });
        }
        Ok(data)
    }
}

// MARK: - Delete

impl BlobStorage {
    pub fn delete(&mut self, file_id: Uuid) -> Result<ObjectLocation> {
        if self.active.is_none() {
            let id = Uuid::now_v7();
            let path = self.layout.path_for(id).active().build();
            self.active = Some(Segment::<Active>::new(
                id,
                path,
                self.capacity,
                self.page_size,
            )?);
        }

        let active = self
            .active
            .as_mut()
            .ok_or_else(|| Error::Internal("active segment lost at deletion".to_string()))?;

        let ingested = match active.delete(file_id) {
            Ok(ingested) => ingested,
            Err(Error::StorageFull { .. }) => {
                return self.rotate_and_put(file_id, Uuid::nil(), &[], FLAG_TOMBSTONE);
            }
            Err(e) => return Err(e),
        };

        Ok(ObjectLocation {
            segment_id: active.id(),
            object_offset: ObjectOffset {
                object_id: Uuid::nil(),
                offset: ingested.offset,
                flags: FLAG_TOMBSTONE,
            },
            length: ingested.length,
            checksum: ingested.checksum,
        })
    }
}

// MARK: - Compaction

// impl BlobStorage {
//     fn plan_compaction(&self) -> Option<CompactionPlan> {
//         let sealed_count = self.sealed.len();
//         let total_bytes: u64 = self
//             .sealed
//             .values()
//             .map(|b| b.file.metadata().map(|m| m.len()).unwrap_or(0))
//             .sum();
//
//         let count_trigger = sealed_count >= self.compaction_policy.max_sealed_files;
//         let space_trigger = total_bytes > self.compaction_policy.max_sealed_bytes;
//         if self.sealed.len() < 5 {
//             return None;
//         }
//
//         let candidates = self.sealed.keys().cloned().collect();
//         Some(CompactionPlan { candidates })
//     }
//
//     pub fn prepare_compaction<F>(&mut self, is_latest: F) -> io::Result<Option<CompactedBlob>>
//     where
//         F: Fn(u64, Uuid, u64) -> bool,
//     {
//         let plan = match self.plan_compaction() {
//             Some(plan) => plan,
//             None => return Ok(None),
//         };
//
//         let sources: Vec<Segment<Sealed>> = plan
//             .candidates
//             .iter()
//             .filter_map(|id| self.sealed.remove(id))
//             .collect();
//
//         let result = sources.compact(
//             self.base_dir.join(COMPACTED_DIR),
//             self.threshold,
//             self.page_size,
//             is_latest,
//         )?;
//
//         Ok(Some(result))
//     }
//
//     pub fn commit_compaction(&mut self, result: CompactedBlob) -> io::Result<()> {
//         let new_id = result.new_blob.id();
//         self.compacted.insert(new_id, result.new_blob);
//
//         for path in result.removed_paths {
//             fs::remove_file(path).ok();
//         }
//
//         Ok(())
//     }
// }

pub struct StorageLayout {
    base_dir: PathBuf,
}

impl StorageLayout {
    pub fn new<P>(base_dir: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
        }
    }

    pub fn initialize(&self) -> Result<()> {
        for dir in [ACTIVE_DIR, SEALED_DIR, COMPACTED_DIR] {
            fs::create_dir_all(self.base_dir.join(dir))?;
        }
        Ok(())
    }

    pub fn active_dir(&self) -> PathBuf {
        self.base_dir.join(ACTIVE_DIR)
    }

    pub fn sealed_dir(&self) -> PathBuf {
        self.base_dir.join(SEALED_DIR)
    }

    pub fn compacted_dir(&self) -> PathBuf {
        self.base_dir.join(COMPACTED_DIR)
    }

    pub fn path_for(&self, id: Uuid) -> SegmentPathBuilder {
        SegmentPathBuilder::new(&self.base_dir, id)
    }
}

pub struct SegmentPathBuilder {
    base_dir: PathBuf,
    id: Uuid,
    is_active: bool,
    is_compacted: bool,
}

impl SegmentPathBuilder {
    pub fn new<P>(base_dir: P, id: Uuid) -> SegmentPathBuilder
    where
        P: AsRef<Path>,
    {
        SegmentPathBuilder {
            base_dir: base_dir.as_ref().to_path_buf(),
            id,
            is_active: false,
            is_compacted: false,
        }
    }

    pub fn active(mut self) -> Self {
        self.is_active = true;
        self.is_compacted = false;
        self
    }

    pub fn sealed(mut self) -> Self {
        self.is_active = false;
        self.is_compacted = false;
        self
    }

    pub fn compacted(mut self) -> Self {
        self.is_compacted = true;
        self.is_active = false;
        self
    }

    pub fn shard_dir(self) -> PathBuf {
        let sub_dir = if self.is_compacted {
            COMPACTED_DIR
        } else if self.is_active {
            ACTIVE_DIR
        } else {
            SEALED_DIR
        };

        if self.is_active {
            self.base_dir.join(sub_dir)
        } else {
            let uuid_str = self.id.to_string();
            let shard = &uuid_str[0..2];
            self.base_dir.join(sub_dir).join(shard)
        }
    }

    pub fn build(self) -> PathBuf {
        let mut file_name = format!("{}{}{}", SEGMENT_PREFIX, self.id, SEGMENT_EXTENSION);

        if self.is_active {
            file_name.push_str(".tmp");
        }

        self.shard_dir().join(file_name)
    }
}
