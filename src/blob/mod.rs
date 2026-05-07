mod format;
mod state;

pub mod compaction;
pub mod core;
pub mod types;

pub use compaction::BlobCompactable;
pub use core::Blob;
pub use state::*;
pub use types::*;

use std::{
    collections::HashMap,
    fs, io,
    path::{Path, PathBuf},
};

use uuid::Uuid;

use crate::blob::{
    compaction::{CompactionPlan, CompactionPolicy},
    core::{FLAG_NONE, FLAG_TOMBSTONE, align_to_page},
    format::{OBJECT_HEADER_SIZE, ObjectHeader},
};

const ACTIVE_DIR: &str = "ingestion";
const SEALED_DIR: &str = "sealed";
const COMPACTED_DIR: &str = "compacted";

pub struct BlobStorage {
    base_dir: PathBuf,
    active: Option<Blob<Active>>,
    sealed: HashMap<Uuid, Blob<Sealed>>,
    compacted: HashMap<Uuid, Blob<Compacted>>,
    threshold: u64,
    page_size: u64,
    compaction_policy: CompactionPolicy,
}

// MARK: - Open

impl BlobStorage {
    pub fn open<P>(
        base_dir: P,
        capacity: u64,
        page_size: u64,
        compaction_policy: CompactionPolicy,
    ) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let root = base_dir.as_ref();

        for dir in [ACTIVE_DIR, SEALED_DIR, COMPACTED_DIR] {
            fs::create_dir_all(root.join(dir))?;
        }

        let active = fs::read_dir(root.join(ACTIVE_DIR))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|ext| ext == "tmp"))
            .map(|path| Blob::<Active>::open(path, page_size))
            .transpose()?;

        let sealed_root = root.join(SEALED_DIR);
        let sealed_paths = Self::discover_sharded_blob_paths(sealed_root, "blob")?;
        let mut sealed = HashMap::with_capacity(sealed_paths.len());
        for path in sealed_paths {
            let blob = Blob::<Sealed>::open_readonly(path, page_size)?;
            sealed.insert(blob.id(), blob);
        }

        let compacted_root = root.join(COMPACTED_DIR);
        let compacted_paths = Self::discover_sharded_blob_paths(compacted_root, "blob")?;
        let mut compacted = HashMap::with_capacity(compacted_paths.len());
        for path in compacted_paths {
            let blob = Blob::<Compacted>::open_readonly(path, page_size)?;
            compacted.insert(blob.id(), blob);
        }

        Ok(Self {
            base_dir: root.to_path_buf(),
            active,
            sealed,
            compacted,
            threshold: capacity,
            page_size,
            compaction_policy,
        })
    }

    fn discover_sharded_blob_paths<P>(root: P, extension: &str) -> io::Result<Vec<PathBuf>>
    where
        P: AsRef<Path>,
    {
        let paths = fs::read_dir(root)?
            .filter_map(|res| res.ok())
            .filter(|e| e.path().is_dir())
            .flat_map(|shard| fs::read_dir(shard.path()).into_iter().flatten())
            .filter_map(|res| res.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == extension))
            .collect();
        Ok(paths)
    }
}

// MARK: - Put

impl BlobStorage {
    pub fn put(&mut self, object_id: u64, data: &[u8]) -> io::Result<ObjectOffset> {
        let total_needed = align_to_page(OBJECT_HEADER_SIZE + data.len() as u64, self.page_size);
        if total_needed > self.threshold {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "data exceeds maximum blob threshold",
            ));
        }

        if self.active.is_none() {
            self.active = Some(Blob::<Active>::new(
                self.base_dir.join(ACTIVE_DIR),
                self.threshold,
                self.page_size,
            )?);
        }

        let active = self
            .active
            .as_mut()
            .ok_or_else(|| io::Error::other("active blob lost during initialization"))?;

        let offset = match active.ingest(object_id, data) {
            Ok(off) => off,
            Err(e) if e.kind() == io::ErrorKind::StorageFull => {
                return self.cycle_and_ingest(object_id, data, FLAG_NONE);
            }
            Err(e) => return Err(e),
        };

        Ok(ObjectOffset {
            object_id,
            offset,
            flags: FLAG_NONE,
        })
    }

    fn cycle_and_ingest(
        &mut self,
        object_id: u64,
        data: &[u8],
        flags: u16,
    ) -> io::Result<ObjectOffset> {
        let old_active = self
            .active
            .take()
            .ok_or_else(|| io::Error::other("active blob missing during cycle"))?;

        let sealed_dir = self.base_dir.join(SEALED_DIR);
        let sealed = old_active.seal(sealed_dir)?;
        self.sealed.insert(sealed.id(), sealed);

        let mut next_active = Blob::<Active>::new(
            self.base_dir.join(ACTIVE_DIR),
            self.threshold,
            self.page_size,
        )?;

        let offset = next_active.ingest(object_id, data)?;
        self.active = Some(next_active);

        Ok(ObjectOffset {
            object_id,
            offset,
            flags,
        })
    }
}

// MARK: - Get

impl BlobStorage {
    pub fn get(&self, blob_id: &Uuid, offset: u64) -> io::Result<Vec<u8>> {
        if let Some(active) = self.active.as_ref()
            && active.id() == *blob_id
        {
            let (header, data) = active.read_entry(offset)?;
            return self.process_header_result(header, data);
        }

        if let Some(blob) = self.sealed.get(blob_id) {
            let (header, data) = blob.read_entry(offset)?;
            return self.process_header_result(header, data);
        }

        if let Some(blob) = self.compacted.get(blob_id) {
            let (header, data) = blob.read_entry(offset)?;
            return self.process_header_result(header, data);
        }

        Err(io::Error::new(io::ErrorKind::NotFound, "blob not found"))
    }

    fn process_header_result(&self, header: ObjectHeader, data: Vec<u8>) -> io::Result<Vec<u8>> {
        if (header.flags() & FLAG_TOMBSTONE) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "object was deleted",
            ));
        }
        Ok(data)
    }
}

// MARK: - Delete

impl BlobStorage {
    pub fn delete(&mut self, object_id: u64) -> io::Result<ObjectOffset> {
        if self.active.is_none() {
            self.active = Some(Blob::<Active>::new(
                self.base_dir.join(ACTIVE_DIR),
                self.threshold,
                self.page_size,
            )?);
        }

        let active = self
            .active
            .as_mut()
            .ok_or_else(|| io::Error::other("active blob lost during deletion"))?;

        let offset = match active.delete(object_id) {
            Ok(off) => off,
            Err(e) if e.kind() == io::ErrorKind::StorageFull => {
                return self.cycle_and_ingest(object_id, &[], FLAG_TOMBSTONE);
            }
            Err(e) => return Err(e),
        };

        Ok(ObjectOffset {
            object_id,
            offset,
            flags: FLAG_TOMBSTONE,
        })
    }
}

// MARK: - Compaction

impl BlobStorage {
    fn plan_compaction(&self) -> Option<CompactionPlan> {
        let sealed_count = self.sealed.len();
        let total_bytes: u64 = self
            .sealed
            .values()
            .map(|b| b.file.metadata().map(|m| m.len()).unwrap_or(0))
            .sum();

        let count_trigger = sealed_count >= self.compaction_policy.max_sealed_files;
        let space_trigger = total_bytes > self.compaction_policy.max_sealed_bytes;
        if self.sealed.len() < 5 {
            return None;
        }

        let candidates = self.sealed.keys().cloned().collect();
        Some(CompactionPlan { candidates })
    }

    pub fn prepare_compaction<F>(&mut self, is_latest: F) -> io::Result<Option<CompactedBlob>>
    where
        F: Fn(u64, Uuid, u64) -> bool,
    {
        let plan = match self.plan_compaction() {
            Some(plan) => plan,
            None => return Ok(None),
        };

        let sources: Vec<Blob<Sealed>> = plan
            .candidates
            .iter()
            .filter_map(|id| self.sealed.remove(id))
            .collect();

        let result = sources.compact(
            self.base_dir.join(COMPACTED_DIR),
            self.threshold,
            self.page_size,
            is_latest,
        )?;

        Ok(Some(result))
    }

    pub fn commit_compaction(&mut self, result: CompactedBlob) -> io::Result<()> {
        let new_id = result.new_blob.id();
        self.compacted.insert(new_id, result.new_blob);

        for path in result.removed_paths {
            fs::remove_file(path).ok();
        }

        Ok(())
    }
}
