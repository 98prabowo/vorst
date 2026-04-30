use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::{Path, PathBuf},
};

use bytemuck::{Pod, Zeroable};

use crate::sys::preallocate;

// MARK: - Constants

const BLOB_THRESHOLD: u64 = 2 * 1024 * 1024 * 1024;
const MAGIC_NUMBER: u32 = 0xDEADBEEF;
const PAGE_SIZE: u64 = 4096;
const SHARD_COUNT: u64 = 100;

// MARK: - Models

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct BlobEntryHeader {
    object_id: u64,
    magic: u32,
    data_len: u32,
    checksum: u32,
    _padding: u32,
}

pub struct OffsetMap {
    pub object_id: u64,
    pub old_offset: u64,
    pub new_offset: u64,
}

// MARK: - State Machine

pub trait BlobState {}

pub struct Active;
pub struct Sealed;
pub struct Optimized {
    pub mapping: Vec<OffsetMap>,
}

impl BlobState for Active {}
impl BlobState for Sealed {}
impl BlobState for Optimized {}

// MARK: - Core Container

struct Blob<S: BlobState> {
    id: u64,
    path: PathBuf,
    file: File,
    pub state: S,
}

impl<S: BlobState> Blob<S> {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn compute_shard_path<P>(base_dir: P, blob_id: u64) -> PathBuf
    where
        P: AsRef<Path>,
    {
        let shard = blob_id % SHARD_COUNT;
        base_dir.as_ref().join(format!("{:02}", shard))
    }
}

impl Blob<Active> {
    pub fn new<P>(id: u64, base_dir: P, threshold: u64) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let shard_dir = Self::compute_shard_path(base_dir, id);
        fs::create_dir_all(&shard_dir)?;

        let filename = format!("v-{0:16}.blob", id);
        let path = shard_dir.join(filename);

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;

        preallocate(&file, threshold)?;

        Ok(Self {
            id,
            path,
            file,
            state: Active,
        })
    }

    fn new_for_compaction<P>(id: u64, base_dir: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let shard_dir = Self::compute_shard_path(base_dir, id);
        fs::create_dir_all(&shard_dir)?;

        let filename = format!("v-{0:16}.blob", id);
        let path = shard_dir.join(filename);

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&path)?;

        Ok(Self {
            id,
            path,
            file,
            state: Active,
        })
    }

    pub fn ingest(&mut self, id: u64, data: &[u8]) -> io::Result<u64> {
        let offset = self.file.stream_position()?;
        let header = BlobEntryHeader {
            object_id: id,
            magic: MAGIC_NUMBER,
            data_len: data.len() as u32,
            checksum: Hasher::hash(data.len(), data),
            _padding: 0,
        };

        let total_len = size_of::<BlobEntryHeader>() + data.len();
        let padded_len = align_to_page(total_len as u64) as usize;

        let mut entry = Vec::with_capacity(padded_len);
        entry.extend_from_slice(bytemuck::bytes_of(&header));
        entry.extend_from_slice(data);
        entry.resize(padded_len, 0);

        self.file.write_all(&entry)?;
        self.file.sync_data()?;

        Ok(offset)
    }

    pub fn seal<P>(mut self, base_dir: P) -> io::Result<Blob<Sealed>>
    where
        P: AsRef<Path>,
    {
        let shard_dir = Self::compute_shard_path(base_dir, self.id);
        fs::create_dir_all(&shard_dir)?;

        let file_name = self
            .path
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid path"))?;
        let sealed_path = shard_dir.join(file_name);

        let final_size = self.file.stream_position()?;
        self.file.set_len(final_size)?;
        self.file.sync_all()?;

        fs::rename(&self.path, &sealed_path)?;
        let file = File::open(&sealed_path)?;

        Ok(Blob {
            id: self.id,
            path: sealed_path,
            file,
            state: Sealed,
        })
    }
}

impl Blob<Sealed> {
    pub fn optimize<P>(self, base_dir: P, threshold: u64) -> io::Result<Blob<Optimized>>
    where
        P: AsRef<Path>,
    {
        let mut ingestor = Blob::<Active>::new_for_compaction(self.id, &base_dir)?;
        let mut mapping = Vec::new();

        let mut source_file = &self.file;
        let file_len = source_file.metadata()?.len();
        let mut cursor = 0;

        while cursor < file_len {
            source_file.seek(io::SeekFrom::Start(cursor))?;

            let mut header_bytes = [0u8; size_of::<BlobEntryHeader>()];
            if source_file.read_exact(&mut header_bytes).is_err() {
                break; // End of file or partial write
            }

            let header: &BlobEntryHeader = bytemuck::from_bytes(&header_bytes);

            if header.magic != MAGIC_NUMBER {
                cursor = align_to_page(cursor + 1);
                continue;
            }

            let mut data = vec![0u8; header.data_len as usize];
            if source_file.read_exact(&mut data).is_err() {
                break; // Partial write
            }

            if header.checksum == Hasher::hash(data.len(), &data) {
                let new_offset = ingestor.ingest(header.object_id, &data)?;
                mapping.push(OffsetMap {
                    object_id: header.object_id,
                    old_offset: cursor,
                    new_offset,
                });
            }

            let entry_total_size = size_of::<BlobEntryHeader>() as u64 + header.data_len as u64;
            cursor = align_to_page(cursor + entry_total_size);
        }

        let Blob {
            path: opt_path,
            file: ingestor_file,
            ..
        } = ingestor;

        ingestor_file.sync_all()?;
        drop(ingestor_file);

        let opt_file = File::open(&opt_path)?;

        Ok(Blob {
            id: self.id,
            path: opt_path,
            file: opt_file,
            state: Optimized { mapping },
        })
    }
}

// MARK: - Utilities

struct Hasher;

impl Hasher {
    fn hash(data_len: usize, data: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&(data_len as u32).to_le_bytes());
        hasher.update(data);
        hasher.finalize()
    }
}

#[inline(always)]
const fn align_to_page(size: u64) -> u64 {
    let page_mask = PAGE_SIZE - 1;
    (size + page_mask) & !page_mask
}
