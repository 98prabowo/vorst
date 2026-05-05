use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use bytemuck::{Pod, Zeroable};
use uuid::Uuid;

use crate::sys::preallocate;

// MARK: - Constants

const BLOB_PREFIX: &str = "vb-";
const BLOB_EXTENSION: &str = ".blob";
const MAGIC_NUMBER: u32 = 0x56424c42; // "VBLB"
const INITIAL_IO_BUFFER_SIZE: usize = 1024 * 1024;
const HEADER_SIZE: u64 = size_of::<BlobEntryHeader>() as u64;

pub const FLAG_NONE: u16 = 0x0000;
pub const FLAG_TOMBSTONE: u16 = 0x0001;
pub const FLAG_COMPRESSED: u16 = 0x0002;
pub const FLAG_ENCRYPTED: u16 = 0x0002;

// MARK: - Models

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct BlobEntryHeader {
    magic: u32,
    version: u16,
    flags: u16,
    data_len: u32,
    checksum: u32,
    object_id: u64,
}

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
    pub new_blob: Blob<Compacted>,
    pub removed_blob_ids: Vec<Uuid>,
}

// MARK: - State Machine

pub trait BlobState {}
pub trait ImmutableBlob: BlobState {}

pub struct Active;
pub struct Sealed;
pub struct Compacted {
    pub mappings: Vec<CompactionMap>,
}

impl BlobState for Active {}
impl BlobState for Sealed {}
impl BlobState for Compacted {}

impl ImmutableBlob for Sealed {}
impl ImmutableBlob for Compacted {}

// MARK: - Storage

pub struct Blob<S: BlobState> {
    id: Uuid,
    path: PathBuf,
    file: File,
    page_size: u64,
    threshold: u64,
    pub state: S,
}

impl<S: BlobState> Blob<S> {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn compute_shard_path<P>(base_dir: P, id: Uuid) -> PathBuf
    where
        P: AsRef<Path>,
    {
        let uuid_str = id.to_string();
        let shard = &uuid_str[uuid_str.len() - 2..];
        base_dir.as_ref().join(shard)
    }

    pub fn generate_filename(uuid: &Uuid) -> String {
        format!("{}{}{}", BLOB_PREFIX, uuid, BLOB_EXTENSION)
    }

    pub fn read_entry(&mut self, offset: u64) -> io::Result<(BlobEntryHeader, Vec<u8>)> {
        let file_len = self.file.metadata()?.len();

        if offset + HEADER_SIZE > file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "offset out of bounds",
            ));
        }

        self.file.seek(SeekFrom::Start(offset))?;

        let mut header_bytes = [0u8; size_of::<BlobEntryHeader>()];
        self.file.read_exact(&mut header_bytes)?;

        let header: BlobEntryHeader = *bytemuck::from_bytes(&header_bytes);

        if header.magic != MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic number",
            ));
        }

        if offset + HEADER_SIZE + header.data_len as u64 > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header claims more data than file contains",
            ));
        }

        let mut data = vec![0u8; header.data_len as usize];
        self.file.read_exact(&mut data)?;

        if header.checksum != Hasher::hash(&data) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checksum mismatch",
            ));
        }

        Ok((header, data))
    }
}

impl<S: ImmutableBlob> Blob<S> {
    pub fn scan_offsets(&mut self) -> io::Result<Vec<ObjectOffset>> {
        let mut index = Vec::new();
        let file_len = self.file.metadata()?.len();
        let mut cursor = 0;

        while cursor + HEADER_SIZE <= file_len {
            self.file.seek(SeekFrom::Start(cursor))?;

            let mut header_bytes = [0u8; size_of::<BlobEntryHeader>()];
            if self.file.read_exact(&mut header_bytes).is_err() {
                break;
            }

            let header: &BlobEntryHeader = bytemuck::from_bytes(&header_bytes);

            if header.magic != MAGIC_NUMBER {
                let next_page = align_to_page(cursor + 1, self.page_size);
                if next_page >= file_len {
                    break;
                }
                cursor = next_page;
                continue;
            }

            let total_entry_size = HEADER_SIZE + header.data_len as u64;
            if cursor + total_entry_size > file_len {
                break; // Corrupted header claims more data than exists.
            }

            index.push(ObjectOffset {
                object_id: header.object_id,
                offset: cursor,
                flags: header.flags,
            });
            cursor = align_to_page(cursor + total_entry_size, self.page_size);
        }

        Ok(index)
    }
}

impl Blob<Active> {
    pub fn new<P>(base_dir: P, threshold: u64, page_size: u64) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let id = Uuid::now_v7();
        let active_dir = Self::compute_shard_path(base_dir, id);
        fs::create_dir_all(&active_dir)?;

        let filename = Self::generate_filename(&id);
        let path = active_dir.join(format!("{filename}.tmp"));

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
            page_size,
            threshold,
            state: Active,
        })
    }

    pub fn delete(&mut self, id: u64) -> io::Result<u64> {
        self.ingest_with_flags(id, &[], FLAG_TOMBSTONE)
    }

    pub fn ingest(&mut self, id: u64, data: &[u8]) -> io::Result<u64> {
        self.ingest_with_flags(id, data, FLAG_NONE)
    }

    fn ingest_with_flags(&mut self, id: u64, data: &[u8], flags: u16) -> io::Result<u64> {
        let offset = self.file.stream_position()?;

        let total_len = HEADER_SIZE + data.len() as u64;
        let padded_len = align_to_page(total_len, self.page_size) as usize;

        if offset + padded_len as u64 > self.threshold {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                format!(
                    "blob capacity reached. offset {} + needed {} > threshold {}",
                    offset, padded_len, self.threshold
                ),
            ));
        }

        let header = BlobEntryHeader {
            magic: MAGIC_NUMBER,
            version: 1,
            flags,
            data_len: data.len() as u32,
            object_id: id,
            checksum: Hasher::hash(data),
        };

        let mut entry = Vec::with_capacity(padded_len);
        entry.extend_from_slice(bytemuck::bytes_of(&header));
        entry.extend_from_slice(data);
        entry.resize(padded_len, 0);

        self.file.write_all(&entry)?;

        Ok(offset)
    }

    pub fn seal<P>(mut self, base_dir: P) -> io::Result<Blob<Sealed>>
    where
        P: AsRef<Path>,
    {
        let final_size = self.file.stream_position()?;
        self.file.set_len(final_size)?; // Trim preallocation
        self.file.sync_all()?;

        let sealed_dir = Self::compute_shard_path(base_dir, self.id);
        fs::create_dir_all(&sealed_dir)?;

        let file_name = self
            .path
            .file_stem()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid path"))?;
        let sealed_path = sealed_dir.join(file_name);

        fs::rename(&self.path, &sealed_path)?;
        let file = File::open(&sealed_path)?;

        let dir = File::open(&sealed_dir)?;
        dir.sync_all()?;

        Ok(Blob {
            id: self.id,
            path: sealed_path,
            file,
            page_size: self.page_size,
            threshold: self.threshold,
            state: Sealed,
        })
    }
}

// MARK: - Compaction

pub trait BlobCompactable {
    fn compact<P, F>(
        self,
        base_dir: P,
        threshold: u64,
        page_size: u64,
        is_latest: F,
    ) -> io::Result<CompactedBlob>
    where
        P: AsRef<Path>,
        F: Fn(u64, Uuid, u64) -> bool;
}

impl<S: ImmutableBlob> BlobCompactable for Vec<Blob<S>> {
    fn compact<P, F>(
        self,
        base_dir: P,
        threshold: u64,
        page_size: u64,
        is_latest: F,
    ) -> io::Result<CompactedBlob>
    where
        P: AsRef<Path>,
        F: Fn(u64, Uuid, u64) -> bool,
    {
        let mut new_blob = Blob::<Active>::new(&base_dir, threshold, page_size)?;

        let mut mappings = Vec::new();
        let mut removed_ids = Vec::new();
        let mut io_buf = Vec::with_capacity(INITIAL_IO_BUFFER_SIZE);

        for mut source in self {
            let file_len = source.file.metadata()?.len();
            let mut cursor = 0;

            removed_ids.push(source.id);

            while cursor + HEADER_SIZE <= file_len {
                source.file.seek(SeekFrom::Start(cursor))?;

                let mut header_bytes = [0u8; size_of::<BlobEntryHeader>()];
                if source.file.read_exact(&mut header_bytes).is_err() {
                    break; // End of file or partial write
                }

                let header: &BlobEntryHeader = bytemuck::from_bytes(&header_bytes);

                if header.magic != MAGIC_NUMBER {
                    let next_page = align_to_page(cursor + 1, page_size);
                    if next_page >= file_len {
                        break;
                    }
                    cursor = next_page;
                    continue;
                }

                let entry_footprint =
                    align_to_page(HEADER_SIZE + header.data_len as u64, page_size);
                let current_pos = new_blob.file.stream_position()?;

                if current_pos + entry_footprint > threshold {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Compaction overflow: blob {} reached threshold",
                            new_blob.id
                        ),
                    ));
                }

                let data_len = header.data_len as usize;
                if data_len > io_buf.capacity() {
                    io_buf.reserve(data_len - io_buf.capacity());
                }

                let is_not_tombstone = (header.flags & FLAG_TOMBSTONE) == 0;
                if is_not_tombstone {
                    let is_current = is_latest(header.object_id, source.id, cursor);
                    if is_current {
                        io_buf.clear();

                        let mut reader = (&mut source.file).take(data_len as u64);
                        reader.read_to_end(&mut io_buf)?;

                        if header.checksum == Hasher::hash(&io_buf) {
                            let new_offset = new_blob.ingest(header.object_id, &io_buf)?;
                            mappings.push(CompactionMap {
                                object_id: header.object_id,
                                old_blob_id: source.id,
                                old_offset: cursor,
                                new_offset,
                            });
                        }
                    }
                }

                let entry_total_size = HEADER_SIZE + header.data_len as u64;
                cursor = align_to_page(cursor + entry_total_size, page_size);
            }
        }

        let sealed_blob = new_blob.seal(&base_dir)?;

        Ok(CompactedBlob {
            new_blob: Blob {
                id: sealed_blob.id,
                path: sealed_blob.path,
                file: sealed_blob.file,
                page_size: sealed_blob.page_size,
                threshold: sealed_blob.threshold,
                state: Compacted { mappings },
            },
            removed_blob_ids: removed_ids,
        })
    }
}

// MARK: - Utilities

struct Hasher;

impl Hasher {
    fn hash(data: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&(data.len() as u32).to_le_bytes());
        hasher.update(data);
        hasher.finalize()
    }
}

#[inline(always)]
const fn align_to_page(size: u64, page_size: u64) -> u64 {
    let page_mask = page_size - 1;
    (size + page_mask) & !page_mask
}
