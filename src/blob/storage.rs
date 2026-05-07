use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    blob::{
        Active, BlobState, ImmutableBlob, ObjectOffset, Sealed,
        format::{FileHeader, OBJECT_HEADER_SIZE, OBJECT_MAGIC, ObjectHeader},
    },
    sys::preallocate,
};

// MARK: - Constants

pub const BLOB_PREFIX: &str = "vb-";
pub const BLOB_EXTENSION: &str = ".blob";

pub const FLAG_NONE: u16 = 0x0000;
pub const FLAG_TOMBSTONE: u16 = 0x0001;
pub const FLAG_CORRUPTED: u16 = 0x0002;

// MARK: - Storage Implementation

// TODO: Implement `io_uring` support for Linux. While this is a file storage layer, the
// ability to perform high-depth asynchronous reads will be critical when the application
// needs to fetch hundreds of raw files simultaneously (e.g., during a batch export or secondary processing

pub struct Blob<S: BlobState> {
    pub id: Uuid,
    pub path: PathBuf,
    pub file: File,
    pub page_size: u64,
    pub created_at: DateTime<Utc>,
    pub state: S,
}

impl<S: BlobState> Blob<S> {
    pub fn id(&self) -> Uuid {
        self.id
    }

    fn compute_shard_path<P>(base_dir: P, id: Uuid) -> PathBuf
    where
        P: AsRef<Path>,
    {
        let uuid_str = id.to_string();
        let shard = &uuid_str[uuid_str.len() - 2..];
        base_dir.as_ref().join(shard)
    }

    fn generate_filename(uuid: &Uuid) -> String {
        format!("{}{}{}", BLOB_PREFIX, uuid, BLOB_EXTENSION)
    }

    pub fn read_entry(&mut self, offset: u64) -> io::Result<(ObjectHeader, Vec<u8>)> {
        // TODO: Add a "scrubber" utility. Over time, bit rot can occur on disk. We need a
        // background process that iterates through sealed blobs and verifies checksums.

        let file_len = self.file.metadata()?.len();

        if offset + OBJECT_HEADER_SIZE > file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "offset out of bounds",
            ));
        }

        let mut object_header_bytes = AlignedBuffer([0u8; OBJECT_HEADER_SIZE as usize]);
        self.file
            .read_exact_at(&mut object_header_bytes.0, offset)?;

        let object_header: ObjectHeader = *bytemuck::from_bytes(&object_header_bytes.0);

        if object_header.magic() != OBJECT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid object magic number",
            ));
        }

        if offset + object_header.object_size() > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "object claims more data than file contains",
            ));
        }

        let mut data = vec![0u8; object_header.data_len() as usize];
        self.file
            .read_exact_at(&mut data, offset + OBJECT_HEADER_SIZE)?;

        if object_header.checksum() != Hasher::hash(&data) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "object checksum mismatch",
            ));
        }

        // TODO: Performance Optimization - `sendfile(2)` or `mmap`. If this blob storage is
        // serving files over a network later, look into using `sendfile` to bypass user-space
        // copying entirely, or `mmap` for frequently accessed "hot" files.

        Ok((object_header, data))
    }

    pub fn scan_offsets(&mut self) -> io::Result<Vec<ObjectOffset>> {
        // TODO: Sidecar Index Files. Reading the entire blob to reconstruct the index on
        // startup is too slow for large files. We should write a `.index` file (containing
        // `object_id -> offset`) alongside the `.blob` during the seal process.

        let mut index = Vec::new();
        let file_len = self.file.metadata()?.len();
        let mut cursor = self.page_size;

        let mut file_header_bytes = AlignedBuffer([0u8; size_of::<FileHeader>()]);
        self.file.read_exact_at(&mut file_header_bytes.0, 0)?;

        let file_header: &FileHeader = bytemuck::from_bytes(&file_header_bytes.0);
        file_header.validate(self.id)?;

        while cursor + OBJECT_HEADER_SIZE <= file_len {
            let mut object_header_bytes = AlignedBuffer([0u8; size_of::<ObjectHeader>()]);
            if self
                .file
                .read_exact_at(&mut object_header_bytes.0, cursor)
                .is_err()
            {
                break;
            }

            let object_header: &ObjectHeader = bytemuck::from_bytes(&object_header_bytes.0);

            if object_header.magic() != OBJECT_MAGIC {
                let next_page = align_to_page(cursor + 1, self.page_size);
                if next_page >= file_len {
                    break;
                }
                cursor = next_page;
                continue;
            }

            if cursor + object_header.object_size() > file_len {
                break; // Corrupted header claims more data than exists.
            }

            index.push(ObjectOffset {
                object_id: object_header.object_id(),
                offset: cursor,
                flags: object_header.flags(),
            });
            cursor = align_to_page(cursor + object_header.object_size(), self.page_size);
        }

        Ok(index)
    }
}

impl<S: ImmutableBlob> Blob<S> {
    pub fn open_readonly<P>(path: P, page_size: u64) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let path_buf = path.as_ref().to_path_buf();

        let filename = path_buf
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid path"))?;

        let id_str = filename
            .strip_prefix(BLOB_PREFIX)
            .and_then(|s| s.split(".").next())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "filename format mismatch")
            })?;

        let id = Uuid::parse_str(id_str)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UUID"))?;

        let file = File::open(&path_buf)?;

        let mut file_header_bytes = AlignedBuffer([0u8; size_of::<FileHeader>()]);
        file.read_exact_at(&mut file_header_bytes.0, 0)?;

        let file_header: &FileHeader = bytemuck::from_bytes(&file_header_bytes.0);
        file_header.validate(id)?;

        let created_at = file_header.created_at()?;

        Ok(Self {
            id,
            path: path_buf,
            file,
            page_size,
            created_at,
            state: S::default(),
        })
    }
}

impl Blob<Active> {
    pub fn new<P>(base_dir: P, data_capacity: u64, page_size: u64) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let threshold = page_size + data_capacity;
        let now = Utc::now();

        let id = Uuid::now_v7();
        let active_dir = Self::compute_shard_path(base_dir, id);
        fs::create_dir_all(&active_dir)?;

        let filename = Self::generate_filename(&id);
        let path = active_dir.join(format!("{filename}.tmp"));

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        let header = FileHeader::new(id, 0, now);
        file.write_all_at(bytemuck::bytes_of(&header), 0)?;
        file.sync_all()?;

        preallocate(&file, threshold)?;

        Ok(Self {
            id,
            path,
            file,
            page_size,
            created_at: now,
            state: Active {
                threshold,
                entries_count: 0,
                write_cursor: page_size,
            },
        })
    }

    pub fn open<P>(path: P, page_size: u64) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let path_buf = path.as_ref().to_path_buf();

        let filename = path_buf
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid path"))?;

        let id_str = filename
            .strip_prefix(BLOB_PREFIX)
            .and_then(|s| s.strip_suffix(".blob.tmp"))
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "filename format mismatch")
            })?;

        let id = Uuid::parse_str(id_str)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UUID in filename"))?;

        let file = OpenOptions::new().read(true).write(true).open(&path_buf)?;

        let mut file_header_bytes = AlignedBuffer([0u8; size_of::<FileHeader>()]);
        file.read_exact_at(&mut file_header_bytes.0, 0)?;
        let file_header: &FileHeader = bytemuck::from_bytes(&file_header_bytes.0);
        file_header.validate(id)?;

        let threshold = file.metadata()?.len();
        let created_at = file_header.created_at()?;

        let mut blob = Self {
            id,
            path: path_buf,
            file,
            page_size,
            created_at,
            state: Active {
                threshold,
                entries_count: 0,
                write_cursor: page_size,
            },
        };

        let object_offsets = blob.scan_offsets()?;

        if let Some(last_object) = object_offsets.last() {
            let mut object_header_bytes = AlignedBuffer([0u8; size_of::<ObjectHeader>()]);
            blob.file
                .read_exact_at(&mut object_header_bytes.0, last_object.offset)?;

            let object_header: &ObjectHeader = bytemuck::from_bytes(&object_header_bytes.0);

            let next_pos =
                align_to_page(last_object.offset + object_header.object_size(), page_size);

            blob.state.write_cursor = next_pos;
            blob.state.entries_count = object_offsets.len() as u32;
        }

        Ok(blob)
    }

    pub fn ingest(&mut self, id: u64, data: &[u8]) -> io::Result<u64> {
        self.ingest_with_flags(id, data, FLAG_NONE)
    }

    pub fn delete(&mut self, id: u64) -> io::Result<u64> {
        self.ingest_with_flags(id, &[], FLAG_TOMBSTONE)
    }

    pub fn corrupted(&mut self, id: u64) -> io::Result<u64> {
        self.ingest_with_flags(id, &[], FLAG_CORRUPTED)
    }

    fn ingest_with_flags(&mut self, id: u64, data: &[u8], flags: u16) -> io::Result<u64> {
        // FIXME: Implement a write-ahead log (WAL) or a "commit" bit for individual
        // objects. Currently, if the process crashes mid-write, a partial object might exist
        // that passes the magic number check but contains garbage data.

        let offset = self.state.write_cursor;

        let total_len = OBJECT_HEADER_SIZE + data.len() as u64;
        let padded_len = align_to_page(total_len, self.page_size) as usize;

        if offset + padded_len as u64 > self.state.threshold {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                format!(
                    "blob capacity reached. offset {} + needed {} > threshold {}",
                    offset, padded_len, self.state.threshold
                ),
            ));
        }

        let data_len = data.len() as u32;
        let checksum = Hasher::hash(data);
        let object_header = ObjectHeader::new(id, data_len, flags, checksum);

        // FIXME: Use `pwritev` (Vectored I/O). Instead of creating a temporary `Vec`
        // and padding it with zeros, use `io::IoSlice` to write the header, data, and
        // padding in a single atomic syscall. This avoids zero-filling memory and
        // extra copies.

        let mut entry = Vec::with_capacity(padded_len);
        entry.extend_from_slice(bytemuck::bytes_of(&object_header));
        entry.extend_from_slice(data);
        entry.resize(padded_len, 0);

        self.file.write_all_at(&entry, offset)?;
        self.state.write_cursor += padded_len as u64;
        self.state.entries_count += 1;

        Ok(offset)
    }

    pub fn seal<P>(self, base_dir: P) -> io::Result<Blob<Sealed>>
    where
        P: AsRef<Path>,
    {
        // TODO: Implement a "Manifest" or "Control File." Currently, the existence of a
        // `.blob` file is the only record of its validity. A manifest would track all active,
        // sealed, and compacted blobs atomically, preventing orphan files after a crash
        // during compaction.

        FileHeader::write_sealed_at(&self.file, Utc::now())?;
        FileHeader::write_entries_count(&self.file, self.state.entries_count)?;

        let final_size = self.state.write_cursor;
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
            created_at: self.created_at,
            state: Sealed,
        })
    }
}

pub struct ObjectIterator<'a, S: ImmutableBlob> {
    blob: &'a Blob<S>,
    cursor: u64,
    file_len: u64,
}

impl<S: ImmutableBlob> Blob<S> {
    pub fn entries(&self) -> ObjectIterator<'_, S> {
        ObjectIterator {
            blob: self,
            cursor: self.page_size,
            file_len: self.file.metadata().map(|m| m.len()).unwrap_or(0),
        }
    }
}

impl<'a, S: ImmutableBlob> Iterator for ObjectIterator<'a, S> {
    type Item = io::Result<(u64, ObjectHeader)>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor + OBJECT_HEADER_SIZE <= self.file_len {
            let mut header_bytes = AlignedBuffer([0u8; size_of::<ObjectHeader>()]);
            if let Err(e) = self
                .blob
                .file
                .read_exact_at(&mut header_bytes.0, self.cursor)
            {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    return None;
                }
                return Some(Err(e));
            }

            let header: ObjectHeader = *bytemuck::from_bytes(&header_bytes.0);
            let offset = self.cursor;

            if header.magic() != OBJECT_MAGIC {
                let page_size = self.blob.page_size;
                let next_page = align_to_page(self.cursor + 1, page_size);
                if next_page >= self.file_len {
                    return None;
                }
                self.cursor = next_page;
                continue;
            }

            let entry_size = OBJECT_HEADER_SIZE + header.data_len() as u64;
            let entry_footprint = align_to_page(entry_size, self.blob.page_size);
            self.cursor += entry_footprint;

            return Some(Ok((offset, header)));
        }
        None
    }
}

// MARK: - Utilities

pub struct Hasher;

impl Hasher {
    pub fn hash(data: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }
}

#[repr(align(8))]
pub struct AlignedBuffer<const N: usize>(pub [u8; N]);

#[inline(always)]
const fn align_to_page(size: u64, page_size: u64) -> u64 {
    // NOTE: Current page alignment is hardcoded to the provided `page_size`. In
    // production, we should query `libc::sysconf(_SC_PAGESIZE)` or use `O_DIRECT`
    // requirements (usually 512 or 4096 bytes) to ensure we are actually hitting
    // the disk's physical sector boundaries for maximum throughput.

    debug_assert!(page_size.is_power_of_two());
    let page_mask = page_size - 1;
    (size + page_mask) & !page_mask
}
