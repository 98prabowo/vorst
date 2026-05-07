use std::{
    fs::{self, File, OpenOptions},
    io,
    mem::offset_of,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use bytemuck::{Pod, Zeroable};
use chrono::{DateTime, Utc, offset};
use uuid::Uuid;

use crate::sys::preallocate;

mod storage;

// MARK: - Constants

const BLOB_PREFIX: &str = "vb-";
const BLOB_EXTENSION: &str = ".blob";
const BLOB_MAGIC_NUMBER: u32 = 0x56424c42; // "VBLB"
const ENTRY_MAGIC_NUMBER: u32 = 0x564F424A; // "VOBJ"
const ENTRY_HEADER_SIZE: u64 = size_of::<BlobEntryHeader>() as u64;
const INITIAL_IO_BUFFER_SIZE: usize = 1024 * 1024;

pub const FLAG_NONE: u16 = 0x0000;
pub const FLAG_TOMBSTONE: u16 = 0x0001;
pub const FLAG_CORRUPTED: u16 = 0x0002;

// MARK: - Models

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct BlobFileHeader {
    magic: u32,
    version: u32,
    created_at: i64,
    sealed_at: i64,
    blob_id: [u8; 16],
    entries_count: u32,
    _padding: [u8; 4],
}

impl BlobFileHeader {
    pub fn new(id: Uuid, count: u32, created_at: DateTime<Utc>) -> Self {
        Self {
            magic: BLOB_MAGIC_NUMBER.to_le(),
            version: 1u32.to_le(),
            created_at: created_at.timestamp().to_le(),
            sealed_at: 0,
            blob_id: id.into_bytes(),
            entries_count: count.to_le(),
            _padding: [0u8; 4],
        }
    }

    pub fn magic(&self) -> u32 {
        u32::from_le(self.magic)
    }

    pub fn created_at(&self) -> io::Result<DateTime<Utc>> {
        let ts = i64::from_le(self.created_at);
        DateTime::from_timestamp_secs(ts)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid blob file header"))
    }

    pub fn sealed_at(&self) -> i64 {
        i64::from_le(self.sealed_at)
    }

    pub fn blob_id(&self) -> Uuid {
        Uuid::from_bytes(self.blob_id)
    }

    pub fn entries_count(&self) -> u32 {
        u32::from_le(self.entries_count)
    }

    pub fn validate(&self, id: Uuid) -> io::Result<()> {
        if self.magic() != BLOB_MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid file header magic number",
            ));
        }

        if self.blob_id() != id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file header UUID mismatch",
            ));
        }

        Ok(())
    }

    pub fn write_sealed_at(file: &File, dt: DateTime<Utc>) -> io::Result<()> {
        let offset = offset_of!(Self, sealed_at) as u64;
        let ts = dt.timestamp();
        file.write_all_at(&ts.to_le_bytes(), offset)
    }

    pub fn write_entries_count(file: &File, count: u32) -> io::Result<()> {
        let offset = offset_of!(Self, entries_count) as u64;
        file.write_all_at(&count.to_le_bytes(), offset)
    }
}

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

impl BlobEntryHeader {
    pub fn new(id: u64, len: u32, flags: u16, checksum: u32) -> Self {
        Self {
            magic: ENTRY_MAGIC_NUMBER.to_le(),
            version: 1u16.to_le(),
            flags: flags.to_le(),
            data_len: len.to_le(),
            checksum: checksum.to_le(),
            object_id: id.to_le(),
        }
    }

    pub fn magic(&self) -> u32 {
        u32::from_le(self.magic)
    }

    pub fn flags(&self) -> u16 {
        u16::from_le(self.flags)
    }

    pub fn object_id(&self) -> u64 {
        u64::from_le(self.object_id)
    }

    pub fn data_len(&self) -> u32 {
        u32::from_le(self.data_len)
    }

    pub fn checksum(&self) -> u32 {
        u32::from_le(self.checksum)
    }
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
    pub removed_paths: Vec<PathBuf>,
}

// MARK: - State Machine

pub trait BlobState: Default {}
pub trait ImmutableBlob: BlobState {}

#[derive(Default)]
pub struct Active {
    threshold: u64,
    entries_count: u32,
    write_cursor: u64,
}

#[derive(Default)]
pub struct Sealed;

#[derive(Default)]
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
    created_at: DateTime<Utc>,
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

    pub fn read_entry(&mut self, offset: u64) -> io::Result<(BlobEntryHeader, Vec<u8>)> {
        let file_len = self.file.metadata()?.len();

        if offset + ENTRY_HEADER_SIZE > file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "offset out of bounds",
            ));
        }

        let mut entry_header_bytes = AlignedBuffer([0u8; size_of::<BlobEntryHeader>()]);
        self.file.read_exact_at(&mut entry_header_bytes.0, offset)?;

        let entry_header: BlobEntryHeader = *bytemuck::from_bytes(&entry_header_bytes.0);

        if entry_header.magic() != ENTRY_MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic number",
            ));
        }

        if offset + ENTRY_HEADER_SIZE + entry_header.data_len() as u64 > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header claims more data than file contains",
            ));
        }

        let mut data = vec![0u8; entry_header.data_len() as usize];
        self.file
            .read_exact_at(&mut data, offset + ENTRY_HEADER_SIZE)?;

        if entry_header.checksum() != Hasher::hash(&data) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checksum mismatch",
            ));
        }

        Ok((entry_header, data))
    }

    pub fn scan_offsets(&mut self) -> io::Result<Vec<ObjectOffset>> {
        let mut index = Vec::new();
        let file_len = self.file.metadata()?.len();
        let mut cursor = self.page_size;

        let mut file_header_bytes = AlignedBuffer([0u8; size_of::<BlobFileHeader>()]);
        self.file.read_exact_at(&mut file_header_bytes.0, 0)?;

        let file_header: &BlobFileHeader = bytemuck::from_bytes(&file_header_bytes.0);
        file_header.validate(self.id)?;

        while cursor + ENTRY_HEADER_SIZE <= file_len {
            let mut entry_header_bytes = AlignedBuffer([0u8; size_of::<BlobEntryHeader>()]);
            if self
                .file
                .read_exact_at(&mut entry_header_bytes.0, cursor)
                .is_err()
            {
                break;
            }

            let entry_header: &BlobEntryHeader = bytemuck::from_bytes(&entry_header_bytes.0);

            if entry_header.magic() != ENTRY_MAGIC_NUMBER {
                let next_page = align_to_page(cursor + 1, self.page_size);
                if next_page >= file_len {
                    break;
                }
                cursor = next_page;
                continue;
            }

            let total_entry_size = ENTRY_HEADER_SIZE + entry_header.data_len() as u64;
            if cursor + total_entry_size > file_len {
                break; // Corrupted header claims more data than exists.
            }

            index.push(ObjectOffset {
                object_id: entry_header.object_id(),
                offset: cursor,
                flags: entry_header.flags(),
            });
            cursor = align_to_page(cursor + total_entry_size, self.page_size);
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

        let mut file_header_bytes = AlignedBuffer([0u8; size_of::<BlobFileHeader>()]);
        file.read_exact_at(&mut file_header_bytes.0, 0)?;

        let file_header: &BlobFileHeader = bytemuck::from_bytes(&file_header_bytes.0);
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

    pub fn entries(&self) -> EntryIterator<'_, S> {
        EntryIterator {
            blob: self,
            cursor: self.page_size,
            file_len: self.file.metadata().map(|m| m.len()).unwrap_or(0),
        }
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

        let header = BlobFileHeader::new(id, 0, now);
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

    pub fn open<P>(path: P, threshold: u64, page_size: u64) -> io::Result<Self>
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

        let mut file_header_bytes = AlignedBuffer([0u8; size_of::<BlobFileHeader>()]);
        file.read_exact_at(&mut file_header_bytes.0, 0)?;
        let file_header: &BlobFileHeader = bytemuck::from_bytes(&file_header_bytes.0);
        file_header.validate(id)?;

        let mut blob = Self {
            id,
            path: path_buf,
            file,
            page_size,
            created_at: file_header.created_at()?,
            state: Active {
                threshold,
                entries_count: 0,
                write_cursor: page_size,
            },
        };

        let offsets = blob.scan_offsets()?;

        if let Some(last_entry) = offsets.last() {
            let mut header_bytes = AlignedBuffer([0u8; size_of::<BlobEntryHeader>()]);
            blob.file
                .read_exact_at(&mut header_bytes.0, last_entry.offset)?;
            let header: &BlobEntryHeader = bytemuck::from_bytes(&header_bytes.0);

            let entry_size = ENTRY_HEADER_SIZE + header.data_len() as u64;
            let next_pos = align_to_page(last_entry.offset + entry_size, page_size);

            blob.state.write_cursor = next_pos;
            blob.state.entries_count = offsets.len() as u32;
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
        let offset = self.state.write_cursor;

        let total_len = ENTRY_HEADER_SIZE + data.len() as u64;
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
        let header = BlobEntryHeader::new(id, data_len, flags, checksum);

        let mut entry = Vec::with_capacity(padded_len);
        entry.extend_from_slice(bytemuck::bytes_of(&header));
        entry.extend_from_slice(data);
        entry.resize(padded_len, 0);

        self.file.write_all_at(&entry, offset)?;
        self.state.write_cursor += padded_len as u64;
        self.state.entries_count += 1;

        Ok(offset)
    }

    pub fn seal<P>(mut self, base_dir: P) -> io::Result<Blob<Sealed>>
    where
        P: AsRef<Path>,
    {
        BlobFileHeader::write_sealed_at(&self.file, Utc::now())?;
        BlobFileHeader::write_entries_count(&self.file, self.state.entries_count)?;

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
        let mut removed_paths = Vec::new();
        let mut io_buf = Vec::with_capacity(INITIAL_IO_BUFFER_SIZE);

        for source in self {
            let mut file_header_bytes = AlignedBuffer([0u8; size_of::<BlobFileHeader>()]);
            source.file.read_exact_at(&mut file_header_bytes.0, 0)?;

            let file_header: &BlobFileHeader = bytemuck::from_bytes(&file_header_bytes.0);
            file_header.validate(source.id)?;

            for entry in source.entries() {
                let (offset, header) = entry?;

                if (header.flags() & FLAG_TOMBSTONE) != 0 {
                    continue;
                }

                if !is_latest(header.object_id(), source.id, offset) {
                    continue;
                }

                // TODO: Transition to Vectored I/O (Gather-Write).
                // Current implementation uses `Vec::resize` which causes unnecessary zero-filling and
                // data copying. Use `File::write_vectored_at` with `std::io::IoSlice` to send
                // [Header, Data, Padding] to the kernel in a single syscall. This eliminates
                // intermediate allocations and reduces memory pressure during heavy ingestion.
                let data_len = header.data_len() as usize;
                io_buf.resize(data_len, 0);

                let data_offset = offset + ENTRY_HEADER_SIZE;
                source.file.read_exact_at(&mut io_buf, data_offset)?;

                let new_offset = if header.checksum() == Hasher::hash(&io_buf) {
                    new_blob.ingest(header.object_id(), &io_buf)?
                } else {
                    new_blob.corrupted(header.object_id())?
                };

                mappings.push(CompactionMap {
                    object_id: header.object_id(),
                    old_blob_id: source.id,
                    old_offset: offset,
                    new_offset,
                });
            }

            removed_ids.push(source.id);
            removed_paths.push(source.path);
        }

        let sealed_blob = new_blob.seal(&base_dir)?;

        Ok(CompactedBlob {
            new_blob: Blob {
                id: sealed_blob.id,
                path: sealed_blob.path,
                file: sealed_blob.file,
                page_size: sealed_blob.page_size,
                created_at: sealed_blob.created_at,
                state: Compacted { mappings },
            },
            removed_blob_ids: removed_ids,
            removed_paths,
        })
    }
}

pub struct EntryIterator<'a, S: ImmutableBlob> {
    blob: &'a Blob<S>,
    cursor: u64,
    file_len: u64,
}

impl<'a, S: ImmutableBlob> Iterator for EntryIterator<'a, S> {
    type Item = io::Result<(u64, BlobEntryHeader)>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor + ENTRY_HEADER_SIZE <= self.file_len {
            let mut header_bytes = AlignedBuffer([0u8; size_of::<BlobEntryHeader>()]);
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

            let header: BlobEntryHeader = *bytemuck::from_bytes(&header_bytes.0);
            let offset = self.cursor;

            if header.magic() != ENTRY_MAGIC_NUMBER {
                let page_size = self.blob.page_size;
                let next_page = align_to_page(self.cursor + 1, page_size);
                if next_page >= self.file_len {
                    return None;
                }
                self.cursor = next_page;
                continue;
            }

            let entry_size = ENTRY_HEADER_SIZE + header.data_len() as u64;
            let entry_footprint = align_to_page(entry_size, self.blob.page_size);
            self.cursor += entry_footprint;

            return Some(Ok((offset, header)));
        }
        None
    }
}

// MARK: - Utilities

struct Hasher;

impl Hasher {
    fn hash(data: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }
}

#[repr(align(8))]
struct AlignedBuffer<const N: usize>([u8; N]);

#[inline(always)]
const fn align_to_page(size: u64, page_size: u64) -> u64 {
    debug_assert!(page_size.is_power_of_two());
    let page_mask = page_size - 1;
    (size + page_mask) & !page_mask
}
