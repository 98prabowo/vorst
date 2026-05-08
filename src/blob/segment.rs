use std::{
    fs::{File, OpenOptions},
    io,
    ops::{Deref, DerefMut},
    os::unix::fs::FileExt,
    path::PathBuf,
};

use chrono::{DateTime, Utc};
use fs2::FileExt as Fs2FileExt;
use uuid::Uuid;

use crate::{
    blob::{
        format::{
            FLAG_CORRUPTED, FLAG_NONE, FLAG_TOMBSTONE, OBJECT_HEADER_SIZE, OBJECT_MAGIC,
            ObjectHeader, SEGMENT_HEADER_SIZE, SegmentHeader, align_to_page,
        },
        state::{Active, ImmutableSegment, Sealed, SegmentState},
        types::ObjectOffset,
    },
    sys::preallocate,
};

// MARK: - Storage Implementation

// TODO: Implement `io_uring` support for Linux. While this is a blob storage layer, the
// ability to perform high-depth asynchronous reads will be critical when the application
// needs to fetch hundreds of raw segmentss simultaneously (e.g., during a batch export or secondary processing

pub struct Segment<S: SegmentState> {
    pub id: Uuid,
    pub path: PathBuf,
    pub file: File,
    pub page_size: u64,
    pub created_at: DateTime<Utc>,
    pub state: S,
}

impl<S: SegmentState> Segment<S> {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn objects<'segment>(&'segment self) -> io::Result<ObjectIterator<'segment, S>> {
        let capacity = self.file.metadata()?.len();
        Ok(ObjectIterator::new(self, capacity))
    }

    pub fn scan_offsets(&self) -> io::Result<Vec<ObjectOffset>> {
        let len = self.file.metadata()?.len();
        ObjectIterator::new(self, len).map(map_offset).collect()
    }

    pub fn read_object(&self, offset: u64) -> io::Result<(ObjectHeader, Vec<u8>)> {
        let file_len = self.file.metadata()?.len();

        if offset + OBJECT_HEADER_SIZE as u64 > file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "offset out of bounds",
            ));
        }

        let mut object_header_bytes = AlignedBuffer::<OBJECT_HEADER_SIZE>::default();
        self.file.read_exact_at(&mut *object_header_bytes, offset)?;

        let object_header: ObjectHeader = *bytemuck::from_bytes(&*object_header_bytes);

        if object_header.magic() != OBJECT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid object magic number",
            ));
        }

        if offset + object_header.object_size() > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "object claims more data than segment contains",
            ));
        }

        let mut data = vec![0u8; object_header.data_len() as usize];
        self.file
            .read_exact_at(&mut data, offset + OBJECT_HEADER_SIZE as u64)?;

        if object_header.checksum() != Hasher::hash(&data) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "object checksum mismatch",
            ));
        }

        // TODO: Performance Optimization - `sendfile(2)` or `mmap`. If this blob storage is
        // serving segments over a network later, look into using `sendfile` to bypass user-space
        // copying entirely, or `mmap` for frequently accessed "hot" segments.

        Ok((object_header, data))
    }
}

impl<S: ImmutableSegment> Segment<S> {
    pub fn open_readonly(id: Uuid, path: PathBuf, page_size: u64) -> io::Result<Self> {
        let file = File::open(&path)?;

        file.try_lock_shared().map_err(|_| {
            io::Error::other(format!(
                "Could not acquire shared lock on segment: {:?}",
                path
            ))
        })?;

        let mut segment_header_bytes = AlignedBuffer::<SEGMENT_HEADER_SIZE>::default();
        file.read_exact_at(&mut *segment_header_bytes, 0)?;

        let segment_header: &SegmentHeader = bytemuck::from_bytes(&*segment_header_bytes);
        segment_header.validate(id)?;

        let created_at = segment_header.created_at()?;

        Ok(Self {
            id,
            path,
            file,
            page_size,
            created_at,
            state: S::default(),
        })
    }
}

impl Segment<Active> {
    pub fn new(id: Uuid, path: PathBuf, data_capacity: u64, page_size: u64) -> io::Result<Self> {
        let capacity = page_size + data_capacity;
        let now = Utc::now();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        file.try_lock_exclusive().map_err(|_| {
            io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("segment is already locked by another process: {:?}", path),
            )
        })?;

        let segment_header = SegmentHeader::new(id, 0, capacity, now);
        file.write_all_at(bytemuck::bytes_of(&segment_header), 0)?;

        preallocate(&file, capacity)?;
        file.sync_all()?;

        if let Some(parent) = path.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }

        Ok(Self {
            id,
            path,
            file,
            page_size,
            created_at: now,
            state: Active {
                capacity,
                entries_count: 0,
                write_cursor: page_size,
            },
        })
    }

    pub fn open(id: Uuid, path: PathBuf, page_size: u64) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        file.try_lock_exclusive().map_err(|_| {
            io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("segment is already locked by another process: {:?}", path),
            )
        })?;

        let mut segment_header_bytes = AlignedBuffer::<SEGMENT_HEADER_SIZE>::default();
        file.read_exact_at(&mut *segment_header_bytes, 0)?;
        let segment_header: &SegmentHeader = bytemuck::from_bytes(&*segment_header_bytes);
        segment_header.validate(id)?;

        let capacity = segment_header.capacity();
        let created_at = segment_header.created_at()?;

        let mut segment = Self {
            id,
            path,
            file,
            page_size,
            created_at,
            state: Active {
                capacity,
                entries_count: 0,
                write_cursor: page_size,
            },
        };

        let object_offsets = segment.scan_offsets()?;

        if let Some(last_object) = object_offsets.last() {
            match segment.read_object(last_object.offset) {
                Ok((header, _data)) => {
                    let next_pos =
                        align_to_page(last_object.offset + header.object_size(), page_size);
                    segment.state.write_cursor = next_pos;
                    segment.state.entries_count = object_offsets.len() as u32;
                }
                Err(_) => {
                    segment.state.write_cursor = last_object.offset;
                    segment.state.entries_count = object_offsets.len() as u32 - 1;
                    eprintln!(
                        "recovered from partial write at offset {}",
                        last_object.offset
                    );
                }
            }
        }

        Ok(segment)
    }

    pub fn build_index(&self) -> io::Result<Vec<ObjectOffset>> {
        ObjectIterator::new(self, self.state.write_cursor)
            .map(map_offset)
            .collect()
    }

    pub fn ingest(&mut self, id: Uuid, data: &[u8]) -> io::Result<u64> {
        self.ingest_with_flags(id, data, FLAG_NONE)
    }

    pub fn delete(&mut self, id: Uuid) -> io::Result<u64> {
        self.ingest_with_flags(id, &[], FLAG_TOMBSTONE)
    }

    pub fn corrupted(&mut self, id: Uuid) -> io::Result<u64> {
        self.ingest_with_flags(id, &[], FLAG_CORRUPTED)
    }

    fn ingest_with_flags(&mut self, id: Uuid, data: &[u8], flags: u16) -> io::Result<u64> {
        // FIXME: Implement a write-ahead log (WAL) or a "commit" bit for individual
        // objects. Currently, if the process crashes mid-write, a partial object might exist
        // that passes the magic number check but contains garbage data.

        let offset = self.state.write_cursor;
        let total_len = OBJECT_HEADER_SIZE as u64 + data.len() as u64;
        let padded_len = align_to_page(total_len, self.page_size) as usize;

        if offset + padded_len as u64 > self.state.capacity {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "segment capacity reached",
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

    pub fn seal(self) -> io::Result<Segment<Sealed>> {
        // TODO: Implement a "Manifest" or "Control File." Currently, the existence of a
        // `.blob` file is the only record of its validity. A manifest would track all active,
        // sealed, and compacted segments atomically, preventing orphan segments after a crash
        // during compaction.

        SegmentHeader::write_sealed_at(&self.file, Utc::now())?;
        SegmentHeader::write_entries_count(&self.file, self.state.entries_count)?;

        let final_size = self.state.write_cursor;
        self.file.set_len(final_size)?; // Trim preallocation
        self.file.sync_all()?;

        if let Err(e) = self.file.lock_shared() {
            eprintln!("Lock downgrade for segment {} returned: {}.", self.id, e);
        }

        Ok(Segment {
            id: self.id,
            path: self.path,
            file: self.file,
            page_size: self.page_size,
            created_at: self.created_at,
            state: Sealed,
        })
    }
}

// MARK: - Iterator

pub struct ObjectIterator<'segment, S: SegmentState> {
    segment: &'segment Segment<S>,
    cursor: u64,
    capacity: u64,
}

impl<'segment, S: SegmentState> ObjectIterator<'segment, S> {
    fn new(segment: &'segment Segment<S>, capacity: u64) -> Self {
        Self {
            segment,
            cursor: segment.page_size,
            capacity,
        }
    }
}

impl<'a, S: SegmentState> Iterator for ObjectIterator<'a, S> {
    type Item = io::Result<(u64, ObjectHeader)>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor + OBJECT_HEADER_SIZE as u64 <= self.capacity {
            let mut header_bytes = AlignedBuffer::<OBJECT_HEADER_SIZE>::default();
            if let Err(e) = self
                .segment
                .file
                .read_exact_at(&mut *header_bytes, self.cursor)
            {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    return None;
                }
                return Some(Err(e));
            }

            let header: ObjectHeader = *bytemuck::from_bytes(&*header_bytes);
            let offset = self.cursor;

            if header.magic() != OBJECT_MAGIC {
                let next_page = align_to_page(self.cursor + 1, self.segment.page_size);
                if next_page >= self.capacity {
                    return None;
                }
                self.cursor = next_page;
                continue;
            }

            self.cursor += align_to_page(header.object_size(), self.segment.page_size);

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

impl<const N: usize> Default for AlignedBuffer<N> {
    fn default() -> Self {
        Self([0u8; N])
    }
}

impl<const N: usize> Deref for AlignedBuffer<N> {
    type Target = [u8; N];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const N: usize> DerefMut for AlignedBuffer<N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

fn map_offset(res: io::Result<(u64, ObjectHeader)>) -> io::Result<ObjectOffset> {
    res.map(|(offset, header)| ObjectOffset {
        object_id: header.object_id(),
        offset,
        flags: header.flags(),
    })
}
