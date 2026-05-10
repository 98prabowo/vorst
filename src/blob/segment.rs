use std::{
    fs::{File, OpenOptions},
    io,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use fs2::FileExt as Fs2FileExt;
use uuid::Uuid;

use crate::{
    blob::{
        format::{
            FLAG_CORRUPTED, FLAG_NONE, FLAG_TOMBSTONE, OBJECT_HEADER_SIZE, OBJECT_MAGIC,
            ObjectHeader, SEGMENT_HEADER_SIZE, SegmentHeader,
        },
        state::{Active, ImmutableSegment, Sealed, SegmentState},
        types::{IngestedObject, ObjectOffset},
        utils::{AlignedBuffer, BlobHasher, align_to_page},
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
    pub page_size: u64,
    pub created_at: DateTime<Utc>,
    pub state: S,
}

impl<S: SegmentState> Segment<S> {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn objects<'a>(&'a self, file: &'a File) -> io::Result<ObjectIterator<'a, S>> {
        let capacity = file.metadata()?.len();
        Ok(ObjectIterator::new(self, file, capacity))
    }

    pub fn scan_offsets(&self, file: &File) -> io::Result<Vec<ObjectOffset>> {
        let len = file.metadata()?.len();
        ObjectIterator::new(self, file, len)
            .map(map_offset)
            .collect()
    }

    pub fn read_object_at(&self, file: &File, offset: u64) -> io::Result<(ObjectHeader, Vec<u8>)> {
        let file_len = file.metadata()?.len();

        if offset + OBJECT_HEADER_SIZE as u64 > file_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "offset out of bounds",
            ));
        }

        let mut object_header_bytes = AlignedBuffer::<OBJECT_HEADER_SIZE>::default();
        file.read_exact_at(&mut *object_header_bytes, offset)?;

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
        file.read_exact_at(&mut data, offset + OBJECT_HEADER_SIZE as u64)?;

        if object_header.checksum() != BlobHasher::hash(&data) {
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
    pub fn open_readonly<P>(id: Uuid, path: P, page_size: u64) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let path_buf = path.as_ref().to_path_buf();
        let created_at = {
            let file = File::open(&path_buf)?;
            // Just read the header and validate
            let mut header_bytes = AlignedBuffer::<SEGMENT_HEADER_SIZE>::default();
            file.read_exact_at(&mut *header_bytes, 0)?;
            let header: &SegmentHeader = bytemuck::from_bytes(&*header_bytes);
            header.validate(id)?;
            header.created_at()?
        };

        Ok(Self {
            id,
            path: path_buf,
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
            page_size,
            created_at: now,
            state: Active {
                capacity,
                entries_count: 0,
                write_cursor: page_size,
                file,
            },
        })
    }

    pub fn open<P>(id: Uuid, path: P, page_size: u64) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        file.try_lock_exclusive().map_err(|_| {
            io::Error::new(
                io::ErrorKind::AddrInUse,
                format!(
                    "segment is already locked by another process: {:?}",
                    &path_buf
                ),
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
            path: path_buf,
            page_size,
            created_at,
            state: Active {
                capacity,
                entries_count: 0,
                write_cursor: page_size,
                file,
            },
        };

        let object_offsets = segment.scan_offsets(&segment.state.file)?;

        if let Some(last_object) = object_offsets.last() {
            match segment.read_object_at(&segment.state.file, last_object.offset) {
                Ok((header, _data)) => {
                    let next_pos = last_object.offset + header.object_size();
                    segment.state.write_cursor = align_to_page(next_pos, page_size);
                    segment.state.entries_count = object_offsets.len() as u32;
                }
                Err(_) => {
                    segment.state.write_cursor = last_object.offset;
                    segment.state.entries_count = (object_offsets.len() - 1) as u32;
                    segment.state.file.set_len(last_object.offset)?;

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
        ObjectIterator::new(self, &self.state.file, self.state.write_cursor)
            .map(map_offset)
            .collect()
    }

    pub fn ingest(&mut self, id: Uuid, data: &[u8]) -> io::Result<IngestedObject> {
        self.ingest_with_flags(id, data, FLAG_NONE)
    }

    pub fn delete(&mut self, id: Uuid) -> io::Result<IngestedObject> {
        self.ingest_with_flags(id, &[], FLAG_TOMBSTONE)
    }

    pub fn corrupted(&mut self, id: Uuid) -> io::Result<IngestedObject> {
        self.ingest_with_flags(id, &[], FLAG_CORRUPTED)
    }

    fn ingest_with_flags(
        &mut self,
        id: Uuid,
        data: &[u8],
        flags: u16,
    ) -> io::Result<IngestedObject> {
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
        let checksum = BlobHasher::hash(data);
        let object_header = ObjectHeader::new(id, data_len, flags, checksum);

        // FIXME: Use `pwritev` (Vectored I/O). Instead of creating a temporary `Vec`
        // and padding it with zeros, use `io::IoSlice` to write the header, data, and
        // padding in a single atomic syscall. This avoids zero-filling memory and
        // extra copies.

        let mut entry = Vec::with_capacity(padded_len);
        entry.extend_from_slice(bytemuck::bytes_of(&object_header));
        entry.extend_from_slice(data);
        entry.resize(padded_len, 0);

        self.state.file.write_all_at(&entry, offset)?;
        self.state.write_cursor += padded_len as u64;
        self.state.entries_count += 1;

        Ok(IngestedObject {
            offset,
            length: data_len,
            checksum,
        })
    }

    pub fn seal(self) -> io::Result<Segment<Sealed>> {
        // TODO: Implement a "Manifest" or "Control File." Currently, the existence of a
        // `.blob` file is the only record of its validity. A manifest would track all active,
        // sealed, and compacted segments atomically, preventing orphan segments after a crash
        // during compaction.

        self.state.file.sync_all()?;

        SegmentHeader::write_sealed_at(&self.state.file, Utc::now())?;
        SegmentHeader::write_entries_count(&self.state.file, self.state.entries_count)?;

        let final_size = self.state.write_cursor;
        self.state.file.set_len(final_size)?; // Trim preallocation
        self.state.file.sync_all()?;

        if let Err(e) = self.state.file.lock_shared() {
            eprintln!("Lock downgrade for segment {} returned: {}.", self.id, e);
        }

        Ok(Segment {
            id: self.id,
            path: self.path,
            page_size: self.page_size,
            created_at: self.created_at,
            state: Sealed,
        })
    }
}

// MARK: - Iterator

pub struct ObjectIterator<'segment, S: SegmentState> {
    segment: &'segment Segment<S>,
    file: &'segment File,
    cursor: u64,
    capacity: u64,
}

impl<'segment, S: SegmentState> ObjectIterator<'segment, S> {
    fn new(segment: &'segment Segment<S>, file: &'segment File, capacity: u64) -> Self {
        Self {
            segment,
            file,
            cursor: segment.page_size,
            capacity,
        }
    }
}

impl<'a, S: SegmentState> Iterator for ObjectIterator<'a, S> {
    type Item = io::Result<(u64, ObjectHeader)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor + OBJECT_HEADER_SIZE as u64 > self.capacity {
            return None;
        }

        let mut header_bytes = AlignedBuffer::<OBJECT_HEADER_SIZE>::default();
        if let Err(e) = self.file.read_exact_at(&mut *header_bytes, self.cursor) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(e));
        }

        let header: ObjectHeader = *bytemuck::from_bytes(&*header_bytes);
        let offset = self.cursor;

        if header.magic() != OBJECT_MAGIC {
            return None;
        }

        if offset + header.object_size() > self.capacity {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "object size exceeding segment capacity",
            )));
        }

        self.cursor += align_to_page(header.object_size(), self.segment.page_size);

        Some(Ok((offset, header)))
    }
}

// MARK: - Utilities

fn map_offset(res: io::Result<(u64, ObjectHeader)>) -> io::Result<ObjectOffset> {
    res.map(|(offset, header)| ObjectOffset {
        object_id: header.object_id(),
        offset,
        flags: header.flags(),
    })
}
