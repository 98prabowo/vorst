use std::{
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use fs2::FileExt as Fs2FileExt;
use uuid::Uuid;

use crate::{
    PAGE_SIZE,
    blob::{
        error::{Error, Result},
        format::{
            FLAG_CORRUPTED, FLAG_NONE, FLAG_TOMBSTONE, OBJECT_HEADER_SIZE, ObjectHeader,
            SEGMENT_HEADER_SIZE, SegmentHeader,
        },
        state::{Active, Compacted, ImmutableSegment, Sealed, SegmentState},
        types::{IngestedObject, ObjectOffset, SegmentStats},
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
    pub created_at: DateTime<Utc>,
    pub state: S,
}

impl<S: SegmentState> Segment<S> {
    pub fn objects<'a>(&'a self, file: &'a File) -> Result<ObjectIterator<'a, S>> {
        let capacity = file.metadata()?.len();
        Ok(ObjectIterator::new(self, file, capacity))
    }

    pub fn scan_offsets(&self, file: &File) -> Result<Vec<ObjectOffset>> {
        let mut offsets = Vec::new();

        for object in self.objects(file)? {
            match object {
                Ok(data) => offsets.push(map_offset(Ok(data))?),
                Err(e) if e.is_recoverable() => return Ok(offsets),
                Err(e) => return Err(e),
            }
        }

        Ok(offsets)
    }

    pub fn read_object_at(&self, file: &File, offset: u64) -> Result<(ObjectHeader, Vec<u8>)> {
        let file_len = file.metadata()?.len();

        if offset + OBJECT_HEADER_SIZE as u64 > file_len {
            return Err(Error::TornWrite { offset });
        }

        let object_header = ObjectHeader::header(file, self.id, offset, file_len)?;

        let mut data = vec![0u8; object_header.data_len() as usize];
        file.read_exact_at(&mut data, offset + OBJECT_HEADER_SIZE as u64)?;

        let object_checksum = object_header.checksum();
        let data_checksum = BlobHasher::hash(&data);
        if object_checksum != data_checksum {
            return Err(Error::ChecksumMismatch {
                id: object_header.object_id(),
                expected: object_checksum,
                found: data_checksum,
            });
        }

        // TODO: Performance Optimization - `sendfile(2)` or `mmap`. If this blob storage is
        // serving segments over a network later, look into using `sendfile` to bypass user-space
        // copying entirely, or `mmap` for frequently accessed "hot" segments.

        Ok((object_header, data))
    }

    pub fn check_health<F>(&self, file: &File, is_alive: &mut F) -> Result<SegmentStats>
    where
        F: FnMut(Uuid, Uuid, u64) -> bool,
    {
        let mut live_bytes = 0u64;
        let mut live_count = 0u32;

        for object in self.objects(file)? {
            let (offset, header) = object?;

            if header.flags() & FLAG_TOMBSTONE != 0 {
                continue;
            }

            if is_alive(self.id, header.object_id(), offset) {
                live_bytes += header.data_len() as u64;
                live_count += 1;
            }
        }

        Ok(SegmentStats {
            id: self.id,
            total_bytes: self.state.total_bytes(),
            live_bytes,
            entry_count: live_count,
            status: self.state.status(),
            created_at: self.created_at,
        })
    }
}

impl<S: ImmutableSegment> Segment<S> {
    pub fn open_readonly<P>(id: Uuid, path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let path_buf = path.as_ref().to_path_buf();
        let created_at = {
            let file = File::open(&path_buf)?;
            // Just read the header and validate
            let segment_header = SegmentHeader::header(&file, id)?;
            segment_header.created_at()?
        };

        Ok(Self {
            id,
            path: path_buf,
            created_at,
            state: S::default(),
        })
    }
}

impl Segment<Active> {
    pub fn new<P>(id: Uuid, path: P, data_size: u64) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let segment_size = PAGE_SIZE + data_size;
        let now = Utc::now();
        let path_buf = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        file.try_lock_exclusive()
            .map_err(|_| Error::SegmentLocked {
                id,
                path: path.as_ref().to_path_buf(),
            })?;

        let segment_header = SegmentHeader::new(id, 0, segment_size, now);
        let mut buf = AlignedBuffer::<{ PAGE_SIZE as usize }>::new_boxed();
        buf.fill(0);
        buf[..SEGMENT_HEADER_SIZE].copy_from_slice(bytemuck::bytes_of(&segment_header));
        file.write_all_at(&buf[..], 0)?;

        preallocate(&file, segment_size)?;
        file.sync_all()?;

        if let Some(parent) = path_buf.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }

        Ok(Self {
            id,
            path: path_buf,
            created_at: now,
            state: Active {
                segment_size,
                entries_count: 0,
                write_cursor: PAGE_SIZE,
                file,
                io_buffer: AlignedBuffer::new_boxed(),
            },
        })
    }

    pub fn open<P>(id: Uuid, path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        file.try_lock_exclusive()
            .map_err(|_| Error::SegmentLocked {
                id,
                path: path.as_ref().to_path_buf(),
            })?;

        let segment_header = SegmentHeader::header(&file, id)?;
        let segment_size = segment_header.segment_size();
        let created_at = segment_header.created_at()?;

        let actual_file_len = file.metadata()?.len();
        if actual_file_len < segment_size {
            return Err(Error::UnexpectedEof {
                id,
                expected: segment_size,
                found: actual_file_len,
            });
        }

        let mut segment = Self {
            id,
            path: path_buf,
            created_at,
            state: Active {
                segment_size,
                entries_count: 0,
                write_cursor: PAGE_SIZE,
                file,
                io_buffer: AlignedBuffer::new_boxed(),
            },
        };

        let object_offsets = segment.scan_offsets(&segment.state.file)?;

        if let Some(last_object) = object_offsets.last() {
            match segment.read_object_at(&segment.state.file, last_object.offset) {
                Ok((header, _data)) => {
                    let next_pos = last_object.offset + header.object_size();
                    segment.state.write_cursor = align_to_page(next_pos);
                    segment.state.entries_count = object_offsets.len() as u32;
                }
                Err(e) if e.is_recoverable() => {
                    segment.state.write_cursor = last_object.offset;
                    segment.state.entries_count = (object_offsets.len() - 1) as u32;

                    eprintln!(
                        "recovered from partial write at offset {}",
                        last_object.offset
                    );
                }
                Err(e) => return Err(e),
            }
        }

        Ok(segment)
    }

    pub fn build_index(&self) -> Result<Vec<ObjectOffset>> {
        ObjectIterator::new(self, &self.state.file, self.state.write_cursor)
            .map(map_offset)
            .collect()
    }

    pub fn ingest(
        &mut self,
        file_id: Uuid,
        object_id: Uuid,
        data: &[u8],
    ) -> Result<IngestedObject> {
        self.ingest_with_flags(file_id, object_id, data, FLAG_NONE)
    }

    pub fn delete(&mut self, file_id: Uuid) -> Result<IngestedObject> {
        self.ingest_with_flags(file_id, Uuid::nil(), &[], FLAG_TOMBSTONE)
    }

    pub fn corrupted(&mut self, file_id: Uuid, object_id: Uuid) -> Result<IngestedObject> {
        self.ingest_with_flags(file_id, object_id, &[], FLAG_CORRUPTED)
    }

    fn ingest_with_flags(
        &mut self,
        file_id: Uuid,
        object_id: Uuid,
        data: &[u8],
        flags: u16,
    ) -> Result<IngestedObject> {
        // FIXME: Implement a write-ahead log (WAL) or a "commit" bit for individual
        // objects. Currently, if the process crashes mid-write, a partial object might exist
        // that passes the magic number check but contains garbage data.

        let offset = self.state.write_cursor;
        let total_len = OBJECT_HEADER_SIZE as u64 + data.len() as u64;
        let padded_len = align_to_page(total_len) as usize;

        assert!(
            padded_len as u64 >= total_len,
            "Alignment logic failure: padded_len is smaller than total_len"
        );
        assert!(
            padded_len <= self.state.io_buffer.len(),
            "Invariant Violation: Object exceeds IO buffer capacity"
        );
        assert!(
            offset.is_multiple_of(PAGE_SIZE),
            "Invariant Violation: Write cursor is unaligned"
        );

        if offset + padded_len as u64 > self.state.segment_size {
            return Err(Error::StorageFull {
                needed: (offset + padded_len as u64) - self.state.segment_size,
            });
        }

        let data_len = data.len() as u32;
        let checksum = BlobHasher::hash(data);
        let object_header = ObjectHeader::new(file_id, object_id, data_len, flags, checksum);

        // FIXME: Use `pwritev` (Vectored I/O). Instead of creating a temporary `Vec`
        // and padding it with zeros, use `io::IoSlice` to write the header, data, and
        // padding in a single atomic syscall. This avoids zero-filling memory and
        // extra copies.

        let buf = &mut self.state.io_buffer;
        buf[total_len as usize..padded_len].fill(0);
        buf[..OBJECT_HEADER_SIZE].copy_from_slice(bytemuck::bytes_of(&object_header));
        buf[OBJECT_HEADER_SIZE..total_len as usize].copy_from_slice(data);

        self.state.file.write_all_at(&buf[..padded_len], offset)?;
        self.state.write_cursor += padded_len as u64;
        self.state.entries_count += 1;

        Ok(IngestedObject {
            offset,
            length: data_len,
            checksum,
        })
    }

    pub fn seal(self) -> Result<Segment<Sealed>> {
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
            created_at: self.created_at,
            state: Sealed {
                total_bytes: final_size,
                entry_count: self.state.entries_count,
            },
        })
    }
}

impl Segment<Sealed> {
    pub fn compacted(self) -> Segment<Compacted> {
        Segment {
            id: self.id,
            path: self.path,
            created_at: self.created_at,
            state: Compacted {
                total_bytes: self.state.total_bytes,
            },
        }
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
            cursor: PAGE_SIZE,
            capacity,
        }
    }
}

impl<'a, S: SegmentState> Iterator for ObjectIterator<'a, S> {
    type Item = Result<(u64, ObjectHeader)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor + OBJECT_HEADER_SIZE as u64 > self.capacity {
            return None;
        }

        match ObjectHeader::header(self.file, self.segment.id, self.cursor, self.capacity) {
            Ok(header) => {
                let offset = self.cursor;
                let object_size = header.object_size();

                self.cursor += align_to_page(object_size);
                Some(Ok((offset, header)))
            }
            Err(Error::InvalidMagic { .. }) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

// MARK: - Utilities

fn map_offset(res: Result<(u64, ObjectHeader)>) -> Result<ObjectOffset> {
    res.map(|(offset, header)| ObjectOffset {
        object_id: header.object_id(),
        offset,
        flags: header.flags(),
    })
}
