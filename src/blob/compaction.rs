use std::{io, os::unix::fs::FileExt, path::Path};

use uuid::Uuid;

use crate::blob::{
    Active, Blob, Compacted, CompactedBlob, CompactionMap, ImmutableBlob,
    format::{FileHeader, OBJECT_HEADER_SIZE},
    storage::{AlignedBuffer, FLAG_TOMBSTONE, Hasher},
};

const INITIAL_IO_BUFFER_SIZE: usize = 1024 * 1024;

// TODO: Implement "Leveled Compaction." Currently, we are compacting a
// `Vec<Blob>`. We should implement a strategy (like RocksDB) where smaller
// blobs are merged into larger ones over time to keep the number of open file
// descriptors manageable.

pub trait BlobCompactable {
    fn compact<P, F>(
        self,
        base_dir: P,
        capacity: u64,
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
        capacity: u64,
        page_size: u64,
        is_latest: F,
    ) -> io::Result<CompactedBlob>
    where
        P: AsRef<Path>,
        F: Fn(u64, Uuid, u64) -> bool,
    {
        let mut new_blob = Blob::<Active>::new(&base_dir, capacity, page_size)?;
        let mut mappings = Vec::new();
        let mut removed_ids = Vec::new();
        let mut removed_paths = Vec::new();
        let mut io_buf = Vec::with_capacity(INITIAL_IO_BUFFER_SIZE);

        // TODO: Parallelize Compaction. Since blobs are immutable once sealed, we can
        // use `rayon` to process multiple source blobs in parallel and then merge them
        // into the new blob using an ordered writer.

        for source in self {
            let mut file_header_bytes = AlignedBuffer([0u8; size_of::<FileHeader>()]);
            source.file.read_exact_at(&mut file_header_bytes.0, 0)?;
            let file_header: &FileHeader = bytemuck::from_bytes(&file_header_bytes.0);
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

                let data_offset = offset + OBJECT_HEADER_SIZE;
                source.file.read_exact_at(&mut io_buf, data_offset)?;

                // TODO: Performance - Checksum Offloading.
                // We are currently "scrubbing" (re-verifying) every object during compaction.
                // While safe, this is CPU-intensive. Consider using hardware-accelerated CRC32
                // or providing a "fast-path" copy for trusted media.
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

        // FIXME: Atomic Metadata Handoff. After the new blob is sealed, the "Source of Truth"
        // (Sled) must be updated to map File IDs to the new `BlobID` and `Offset` before the
        // old files are unlinked. We need a two-phase commit or a "tombstone" record in the
        // metadata to ensure we don't lose file access if we crash mid-cleanup.

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
