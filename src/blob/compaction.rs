use std::{io, os::unix::fs::FileExt, path::Path};

use uuid::Uuid;

use crate::blob::{
    Active, Blob, Compacted, CompactedBlob, CompactionMap, ImmutableBlob,
    format::{FileHeader, OBJECT_HEADER_SIZE},
    storage::{AlignedBuffer, FLAG_TOMBSTONE, Hasher},
};

const INITIAL_IO_BUFFER_SIZE: usize = 1024 * 1024;

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
