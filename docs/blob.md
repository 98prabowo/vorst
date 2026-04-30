# Blob Storage

## File Lifecycle

To keep the system fast and the data clean, we manage data in three distinc stages:

- **Hot (`/data/ingestion/`)**: The active file where new uploads are appended. The only file with write lock.
- **Warm (`/data/sealed/`)**: Once a Hot file hits **threshold (e.g., 2GB)**, it is renamed and moved here. It is read-only but may contains "holes" (deleted data or partial writes from a crash).
- **Cold (`/data/optimized/`)**: The background **Compaction Task** reads warm files, remove garbages/deleted entries, and writes the surviving data into these perfectly optimized, permanent files.

> [!NOTE]
> **Performance**: The Hot/Cold separation ensures that heavy background compaction never slows down new user uploads.

> [!IMPORTANT]
> **Cleanliness**: Compacting every warm file ensures the cold storage is always at 100% density with zero wasted space.

## File Format & Integrity

Each `.blob` file is not just raw bytes. It follows a strict internal structure:

- **Header-First**: Every entry starts with a `BlobEntryHeader` containing a **Magic Number**, **Data Length**, and a **CRC32 Checksum**.
- **4KB Alignment**: Every entry is padded with zeros to ensure the next entry starts on a **4096-byte boundary**.
  This optimize the system for modern SSD/HDD sector sizes and enables faster OS-level I/O.
- **Naming**: Files use a **16 digits zero-padded sequence** (e.g., `v-0000000000000001.blob`) to ensure they sort chronologically in the filesystem.

> [!IMPORTANT]
> **Scalability**: The 16-digit ID allows for $10^{16}$ files (enough for 2000 Exabytes of data).

## The Crash Tolerant

We ensure the system is crash-tolerant without the overhead of a dedicated Blob WAL by using a specific order of operations:

1. **Blob Write**: Append the raw file + header + padding to the **Hot** blob.
1. **Hardware Sync**: Call `fsync()` to ensure the bytes are physically on the disk.
1. **Vector WAL**: Log the vector data (for the search engine) to WAL storage.
1. **Metadata Commit**: Update **Sled** with the `vector_id -> (blob_path, offset, length)` mapping.

> [!NOTE]
> **Why this is safe**: if the system crashes after step 1, **Sled** won't have the mapping.
> On reboot, the blob storage should check the last entry in **Sled** for the current Hot blob.
> If the file size on disk is larger than the `offset + length` of the last successful Sled entry, call `truncate()` to remove those "ghost bytes".
> This keeps Hot file cleaner.

## Optimizations

1. **Modulo Sharding**: Files are distributed into 100 directory (`00-99`) based on `blob_id % 100`.
   This prevents filesystem bottlenecks and directory bloat, even with millions of files.
1. **Skip Cache**: Bypasses kernel caching for raw, aligned disk writes (`O_DIRECT`).
1. **Preallocated**: Pre-allocates (e.g., 2GB) of contiguous disk space to avoid fragmentation (`fallocate`).
1. **Zero-Copy Serving**: Enable reading, streaming data directly from disk to network (`sendfile`).
