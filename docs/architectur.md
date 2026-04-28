# Verst: Single-Node Vector Object Storage

## Vision

To provide an affordable, self-hosted alternative to expensive cloud vector databases,
focusing on data sovereignty and cost-efficiency for personal and startup use.

## Core Components

1. **Blob Store**: UUID-based file system storage for raw data assets.
1. **Metadata Store (Sled)**: ACID-compliant KV store mapping object IDs to physical paths and vector offsets.
1. **Vector Store (mmap)**: A disk-persistent flat index for high-dimension vectors, leveraging memory mapping for zero-copy access.
1. **Search Interface**:
   - **MVP**: Flat index (Brute-force) with SIMD optimization.
   - **Production**: Vamana-based Graph (DiskANN) for sub-linear search time.

## Design Philosiphy

- **Rust-First**: Safety, speed, and zero-cost abstractions.
- **Disk-Centric**: Optimize for low RAM usage by offloading indices to NVMe SSDs.
- **Atomic Operations**: Ensure data consistency between Blob, Metadata, and Vector stores.
