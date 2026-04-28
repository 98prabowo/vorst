# Technical Implementation Context

## Tech Stack Constraints

- **Language**: Rust (Stable)
- **Async Runtime**: Tokio
- **Embedding Engine**: Candle (locally hosted models, no external API)
- **Serialization**: Bincode for disk, Serde for JSON API
- **Error Handling**: Anyhow for high-level, thiserror for library-level

## Data Structures

- **Vector Dimension**: 1536 (default for OpenAI-style embeddings) or 384 (all-MiniLM-L6-v2)
- **Precision**: f32 (4 bytes per dimension)
- **Record Layout**: [UUID (16 bytes) | Vector (dim * 4 bytes)]
- **Metadata Format**: Bincode-serialized structs stored in Sled

## Critical Implementation Rules

1. **Zero-Copy**: Use memory mapping (memmap2) for reading vectors; do not load the entire vector file into a Vec<f32>.
2. **SIMD**: Prefer vectorized operations for similarity math (dot product/cosine).
3. **Atomic Writes**: A "Commit" only happens after:
   (1) Blob is saved
   (2) Vector is appended
   (3) Sled metadata is updated.
   Otherwise, roll back (truncate vector file).
