# User Workflow

## 1. Ingestion Workflow (Storing Data)

This is the "Write" path triggered when a user uploads a file (e.g., `POST /upload`).

1. **API Layer**: Receive the file stream.
1. **Blob Storage**: Saves the raw file to the disk using **UUID** as the filename to avoid collisions.
   - _Path Example_: `/data/blobs/uuid-123.blob`
1. **Data Transformation Pipeline**:
   - **Chunking**: The text is extracted and split into smaller "chunks" (e.g., 500 words each) because models have input limits.
   - **Embedding Model (Candle)**: Each chunk is passed through a local AI model to produce Vector (a list of `f32` numbers).
1. **Vector Storage (Write)**:
   - **WAL Check**: The system logs the intent to write to ensure reliability.
   - **Append**: The vectors are appended to the end of `vectors.bin`.
   - _Result_: Record the **Byte Offset** (where this specific vector starts in the file).
1. **Metadata Storage (Sled)**:
   - Save the "Map" that links everything together:
   - `Key: UUID` -> `Value: { "file_path": "...", "vector_offsets": [5000, 11144, ...], "original_name": "laporan.pdf" }`
1. **Success**: API give response `201 Created`.

## 2. Search Workflow (Finding Meaning)

This is the "Read" path triggered when a user asks a question (e.g., `GET /search?q="laporan pajak"`).

1. **API Layer**: Receive the query string.
1. **Embedding Model**: Converts the user's question into a **Query Vector** using the same model used during ingestion.
1. **Vector Engineering (The Engine)**:
   - **mmap**: The system accesses `vectors.bin` via memory mapping. It feels like the data is in RAM, but the OS manages the disk loading.
   - **Flat Index + SIMD**: The CPU scans the vectors. Thanks to **SIMD**, Rust compares the Query Vector against thousands of stored vectors simultaneously.
   - **Similarity Metric**: Calculates the **Cosine Similarity** score for each.
   - _Result_: Returns the top 5 **Object IDs (UUIDs)** with the highest scores.
1. **Metadata Storage**: Fetches the file details (original name, creation date) for those 5 UUIDs.
1. **Response**: Returns a list of the most relevant files to the user.

## 3. The Reliability & Concurrency Layer (The "Shield")

This ensures the system doesn't crash or lose data under pressure.

1. **WAL (Write-Ahead Log)**: If the power goes out while writing to `vectors.bin`, the system checks the WAL upon reboot and "truncates" (cuts) the corrupted partial
   data back to the last known good state.
1. **LSM-Tree (via Sled)**: Handles metadata updates efficiently. Even if you have millions of files, finding the metadata for a specific UUID remains nearly instant.
1. **Concurrency (RwLock/Arc)**:
   - **Multiple Readers**: Many users can perform searches at the exact same time without blocking each other.
   - **Single Writer**: When a new file is being indexed, the system briefly locks the write-path to ensure the `vectors.bin` file doesn't get corrupted.
