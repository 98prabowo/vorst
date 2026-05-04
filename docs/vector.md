# Vector Storage

| Phase       | Target Scale | Performance Profile    | Behavioral Goal                                    |
| ----------- | ------------ | ---------------------- | -------------------------------------------------- |
| **0 (MVP)** | < 1M vectors | **CPU Bound** (SIMD)   | Maximum accuracy and simplicity for local dev      |
| **1**       | 1M - 100M    | **RAM Bound** (SQ8/PQ) | Balance speed and memory via tiered compaction     |
| **2**       | 1B+ Vectors  | **IO Bounds** (AISAQ)  | "DRAM-free" scaling. Bottleneck shifts to SSD IOPS |

## Phase 0 (MVP)

We use **Flat Index** with **mmap** to index vector at MVP phase.
This algorithm is the simplest to implement and give most accurate result.

### Storage Architecture

- **Segmented files**: Immutable chunks of raw vectors mapped into memory.
- **Flat index**: Brute force scans all vectors in all active segment.
- **Execution**: Use `mmap` to map all segmented files into the address space.
- **Acceleration**: Use **SIMD** (AVX2/AVX-512 and SSE for x86, NEON for ARM) to calculate vector math.

### Vector Dimension

Since Vorst only support **text-based** file format for initial phase. We decides to use 1024 dimension.

- **Memory Alignment**: 1024 is a perfect power of two ($2^{10}$).
  When system load a segment into a SIMD register, 1024-bit chunks align perfectly with cache lines (usually 64 bytes).
- **The "Matryoshka" Effect**: Many models use "Matryoshka Representation Learning", allowing users to truncate a 3072-dim vector down to **1024** with almost zero loss in accuracy.
  This makes 1024 the "high-performance" choice for profesional users.
  - [**Matryoshka Representation Learning**](https://arxiv.org/abs/2205.13147)
  - [**OpenAI more dimension != more quality**](https://openai.com/index/new-embedding-models-and-api-updates/)
  - [**OpenAI reducing embedding dimensions**](https://developers.openai.com/api/docs/guides/embeddings)
  - [**Hugging Face MRL**](https://huggingface.co/nomic-ai/nomic-embed-text-v1.5)
  - [**Nomic Embed technical report**](https://arxiv.org/abs/2402.01613)
  - [**Weaviate MRL**](https://weaviate.io/blog/openais-matryoshka-embeddings-in-weaviate)
  - [**Supabase MRL**](https://supabase.com/blog/matryoshka-embeddings)

### Distance Metric

Standardize to **Cosine Similarity** (via **Normalized Inner Product**) for all text-based collections.

- **The "Unit Vector" Trick**: Vorst $L_2$-normalizes every vector during the **ingestion/WAL** phase so its magnitude (length) is exactly **1.0**.
- **Perfromance Impact**: Because the vectors are normalized, **Cosine Similarity** is mathematically identical to the **Inner Product (Dot Product)**:
  $$ \text{Cosine}(A, B) = \frac{A \cdot B}{\|A\| \|B\|} \xrightarrow{\text{if } \|A\|,\|B\|=1} A \cdot B $$
- **Why this wins**:
  - **Zero Division**: It removes the expensive square root and division operations from the critical search path.
  - **SIMD Friendly**: Modern CPUs (AVX-512, NEON) can calculate a dot product in a single instruction cycle.
  - **Consistency**: Most modern text models (OpenAI, BGE, Nomic) recommend Cosine similarity.
    This ensures Vorst result match the model's intended "semantic distance".

### Storage Access

- **Memory Map**: Utilize `mmap` to access all segmented files.
- **Sequential Pre-Fetching**: Uses `madvise(MADV_SEQUENTIAL)` on mmaps to saturate disk I/O throughput during brute-force (flat index) scans.

### Delete Strategy

Vorst use immutable segmented files, it can't just delete a vector from the middle of a file.

- **The Addition**: add a **"Tombstone Bitset"** for each segment.
- **How it works**: When a user deletes a vector, Vorst marks its index in a tiny bitset file.
  During the scans, if `bitset[i] == 1`, the CPU simply skips that vector.
  This is nearly zero-overhead and keeps segments immutable.

## Phase 1

In this phase, Vorst introduces the **Tiered Segment State**.
The primary goal is to maintain sub-100ms latency even as the collection grows from millions to billions of vectors by trading off bit-precision for search speed.

### The Segment Lifecycle

Vorst managess 3 distinct levels of data. As segments age or grow, they are moved into higher levels with more aggressive compression.
Once a segment is moved to L1 (IVF-SQ8), it is **sealed**. This ensures that the background K-means training isn't interrupted by incoming writes.

| Level   | State    | Indexing & Quantization           | Compaction Trigger                       |
| ------- | -------- | --------------------------------- | ---------------------------------------- |
| **L0**  | **Hot**  | **Flat (FP32/SIMD)**              | When WAL reaches _WAL threshold_.        |
| **L1**  | **Warm** | **IVF-SQ8** (Scalar Quantization) | When total L0 segments > _L0 threshold_. |
| **L2+** | **Cold** | **IVF-PQ**                        | When L1 segments exceed _L1 threshold_.  |

Current thresholds are inspired by **LSM-tree (Log-Structured Merge-tree)** standard used in database like RocksDB and BigTable, adapted for vector hardware constraint.
Each thresholds needs sensitivity analysis.

- _WAL threshold_: ~64MB
- _L0 threshold_: 10 segments
- _L1 threshold_: 1GB

#### IVF-SQ8: The "Warm" Optimization

When L0 segments merge into L1, Vorst performs its first major optimization:

- **Centroid Training**: Vorst runs a fast K-Means on the merged data to create $K$ centroids (clusters).
- **Scalar Quantization (SQ8)**: Instead of storing 32-bit floats, it maps the range of values to an 8-bit integer (0 - 255).
- **Benefit**: Reduces memory/disk footprint by **4x** while maintaining > **99% recall**.
  [IVF_SQ8 Perfromance Benchmarks](https://milvus.io/docs/ivf-sq8.md)
  [Accelerating Similarity Search](https://zilliz.com/blog/Milvus-Index-Types-Supported)

#### IVF-PQ: The "Cold" Compression

For historical data that doesn't need high-precision reranking immediately:

- **How it works**:
  1. Divide the **1024-dim** vector into **64 sub-spaces** (each 16 dimensions).
  1. For each sub-space, Vorst train a small codebook (256 "mini-centroids").
  1. Instead of storing 16 floats, we store **1 byte** (the ID of the closest mini-centroid).
- **The Compression**: This takes a 1024-dim vector from 4096 bytes down to **64 bytes**.
  That is a **64x reduction** from the original FP32.
- **The Use Case**: This is for "Billions of vectors" scale.
  It is lossy, so it _requires_ the reranking step, but it allows to store a massive archive on a single cheap NVMe drive.

### Centroid Evolution & Distortion Logic

To prevent the index from becoming "stale" as user data shifts (e.g., the user starts uploading medical text instead of legal text), Vorst monitors **Distortion**.

#### Distortion Measurement

Distortion is calculated as the **Mean Squared Error (MSE)** between vectors and their assigned centroids during the L0 $\rightarrow$ L1 move.

| Distortion Scale | Action                | Logic                                                                                             |
| ---------------- | --------------------- | ------------------------------------------------------------------------------------------------- |
| **< 10%**        | **Maintain**          | The current map of the "universe" is still accurate                                               |
| **10% - 25%**    | **Incremental Shift** | Apply **Mini-Batch K-Means**. Gently nudge existing centroids toward the new data cluster centers |
| **> 25%**        | **Global Re-Anchor**  | Trigger a full re-train. The data distribution has drifted too far for simple nudges.             |

### Multi-Format Separation

Since Vorst handles text, image, sound, and video. Phase 2 implements **Implicit Collection Namespacing**.

- **Format-Aware Routing**: The binary detects the file type or dimension on the received object and routes it to a format-specific directory structure.
- **No Cross-Pollination**: A search in `/vectors/images` will never scan `/vectors/text` segments, as their centroid and dimensions are incompatible.

```text
/vectors/
├── text/       (Dim: 1024, Metric: Cosine, State: IVF-PQ)
├── images/     (Dim: 512,  Metric: L2,     State: IVF-SQ8)
└── audio/      (Dim: 256,  Metric: IP,     State: Flat)
```

## Phase 2

While Phase 2 optimizes memory by compressing vectors (IVF-PQ), they still typically reside in RAM.
**Phase 3** shifts the paradigm to support datasets larger than available RAM by utilizing **All-in-Storage ANNS with Product Quantization (AISAQ)**.

### Shift from RAM to Storage-Centric Indexing

Vorst moves the "Cold" (L2+) segments to an AISAQ model.

| Feature            | IVF-PQ (Phase 2)                  | AISAQ (Phase 3)                                    |
| ------------------ | --------------------------------- | -------------------------------------------------- |
| **Index Location** | Resides in RAM for fast access.   | Resides on **SSD/NVMe**. Pulled as needed.         |
| **Memory Cost**    | Scalable (Linear with data size). | **Constant** (~10-50MB for codebooks).             |
| **I/O Mechanism**  | Standard `mmap` paging            | **Async I/O** (Linux `io_uring` or Windows `IOCP`) |

### How AISAQ Works in Vorst

AISAQ allows you to index billions of vectors on a machine with only 8GB of RAM.

1. **Compact Codebooks**: Only the centroids and the "navigation graph" (if added) stay in RAM.
1. **Vector Beamwidth**: Instead of loading everything, Vorst issue parallel asynchronous reads to the SSD to fetch only the relevant "clusters" of PQ codes identified by the search.
   This is **tunable parameter**. A higher beamwidth increase recall (by search more clusters) but hits the IOPS harder.
   This allows users on slow SATA SSDs vs. fast NVMe drives to optimize their experience.
1. **Anisotropic Quantization**: AISAQ often utilizes Anisotropic Product Quantization.
   Unlike standard PQ, which treats all dimensions equally, this method minimizes error in the directions that matter most for distance ranking.
   This is perfect for **Matryoshka** vector where the first 256-512 dimensions carry more weight.

### Implementation Goals

- **Zero-Copy Reranking**: When AISAQ indetifies top candidates on disk, the system uses direct I/O to read the original **FP32** vectors for final verification without double-buffering.
- **Hardware Saturation**: The bottleneck shifts from CPU cycles to **IOPS** (Input/Output Operations Per Second). Vorst will be limited only by the speed of the user's SSD.
