use std::{
    fs::{File, OpenOptions},
    io,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use bytemuck::{Pod, Zeroable};
use chrono::Utc;
use memmap2::Mmap;

use crate::sys::preallocate;

pub const VRST: u32 = 0x56525354;
pub const VECTOR_DIM: usize = 1024;
pub const VECTOR_SIZE: usize = VECTOR_DIM * size_of::<f32>();
pub const SEGMENT_ALIGNMENT: usize = 64 * 1024;
pub const SEGMENT_SIZE: usize = 1024 * 1024 * 1024;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct VectorSegmentHeader {
    pub magic: u32,
    pub version: u32,
    pub vector_count: u64,
    pub created_at: i64,
}

pub trait VectorSegmentState {
    fn cleanup(&mut self) {}
}

pub struct Active {
    file: File,
    count: u64,
    path: PathBuf,
    active_vectors: Vec<f32>,
}
pub struct Sealed {
    header: VectorSegmentHeader,
    mmap: Mmap,
}

impl VectorSegmentState for Active {
    fn cleanup(&mut self) {
        self.file.sync_all().ok();
    }
}

impl VectorSegmentState for Sealed {}

pub struct VectorSegment<S: VectorSegmentState> {
    state: S,
}

impl VectorSegment<Active> {
    pub fn create<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        preallocate(&file, SEGMENT_SIZE as u64)?;

        let header = VectorSegmentHeader {
            magic: VRST,
            version: 1,
            vector_count: 0,
            created_at: Utc::now().timestamp(),
        };
        let header_bytes = bytemuck::bytes_of(&header);
        file.write_all_at(header_bytes, 0)?;

        Ok(VectorSegment {
            state: Active {
                file,
                count: 0,
                path: path.as_ref().to_path_buf(),
                active_vectors: vec![],
            },
        })
    }

    pub fn open<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        let mut header_bytes = [0u8; size_of::<VectorSegmentHeader>()];
        file.read_exact_at(&mut header_bytes, 0)?;
        let header: VectorSegmentHeader = *bytemuck::from_bytes(&header_bytes);

        let mut active_vectors = Vec::new();
        let vector_count = if header.magic == VRST {
            header.vector_count
        } else {
            0
        };

        if vector_count > 0 {
            let data_len_bytes = vector_count as usize * VECTOR_SIZE;
            let mut buffer = vec![0u8; data_len_bytes];
            file.read_exact_at(&mut buffer, SEGMENT_ALIGNMENT as u64)?;
            let vectors = bytemuck::cast_slice(&buffer);
            active_vectors.extend_from_slice(vectors);
        }

        let count = (active_vectors.len() / VECTOR_DIM) as u64;

        Ok(VectorSegment {
            state: Active {
                file,
                count,
                path: path.as_ref().to_path_buf(),
                active_vectors,
            },
        })
    }

    pub fn append(&mut self, vector: &[f32]) -> io::Result<usize> {
        let offset = SEGMENT_ALIGNMENT as u64 + (self.state.count * VECTOR_SIZE as u64);

        let bytes = bytemuck::cast_slice(vector);
        self.state.file.write_all_at(bytes, offset)?;

        self.state.active_vectors.extend_from_slice(vector);

        let index = self.state.count as usize;
        self.state.count += 1;

        let header_offset = size_of::<u32>() + size_of::<u32>();
        self.state
            .file
            .write_all_at(&self.state.count.to_le_bytes(), header_offset as u64)?;

        Ok(index)
    }

    pub fn seal(self) -> io::Result<VectorSegment<Sealed>> {
        VectorSegment::<Sealed>::open(&self.state.path)
    }

    #[inline(always)]
    pub fn vectors(&self) -> &[f32] {
        &self.state.active_vectors
    }

    #[inline(always)]
    pub fn vector_at(&self, index: usize) -> &[f32] {
        let start = index * VECTOR_DIM;
        let end = start + VECTOR_DIM;
        &self.state.active_vectors[start..end]
    }
}

impl VectorSegment<Sealed> {
    pub fn open<P>(path: P) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        if mmap.len() < SEGMENT_ALIGNMENT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file too small for header zone",
            ));
        }

        let header_bytes = &mmap[..size_of::<VectorSegmentHeader>()];
        let header: VectorSegmentHeader = *bytemuck::from_bytes(header_bytes);

        if header.magic != VRST {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic bytes",
            ));
        }

        #[cfg(unix)]
        unsafe {
            libc::madvise(
                mmap.as_ptr() as *mut libc::c_void,
                mmap.len(),
                libc::MADV_SEQUENTIAL,
            );
        }

        Ok(Self {
            state: Sealed { header, mmap },
        })
    }

    #[inline(always)]
    pub fn vectors(&self) -> &[f32] {
        let byte_len = self.vector_count() * VECTOR_SIZE;
        let start = SEGMENT_ALIGNMENT;
        let end = SEGMENT_ALIGNMENT + byte_len;
        bytemuck::cast_slice(&self.state.mmap[start..end])
    }

    #[inline(always)]
    pub fn vector_at(&self, index: usize) -> &[f32] {
        let start = SEGMENT_ALIGNMENT + (index * VECTOR_SIZE);
        let end = start + VECTOR_SIZE;
        bytemuck::cast_slice(&self.state.mmap[start..end])
    }

    #[inline(always)]
    pub fn vector_count(&self) -> usize {
        self.state.header.vector_count as usize
    }
}

impl<S: VectorSegmentState> Drop for VectorSegment<S> {
    fn drop(&mut self) {
        self.state.cleanup();
    }
}
