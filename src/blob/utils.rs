use std::ops::{Deref, DerefMut};

pub struct BlobHasher;

impl BlobHasher {
    pub fn hash(data: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }
}

#[repr(align(8))]
pub struct AlignedBuffer<const N: usize>(pub [u8; N]);

impl<const N: usize> Default for AlignedBuffer<N> {
    fn default() -> Self {
        Self([0u8; N])
    }
}

impl<const N: usize> Deref for AlignedBuffer<N> {
    type Target = [u8; N];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const N: usize> DerefMut for AlignedBuffer<N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[inline(always)]
pub const fn align_to_page(size: u64, page_size: u64) -> u64 {
    // NOTE: Current page alignment is hardcoded to the provided `page_size`. In
    // production, we should query `libc::sysconf(_SC_PAGESIZE)` or use `O_DIRECT`
    // requirements (usually 512 or 4096 bytes) to ensure we are actually hitting
    // the disk's physical sector boundaries for maximum throughput.

    debug_assert!(page_size.is_power_of_two());
    let page_mask = page_size - 1;
    (size + page_mask) & !page_mask
}
