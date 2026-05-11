use std::ops::{Deref, DerefMut};

use crate::PAGE_SIZE;

pub struct BlobHasher;

impl BlobHasher {
    #[inline(always)]
    pub fn hash(data: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }
}

#[repr(align(65536))]
pub struct AlignedBuffer<const N: usize>(pub [u8; N]);

impl<const N: usize> AlignedBuffer<N> {
    pub fn new_boxed() -> Box<Self> {
        let boxed_slice = vec![0u8; N].into_boxed_slice();
        let ptr = Box::into_raw(boxed_slice) as *mut AlignedBuffer<N>;
        unsafe { Box::from_raw(ptr) }
    }
}

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
pub const fn align_to_page(size: u64) -> u64 {
    const { assert!(PAGE_SIZE.is_power_of_two()) };
    let page_mask = PAGE_SIZE - 1;
    (size + page_mask) & !page_mask
}
