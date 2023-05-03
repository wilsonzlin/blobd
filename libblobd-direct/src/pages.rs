use bufpool_fixed::buf::FixedBuf;
use bufpool_fixed::FixedBufPool;
use off64::usz;
use std::ops::Deref;
use std::sync::Arc;

pub(crate) struct Inner {
  pub lpage_size_pow2: u8,
  pub spage_size_pow2: u8,
  pool: FixedBufPool,
}

#[derive(Clone)]
pub(crate) struct Pages(Arc<Inner>);

impl Pages {
  pub fn new(spage_size_pow2: u8, lpage_size_pow2: u8) -> Self {
    Self(Arc::new(Inner {
      lpage_size_pow2,
      spage_size_pow2,
      pool: FixedBufPool::with_alignment(1 << spage_size_pow2),
    }))
  }

  pub fn lpage_size(&self) -> u64 {
    1 << self.lpage_size_pow2
  }

  pub fn spage_size(&self) -> u64 {
    1 << self.spage_size_pow2
  }

  /// `size` must be a multiple of the spage size.
  pub fn allocate_with_zeros(&self, size: u64) -> FixedBuf {
    self.pool.allocate_with_zeros(usz!(size))
  }

  /// `data.len()` must be a multiple of the spage size.
  pub fn allocate_from_data(&self, data: &[u8]) -> FixedBuf {
    let mut buf = self.pool.allocate_with_zeros(data.len());
    buf.copy_from_slice(data);
    buf
  }
}

impl Deref for Pages {
  type Target = Inner;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}
