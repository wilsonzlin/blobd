use bufpool::buf::Buf;
use bufpool::BufPool;
use off64::usz;
use std::ops::Deref;
use std::sync::Arc;

pub(crate) struct Inner {
  pub lpage_size_pow2: u8,
  pub spage_size_pow2: u8,
  pool: BufPool,
}

#[derive(Clone)]
pub(crate) struct Pages(Arc<Inner>);

impl Pages {
  pub fn new(spage_size_pow2: u8, lpage_size_pow2: u8) -> Self {
    Self(Arc::new(Inner {
      lpage_size_pow2,
      spage_size_pow2,
      pool: BufPool::with_alignment(1 << spage_size_pow2),
    }))
  }

  pub fn lpage_size(&self) -> u64 {
    1 << self.lpage_size_pow2
  }

  pub fn spage_size(&self) -> u64 {
    1 << self.spage_size_pow2
  }

  /// `size` must be a multiple of the spage size.
  pub fn allocate_with_zeros(&self, size: u64) -> Buf {
    self.pool.allocate_with_zeros(usz!(size))
  }

  /// `data.len()` must be a multiple of the spage size.
  pub fn allocate_from_data(&self, data: &[u8]) -> Buf {
    self.pool.allocate_from_data(data)
  }
}

impl Deref for Pages {
  type Target = Inner;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}
