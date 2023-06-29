use bufpool::buf::Buf;
use bufpool::BufPool;
use off64::usz;
use std::ops::Deref;
use std::sync::Arc;
use tracing::trace;

pub(crate) struct Inner {
  pub lpage_size_pow2: u8,
  pub spage_size_pow2: u8,
  pool: BufPool,
}

#[derive(Clone)]
pub(crate) struct Pages(Arc<Inner>);

#[allow(unused)]
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

  /// `cap` must be a multiple of the spage size.
  pub fn allocate(&self, cap: u64) -> Buf {
    trace!(cap, "allocating buffer");
    let res = self.pool.allocate(usz!(cap));
    trace!(cap, "allocated buffer");
    res
  }

  /// `len` must be a multiple of the spage size.
  // Prefer this over `allocate_with_zeros` as it's extremely slow. If you need it, use `slow_allocate_with_zeros`.
  pub fn allocate_uninitialised(&self, len: u64) -> Buf {
    self.pool.allocate_uninitialised(usz!(len))
  }

  /// This is slow, so avoid it in hot paths.
  pub fn slow_allocate_with_zeros(&self, len: u64) -> Buf {
    self.pool.allocate_with_zeros(usz!(len))
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
