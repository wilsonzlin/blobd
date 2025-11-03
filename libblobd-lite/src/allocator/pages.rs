pub(crate) const MIN_PAGE_SIZE_POW2: u8 = 8;
pub(crate) const MAX_PAGE_SIZE_POW2: u8 = 32;

#[derive(Clone, Copy)]
pub(crate) struct Pages {
  pub lpage_size_pow2: u8,
  pub spage_size_pow2: u8,
}

impl Pages {
  pub fn new(spage_size_pow2: u8, lpage_size_pow2: u8) -> Pages {
    Pages {
      lpage_size_pow2,
      spage_size_pow2,
    }
  }

  #[allow(unused)]
  pub fn spage_size(&self) -> u64 {
    1 << self.spage_size_pow2
  }

  #[allow(unused)]
  pub fn lpage_size(&self) -> u64 {
    1 << self.lpage_size_pow2
  }
}
