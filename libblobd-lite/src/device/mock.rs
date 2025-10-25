use crate::device::IDevice;
use crate::util::div_mod_pow2;
use ahash::HashSet;
use async_trait::async_trait;
use dashmap::DashMap;
use off64::Off64AsyncRead;
use off64::Off64AsyncWrite;
use off64::Off64WriteMut;
use off64::chrono::Off64AsyncReadChrono;
use off64::chrono::Off64AsyncWriteChrono;
use off64::int::Off64AsyncReadInt;
use off64::int::Off64AsyncWriteInt;
use off64::u64;
use off64::usz;
use std::cmp::min;

const PAGE_SIZE_POW2: u8 = 12;
const PAGE_SIZE: u64 = 1 << PAGE_SIZE_POW2;

/// NOTE: When testing, prefer to build expected version and test for equality, instead of performing/testing individual reads, to ensure entire device is correct (e.g. no writes to invalid locations).
#[derive(Clone)]
pub struct MockDevice {
  pub pages: DashMap<u64, Box<[u8; PAGE_SIZE as usize]>>,
}

impl MockDevice {
  pub fn new() -> Self {
    Self {
      pages: Default::default(),
    }
  }
}

impl MockDevice {
  pub fn read_at(&self, start: u64, len: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(usz!(len));

    let end = start + len;
    let mut next = start;
    while next < end {
      let (page, offset_within_page) = div_mod_pow2(next, PAGE_SIZE_POW2);
      let n = min(end - next, PAGE_SIZE - offset_within_page);
      data.extend_from_slice(
        &self
          .pages
          .entry(page)
          .or_insert_with(|| Box::new([0u8; PAGE_SIZE as usize]))
          [usz!(offset_within_page)..usz!(offset_within_page + n)],
      );
      next += n;
    }
    data.into()
  }

  pub fn write_at<D: AsRef<[u8]>>(&self, start: u64, data: D) {
    let mut cur = &data.as_ref()[..];

    let end = start + u64!(cur.len());
    let mut next = start;
    while next < end {
      let (page, offset_within_page) = div_mod_pow2(next, PAGE_SIZE_POW2);
      let n = min(end - next, PAGE_SIZE - offset_within_page);
      self
        .pages
        .entry(page)
        .or_insert_with(|| Box::new([0u8; PAGE_SIZE as usize]))
        .write_at(offset_within_page, &cur[..usz!(n)]);
      cur = &cur[usz!(n)..];
      next += n;
    }
  }
}

#[async_trait]
impl IDevice for MockDevice {}

impl PartialEq for MockDevice {
  fn eq(&self, other: &Self) -> bool {
    let mut missing_self: HashSet<u64> = self.pages.iter().map(|e| *e.key()).collect();
    for e in other.pages.iter() {
      let (offset, page) = e.pair();
      if !missing_self.remove(&offset) {
        return false;
      };
      if self.pages.get(&offset).unwrap().as_slice() != page.as_slice() {
        return false;
      };
    }
    missing_self.is_empty()
  }
}

impl Eq for MockDevice {}

#[async_trait]
impl<'a> Off64AsyncRead<'a, Vec<u8>> for MockDevice {
  async fn read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    self.read_at(offset, len)
  }
}
impl<'a> Off64AsyncReadChrono<'a, Vec<u8>> for MockDevice {}
impl<'a> Off64AsyncReadInt<'a, Vec<u8>> for MockDevice {}

#[async_trait]
impl Off64AsyncWrite for MockDevice {
  async fn write_at(&self, offset: u64, value: &[u8]) {
    self.write_at(offset, value)
  }
}
impl Off64AsyncWriteChrono for MockDevice {}
impl Off64AsyncWriteInt for MockDevice {}
