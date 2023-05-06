use crate::backing_store::BackingStore;
use crate::util::floor_pow2;
use async_trait::async_trait;
use bufpool::buf::Buf;
use bufpool::BUFPOOL;
use dashmap::DashMap;
use off64::int::Off64AsyncReadInt;
use off64::usz;
use off64::Off64AsyncRead;
use off64::Off64Read;
use rustc_hash::FxHasher;
use std::cmp::min;
use std::hash::BuildHasherDefault;
use std::sync::Arc;

/// This is thread safe and can be shared (even mutably), and coalesces simultaneous reads to the same the same underlying page.
pub(crate) struct UnalignedReader {
  dev: Arc<dyn BackingStore>,
  page_size_pow2: u8,
  cache: DashMap<u64, Arc<tokio::sync::Mutex<Option<Buf>>>, BuildHasherDefault<FxHasher>>,
}

impl UnalignedReader {
  pub fn new(dev: Arc<dyn BackingStore>, page_size_pow2: u8) -> Self {
    Self {
      dev,
      page_size_pow2,
      cache: Default::default(),
    }
  }

  /// It's OK if len` would cross a page boundary, even if smaller than the page size. (It's not efficient, but it's safe and correct.)
  pub async fn read(&self, offset: u64, len: u64) -> Buf {
    let mut out = BUFPOOL.allocate(usz!(len));
    let mut next = offset;
    let end = offset + len;
    while next < end {
      let page_dev_offset = floor_pow2(offset, self.page_size_pow2);
      let start_within_page = next - page_dev_offset;
      let len_within_page = min(1 << self.page_size_pow2, end - next);
      // WARNING: We must `Arc::clone` as otherwise we'll actually hold the DashMap lock, which will very likely deadlock if held across `await`.
      let container = self.cache.entry(page_dev_offset).or_default().clone();
      let mut map_entry = container.lock().await;
      if let Some(o) = map_entry.as_ref() {
        out.extend_from_slice(o.read_at(start_within_page, len_within_page));
      } else {
        let page = self
          .dev
          .read_at(page_dev_offset, 1 << self.page_size_pow2)
          .await;
        out.extend_from_slice(page.read_at(start_within_page, len_within_page));
        *map_entry = Some(page);
      };
      next += len_within_page;
    }
    out
  }
}

#[async_trait]
impl<'a> Off64AsyncRead<'a, Buf> for UnalignedReader {
  async fn read_at(&self, offset: u64, len: u64) -> Buf {
    self.dev.read_at(offset, len).await
  }
}

impl<'a> Off64AsyncReadInt<'a, Buf> for UnalignedReader {}
