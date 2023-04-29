use crate::uring::Uring;
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
use std::error::Error;
use std::fmt::Display;
use std::fmt::Write;
use std::hash::BuildHasherDefault;
use std::sync::Arc;

pub mod commit_object;
pub mod create_object;
pub mod delete_object;
pub mod inspect_object;
pub mod read_object;
pub mod write_object;

pub type OpResult<T> = Result<T, OpError>;

#[derive(Debug)]
pub enum OpError {
  DataStreamError(Box<dyn Error + Send + Sync>),
  DataStreamLengthMismatch,
  InexactWriteLength,
  ObjectMetadataTooLarge,
  ObjectNotFound,
  RangeOutOfBounds,
  UnalignedWrite,
}

impl Display for OpError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      OpError::DataStreamError(e) => {
        write!(f, "an error occurred while reading the input data: {e}")
      }
      OpError::DataStreamLengthMismatch => write!(
        f,
        "the input data stream contains more or less bytes than specified"
      ),
      OpError::InexactWriteLength => write!(f, "data to write is not an exact chunk"),
      OpError::ObjectMetadataTooLarge => write!(f, "object metadata is too large"),
      OpError::ObjectNotFound => write!(f, "object does not exist"),
      OpError::RangeOutOfBounds => write!(f, "requested range to read or write is invalid"),
      OpError::UnalignedWrite => {
        write!(f, "data to write does not start at a multiple of TILE_SIZE")
      }
    }
  }
}

impl Error for OpError {}

#[allow(unused)]
pub(crate) fn key_debug_str(key: &[u8]) -> String {
  std::str::from_utf8(key)
    .map(|k| format!("lit:{k}"))
    .unwrap_or_else(|_| {
      let mut nice = "hex:".to_string();
      for (i, b) in key.iter().enumerate() {
        write!(nice, "{:02x}", b).unwrap();
        if i == 12 {
          nice.push('…');
          break;
        };
      }
      write!(nice, " ({})", key.len()).unwrap();
      nice
    })
}

/// This isn't intended to be shared/thread-safe, but in order to implement Off64AsyncRead we need to make `read` not require a mut reference, so we use an async lock over the map. This should be reasonably fast because it shouldn't be contended, and also has the nice benefit of coalescing the same underlying page reads *should* this be used concurrently/from multiple threads (which it isn't designed to).
struct UnalignedReader {
  dev: Uring,
  page_size_pow2: u8,
  cache: DashMap<u64, Arc<tokio::sync::Mutex<Option<Buf>>>, BuildHasherDefault<FxHasher>>,
}

impl UnalignedReader {
  pub fn new(dev: Uring, page_size_pow2: u8) -> Self {
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
          .read(page_dev_offset, 1 << self.page_size_pow2)
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
    self.read(offset, len).await
  }
}

impl<'a> Off64AsyncReadInt<'a, Buf> for UnalignedReader {}
