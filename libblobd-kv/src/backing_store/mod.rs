pub mod file;
#[cfg(target_os = "linux")]
pub mod uring;

use async_trait::async_trait;
use bufpool::buf::Buf;
use off64::u64;
use std::sync::Arc;

#[async_trait]
pub(crate) trait BackingStore: Send + Sync {
  /// `offset` and `len` must be multiples of the underlying device's sector size.
  async fn read_at(&self, offset: u64, len: u64) -> Buf;

  /// `offset` and `data.len()` must be multiples of the underlying device's sector size.
  /// Returns the original `data` so that it can be reused, if desired.
  async fn write_at(&self, offset: u64, data: Buf) -> Buf;

  /// Even when using direct I/O, `fsync` is still necessary, as it ensures the device itself has flushed any internal caches.
  async fn sync(&self);
}

#[derive(Clone)]
pub(crate) struct BoundedStore {
  backing_store: Arc<dyn BackingStore>,
  offset: u64,
  len: u64,
}

impl BoundedStore {
  pub fn new(backing_store: Arc<dyn BackingStore>, offset: u64, len: u64) -> Self {
    Self {
      backing_store,
      len,
      offset,
    }
  }

  pub async fn read_at(&self, offset: u64, len: u64) -> Buf {
    assert!(
      offset + len <= self.len,
      "attempted to read at {} with {} bytes but store ends at {}",
      offset,
      len,
      self.len
    );
    self.backing_store.read_at(self.offset + offset, len).await
  }

  pub async fn write_at(&self, offset: u64, data: Buf) {
    assert!(
      offset + u64!(data.len()) <= self.len,
      "attempted to write at {} with {} bytes but store ends at {}",
      offset,
      data.len(),
      self.len
    );
    self
      .backing_store
      .write_at(self.offset + offset, data)
      .await;
  }
}
