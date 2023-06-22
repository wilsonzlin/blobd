pub mod file;
#[cfg(target_os = "linux")]
pub mod uring;

use async_trait::async_trait;
use bufpool::buf::Buf;
use off64::u64;
use std::sync::Arc;

/*

Why use three different types instead of simply one?
- Offsets: lots of subtleties due to different levels of offsets, with different interpretations by different components. For example, the journal always writes in offsets relative to the partition, but if there's only one type, then a bounded type only knows about itself and the root (i.e. device), giving incorrect offsets.
- Names: similar to the previous point, the type name gets quite confusing, as some are used partition level while others are bounded to a specific range, but they all have the same type and API.

Using a generic trait for BackingStore allows us to provide different variants for testing, in-memory, different platforms, user configurability, etc.

*/

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
pub(crate) struct PartitionStore {
  backing_store: Arc<dyn BackingStore>,
  offset: u64,
  len: u64,
}

impl PartitionStore {
  pub fn new(backing_store: Arc<dyn BackingStore>, offset: u64, len: u64) -> Self {
    Self {
      backing_store,
      offset,
      len,
    }
  }

  pub fn offset(&self) -> u64 {
    self.offset
  }

  pub fn len(&self) -> u64 {
    self.len
  }

  pub async fn read_at(&self, offset: u64, len: u64) -> Buf {
    assert!(
      offset + len <= self.len,
      "attempted to read at {} with length {} but partition has length {}",
      offset,
      len,
      self.len
    );
    self.backing_store.read_at(self.offset + offset, len).await
  }

  pub async fn write_at(&self, offset: u64, data: Buf) {
    let len = u64!(data.len());
    assert!(
      offset + len <= self.len,
      "attempted to write at {} with length {} but partition has length {}",
      offset,
      len,
      self.len
    );
    self
      .backing_store
      .write_at(self.offset + offset, data)
      .await;
  }

  pub async fn sync(&self) {
    self.backing_store.sync().await;
  }

  /// `offset` must be a multiple of the underlying device's sector size.
  pub fn bounded(&self, offset: u64, len: u64) -> BoundedStore {
    assert!(offset + len <= self.len);
    BoundedStore {
      partition_store: self.clone(),
      offset,
      len,
    }
  }
}

#[derive(Clone)]
pub(crate) struct BoundedStore {
  partition_store: PartitionStore,
  offset: u64,
  len: u64,
}

impl BoundedStore {
  pub fn len(&self) -> u64 {
    self.len
  }

  pub async fn read_at(&self, offset: u64, len: u64) -> Buf {
    assert!(offset + len <= self.len);
    self
      .partition_store
      .read_at(self.offset + offset, len)
      .await
  }

  pub async fn write_at(&self, offset: u64, data: Buf) {
    assert!(offset + u64!(data.len()) <= self.len);
    self
      .partition_store
      .write_at(self.offset + offset, data)
      .await;
  }
}
