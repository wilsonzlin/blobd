pub mod file;
#[cfg(target_os = "linux")]
pub mod uring;

use async_trait::async_trait;
use bufpool::buf::Buf;

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
