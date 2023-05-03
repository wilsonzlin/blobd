use serde::Serialize;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

#[derive(Default, Serialize)]
pub struct BlobdMetrics {
  // To determine internal fragmentation, take `used_bytes` and subtract `object_data_bytes + object_metadata_bytes`. This is easier than attempting to track internal fragmentation directly.
  object_data_bytes: AtomicU64,
  object_metadata_bytes: AtomicU64,
  // Sum of non-free page sizes.
  used_bytes: AtomicU64,
}

#[rustfmt::skip]
impl BlobdMetrics {
  pub fn object_data_bytes(&self) -> u64 { self.object_data_bytes.load(Relaxed) }
  pub fn object_metadata_bytes(&self) -> u64 { self.object_metadata_bytes.load(Relaxed) }
  pub fn used_bytes(&self) -> u64 { self.used_bytes.load(Relaxed) }

  pub(crate) fn incr_object_data_bytes(&self, d: u64) { self.object_data_bytes.fetch_add(d, Relaxed); }
  pub(crate) fn incr_object_metadata_bytes(&self, d: u64) { self.object_metadata_bytes.fetch_add(d, Relaxed); }
  pub(crate) fn incr_used_bytes(&self, d: u64) { self.used_bytes.fetch_add(d, Relaxed); }

  pub(crate) fn decr_object_data_bytes(&self, d: u64) { self.object_data_bytes.fetch_sub(d, Relaxed); }
  pub(crate) fn decr_object_metadata_bytes(&self, d: u64) { self.object_metadata_bytes.fetch_sub(d, Relaxed); }
  pub(crate) fn decr_used_bytes(&self, d: u64) { self.used_bytes.fetch_sub(d, Relaxed); }
}
