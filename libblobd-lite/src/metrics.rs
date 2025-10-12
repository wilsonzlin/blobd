use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

pub struct BlobdMetrics {
  // This can only increase. TODO WHY?
  pub(crate) allocated_bytes: AtomicU64,
  pub(crate) deleted_object_count: AtomicU64,
  pub(crate) incomplete_object_count: AtomicU64,
  // This includes incomplete and deleted objects.
  pub(crate) object_count: AtomicU64,
  pub(crate) object_data_bytes: AtomicU64,
  pub(crate) object_metadata_bytes: AtomicU64,
}

#[rustfmt::skip]
impl BlobdMetrics {
  pub(crate) fn new() -> Self {
    Self {
      allocated_bytes: AtomicU64::new(0),
      deleted_object_count: AtomicU64::new(0),
      incomplete_object_count: AtomicU64::new(0),
      object_count: AtomicU64::new(0),
      object_data_bytes: AtomicU64::new(0),
      object_metadata_bytes: AtomicU64::new(0),
    }
  }

  pub fn allocated_bytes(&self) -> u64 { self.allocated_bytes.load(Relaxed) }
  pub fn deleted_object_count(&self) -> u64 { self.deleted_object_count.load(Relaxed) }
  pub fn incomplete_object_count(&self) -> u64 { self.incomplete_object_count.load(Relaxed) }
  pub fn object_count(&self) -> u64 { self.object_count.load(Relaxed) }
  pub fn object_data_bytes(&self) -> u64 { self.object_data_bytes.load(Relaxed) }
  pub fn object_metadata_bytes(&self) -> u64 { self.object_metadata_bytes.load(Relaxed) }
}
