use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

pub struct BlobdMetrics {
  // This can only increase.
  // TODO Corresponding deallocated_bytes.
  pub(crate) allocated_bytes: AtomicU64,

  pub(crate) incomplete_object_count: AtomicU64,
  // This includes incomplete objects.
  pub(crate) object_count: AtomicU64,
  pub(crate) object_data_bytes: AtomicU64,
  pub(crate) object_metadata_bytes: AtomicU64,

  pub(crate) state_lock_acq_ns: AtomicU64,
  pub(crate) state_lock_held_ns: AtomicU64,
}

#[rustfmt::skip]
impl BlobdMetrics {
  pub(crate) fn new() -> Self {
    Self {
      allocated_bytes: AtomicU64::new(0),
      incomplete_object_count: AtomicU64::new(0),
      object_count: AtomicU64::new(0),
      object_data_bytes: AtomicU64::new(0),
      object_metadata_bytes: AtomicU64::new(0),
      state_lock_acq_ns: AtomicU64::new(0),
      state_lock_held_ns: AtomicU64::new(0),
    }
  }

  pub fn allocated_bytes(&self) -> u64 { self.allocated_bytes.load(Relaxed) }
  pub fn incomplete_object_count(&self) -> u64 { self.incomplete_object_count.load(Relaxed) }
  pub fn object_count(&self) -> u64 { self.object_count.load(Relaxed) }
  pub fn object_data_bytes(&self) -> u64 { self.object_data_bytes.load(Relaxed) }
  pub fn object_metadata_bytes(&self) -> u64 { self.object_metadata_bytes.load(Relaxed) }
  pub fn state_lock_acq_ns(&self) -> u64 { self.state_lock_acq_ns.load(Relaxed) }
  pub fn state_lock_held_ns(&self) -> u64 { self.state_lock_held_ns.load(Relaxed) }
}
