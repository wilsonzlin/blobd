use crate::allocator::Allocator;
use crate::allocator::pages::Pages;
use crate::bucket::Buckets;
use crate::device::IDevice;
use crate::metrics::BlobdMetrics;
use crate::overlay::Overlay;
use parking_lot::Mutex as SyncMutex;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

pub(crate) struct Ctx {
  pub allocator: SyncMutex<Allocator>,
  pub buckets: Buckets,
  pub device: Arc<dyn IDevice>,
  pub metrics: Arc<BlobdMetrics>,
  pub object_id_serial: AtomicU64,
  pub overlay: Arc<Overlay>,
  pub pages: Pages,
  // This value controls:
  // - How long tokens live for, which forces the object to live at least that long (even if marked as deleted).
  // - How long before incomplete objects are automatically sent for deletion.
  pub reap_objects_after_secs: u64,
  pub versioning: bool,
}
