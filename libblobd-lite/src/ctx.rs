use crate::allocator::Allocator;
use crate::allocator::pages::Pages;
use crate::bucket::Buckets;
use crate::device::IDevice;
use crate::metrics::BlobdMetrics;
use crate::overlay::Overlay;
use parking_lot::Mutex as SyncMutex;
use std::sync::Arc;

pub(crate) struct Ctx {
  pub allocator: SyncMutex<Allocator>,
  pub buckets: Buckets,
  pub device: Arc<dyn IDevice>,
  pub metrics: Arc<BlobdMetrics>,
  pub overlay: Arc<Overlay>,
  pub pages: Pages,
  pub reap_incomplete_objects_after_secs: u64,
  pub versioning: bool,
}
