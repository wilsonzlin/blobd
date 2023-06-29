use crate::allocator::Allocator;
use crate::backing_store::BackingStore;
use crate::bundles::Bundles;
use crate::metrics::BlobdMetrics;
use crate::pages::Pages;
use parking_lot::Mutex;
use std::sync::Arc;

pub(crate) struct Ctx {
  pub bundles: Bundles,
  pub device: Arc<dyn BackingStore>,
  pub heap_allocator: Mutex<Allocator>,
  pub metrics: BlobdMetrics,
  pub pages: Pages,
}
