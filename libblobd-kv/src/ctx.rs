use crate::allocator::Allocator;
use crate::backing_store::BackingStore;
use crate::log_buffer::LogBuffer;
use crate::metrics::BlobdMetrics;
use crate::pages::Pages;
use parking_lot::Mutex;
use std::sync::Arc;

pub(crate) struct Ctx {
  pub device: Arc<dyn BackingStore>,
  pub heap_allocator: Mutex<Allocator>,
  pub log_buffer: LogBuffer,
  pub metrics: BlobdMetrics,
  pub pages: Pages,
}
