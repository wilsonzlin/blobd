use crate::allocator::Allocator;
use crate::backing_store::PartitionStore;
use crate::metrics::BlobdMetrics;
use crate::objects::CommittedObjects;
use crate::objects::IncompleteObjects;
use crate::pages::Pages;
use crate::tuples::Tuples;
use parking_lot::Mutex;

pub(crate) struct Ctx {
  pub committed_objects: CommittedObjects,
  pub device: PartitionStore,
  pub heap_allocator: Mutex<Allocator>,
  pub incomplete_objects: IncompleteObjects,
  pub metrics: BlobdMetrics,
  pub pages: Pages,
  pub partition_idx: usize,
  pub tuples: Tuples,
}
