use crate::allocator::Allocator;
use crate::backing_store::PartitionStore;
use crate::objects::CommittedObjects;
use crate::objects::IncompleteObjects;
use crate::pages::Pages;
use crate::tuples::Tuples;
use cadence::StatsdClient;
use parking_lot::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub(crate) struct Ctx {
  pub committed_objects: CommittedObjects,
  pub device: PartitionStore,
  pub heap_allocator: Mutex<Allocator>,
  pub incomplete_objects: IncompleteObjects,
  pub next_object_id: AtomicU64,
  pub pages: Pages,
  pub partition_idx: usize,
  pub statsd: Option<Arc<StatsdClient>>,
  pub tuples: Tuples,
}
