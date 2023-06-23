use crate::backing_store::PartitionStore;
use crate::ctx::Ctx;
use crate::objects::format_device_for_objects;
use crate::objects::load_objects_from_device;
use crate::objects::LoadedObjects;
use crate::pages::Pages;
use crate::util::ceil_pow2;
use crate::util::floor_pow2;
use crate::BlobdCfg;
use cadence::StatsdClient;
use parking_lot::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::info;

pub(crate) struct PartitionLoader {
  dev: PartitionStore,
  pages: Pages,
  statsd: Option<Arc<StatsdClient>>,
  partition_idx: usize,
  heap_dev_offset: u64,
  heap_size: u64,
}

impl PartitionLoader {
  pub fn new(
    partition_idx: usize,
    partition_store: PartitionStore,
    cfg: BlobdCfg,
    pages: Pages,
  ) -> Self {
    let tuples_area_size = ceil_pow2(cfg.object_tuples_area_reserved_space, cfg.lpage_size_pow2);
    let heap_dev_offset = tuples_area_size;
    let heap_end = floor_pow2(partition_store.len(), pages.lpage_size_pow2);
    let heap_size = heap_end - heap_dev_offset;
    assert!(tuples_area_size + heap_dev_offset <= heap_end);

    info!(
      partition_number = partition_idx,
      partition_offset = partition_store.offset(),
      partition_size = partition_store.len(),
      heap_dev_offset = heap_dev_offset,
      heap_size,
      "init partition",
    );

    Self {
      dev: partition_store,
      heap_dev_offset,
      heap_size,
      pages,
      partition_idx,
      statsd: cfg.statsd.clone(),
    }
  }

  pub async fn format(&self) {
    format_device_for_objects(self.dev.bounded(0, self.heap_dev_offset), &self.pages).await;
    self.dev.sync().await;
  }

  pub async fn load_and_start(self) -> Partition {
    let dev = &self.dev;
    let pages = &self.pages;

    let LoadedObjects {
      committed_objects,
      heap_allocator,
      incomplete_objects,
      next_object_id,
      tuples,
    } = load_objects_from_device(
      dev.clone(),
      pages.clone(),
      self.statsd.clone(),
      self.heap_dev_offset,
      self.heap_size,
    )
    .await;

    let ctx = Arc::new(Ctx {
      committed_objects,
      device: self.dev.clone(),
      heap_allocator: Mutex::new(heap_allocator),
      incomplete_objects,
      next_object_id: AtomicU64::new(next_object_id),
      pages: pages.clone(),
      partition_idx: self.partition_idx,
      statsd: self.statsd,
      tuples,
    });

    Partition { ctx }
  }
}

pub(crate) struct Partition {
  pub ctx: Arc<Ctx>,
}
