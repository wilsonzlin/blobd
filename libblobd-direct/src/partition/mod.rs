use crate::backing_store::PartitionStore;
use crate::ctx::Ctx;
use crate::metrics::BlobdMetrics;
use crate::objects::format_device_for_objects;
use crate::objects::load_objects_from_device;
use crate::objects::ClusterLoadProgress;
use crate::objects::LoadedObjects;
use crate::pages::Pages;
use crate::tuples::load_raw_tuples_area_from_device;
use crate::util::ceil_pow2;
use crate::util::floor_pow2;
use crate::BlobdCfg;
use bufpool::buf::Buf;
use parking_lot::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::spawn;
use tracing::info;

pub(crate) struct PartitionLoader {
  pub(crate) dev: PartitionStore,
  pages: Pages,
  metrics: BlobdMetrics,
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
    metrics: BlobdMetrics,
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
      metrics,
      pages,
      partition_idx,
    }
  }

  pub async fn format(&self) {
    format_device_for_objects(self.dev.bounded(0, self.heap_dev_offset), &self.pages).await;
    self.dev.sync().await;
  }

  pub async fn load_raw_tuples_area(&self) -> Buf {
    load_raw_tuples_area_from_device(&self.dev, self.heap_dev_offset).await
  }

  pub async fn load_and_start(self, load_progress: Arc<ClusterLoadProgress>) -> Partition {
    let dev = &self.dev;
    let pages = &self.pages;

    let LoadedObjects {
      committed_objects,
      heap_allocator,
      incomplete_objects,
      next_object_id,
      tuples,
    } = load_objects_from_device(
      load_progress,
      dev.clone(),
      pages.clone(),
      self.metrics.clone(),
      self.heap_dev_offset,
      self.heap_size,
    )
    .await;

    let ctx = Arc::new(Ctx {
      committed_objects,
      device: self.dev.clone(),
      heap_allocator: Mutex::new(heap_allocator),
      incomplete_objects,
      metrics: self.metrics.clone(),
      next_object_id: AtomicU64::new(next_object_id),
      pages: pages.clone(),
      partition_idx: self.partition_idx,
      tuples: tuples.clone(),
    });

    spawn({
      let dev = dev.bounded(0, self.heap_dev_offset);
      let pages = pages.clone();
      async move { tuples.start_background_commit_loop(dev, pages).await }
    });

    Partition { ctx }
  }
}

pub(crate) struct Partition {
  pub ctx: Arc<Ctx>,
}
