use crate::backing_store::PartitionStore;
use crate::ctx::Ctx;
use crate::journal::Journal;
use crate::metrics::BlobdMetrics;
use crate::object::id::ObjectIdSerial;
use crate::objects::format_device_for_objects;
use crate::objects::load_objects_from_device;
use crate::objects::LoadedObjects;
use crate::pages::Pages;
use crate::state::State;
use crate::state::StateWorker;
use crate::stream::Stream;
use crate::util::ceil_pow2;
use crate::util::floor_pow2;
use crate::BlobdCfg;
use std::sync::Arc;
use tokio::join;
use tracing::info;

/*

PARTITION
=========

Structure
---------

object_id_serial
stream
journal // Placed here to make use of otherwise unused space due to heap alignment.
heap

*/

pub(crate) struct PartitionLoader {
  dev: PartitionStore,
  journal: Journal,
  pages: Pages,
  metrics: Arc<BlobdMetrics>,
  partition_idx: usize,

  object_id_serial_dev_offset: u64,
  object_id_serial_size: u64,
  stream_dev_offset: u64,
  stream_size: u64,
  heap_dev_offset: u64,
  metadata_heap_size: u64,
  data_heap_size: u64,
}

impl PartitionLoader {
  pub fn new(
    partition_idx: usize,
    partition_store: PartitionStore,
    cfg: BlobdCfg,
    pages: Pages,
    metrics: Arc<BlobdMetrics>,
  ) -> Self {
    let object_id_serial_dev_offset = 0;
    let object_id_serial_size = pages.spage_size();

    let stream_dev_offset = object_id_serial_dev_offset + object_id_serial_size;
    let stream_size = floor_pow2(cfg.event_stream_size, cfg.spage_size_pow2);

    let journal_dev_offset = stream_dev_offset + stream_size;
    let min_reserved_space = journal_dev_offset + cfg.journal_size_min;

    // `heap_dev_offset` is equivalent to the reserved size.
    let heap_dev_offset = ceil_pow2(min_reserved_space, pages.lpage_size_pow2);
    let metadata_heap_size = floor_pow2(cfg.object_metadata_reserved_space, cfg.lpage_size_pow2);
    let heap_end = floor_pow2(partition_store.len(), pages.lpage_size_pow2);
    assert!(heap_dev_offset + metadata_heap_size < heap_end);
    let journal_size = floor_pow2(heap_dev_offset - journal_dev_offset, pages.spage_size_pow2);

    let mut journal = Journal::new(
      partition_store.clone(),
      journal_dev_offset,
      journal_size,
      pages.clone(),
    );
    if cfg.dangerously_disable_journal {
      journal.dangerously_disable_journal();
    };

    let data_heap_size = heap_end - heap_dev_offset - metadata_heap_size;

    info!(
      partition_number = partition_idx,
      partition_size = partition_store.len(),
      journal_size,
      reserved_size = heap_dev_offset,
      metadata_heap_size,
      data_heap_size,
      heap_end,
      spage_size = pages.spage_size(),
      lpage_size = pages.lpage_size(),
      "init partition",
    );

    Self {
      data_heap_size,
      dev: partition_store,
      heap_dev_offset,
      journal,
      metadata_heap_size,
      metrics,
      object_id_serial_dev_offset,
      object_id_serial_size,
      pages,
      partition_idx,
      stream_dev_offset,
      stream_size,
    }
  }

  pub async fn format(&self) {
    let dev = &self.dev;
    join! {
      ObjectIdSerial::format_device(dev.bounded(self.object_id_serial_dev_offset, self.object_id_serial_size), &self.pages),
      Stream::format_device(dev.bounded(self.stream_dev_offset, self.stream_size), &self.pages),
      format_device_for_objects(dev.bounded(self.heap_dev_offset, self.heap_dev_offset + self.metadata_heap_size), &self.pages),
      self.journal.format_device(),
    };
    dev.sync().await;
  }

  pub async fn load_and_start(self) -> Partition {
    self.journal.recover().await;

    let dev = &self.dev;
    let pages = &self.pages;

    // Ensure journal has been recovered first before loading any other data.
    let (
      object_id_serial,
      stream,
      LoadedObjects {
        committed_objects,
        data_allocator,
        incomplete_objects,
        metadata_allocator,
      },
    ) = join! {
      ObjectIdSerial::load_from_device(dev.bounded(self.object_id_serial_dev_offset, self.object_id_serial_size), pages.clone()),
      Stream::load_from_device(dev.bounded(self.stream_dev_offset, self.stream_size), pages.clone()),
      load_objects_from_device(dev.clone(), pages.clone(), self.metrics.clone(), self.heap_dev_offset, self.metadata_heap_size, self.data_heap_size)
    };

    let stream = Arc::new(parking_lot::RwLock::new(stream));

    let state = State {
      committed_objects: committed_objects.clone(),
      data_allocator,
      incomplete_objects: incomplete_objects.clone(),
      metadata_allocator,
      metrics: self.metrics.clone(),
      object_id_serial: object_id_serial.clone(),
      pages: pages.clone(),
      partition_idx: self.partition_idx,
      stream: stream.clone(),
    };

    let state_worker = StateWorker::start(state, self.journal);

    let ctx = Arc::new(Ctx {
      committed_objects,
      device: self.dev,
      incomplete_objects,
      object_id_serial,
      pages: pages.clone(),
      state: state_worker,
    });

    Partition { ctx, stream }
  }
}

pub(crate) struct Partition {
  pub ctx: Arc<Ctx>,
  pub stream: Arc<parking_lot::RwLock<Stream>>,
}
