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
use crate::uring::UringBounded;
use crate::util::ceil_pow2;
use crate::util::floor_pow2;
use crate::BlobdCfg;
use std::fs::OpenOptions;
use std::os::unix::prelude::OpenOptionsExt;
use std::sync::Arc;
use tokio::join;
use tracing::info;

/**

PARTITION
=========

Structure
---------

object_id_serial
stream
journal // Placed here to make use of otherwise unused space due to heap alignment.
heap

**/

pub(crate) struct PartitionLoader {
  dev: UringBounded,
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
    cfg: BlobdCfg,
    partition_idx: usize,
    pages: Pages,
    metrics: Arc<BlobdMetrics>,
  ) -> Self {
    let part = &cfg.partitions[partition_idx];
    let part_file = OpenOptions::new()
      .read(true)
      .write(true)
      .custom_flags(libc::O_DIRECT)
      .open(&part.path)
      .unwrap();
    let dev = UringBounded::new(part_file, part.offset, part.len, pages.clone());

    let object_id_serial_dev_offset = 0;
    let object_id_serial_size = pages.spage_size();

    let stream_dev_offset = object_id_serial_dev_offset + object_id_serial_size;
    let stream_size = cfg.event_stream_size;

    let journal_dev_offset = stream_dev_offset + stream_size;
    let min_reserved_space = journal_dev_offset + cfg.journal_size_min;

    // `heap_dev_offset` is equivalent to the reserved size.
    let heap_dev_offset = ceil_pow2(min_reserved_space, pages.lpage_size_pow2);
    let heap_end = floor_pow2(dev.len(), pages.lpage_size_pow2);
    assert!(heap_dev_offset < heap_end);
    let journal_size = floor_pow2(heap_dev_offset - journal_dev_offset, pages.spage_size_pow2);

    let journal = Journal::new(dev.clone(), journal_dev_offset, journal_size, pages.clone());

    let metadata_heap_size = cfg.object_metadata_reserved_space;
    let data_heap_size = heap_end - heap_dev_offset - metadata_heap_size;

    info!(
      partition_number = partition_idx,
      partition_file = part.path.to_string_lossy().to_string(),
      partition_offset = part.offset,
      partition_size = part.len,
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
      dev,
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

  pub async fn load(self) -> Partition {
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
      load_objects_from_device(dev.bounded(self.heap_dev_offset, self.heap_dev_offset + self.metadata_heap_size), pages.clone(), self.metrics.clone(), self.heap_dev_offset, self.metadata_heap_size, self.data_heap_size)
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
      stream,
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

    Partition { ctx }
  }
}

pub(crate) struct Partition {
  pub ctx: Arc<Ctx>,
}
