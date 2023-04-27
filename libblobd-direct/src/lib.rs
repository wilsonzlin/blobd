#![allow(non_snake_case)]

use crate::allocator::Allocator;
use crate::bucket::BucketsWriter;
use crate::incomplete_list::IncompleteListWriter;
use crate::object::id::ObjectIdSerial;
use crate::stream::Stream;
use crate::util::ceil_pow2;
use ctx::Ctx;
use futures::join;
use journal::Journal;
use metrics::BlobdMetrics;
use op::commit_object::op_commit_object;
use op::commit_object::OpCommitObjectInput;
use op::commit_object::OpCommitObjectOutput;
use op::create_object::op_create_object;
use op::create_object::OpCreateObjectInput;
use op::create_object::OpCreateObjectOutput;
use op::delete_object::op_delete_object;
use op::delete_object::OpDeleteObjectInput;
use op::delete_object::OpDeleteObjectOutput;
use op::inspect_object::op_inspect_object;
use op::inspect_object::OpInspectObjectInput;
use op::inspect_object::OpInspectObjectOutput;
use op::read_object::op_read_object;
use op::read_object::OpReadObjectInput;
use op::read_object::OpReadObjectOutput;
use op::write_object::op_write_object;
use op::write_object::OpWriteObjectInput;
use op::write_object::OpWriteObjectOutput;
use op::OpResult;
use state::ObjectsPendingDrop;
use state::State;
use state::StateWorker;
use std::error::Error;
use std::fs::File;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use stream::StreamEvent;
use stream::StreamEventExpiredError;
use tracing::info;
use uring::Uring;

pub mod allocator;
pub mod bucket;
pub mod ctx;
pub mod incomplete_list;
pub mod incomplete_token;
pub mod journal;
pub mod metrics;
pub mod object;
pub mod op;
pub mod persisted_collection;
pub mod state;
pub mod stream;
pub mod uring;
pub mod util;

/**

DEVICE
======

Structure
---------

metrics
object_id_serial
stream
incomplete_list (first page)
buckets (first page)
allocator (initial bitmap container pages)
journal // Placed here to make use of otherwise unused space due to heap alignment.
heap

**/

#[derive(Clone, Debug)]
pub struct BlobdCfg {
  pub event_stream_spage_capacity: u64,
  pub lpage_size_pow2: u8,
  pub reap_objects_after_secs: u64,
  pub spage_size_pow2: u8,
  pub versioning: bool,
}

impl BlobdCfg {
  pub fn lpage_size(&self) -> u64 {
    1 << self.lpage_size_pow2
  }

  pub fn spage_size(&self) -> u64 {
    1 << self.spage_size_pow2
  }
}

pub struct BlobdLoader {
  device: Uring,
  device_size: Arc<AtomicU64>, // To allow online resizing, this must be atomically mutable at any time.
  cfg: BlobdCfg,
  journal: Journal,

  metrics_dev_offset: u64,
  object_id_serial_dev_offset: u64,
  stream_dev_offset: u64,
  incomplete_list_dev_offset: u64,
  buckets_dev_offset: u64,
  allocator_dev_offset: u64,
  heap_dev_offset: u64,
}

impl BlobdLoader {
  pub fn new(device: File, device_size: u64, cfg: BlobdCfg) -> Self {
    assert!(cfg.reap_objects_after_secs > 0);

    const JOURNAL_SIZE_MIN: u64 = 1024 * 1024 * 1024 * 1;
    let allocator_size = Allocator::initial_bitmap_container_page_count(
      device_size,
      cfg.spage_size_pow2,
      cfg.lpage_size_pow2,
    ) * cfg.spage_size();

    let device = Uring::new(device);

    let metrics_dev_offset = 0;
    let object_id_serial_dev_offset = metrics_dev_offset + cfg.spage_size();
    let stream_dev_offset = object_id_serial_dev_offset + cfg.spage_size();
    let incomplete_list_dev_offset = stream_dev_offset + cfg.spage_size();
    let buckets_dev_offset = incomplete_list_dev_offset + cfg.spage_size();
    let allocator_dev_offset = buckets_dev_offset + cfg.spage_size();
    let journal_dev_offset = buckets_dev_offset + allocator_size;
    let min_reserved_space = journal_dev_offset + JOURNAL_SIZE_MIN;

    // `heap_dev_offset` is equivalent to the reserved size.
    let heap_dev_offset = ceil_pow2(min_reserved_space, cfg.lpage_size_pow2);
    let journal_size = heap_dev_offset - journal_dev_offset;

    info!(
      device_size,
      journal_size,
      reserved_size = heap_dev_offset,
      lpage_size = 1 << cfg.lpage_size_pow2,
      spage_size = 1 << cfg.spage_size_pow2,
      "init",
    );

    let journal = Journal::new(
      device.clone(),
      journal_dev_offset,
      journal_size,
      cfg.spage_size_pow2,
    );

    Self {
      allocator_dev_offset,
      buckets_dev_offset,
      cfg,
      device_size: Arc::new(AtomicU64::new(device_size)),
      device,
      heap_dev_offset,
      journal,
      incomplete_list_dev_offset,
      metrics_dev_offset,
      object_id_serial_dev_offset,
      stream_dev_offset,
    }
  }

  pub async fn format(&self) {
    let dev = &self.device;
    join! {
      BlobdMetrics::format_device(dev.clone(), self.metrics_dev_offset, self.cfg.spage_size()),
      ObjectIdSerial::format_device(dev.clone(), self.object_id_serial_dev_offset, self.cfg.spage_size()),
      Stream::format_device(dev.clone(), self.stream_dev_offset, self.stream_dev_offset + self.cfg.spage_size(), self.cfg.event_stream_spage_capacity, self.cfg.spage_size()),
      IncompleteListWriter::format_device(dev.clone(), self.incomplete_list_dev_offset, self.cfg.spage_size()),
      BucketsWriter::format_device(dev.clone(), self.buckets_dev_offset, self.cfg.spage_size()),
      Allocator::format_device(dev.clone(), self.device_size.load(std::sync::atomic::Ordering::Relaxed), self.cfg.spage_size_pow2, self.cfg.lpage_size_pow2, self.allocator_dev_offset),
      self.journal.format_device(),
    };
    dev.sync().await;
  }

  pub async fn load(self) -> Blobd {
    self.journal.recover().await;

    let dev = &self.device;

    let objects_pending_drop = Arc::new(ObjectsPendingDrop::default());

    // Ensure journal has been recovered first before loading any other data.
    let metrics = Arc::new(
      BlobdMetrics::load_from_device(dev.clone(), self.metrics_dev_offset, self.cfg.spage_size())
        .await,
    );
    let (
      object_id_serial,
      stream,
      (incomplete_list_reader, incomplete_list_writer),
      (buckets_reader, buckets_writer),
      allocator,
    ) = join! {
      ObjectIdSerial::load_from_device(dev.clone(), self.object_id_serial_dev_offset, self.cfg.spage_size()),
      Stream::load_from_device(dev.clone(), self.stream_dev_offset, self.stream_dev_offset + self.cfg.spage_size(), self.cfg.event_stream_spage_capacity, self.cfg.spage_size()),
      IncompleteListWriter::load_from_device(dev.clone(), self.incomplete_list_dev_offset, self.cfg.spage_size(), self.cfg.reap_objects_after_secs, objects_pending_drop.clone()),
      BucketsWriter::load_from_device(dev.clone(), self.buckets_dev_offset, self.cfg.spage_size(), &objects_pending_drop),
      Allocator::load_from_device(dev.clone(), self.allocator_dev_offset, self.heap_dev_offset, self.cfg.spage_size_pow2, self.cfg.lpage_size_pow2, metrics.clone()),
    };
    let stream = Arc::new(parking_lot::RwLock::new(stream));

    let state = State {
      allocator,
      buckets: buckets_writer,
      cfg: self.cfg.clone(),
      incomplete_list: incomplete_list_writer,
      metrics: metrics.clone(),
      object_id_serial,
      stream: stream.clone(),
    };

    let state_worker = StateWorker::start(state, self.journal, objects_pending_drop.clone());

    let ctx = Arc::new(Ctx {
      buckets: buckets_reader,
      device: dev.clone(),
      incomplete_list: incomplete_list_reader,
      lpage_size_pow2: self.cfg.lpage_size_pow2,
      spage_size_pow2: self.cfg.spage_size_pow2,
      state: state_worker,
      versioning: self.cfg.versioning,
    });

    Blobd {
      cfg: self.cfg,
      ctx: ctx.clone(),
      metrics: metrics.clone(),
      stream: stream.clone(),
    }
  }
}

#[derive(Clone)]
pub struct Blobd {
  cfg: BlobdCfg,
  ctx: Arc<Ctx>,
  metrics: Arc<BlobdMetrics>,
  stream: Arc<parking_lot::RwLock<Stream>>,
}

impl Blobd {
  // Provide getter to prevent mutating BlobdCfg.
  pub fn cfg(&self) -> &BlobdCfg {
    &self.cfg
  }

  pub fn metrics(&self) -> &Arc<BlobdMetrics> {
    &self.metrics
  }

  pub async fn get_stream_event(
    &self,
    id: u64,
  ) -> Result<Option<StreamEvent>, StreamEventExpiredError> {
    self.stream.read().get_event(id)
  }

  pub async fn commit_object(&self, input: OpCommitObjectInput) -> OpResult<OpCommitObjectOutput> {
    op_commit_object(self.ctx.clone(), input).await
  }

  pub async fn create_object(&self, input: OpCreateObjectInput) -> OpResult<OpCreateObjectOutput> {
    op_create_object(self.ctx.clone(), input).await
  }

  pub async fn delete_object(&self, input: OpDeleteObjectInput) -> OpResult<OpDeleteObjectOutput> {
    op_delete_object(self.ctx.clone(), input).await
  }

  pub async fn inspect_object(
    &self,
    input: OpInspectObjectInput,
  ) -> OpResult<OpInspectObjectOutput> {
    op_inspect_object(self.ctx.clone(), input).await
  }

  pub async fn read_object(&self, input: OpReadObjectInput) -> OpResult<OpReadObjectOutput> {
    op_read_object(self.ctx.clone(), input).await
  }

  pub async fn write_object<
    D: AsRef<[u8]>,
    S: Unpin + futures::Stream<Item = Result<D, Box<dyn Error + Send + Sync>>>,
  >(
    &self,
    input: OpWriteObjectInput<D, S>,
  ) -> OpResult<OpWriteObjectOutput> {
    op_write_object(self.ctx.clone(), input).await
  }
}
