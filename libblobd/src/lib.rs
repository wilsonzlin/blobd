#![allow(non_snake_case)]

use crate::allocator::Allocator;
use crate::allocator::ALLOCSTATE_SIZE;
use crate::bucket::Buckets;
use crate::deleted_list::start_deleted_list_reaper_background_loop;
use crate::deleted_list::DeletedList;
use crate::deleted_list::DELETED_LIST_STATE_SIZE;
use crate::incomplete_list::start_incomplete_list_reaper_background_loop;
use crate::incomplete_list::IncompleteList;
use crate::incomplete_list::INCOMPLETE_LIST_STATE_SIZE;
use crate::metrics::METRICS_STATE_SIZE;
use crate::object_id::ObjectIdSerial;
use crate::stream::Stream;
use crate::stream::STREAM_SIZE;
#[cfg(test)]
use crate::test_util::device::TestSeekableAsyncFile as SeekableAsyncFile;
#[cfg(test)]
use crate::test_util::journal::TestWriteJournal as WriteJournal;
use crate::util::ceil_pow2;
use bucket::BUCKETS_SIZE;
use ctx::Ctx;
use ctx::State;
use futures::join;
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
use page::Pages;
#[cfg(not(test))]
use seekable_async_file::SeekableAsyncFile;
use std::error::Error;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use stream::StreamEvent;
use stream::StreamEventExpiredError;
use tokio::sync::Mutex;
use tracing::info;
#[cfg(not(test))]
use write_journal::WriteJournal;

pub mod allocator;
pub mod bucket;
pub mod bufpool;
pub mod ctx;
pub mod deleted_list;
pub mod incomplete_list;
pub mod incomplete_token;
pub mod metrics;
pub mod object;
pub mod object_id;
pub mod op;
pub mod page;
pub mod stream;
#[cfg(test)]
pub mod test_util;
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
incomplete_list_state
deleted_list_state
allocator_state
buckets
journal // Placed here to make use of otherwise unused space due to heap alignment.
heap

**/

#[derive(Clone, Debug)]
pub struct BlobdCfg {
  pub bucket_count_log2: u8,
  pub bucket_lock_count_log2: u8,
  pub reap_objects_after_secs: u64,
  pub lpage_size_pow2: u8,
  pub spage_size_pow2: u8,
  pub versioning: bool,
}

impl BlobdCfg {
  pub fn bucket_count(&self) -> u64 {
    1 << self.bucket_count_log2
  }

  pub fn bucket_lock_count(&self) -> u64 {
    1 << self.bucket_lock_count_log2
  }

  pub fn lpage_size(&self) -> u64 {
    1 << self.lpage_size_pow2
  }

  pub fn spage_size(&self) -> u64 {
    1 << self.spage_size_pow2
  }
}

pub struct BlobdLoader {
  device: SeekableAsyncFile,
  device_size: Arc<AtomicU64>, // To allow online resizing, this must be atomically mutable at any time.
  journal: Arc<WriteJournal>,
  cfg: BlobdCfg,

  metrics_dev_offset: u64,
  object_id_serial_dev_offset: u64,
  stream_dev_offset: u64,
  incomplete_list_dev_offset: u64,
  deleted_list_dev_offset: u64,
  allocator_dev_offset: u64,
  buckets_dev_offset: u64,

  heap_dev_offset: u64,
}

impl BlobdLoader {
  pub fn new(device: SeekableAsyncFile, device_size: u64, cfg: BlobdCfg) -> Self {
    assert!(cfg.bucket_count_log2 >= 12 && cfg.bucket_count_log2 <= 48);
    let bucket_count = 1u64 << cfg.bucket_count_log2;

    assert!(cfg.reap_objects_after_secs > 0);

    const JOURNAL_SIZE_MIN: u64 = 1024 * 1024 * 32;

    let metrics_dev_offset = 0;
    let object_id_serial_dev_offset = metrics_dev_offset + METRICS_STATE_SIZE;
    let stream_dev_offset = object_id_serial_dev_offset + 8;
    let incomplete_list_dev_offset = stream_dev_offset + STREAM_SIZE;
    let deleted_list_dev_offset = incomplete_list_dev_offset + INCOMPLETE_LIST_STATE_SIZE;
    let allocator_dev_offset = deleted_list_dev_offset + DELETED_LIST_STATE_SIZE;
    let buckets_dev_offset = allocator_dev_offset + ALLOCSTATE_SIZE;
    let buckets_size = BUCKETS_SIZE(bucket_count);
    let journal_dev_offset = buckets_dev_offset + buckets_size;
    let min_reserved_space = journal_dev_offset + JOURNAL_SIZE_MIN;

    // `heap_dev_offset` is equivalent to the reserved size.
    let heap_dev_offset = ceil_pow2(min_reserved_space, cfg.lpage_size_pow2);
    let journal_size = heap_dev_offset - journal_dev_offset;

    info!(
      device_size,
      buckets_size,
      journal_size,
      reserved_size = heap_dev_offset,
      lpage_size = 1 << cfg.lpage_size_pow2,
      spage_size = 1 << cfg.spage_size_pow2,
      "init",
    );

    #[cfg(not(test))]
    let journal = Arc::new(WriteJournal::new(
      device.clone(),
      journal_dev_offset,
      journal_size,
      std::time::Duration::from_micros(200),
    ));
    #[cfg(test)]
    let journal = Arc::new(WriteJournal::new(device.clone()));

    Self {
      allocator_dev_offset,
      buckets_dev_offset,
      cfg,
      deleted_list_dev_offset,
      device_size: Arc::new(AtomicU64::new(device_size)),
      device,
      heap_dev_offset,
      incomplete_list_dev_offset,
      journal,
      metrics_dev_offset,
      object_id_serial_dev_offset,
      stream_dev_offset,
    }
  }

  pub async fn format(&self) {
    let dev = &self.device;
    join! {
      BlobdMetrics::format_device(dev, self.metrics_dev_offset),
      ObjectIdSerial::format_device(dev, self.object_id_serial_dev_offset),
      Stream::format_device(dev, self.stream_dev_offset),
      IncompleteList::format_device(dev, self.incomplete_list_dev_offset),
      DeletedList::format_device(dev, self.deleted_list_dev_offset),
      Allocator::format_device(dev, self.allocator_dev_offset, self.heap_dev_offset),
      Buckets::format_device(dev, self.buckets_dev_offset, self.cfg.bucket_count_log2),
      self.journal.format_device(),
    };
    dev.sync_data().await;
  }

  pub async fn load(self) -> Blobd {
    self.journal.recover().await;

    let dev = &self.device;

    let pages = Arc::new(Pages::new(
      self.journal.clone(),
      self.heap_dev_offset,
      self.cfg.spage_size_pow2,
      self.cfg.lpage_size_pow2,
    ));

    // Ensure journal has been recovered first before loading any other data.
    let metrics = Arc::new(BlobdMetrics::load_from_device(dev, self.metrics_dev_offset).await);
    let (
      object_id_serial,
      (stream, stream_in_memory),
      incomplete_list,
      deleted_list,
      allocator,
      buckets,
    ) = join! {
      ObjectIdSerial::load_from_device(dev, self.object_id_serial_dev_offset),
      Stream::load_from_device(dev, self.stream_dev_offset),
      IncompleteList::load_from_device(dev.clone(), self.incomplete_list_dev_offset, pages.clone(), metrics.clone(), self.cfg.reap_objects_after_secs),
      DeletedList::load_from_device(dev.clone(), self.deleted_list_dev_offset, pages.clone(), metrics.clone(), self.cfg.reap_objects_after_secs),
      Allocator::load_from_device(dev, self.device_size.clone(), self.allocator_dev_offset, pages.clone(), metrics.clone(), self.heap_dev_offset),
      Buckets::load_from_device(dev.clone(), self.journal.clone(), pages.clone(), self.buckets_dev_offset, self.cfg.bucket_lock_count_log2),
    };

    let ctx = Arc::new(Ctx {
      buckets,
      device: dev.clone(),
      journal: self.journal.clone(),
      metrics: metrics.clone(),
      pages: pages.clone(),
      reap_objects_after_secs: self.cfg.reap_objects_after_secs,
      stream_in_memory,
      versioning: self.cfg.versioning,
      state: Mutex::new(State {
        allocator,
        deleted_list,
        incomplete_list,
        object_id_serial,
        stream,
      }),
    });

    Blobd { cfg: self.cfg, ctx }
  }
}

#[derive(Clone)]
pub struct Blobd {
  cfg: BlobdCfg,
  ctx: Arc<Ctx>,
}

impl Blobd {
  // Provide getter to prevent mutating BlobdCfg.
  pub fn cfg(&self) -> &BlobdCfg {
    &self.cfg
  }

  pub fn metrics(&self) -> &Arc<BlobdMetrics> {
    &self.ctx.metrics
  }

  // WARNING: `device.start_delayed_data_sync_background_loop()` must also be running. Since `device` was provided, it's left up to the provider to run it.
  pub async fn start(&self) {
    join! {
      self.ctx.journal.start_commit_background_loop(),
      start_incomplete_list_reaper_background_loop(self.ctx.clone()),
      start_deleted_list_reaper_background_loop(self.ctx.clone()),
    };
  }

  pub async fn get_stream_event(
    &self,
    id: u64,
  ) -> Result<Option<StreamEvent>, StreamEventExpiredError> {
    self.ctx.stream_in_memory.get_event(id)
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
