#![allow(non_snake_case)]

use crate::allocator::AllocDir;
use crate::allocator::Allocator;
use crate::bucket::Buckets;
use crate::bucket::BUCKETS_OFFSETOF_BUCKET;
use crate::deleted_list::start_deleted_list_reaper_background_loop;
use crate::deleted_list::DeletedList;
use crate::deleted_list::DELETED_LIST_OFFSETOF_HEAD;
use crate::deleted_list::DELETED_LIST_OFFSETOF_TAIL;
use crate::deleted_list::DELETED_LIST_STATE_SIZE;
use crate::device::IDevice;
use crate::incomplete_list::start_incomplete_list_reaper_background_loop;
use crate::incomplete_list::IncompleteList;
use crate::incomplete_list::INCOMPLETE_LIST_OFFSETOF_HEAD;
use crate::incomplete_list::INCOMPLETE_LIST_OFFSETOF_TAIL;
use crate::incomplete_list::INCOMPLETE_LIST_STATE_SIZE;
use crate::journal::IJournal;
use crate::object::calc_object_layout;
use crate::object::OBJECT_KEY_LEN_MAX;
use crate::object::OBJECT_OFF;
use crate::object_header::Headers;
use crate::object_header::ObjectHeader;
use crate::object_header::OBJECT_HEADER_SIZE;
use crate::object_id::ObjectIdSerial;
use crate::page::MIN_PAGE_SIZE_POW2;
use crate::util::ceil_pow2;
use bucket::BUCKETS_SIZE;
use ctx::Ctx;
use ctx::State;
use futures::join;
use futures::stream::iter;
use futures::StreamExt;
use metrics::BlobdMetrics;
use off64::int::Off64ReadInt;
use off64::usz;
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
use parking_lot::Mutex as SyncMutex;
use seekable_async_file::SeekableAsyncFile;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::info;
use write_journal::WriteJournal;

pub mod allocator;
pub mod bucket;
pub mod ctx;
pub mod deleted_list;
pub mod device;
pub mod incomplete_list;
pub mod incomplete_token;
pub mod journal;
pub mod metrics;
pub mod object;
pub mod object_header;
pub mod object_id;
pub mod op;
pub mod page;
pub mod util;

/**

DEVICE
======

Structure
---------

object_id_serial
incomplete_list_state
deleted_list_state
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

struct ObjectsLoad {
  allocator: Arc<SyncMutex<Allocator>>,
  headers: Headers,
  metrics: Arc<BlobdMetrics>,
}

pub struct BlobdLoader {
  device: Arc<dyn IDevice>,
  journal: Arc<dyn IJournal>,
  cfg: BlobdCfg,
  pages: Pages,

  object_id_serial_dev_offset: u64,
  incomplete_list_dev_offset: u64,
  deleted_list_dev_offset: u64,
  buckets_dev_offset: u64,

  heap_dev_offset: u64,
  heap_size: u64,
}

impl BlobdLoader {
  pub fn new(device: SeekableAsyncFile, device_size: u64, cfg: BlobdCfg) -> Self {
    Self::new_with_device_and_journal(
      Arc::new(device.clone()),
      |journal_dev_offset, journal_size| {
        Arc::new(WriteJournal::new(
          device.clone(),
          journal_dev_offset,
          journal_size,
          Duration::from_micros(200),
        ))
      },
      device_size,
      cfg,
    )
  }

  pub(crate) fn new_with_device_and_journal(
    device: Arc<dyn IDevice>,
    journal: impl FnOnce(u64, u64) -> Arc<dyn IJournal>,
    device_size: u64,
    cfg: BlobdCfg,
  ) -> Self {
    assert!(cfg.bucket_count_log2 >= 12 && cfg.bucket_count_log2 <= 48);
    let bucket_count = 1u64 << cfg.bucket_count_log2;

    assert!(cfg.reap_objects_after_secs > 0);

    const JOURNAL_SIZE_MIN: u64 = 1024 * 1024 * 32;

    let object_id_serial_dev_offset = 0;
    let incomplete_list_dev_offset = object_id_serial_dev_offset + 8;
    let deleted_list_dev_offset = incomplete_list_dev_offset + INCOMPLETE_LIST_STATE_SIZE;
    let buckets_dev_offset = deleted_list_dev_offset + DELETED_LIST_STATE_SIZE;
    let buckets_size = BUCKETS_SIZE(bucket_count);
    let journal_dev_offset = buckets_dev_offset + buckets_size;
    let min_reserved_space = journal_dev_offset + JOURNAL_SIZE_MIN;

    // `heap_dev_offset` is equivalent to the reserved size.
    let heap_dev_offset = ceil_pow2(min_reserved_space, cfg.lpage_size_pow2);
    let journal_size = heap_dev_offset - journal_dev_offset;
    let heap_size = device_size - heap_dev_offset;

    let pages = Pages::new(cfg.spage_size_pow2, cfg.lpage_size_pow2);

    info!(
      device_size,
      buckets_size,
      journal_size,
      reserved_size = heap_dev_offset,
      heap_size,
      lpage_size = 1 << cfg.lpage_size_pow2,
      spage_size = 1 << cfg.spage_size_pow2,
      "init",
    );

    let journal = journal(journal_dev_offset, journal_size);

    Self {
      buckets_dev_offset,
      cfg,
      deleted_list_dev_offset,
      device,
      heap_dev_offset,
      heap_size,
      incomplete_list_dev_offset,
      journal,
      object_id_serial_dev_offset,
      pages,
    }
  }

  pub async fn format(&self) {
    let dev = &self.device;
    join! {
      ObjectIdSerial::format_device(dev, self.object_id_serial_dev_offset),
      IncompleteList::format_device(dev, self.incomplete_list_dev_offset),
      DeletedList::format_device(dev, self.deleted_list_dev_offset),
      Buckets::format_device(dev, self.buckets_dev_offset, self.cfg.bucket_count_log2),
      self.journal.format_device(),
    };
    dev.sync_data().await;
  }

  async fn load_object_list(&self, l: &ObjectsLoad, mut cur: u64) {
    while cur != 0 {
      let raw = self
        .device
        .read_at(
          cur,
          OBJECT_OFF.with_key_len(OBJECT_KEY_LEN_MAX).assoc_data_len(),
        )
        .await;
      let hdr = ObjectHeader::deserialize(&raw[..usz!(OBJECT_HEADER_SIZE)]);
      let size = raw.read_u40_be_at(OBJECT_OFF.size());
      let alloc = calc_object_layout(self.pages, size);
      let mut allocator = l.allocator.lock();
      for i in 0..alloc.lpage_count {
        let lpage_dev_offset = raw.read_u48_be_at(OBJECT_OFF.lpage(i));
        allocator.mark_as_allocated(lpage_dev_offset, self.pages.lpage_size_pow2);
      }
      for (i, pow2) in alloc.tail_page_sizes_pow2.into_iter() {
        let tail_page_dev_offset = raw.read_u48_be_at(OBJECT_OFF.tail_page(i));
        allocator.mark_as_allocated(tail_page_dev_offset, pow2);
      }
      cur = hdr.next;
    }
  }

  async fn load_buckets(&self, l: &ObjectsLoad) {
    // TODO Allow configuring concurrency.
    iter(0..self.cfg.bucket_count())
      .for_each_concurrent(64, async |bkt_id| {
        let cur = self
          .device
          .read_u40_be_at(self.buckets_dev_offset + BUCKETS_OFFSETOF_BUCKET(bkt_id))
          .await
          >> MIN_PAGE_SIZE_POW2;
        self.load_object_list(l, cur).await;
      })
      .await;
  }

  async fn load_incomplete_list(&self, l: &ObjectsLoad) -> IncompleteList {
    let dev_offset = self.incomplete_list_dev_offset;
    let head = self
      .device
      .read_u48_be_at(dev_offset + INCOMPLETE_LIST_OFFSETOF_HEAD)
      .await;
    let tail = self
      .device
      .read_u48_be_at(dev_offset + INCOMPLETE_LIST_OFFSETOF_TAIL)
      .await;
    self.load_object_list(l, head).await;
    IncompleteList::new(
      self.device.clone(),
      dev_offset,
      l.headers.clone(),
      l.metrics.clone(),
      self.cfg.reap_objects_after_secs,
      head,
      tail,
    )
  }

  async fn load_deleted_list(&self, l: &ObjectsLoad) -> DeletedList {
    let dev_offset = self.deleted_list_dev_offset;
    let head = self
      .device
      .read_u48_be_at(dev_offset + DELETED_LIST_OFFSETOF_HEAD)
      .await;
    let tail = self
      .device
      .read_u48_be_at(dev_offset + DELETED_LIST_OFFSETOF_TAIL)
      .await;
    self.load_object_list(l, head).await;
    DeletedList::new(
      self.device.clone(),
      dev_offset,
      l.headers.clone(),
      self.pages,
      l.metrics.clone(),
      self.cfg.reap_objects_after_secs,
      head,
      tail,
    )
  }

  pub async fn load(self) -> Blobd {
    self.journal.recover().await;

    // Ensure journal has been recovered first before loading any other data.
    let dev = self.device.clone();
    let journal = self.journal.clone();
    let metrics = Arc::new(BlobdMetrics::new());
    let pages = self.pages;
    let headers = Headers::new(
      journal.clone(),
      self.heap_dev_offset,
      self.pages.spage_size_pow2,
    );
    let buckets = Buckets::new(
      dev.clone(),
      journal.clone(),
      headers.clone(),
      self.buckets_dev_offset,
      self.cfg.bucket_count_log2,
      self.cfg.bucket_lock_count_log2,
    );
    let objects_load = ObjectsLoad {
      allocator: Arc::new(SyncMutex::new(Allocator::new(
        self.heap_dev_offset,
        self.heap_size,
        pages,
        AllocDir::Left,
        metrics.clone(),
      ))),
      headers: headers.clone(),
      metrics: metrics.clone(),
    };
    let (object_id_serial, incomplete_list, deleted_list, _) = join! {
      ObjectIdSerial::load_from_device(&dev, self.object_id_serial_dev_offset),
      self.load_incomplete_list(&objects_load),
      self.load_deleted_list(&objects_load),
      self.load_buckets(&objects_load),
    };
    let ObjectsLoad { allocator, .. } = objects_load;
    let allocator = Arc::try_unwrap(allocator).ok().unwrap();

    let ctx = Arc::new(Ctx {
      allocator,
      buckets,
      device: dev,
      headers,
      journal,
      metrics,
      pages,
      reap_objects_after_secs: self.cfg.reap_objects_after_secs,
      versioning: self.cfg.versioning,
      state: Mutex::new(State {
        deleted_list,
        incomplete_list,
        object_id_serial,
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
