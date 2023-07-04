#![allow(non_snake_case)]

use crate::allocator::Allocator;
use crate::backing_store::file::FileBackingStore;
#[cfg(target_os = "linux")]
use crate::backing_store::uring::UringBackingStore;
#[cfg(target_os = "linux")]
use crate::backing_store::uring::UringCfg;
use crate::backing_store::BackingStore;
use crate::object::LPAGE_SIZE_POW2;
use crate::object::OBJECT_TUPLE_DATA_LEN_INLINE_THRESHOLD;
use crate::object::SPAGE_SIZE_POW2_MIN;
use crate::pages::Pages;
use crate::util::ceil_pow2;
use crate::util::floor_pow2;
use backing_store::BoundedStore;
use ctx::Ctx;
use log_buffer::LogBuffer;
use metrics::BlobdMetrics;
use object::format_device_for_tuples;
use object::load_tuples_from_device;
use op::delete_object::op_delete_object;
use op::delete_object::OpDeleteObjectInput;
use op::delete_object::OpDeleteObjectOutput;
use op::read_object::op_read_object;
use op::read_object::OpReadObjectInput;
use op::read_object::OpReadObjectOutput;
use op::write_object::op_write_object;
use op::write_object::OpWriteObjectInput;
use op::write_object::OpWriteObjectOutput;
use op::OpResult;
use parking_lot::Mutex;
use std::fs::OpenOptions;
#[cfg(target_os = "linux")]
use std::os::unix::prelude::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::join;
use tracing::info;

pub mod allocator;
pub mod backing_store;
pub mod ctx;
pub mod log_buffer;
pub mod metrics;
pub mod object;
pub mod op;
pub mod pages;
pub mod util;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BlobdCfgBackingStore {
  #[cfg(target_os = "linux")]
  Uring,
  File,
}

#[derive(Clone, Debug)]
pub struct BlobdCfg {
  pub backing_store: BlobdCfgBackingStore,
  /// This file will be opened with O_RDWR | O_DIRECT.
  pub device_path: PathBuf,
  /// This must be a multiple of the lpage size.
  pub device_len: u64,
  pub log_buffer_commit_threshold: u64,
  pub log_buffer_size: u64,
  // This should be not too high, as otherwise we'll do a lot of double writes when moving out of the log buffer into the heap, and that will also cause log commits to take a long time. The ideal value is the smallest possible such that the write ops per second is the peak.
  pub log_entry_data_len_inline_threshold: usize,
  /// The amount of bytes to reserve for storing object tuples. This cannot be changed later on. This will be rounded up to the nearest multiple of the lpage size.
  pub object_tuples_area_reserved_space: u64,
  /// The device must support atomic writes of this size. It's recommended to use the physical sector size, instead of the logical sector size, for better performance. On Linux, use `blockdev --getpbsz /dev/my_device` to get the physical sector size.
  pub spage_size_pow2: u8,
  /// Advanced options, only change if you know what you're doing.
  #[cfg(target_os = "linux")]
  pub uring_coop_taskrun: bool,
  #[cfg(target_os = "linux")]
  pub uring_defer_taskrun: bool,
  #[cfg(target_os = "linux")]
  pub uring_iopoll: bool,
  #[cfg(target_os = "linux")]
  pub uring_sqpoll: Option<u32>,
}

impl BlobdCfg {
  pub fn lpage_size(&self) -> u64 {
    1 << LPAGE_SIZE_POW2
  }

  pub fn spage_size(&self) -> u64 {
    1 << self.spage_size_pow2
  }
}

pub struct BlobdLoader {
  cfg: BlobdCfg,
  dev: Arc<dyn BackingStore>,
  heap_dev_offset: u64,
  heap_size: u64,
  log_commit_threshold: u64,
  log_data_dev_offset: u64,
  log_data_size: u64,
  log_entry_data_len_inline_threshold: usize,
  log_state_dev_offset: u64,
  metrics: BlobdMetrics,
  pages: Pages,
}

impl BlobdLoader {
  pub fn new(cfg: BlobdCfg) -> Self {
    assert!(cfg.spage_size_pow2 >= SPAGE_SIZE_POW2_MIN);
    assert!(cfg.spage_size_pow2 <= LPAGE_SIZE_POW2);

    let dev_end_aligned = floor_pow2(cfg.device_len, cfg.spage_size_pow2);
    let log_data_size = cfg.log_buffer_size;
    let log_commit_threshold = cfg.log_buffer_commit_threshold;
    assert!(log_data_size > 1024 * 1024 * 64); // Sanity check: ensure reasonable value and not misconfiguration.
    assert!(log_commit_threshold < log_data_size);

    let log_entry_data_len_inline_threshold = cfg.log_entry_data_len_inline_threshold;
    assert!(log_entry_data_len_inline_threshold > OBJECT_TUPLE_DATA_LEN_INLINE_THRESHOLD);

    let tuples_area_size = ceil_pow2(cfg.object_tuples_area_reserved_space, LPAGE_SIZE_POW2);
    let heap_dev_offset = tuples_area_size;
    let log_state_dev_offset = dev_end_aligned.checked_sub(cfg.spage_size()).unwrap();
    let log_data_dev_offset = log_state_dev_offset.checked_sub(log_data_size).unwrap();
    let heap_size = floor_pow2(
      log_state_dev_offset.checked_sub(heap_dev_offset).unwrap(),
      LPAGE_SIZE_POW2,
    );
    assert!(heap_size > 1024 * 1024 * 64); // Sanity check: ensure reasonable value and not misconfiguration.

    let metrics = BlobdMetrics::default();
    let pages = Pages::new(cfg.spage_size_pow2, LPAGE_SIZE_POW2);
    let file = {
      let mut opt = OpenOptions::new();
      opt.read(true).write(true);
      #[cfg(target_os = "linux")]
      opt.custom_flags(libc::O_DIRECT);
      opt.open(&cfg.device_path).unwrap()
    };
    let dev: Arc<dyn BackingStore> = match cfg.backing_store {
      #[cfg(target_os = "linux")]
      BlobdCfgBackingStore::Uring => Arc::new(UringBackingStore::new(
        file,
        pages.clone(),
        metrics.clone(),
        UringCfg {
          coop_taskrun: cfg.uring_coop_taskrun,
          defer_taskrun: cfg.uring_defer_taskrun,
          iopoll: cfg.uring_iopoll,
          sqpoll: cfg.uring_sqpoll,
        },
      )),
      BlobdCfgBackingStore::File => Arc::new(FileBackingStore::new(file, pages.clone())),
    };

    Self {
      cfg,
      dev,
      heap_dev_offset,
      heap_size,
      log_commit_threshold,
      log_data_dev_offset,
      log_data_size,
      log_entry_data_len_inline_threshold,
      log_state_dev_offset,
      metrics,
      pages,
    }
  }

  pub async fn format(&self) {
    format_device_for_tuples(&self.dev, &self.pages, self.heap_dev_offset).await;
    LogBuffer::format_device(
      &BoundedStore::new(
        self.dev.clone(),
        self.log_state_dev_offset,
        self.pages.spage_size(),
      ),
      &self.pages,
    )
    .await;
    self.dev.sync().await;
  }

  pub async fn load_and_start(self) -> Blobd {
    info!(
      heap_dev_offset = self.heap_dev_offset,
      heap_size = self.heap_size,
      log_data_dev_offset = self.log_data_dev_offset,
      log_data_size = self.log_data_size,
      log_state_dev_offset = self.log_state_dev_offset,
      "loading blobd",
    );

    let heap_allocator = Arc::new(Mutex::new(Allocator::new(
      self.heap_dev_offset,
      self.heap_size,
      self.pages.clone(),
      self.metrics.clone(),
    )));

    let (_, log_buffer) = join! {
      load_tuples_from_device(
        &self.dev,
        &self.pages,
        &self.metrics,
        heap_allocator.clone(),
        self.heap_dev_offset,
      ),
      LogBuffer::load_from_device(
        self.dev.clone(),
        BoundedStore::new(self.dev.clone(), 0, self.heap_dev_offset),
        BoundedStore::new(
          self.dev.clone(),
          self.log_data_dev_offset,
          self.log_data_size,
        ),
        BoundedStore::new(
          self.dev.clone(),
          self.log_state_dev_offset,
          self.pages.spage_size(),
        ),
        heap_allocator.clone(),
        self.pages.clone(),
        self.metrics.clone(),
        self.heap_dev_offset / self.pages.spage_size(),
        self.log_commit_threshold,
      ),
    };

    // WARNING: Only start after `load_tuples_from_device` has been completed; we cannot simply call this immediately after `LogBuffer::load_from_device`.
    log_buffer.start_background_threads().await;

    let ctx = Arc::new(Ctx {
      device: self.dev,
      heap_allocator,
      log_buffer,
      log_entry_data_len_inline_threshold: self.log_entry_data_len_inline_threshold,
      metrics: self.metrics.clone(),
      pages: self.pages,
    });

    Blobd {
      cfg: self.cfg,
      ctx,
      metrics: self.metrics,
    }
  }
}

#[derive(Clone)]
pub struct Blobd {
  cfg: BlobdCfg,
  ctx: Arc<Ctx>,
  metrics: BlobdMetrics,
}

impl Blobd {
  // Provide getter to prevent mutating BlobdCfg.
  pub fn cfg(&self) -> &BlobdCfg {
    &self.cfg
  }

  pub fn metrics(&self) -> &BlobdMetrics {
    &self.metrics
  }

  pub async fn wait_for_any_current_log_buffer_commit(&self) {
    self.ctx.log_buffer.wait_for_any_current_commit().await;
  }

  pub async fn delete_object(&self, input: OpDeleteObjectInput) -> OpResult<OpDeleteObjectOutput> {
    op_delete_object(self.ctx.clone(), input).await
  }

  pub async fn read_object(&self, input: OpReadObjectInput) -> OpResult<OpReadObjectOutput> {
    op_read_object(self.ctx.clone(), input).await
  }

  pub async fn write_object(&self, input: OpWriteObjectInput) -> OpResult<OpWriteObjectOutput> {
    op_write_object(self.ctx.clone(), input).await
  }
}
