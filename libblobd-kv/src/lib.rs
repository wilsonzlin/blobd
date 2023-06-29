#![allow(non_snake_case)]

use crate::backing_store::file::FileBackingStore;
#[cfg(target_os = "linux")]
use crate::backing_store::uring::UringBackingStore;
#[cfg(target_os = "linux")]
use crate::backing_store::uring::UringCfg;
use crate::backing_store::BackingStore;
use crate::object::LPAGE_SIZE_POW2;
use crate::object::SPAGE_SIZE_POW2_MIN;
use crate::pages::Pages;
use crate::util::ceil_pow2;
use crate::util::floor_pow2;
use bundles::Bundles;
use ctx::Ctx;
use metrics::BlobdMetrics;
use object::format_device_for_tuples;
use object::load_tuples_from_device;
use object::LoadedTuplesFromDevice;
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

pub mod allocator;
pub mod backing_store;
pub mod bundles;
pub mod ctx;
pub mod key_lock;
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
  pages: Pages,
  metrics: BlobdMetrics,
  heap_dev_offset: u64,
  heap_size: u64,
}

impl BlobdLoader {
  pub fn new(cfg: BlobdCfg) -> Self {
    assert!(cfg.spage_size_pow2 >= SPAGE_SIZE_POW2_MIN);
    assert!(cfg.spage_size_pow2 <= LPAGE_SIZE_POW2);

    let tuples_area_size = ceil_pow2(cfg.object_tuples_area_reserved_space, LPAGE_SIZE_POW2);
    let heap_dev_offset = tuples_area_size;
    let heap_end = floor_pow2(cfg.device_len, LPAGE_SIZE_POW2);
    let heap_size = heap_end - heap_dev_offset;
    assert!(tuples_area_size + heap_dev_offset <= heap_end);

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
      BlobdCfgBackingStore::Uring => {
        Arc::new(UringBackingStore::new(file, pages.clone(), UringCfg {
          coop_taskrun: cfg.uring_coop_taskrun,
          defer_taskrun: cfg.uring_defer_taskrun,
          iopoll: cfg.uring_iopoll,
          sqpoll: cfg.uring_sqpoll,
        }))
      }
      BlobdCfgBackingStore::File => Arc::new(FileBackingStore::new(file, pages.clone())),
    };

    Self {
      cfg,
      dev,
      heap_dev_offset,
      heap_size,
      metrics,
      pages,
    }
  }

  pub async fn format(&self) {
    format_device_for_tuples(&self.dev, &self.pages, self.heap_dev_offset).await;
    self.dev.sync().await;
  }

  pub async fn load_and_start(self) -> Blobd {
    let LoadedTuplesFromDevice {
      heap_allocator: allocator,
    } = load_tuples_from_device(
      &self.dev,
      &self.pages,
      &self.metrics,
      self.heap_dev_offset,
      self.heap_size,
    )
    .await;

    let ctx = Arc::new(Ctx {
      bundles: Bundles::new(self.heap_dev_offset / self.pages.spage_size()),
      device: self.dev,
      heap_allocator: Mutex::new(allocator),
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
