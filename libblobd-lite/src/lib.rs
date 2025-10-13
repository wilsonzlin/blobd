#![allow(non_snake_case)]

use crate::allocator::AllocDir;
use crate::allocator::Allocator;
use crate::allocator::layout::ObjectLayout;
use crate::allocator::layout::calc_object_layout;
use crate::allocator::pages::MIN_PAGE_SIZE_POW2;
use crate::allocator::pages::Pages;
use crate::bucket::Buckets;
use crate::device::IDevice;
use crate::journal::IJournal;
use crate::object::OBJECT_OFF;
use crate::object::ObjectState;
use crate::overlay::Overlay;
use crate::util::ceil_pow2;
use bucket::BUCKETS_SIZE;
use ctx::Ctx;
use futures::StreamExt;
use futures::join;
use futures::stream::iter;
use metrics::BlobdMetrics;
use num_traits::FromPrimitive;
use off64::int::Off64ReadInt;
use off64::u64;
use op::OpResult;
use op::commit_object::OpCommitObjectInput;
use op::commit_object::OpCommitObjectOutput;
use op::commit_object::op_commit_object;
use op::create_object::OpCreateObjectInput;
use op::create_object::OpCreateObjectOutput;
use op::create_object::op_create_object;
use op::delete_object::OpDeleteObjectInput;
use op::delete_object::OpDeleteObjectOutput;
use op::delete_object::op_delete_object;
use op::inspect_object::OpInspectObjectInput;
use op::inspect_object::OpInspectObjectOutput;
use op::inspect_object::op_inspect_object;
use op::read_object::OpReadObjectInput;
use op::read_object::OpReadObjectOutput;
use op::read_object::op_read_object;
use op::write_object::OpWriteObjectInput;
use op::write_object::OpWriteObjectOutput;
use op::write_object::op_write_object;
use parking_lot::Mutex as SyncMutex;
use rayon::iter::ParallelIterator;
use rayon::slice::ParallelSlice;
use seekable_async_file::SeekableAsyncFile;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use tokio::spawn;
use tokio::task::spawn_blocking;
use tracing::info;
use write_journal::WriteJournal;

pub mod allocator;
pub mod bucket;
pub mod ctx;
pub mod device;
pub mod journal;
pub mod metrics;
pub mod object;
pub mod op;
pub mod overlay;
pub mod util;

/**

DEVICE
======

Structure
---------

buckets
journal // Placed here to make use of otherwise unused space due to heap alignment.
heap

**/

#[derive(Clone, Debug)]
pub struct BlobdCfg {
  pub bucket_count_log2: u8,
  pub bucket_lock_count_log2: u8,
  pub reap_incomplete_objects_after_secs: u64,
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
}

struct ObjectsLoader {
  allocator: Arc<SyncMutex<Allocator>>,
  device: Arc<dyn IDevice>,
  pages: Pages,
  metrics: Arc<BlobdMetrics>,
}

impl ObjectsLoader {
  async fn load_object_list(&self, head: u64) {
    let mut cur = head;
    while cur != 0 {
      let key_len = self.device.read_u16_be_at(cur + OBJECT_OFF.key_len()).await;
      let size = self.device.read_u40_be_at(cur + OBJECT_OFF.size()).await;
      let ObjectLayout {
        lpage_count,
        tail_page_sizes_pow2,
      } = calc_object_layout(self.pages, size);
      let off = OBJECT_OFF
        .with_key_len(key_len)
        .with_lpages(lpage_count)
        .with_tail_pages(tail_page_sizes_pow2.len());
      let raw = self.device.read_at(cur, off.assoc_data_len()).await;
      let state = ObjectState::from_u8(raw.read_u8_at(off.state())).unwrap();
      let meta_size = 1u64 << raw.read_u8_at(off.metadata_size_pow2());
      let mut allocator = self.allocator.lock();
      for i in 0..lpage_count {
        let lpage_dev_offset = raw.read_u48_be_at(off.lpage(i));
        allocator.mark_as_allocated(lpage_dev_offset, self.pages.lpage_size_pow2);
      }
      for (i, pow2) in tail_page_sizes_pow2.into_iter() {
        let tail_page_dev_offset = raw.read_u48_be_at(off.tail_page(i));
        allocator.mark_as_allocated(tail_page_dev_offset, pow2);
      }
      cur = raw.read_u48_be_at(off.next_node_dev_offset());

      self.metrics.object_count.fetch_add(1, Relaxed);
      if state == ObjectState::Incomplete {
        self.metrics.incomplete_object_count.fetch_add(1, Relaxed);
      }
      self.metrics.object_data_bytes.fetch_add(size, Relaxed);
      self
        .metrics
        .object_metadata_bytes
        .fetch_add(meta_size, Relaxed);
    }
  }
}

pub struct BlobdLoader {
  device: Arc<dyn IDevice>,
  journal: Arc<dyn IJournal>,
  cfg: BlobdCfg,
  pages: Pages,

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

    assert!(cfg.reap_incomplete_objects_after_secs > 0);

    const JOURNAL_SIZE_MIN: u64 = 1024 * 1024 * 32;

    let buckets_dev_offset = 0;
    let buckets_size = BUCKETS_SIZE(bucket_count);
    let journal_dev_offset = buckets_dev_offset + buckets_size;
    let min_reserved_space = journal_dev_offset + JOURNAL_SIZE_MIN;

    // `heap_dev_offset` is equivalent to the reserved size.
    let heap_dev_offset = ceil_pow2(min_reserved_space, cfg.lpage_size_pow2);
    let journal_size = heap_dev_offset - journal_dev_offset;
    let heap_size = ceil_pow2(device_size - heap_dev_offset, cfg.lpage_size_pow2);

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
      device,
      heap_dev_offset,
      heap_size,
      journal,
      pages,
    }
  }

  pub async fn format(&self) {
    let dev = &self.device;
    join! {
      Buckets::format_device(dev, self.buckets_dev_offset, self.cfg.bucket_count_log2),
      self.journal.format_device(),
    };
    dev.sync_data().await;
  }

  async fn load_buckets(&self, l: &Arc<ObjectsLoader>) {
    let bucket_count = self.cfg.bucket_count();
    let buckets_dev_offset = self.buckets_dev_offset;
    let buckets_size = BUCKETS_SIZE(bucket_count);
    // Making a lot of small reads is extremely inefficient.
    info!(buckets_dev_offset, buckets_size, "loading buckets");
    // Having futures::stream::iter parse and filter seems to be really slow, likely because it's expensive compute but 1) running on single thread and 2) blocking an async task.
    let raw = self.device.read_at(buckets_dev_offset, buckets_size).await;
    let heads = spawn_blocking(move || {
      raw
        .par_chunks_exact(5)
        .map(|raw| raw.read_u40_be_at(0) >> MIN_PAGE_SIZE_POW2)
        .filter(|head| *head != 0)
        .collect::<Vec<_>>()
    })
    .await
    .unwrap();
    let head_count = heads.len();

    let progress = Arc::new(AtomicU64::new(0));
    let print_interval = (u64!(head_count) / 5).max(1);
    info!(head_count, bucket_count, "traversing buckets");
    // TODO Allow configuring concurrency.
    iter(heads)
      .for_each_concurrent(256, async |bkt_ptr| {
        let progress = progress.clone();
        let l = l.clone();
        spawn(async move {
          l.load_object_list(bkt_ptr).await;
          let progress = progress.fetch_add(1, Ordering::Relaxed) + 1;
          if progress % print_interval == 0 {
            info!(
              progress = format!("{:.2}%", progress as f64 / bucket_count as f64 * 100.0),
              bucket_count, "traversing committed objects"
            );
          }
        })
        .await
        .unwrap();
      })
      .await;
    info!(bucket_count, "traversed committed objects");
  }

  pub async fn load(self) -> Blobd {
    self.journal.recover().await;

    // Ensure journal has been recovered first before loading any other data.
    let dev = self.device.clone();
    let journal = self.journal.clone();
    let metrics = Arc::new(BlobdMetrics::new());
    let pages = self.pages;
    let overlay = Arc::new(Overlay::new());
    let buckets = Buckets::new(
      dev.clone(),
      journal.clone(),
      metrics.clone(),
      pages,
      overlay.clone(),
      self.buckets_dev_offset,
      self.cfg.bucket_count_log2,
      self.cfg.bucket_lock_count_log2,
    );
    let allocator = {
      let allocator = Arc::new(SyncMutex::new(Allocator::new(
        self.heap_dev_offset,
        self.heap_size,
        pages,
        AllocDir::Left,
        metrics.clone(),
      )));
      let objects_loader = Arc::new(ObjectsLoader {
        allocator: allocator.clone(),
        device: dev.clone(),
        pages,
        metrics: metrics.clone(),
      });
      self.load_buckets(&objects_loader).await;
      drop(objects_loader);
      Arc::try_unwrap(allocator).ok().unwrap()
    };

    let ctx = Arc::new(Ctx {
      allocator,
      buckets,
      device: dev,
      overlay,
      metrics,
      pages,
      reap_incomplete_objects_after_secs: self.cfg.reap_incomplete_objects_after_secs,
      versioning: self.cfg.versioning,
    });

    Blobd {
      cfg: self.cfg,
      ctx,
      journal,
    }
  }
}

#[derive(Clone)]
pub struct Blobd {
  cfg: BlobdCfg,
  ctx: Arc<Ctx>,
  journal: Arc<dyn IJournal>,
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
    // TODO Start background incomplete reaper.
    join! {
      self.journal.start_commit_background_loop(),
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
