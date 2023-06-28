#![allow(non_snake_case)]

use crate::backing_store::file::FileBackingStore;
#[cfg(target_os = "linux")]
use crate::backing_store::uring::UringBackingStore;
#[cfg(target_os = "linux")]
use crate::backing_store::uring::UringCfg;
use crate::backing_store::BackingStore;
use crate::backing_store::PartitionStore;
use crate::pages::Pages;
use crate::partition::PartitionLoader;
use chrono::DateTime;
use chrono::Utc;
use exporter::BlobdExporter;
use exporter::BlobdExporterMarker;
use futures::future::join_all;
use futures::stream::iter;
use futures::StreamExt;
use itertools::Itertools;
use metrics::BlobdMetrics;
use objects::ClusterLoadProgress;
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
use partition::Partition;
use rustc_hash::FxHashMap;
use std::error::Error;
use std::fs::OpenOptions;
#[cfg(target_os = "linux")]
use std::os::unix::prelude::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tinybuf::TinyBuf;
use tokio::spawn;
use tokio::time::sleep;
use tracing::info;
use tracing::info_span;
use tracing::Instrument;

pub mod allocator;
pub mod backing_store;
pub mod ctx;
pub mod exporter;
pub mod incomplete_token;
pub mod metrics;
pub mod object;
pub mod objects;
pub mod op;
pub mod pages;
pub mod partition;
pub mod tuples;
pub mod util;

#[derive(Clone, Debug)]
pub struct BlobdCfgPartition {
  /// This file will be opened with O_RDWR | O_DIRECT.
  pub path: PathBuf,
  /// This must be a multiple of the lpage size.
  pub offset: u64,
  /// This must be a multiple of the lpage size.
  pub len: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BlobdCfgBackingStore {
  #[cfg(target_os = "linux")]
  Uring,
  File,
}

#[derive(Clone, Debug)]
pub struct BlobdCfg {
  pub backing_store: BlobdCfgBackingStore,
  /// This must be much greater than zero.
  pub expire_incomplete_objects_after_secs: u64,
  pub lpage_size_pow2: u8,
  /// The amount of bytes per partition to reserve for storing object tuples. This can be expanded online later on, but only up to the leftmost heap allocation, so it's worth setting this to a high value. This will be rounded up to the nearest multiple of the lpage size.
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
    1 << self.lpage_size_pow2
  }

  pub fn spage_size(&self) -> u64 {
    1 << self.spage_size_pow2
  }
}

pub struct BlobdLoader {
  cfg: BlobdCfg,
  pages: Pages,
  partitions: Vec<PartitionLoader>,
  metrics: BlobdMetrics,
}

impl BlobdLoader {
  pub fn new(partition_cfg: Vec<BlobdCfgPartition>, cfg: BlobdCfg) -> Self {
    assert!(cfg.expire_incomplete_objects_after_secs > 0);

    let metrics = BlobdMetrics::default();
    let pages = Pages::new(cfg.spage_size_pow2, cfg.lpage_size_pow2);
    let mut devices = FxHashMap::<PathBuf, Arc<dyn BackingStore>>::default();
    let partitions = partition_cfg
      .into_iter()
      .enumerate()
      .map(|(i, part)| {
        let dev = devices.entry(part.path.clone()).or_insert_with(|| {
          let file = {
            let mut opt = OpenOptions::new();
            opt.read(true).write(true);
            #[cfg(target_os = "linux")]
            opt.custom_flags(libc::O_DIRECT);
            opt.open(&part.path).unwrap()
          };
          match cfg.backing_store {
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
          }
        });
        PartitionLoader::new(
          i,
          PartitionStore::new(dev.clone(), part.offset, part.len),
          cfg.clone(),
          pages.clone(),
          metrics.clone(),
        )
      })
      .collect_vec();

    Self {
      cfg,
      pages,
      partitions,
      metrics,
    }
  }

  pub async fn format(&self) {
    iter(&self.partitions)
      .for_each_concurrent(None, |p| async move {
        p.format().await;
      })
      .await;
  }

  /// Provide BlobdExporterEntry::default if no offset.
  pub async fn export(&self, offset: BlobdExporterMarker) -> BlobdExporter {
    BlobdExporter::new(&self.partitions, &self.pages, offset).await
  }

  pub async fn load_and_start(self) -> Blobd {
    let progress: Arc<ClusterLoadProgress> = Default::default();

    spawn({
      let partition_count = self.partitions.len();
      let progress = progress.clone();
      async move {
        loop {
          sleep(std::time::Duration::from_secs(3)).await;
          if progress.partitions_completed.load(Ordering::Relaxed) >= partition_count {
            break;
          };
          info!(
            objects_loaded = progress.objects_loaded.load(Ordering::Relaxed),
            objects_total = progress.objects_total.load(Ordering::Relaxed),
            "initial loading progress"
          );
        }
      }
    });

    let partitions = join_all(self.partitions.into_iter().map(|p| {
      let progress = progress.clone();
      async move { p.load_and_start(progress).await }
    }))
    .await;

    Blobd {
      cfg: self.cfg,
      partitions: Arc::new(partitions),
      metrics: self.metrics,
    }
  }
}

#[derive(Clone)]
pub struct Blobd {
  cfg: BlobdCfg,
  partitions: Arc<Vec<Partition>>,
  metrics: BlobdMetrics,
}

pub struct BlobdListObjectsOutputObject {
  pub key: TinyBuf,
  pub created: DateTime<Utc>,
  pub size: u64,
  pub id: u64,
}

impl Blobd {
  // Provide getter to prevent mutating BlobdCfg.
  pub fn cfg(&self) -> &BlobdCfg {
    &self.cfg
  }

  pub fn metrics(&self) -> &BlobdMetrics {
    &self.metrics
  }

  fn get_partition_index_by_object_key(&self, key: &[u8]) -> usize {
    let hash = twox_hash::xxh3::hash64(key);
    // We support partition counts that are not power-of-two because that's too inflexible and costly.
    usz!(hash) % self.partitions.len()
  }

  pub async fn commit_object(&self, input: OpCommitObjectInput) -> OpResult<OpCommitObjectOutput> {
    let partition_index = input.incomplete_token.partition_idx;
    let span = info_span!("commit op", partition_index);
    op_commit_object(self.partitions[partition_index].ctx.clone(), input)
      .instrument(span)
      .await
  }

  pub async fn create_object(&self, input: OpCreateObjectInput) -> OpResult<OpCreateObjectOutput> {
    let partition_index = self.get_partition_index_by_object_key(&input.key);
    let span = info_span!("create op", partition_index);
    op_create_object(self.partitions[partition_index].ctx.clone(), input)
      .instrument(span)
      .await
  }

  pub async fn delete_object(&self, input: OpDeleteObjectInput) -> OpResult<OpDeleteObjectOutput> {
    let partition_index = self.get_partition_index_by_object_key(&input.key);
    let span = info_span!("delete op", partition_index);
    op_delete_object(self.partitions[partition_index].ctx.clone(), input)
      .instrument(span)
      .await
  }

  pub async fn inspect_object(
    &self,
    input: OpInspectObjectInput,
  ) -> OpResult<OpInspectObjectOutput> {
    let partition_index = self.get_partition_index_by_object_key(&input.key);
    let span = info_span!("inspect op", partition_index);
    op_inspect_object(self.partitions[partition_index].ctx.clone(), input)
      .instrument(span)
      .await
  }

  /// WARNING: Use this method sparingly and with awareness of its behaviour:
  /// - Deadlocks could occur if iteration occurs across threads, locks, or await points.
  /// - There could be significant performance drops; some or all state operations (e.g. create, commit, delete) may be locked for the entirety of the iteration.
  /// - There is no guarantee of consistency; object entries could be duplicated or skipped, and how entries for objects that are created, committed, or deleted during iteration are iterated is undefined.
  /// - There is definitely no defined order.
  pub fn list_objects(&self) -> impl Iterator<Item = BlobdListObjectsOutputObject> + '_ {
    self.partitions.iter().flat_map(|partition| {
      partition
        .ctx
        .committed_objects
        .iter()
        .map(|o| BlobdListObjectsOutputObject {
          created: o.created,
          id: o.id(),
          key: o.key.clone(),
          size: o.size,
        })
    })
  }

  pub async fn read_object(&self, input: OpReadObjectInput) -> OpResult<OpReadObjectOutput> {
    let partition_index = self.get_partition_index_by_object_key(&input.key);
    let span = info_span!("read op", partition_index);
    op_read_object(self.partitions[partition_index].ctx.clone(), input)
      .instrument(span)
      .await
  }

  pub async fn write_object<
    D: AsRef<[u8]>,
    S: Unpin + futures::Stream<Item = Result<D, Box<dyn Error + Send + Sync>>>,
  >(
    &self,
    input: OpWriteObjectInput<D, S>,
  ) -> OpResult<OpWriteObjectOutput> {
    let partition_index = input.incomplete_token.partition_idx;
    let span = info_span!("write op", partition_index);
    op_write_object(self.partitions[partition_index].ctx.clone(), input)
      .instrument(span)
      .await
  }
}
