#![allow(non_snake_case)]

use crate::backing_store::file::FileBackingStore;
use crate::backing_store::uring::UringBackingStore;
use crate::backing_store::uring::UringCfg;
use crate::backing_store::BackingStore;
use crate::backing_store::PartitionStore;
use crate::pages::Pages;
use crate::partition::PartitionLoader;
use futures::future::join_all;
use futures::stream::iter;
use futures::StreamExt;
use itertools::Itertools;
use metrics::BlobdMetrics;
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
use std::os::unix::prelude::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use stream::StreamEventExpiredError;
use stream::StreamEventId;
use stream::StreamEventOwned;
use tracing::info_span;
use tracing::Instrument;

pub mod allocator;
pub mod backing_store;
pub mod ctx;
pub mod incomplete_token;
pub mod journal;
pub mod metrics;
pub mod object;
pub mod objects;
pub mod op;
pub mod pages;
pub mod partition;
pub mod ring_buf;
pub mod state;
pub mod stream;
pub mod unaligned_reader;
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
  /// WARNING: This is dangerous to enable. Only enable this if the blobd instance will be discarded after the process exits.
  /// Disabling this will likely result in corruption on exit, even when terminating normally via SIGINT/SIGTERM, and almost certainly on crash or power loss. Corruption doesn't just mean data loss, it means any possible metadata state, likely invalid.
  pub dangerously_disable_journal: bool,
  /// This will be rounded down to the nearest multiple of the spage size.
  pub event_stream_size: u64,
  /// This must be much greater than zero.
  pub expire_incomplete_objects_after_secs: u64,
  /// This should be at least 1 GiB; the more the better.
  pub journal_size_min: u64,
  pub lpage_size_pow2: u8,
  /// The amount of bytes to reserve for storing the metadata of all objects. This can be expanded online later on, but only up to the leftmost object data allocation, so it's worth setting this to a high value. This will be rounded down to the nearest multiple of the lpage size.
  pub object_metadata_reserved_space: u64,
  /// It's recommended to use the physical sector size, instead of the logical sector size, for better performance. On Linux, use `blockdev --getpbsz /dev/my_device` to get the physical sector size.
  pub spage_size_pow2: u8,
  /// Advanced options, only change if you know what you're doing.
  pub uring_coop_taskrun: bool,
  pub uring_defer_taskrun: bool,
  pub uring_iopoll: bool,
  pub uring_sqpoll: Option<u32>,
}

pub struct BlobdLoader {
  cfg: BlobdCfg,
  metrics: Arc<BlobdMetrics>,
  partitions: Vec<PartitionLoader>,
}

impl BlobdLoader {
  /// The amount of partitions must be a power of two.
  pub fn new(partition_cfg: Vec<BlobdCfgPartition>, cfg: BlobdCfg) -> Self {
    assert!(cfg.expire_incomplete_objects_after_secs > 0);

    let pages = Pages::new(cfg.spage_size_pow2, cfg.lpage_size_pow2);
    let metrics = Arc::new(BlobdMetrics::default());
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
      metrics,
      partitions,
    }
  }

  pub async fn format(&self) {
    iter(&self.partitions)
      .for_each_concurrent(None, |p| async move {
        p.format().await;
      })
      .await;
  }

  pub async fn load_and_start(self) -> Blobd {
    let partitions = join_all(
      self
        .partitions
        .into_iter()
        .map(|p| async move { p.load_and_start().await }),
    )
    .await;

    Blobd {
      cfg: self.cfg,
      metrics: self.metrics,
      partitions: Arc::new(partitions),
    }
  }
}

#[derive(Clone)]
pub struct Blobd {
  cfg: BlobdCfg,
  metrics: Arc<BlobdMetrics>,
  partitions: Arc<Vec<Partition>>,
}

// TODO get_stream_event
impl Blobd {
  // Provide getter to prevent mutating BlobdCfg.
  pub fn cfg(&self) -> &BlobdCfg {
    &self.cfg
  }

  pub fn metrics(&self) -> &Arc<BlobdMetrics> {
    &self.metrics
  }

  fn get_partition_index_by_object_key(&self, key: &[u8]) -> usize {
    let hash = twox_hash::xxh3::hash64(key);
    // We support partition counts that are not power-of-two because that's too inflexible and costly.
    usz!(hash) % self.partitions.len()
  }

  /// Returns `Err(StreamEventExpiredError)` if the event no longer exists.
  /// Returns `Ok(None)` if the event is not yet ready.
  pub fn get_stream_event(
    &self,
    partition_idx: usize,
    event_id: StreamEventId,
  ) -> Result<Option<StreamEventOwned>, StreamEventExpiredError> {
    // We must copy it to an owned value, as otherwise we'll hold the lock the entire time.
    self.partitions[partition_idx]
      .stream
      .read()
      .get_event(event_id)
      .map(|e| e.map(|e| e.to_owned()))
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
