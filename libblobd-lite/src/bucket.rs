use crate::allocator::Allocations;
use crate::allocator::pages::MIN_PAGE_SIZE_POW2;
use crate::allocator::pages::Pages;
use crate::device::IDevice;
use crate::journal::IJournal;
use crate::journal::real::Transaction;
use crate::metrics::BlobdMetrics;
use crate::object::OBJECT_OFF;
use crate::object::ObjectMeta;
use crate::object::ObjectState;
use crate::overlay::Overlay;
use crate::overlay::OverlayTicket;
use arbitrary_lock::ArbitraryLock;
use arbitrary_lock::ArbitraryLockEntry;
use futures::Stream;
use futures::StreamExt;
use futures::pin_mut;
use futures::stream::iter;
use off64::int::create_u40_be;
use off64::u64;
use off64::usz;
use signal_future::SignalFuture;
use std::cmp::min;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;
use tokio::time::Instant;
use tracing::instrument;
use twox_hash::xxh3::hash64;

/**

BUCKET
======

Since we hash keys, we expect keys to have no correlation to their buckets. As such, we will likely jump between random buckets on each key lookup, and if they are not loaded into memory, we'll end up continuously paging in and out of disk, ruining any performance gain of using a hash map structure.

The limit on the amount of buckets is somewhat arbitrary, but provides reasonable constraints that we can target and optimise for.

Because we allow deletion of objects and aim for immediate freeing of space, we must use locks on each bucket, as deleting requires detaching the inode, which means modifying non-atomic heap data. If we modified the memory anyway, other readers/writers may jump to arbitrary positions and possibly leak sensitive data or crash. Also, simultaneous creations/deletions on the same bucket would cause race conditions. Creating an object also requires a lock; it's possible to do an atomic CAS on the in-memory bucket head, but then the bucket would point to an inode with an uninitialised or zero "next" link.

Structure
---------

u40[] dev_offset_rshift8_or_zero

**/

pub(crate) fn BUCKETS_OFFSETOF_BUCKET(bkt_id: u64) -> u64 {
  bkt_id * 5
}

pub(crate) fn BUCKETS_SIZE(bkt_cnt: u64) -> u64 {
  BUCKETS_OFFSETOF_BUCKET(bkt_cnt)
}

pub(crate) struct FoundObject {
  pub dev_offset: u64,
  pub prev_dev_offset: Option<u64>,
  pub meta: ObjectMeta,
}

impl Deref for FoundObject {
  type Target = ObjectMeta;

  fn deref(&self) -> &Self::Target {
    &self.meta
  }
}

// This type exists to make sure methods are called only when holding appropriate lock.
#[derive(Clone)]
pub(crate) struct LockedBucket<'b, 'k> {
  bucket_id: u64,
  buckets: &'b Buckets,
  key: &'k [u8],
}

impl<'b, 'k> LockedBucket<'b, 'k> {
  pub fn iter(&self) -> impl Stream<Item = FoundObject> {
    async_stream::stream! {
      let mut dev_offset = self.get_head().await;
      let mut prev_dev_offset = None;
      while dev_offset != 0 {
        // SAFETY: We're holding a read lock, so the linked list cannot be in an invalid/intermediate state, and no objects in it can be deallocated while this lock is held.
        let key_len = self.buckets.dev.read_u16_be_at(dev_offset + OBJECT_OFF.key_len()).await;
        let raw = self.buckets.dev.read_at(dev_offset, OBJECT_OFF.with_key_len(key_len).lpages()).await;
        let meta = self.buckets.obj(raw);
        let next = meta.next_node_dev_offset();
        if meta.key() == self.key {
          yield FoundObject {
            dev_offset,
            prev_dev_offset,
            meta,
          };
        };
        prev_dev_offset = Some(dev_offset);
        dev_offset = next.unwrap_or(0);
      }
    }
  }

  #[instrument(skip_all)]
  pub async fn find_object(
    &self,
    expected_state: ObjectState,
    expected_id: Option<u128>,
  ) -> Option<FoundObject> {
    let iter = self.iter();
    pin_mut!(iter);
    while let Some(o) = iter.next().await {
      if o.state() == expected_state && expected_id.is_none_or(|r| r == o.id()) {
        return Some(o);
      };
    }
    None
  }

  #[instrument(skip_all)]
  pub async fn get_head(&self) -> u64 {
    if let Some(head) = self.buckets.overlay.get_bucket_head(self.bucket_id) {
      return head;
    }
    self
      .buckets
      .dev
      .read_u40_be_at(self.buckets.dev_offset + BUCKETS_OFFSETOF_BUCKET(self.bucket_id))
      .await
      << MIN_PAGE_SIZE_POW2
  }
}

pub(crate) struct ReadLockedBucket<'b, 'k, 'l> {
  bucket: LockedBucket<'b, 'k>,
  acquired: Instant,
  _lock: RwLockReadGuard<'l, ()>,
}

impl<'b, 'k, 'l> Deref for ReadLockedBucket<'b, 'k, 'l> {
  type Target = LockedBucket<'b, 'k>;

  fn deref(&self) -> &Self::Target {
    &self.bucket
  }
}

impl<'b, 'k, 'l> Drop for ReadLockedBucket<'b, 'k, 'l> {
  fn drop(&mut self) {
    self
      .buckets
      .metrics
      .bucket_lock_read_held_ns
      .fetch_add(u64!(self.acquired.elapsed().as_nanos()), Relaxed);
  }
}

pub(crate) struct WriteLockedBucket<'b, 'k, 'l> {
  bucket: LockedBucket<'b, 'k>,
  acquired: Instant,
  _lock: RwLockWriteGuard<'l, ()>,
}

impl<'b, 'k, 'l> Deref for WriteLockedBucket<'b, 'k, 'l> {
  type Target = LockedBucket<'b, 'k>;

  fn deref(&self) -> &Self::Target {
    &self.bucket
  }
}

impl<'b, 'k, 'l> Drop for WriteLockedBucket<'b, 'k, 'l> {
  fn drop(&mut self) {
    self
      .buckets
      .metrics
      .bucket_lock_write_held_ns
      .fetch_add(u64!(self.acquired.elapsed().as_nanos()), Relaxed);
  }
}

impl<'b, 'k, 'l> WriteLockedBucket<'b, 'k, 'l> {
  // Use this (instead of directly `journal.begin_transaction()`) to ensure transactions are started after acquiring lock, to avoid race conditions where later transaction actually mutates state earlier than an earlier transaction, corrupting state.
  pub fn begin_transaction(&mut self) -> Transaction {
    self.buckets.journal.begin_transaction()
  }

  pub fn update_head(&mut self, txn: &mut Transaction, dev_offset: u64) -> OverlayTicket {
    let overlay_entry = self
      .buckets
      .overlay
      .set_bucket_head(self.bucket_id, dev_offset);
    txn.write(
      self.buckets.dev_offset + BUCKETS_OFFSETOF_BUCKET(self.bucket_id),
      create_u40_be(dev_offset >> MIN_PAGE_SIZE_POW2).to_vec(),
    );
    overlay_entry
  }

  pub async fn delete_object(
    &mut self,
    txn: &mut Transaction,
    to_free: &mut Allocations,
    obj: FoundObject,
  ) -> OverlayTicket {
    let dev = &self.buckets.dev;
    let overlay = &self.buckets.overlay;
    let pages = self.buckets.pages;
    let new_next = obj.next_node_dev_offset().unwrap_or(0);

    // Detach from bucket.
    let overlay_entry = match obj.prev_dev_offset {
      Some(prev_inode_dev_offset) => {
        // Update next pointer of previous inode.
        let overlay_entry = overlay.set_object_next(obj.id(), new_next);
        dev
          .write_u48_be_at(
            prev_inode_dev_offset + OBJECT_OFF.next_node_dev_offset(),
            new_next,
          )
          .await;
        overlay_entry
      }
      None => {
        // Update bucket head.
        self.update_head(txn, new_next)
      }
    };

    // Read full inode to get lpages and tail pages.
    let raw = dev.read_at(obj.dev_offset, obj.metadata_size()).await;
    let meta = self.buckets.obj(raw);
    for i in 0..meta.lpage_count() {
      let page_dev_offset = meta.lpage(i);
      to_free.add(page_dev_offset, pages.lpage_size_pow2);
    }
    for (i, tail_page_size_pow2) in meta.tail_page_sizes_pow2() {
      let page_dev_offset = meta.tail_page(i);
      to_free.add(page_dev_offset, tail_page_size_pow2);
    }
    to_free.add(obj.dev_offset, obj.metadata_size_pow2());

    let metrics = &self.buckets.metrics;
    metrics.object_count.fetch_sub(1, Relaxed);
    metrics.object_data_bytes.fetch_sub(obj.size(), Relaxed);
    metrics
      .object_metadata_bytes
      .fetch_sub(obj.metadata_size(), Relaxed);

    overlay_entry
  }
}

pub(crate) struct BucketLocker<'b, 'k> {
  locker: ArbitraryLockEntry<u64, RwLock<()>>,
  bucket: LockedBucket<'b, 'k>,
}

impl<'b, 'k> BucketLocker<'b, 'k> {
  pub async fn read<'l>(&'l self) -> ReadLockedBucket<'b, 'k, 'l> {
    let started = Instant::now();
    let lock = self.locker.read().await;
    self
      .bucket
      .buckets
      .metrics
      .bucket_lock_read_acq_ns
      .fetch_add(u64!(started.elapsed().as_nanos()), Relaxed);
    let acquired = Instant::now();
    ReadLockedBucket {
      bucket: self.bucket.clone(),
      acquired,
      _lock: lock,
    }
  }

  pub async fn write<'l>(&'l self) -> WriteLockedBucket<'b, 'k, 'l> {
    let started = Instant::now();
    let lock = self.locker.write().await;
    self
      .bucket
      .buckets
      .metrics
      .bucket_lock_write_acq_ns
      .fetch_add(u64!(started.elapsed().as_nanos()), Relaxed);
    let acquired = Instant::now();
    WriteLockedBucket {
      bucket: self.bucket.clone(),
      acquired,
      _lock: lock,
    }
  }
}

pub(crate) struct Buckets {
  bucket_count_pow2: u8,
  bucket_locks: ArbitraryLock<u64, RwLock<()>>,
  dev_offset: u64,
  dev: Arc<dyn IDevice>,
  journal: Arc<dyn IJournal>,
  metrics: Arc<BlobdMetrics>,
  overlay: Arc<Overlay>,
  pages: Pages,
}

impl Buckets {
  pub fn new(
    dev: Arc<dyn IDevice>,
    journal: Arc<dyn IJournal>,
    metrics: Arc<BlobdMetrics>,
    pages: Pages,
    overlay: Arc<Overlay>,
    dev_offset: u64,
    bucket_count_pow2: u8,
  ) -> Buckets {
    Buckets {
      bucket_count_pow2,
      bucket_locks: ArbitraryLock::new(),
      dev_offset,
      dev,
      journal,
      metrics,
      overlay,
      pages,
    }
  }

  #[instrument(skip_all)]
  pub async fn format_device(dev: &Arc<dyn IDevice>, dev_offset: u64, bucket_count_pow2: u8) {
    const BLKSZ: usize = 1024 * 1024 * 16;
    let block = vec![0u8; BLKSZ];
    let len = BUCKETS_SIZE(1 << bucket_count_pow2);
    iter((0..len).step_by(BLKSZ))
      .for_each_concurrent(None, async |offset| {
        let end = min(offset + u64!(BLKSZ), len);
        let raw = &block[..usz!(end - offset)];
        dev.write_at(dev_offset + offset, raw).await;
      })
      .await;
  }

  pub fn obj(&self, raw: Vec<u8>) -> ObjectMeta {
    ObjectMeta {
      raw,
      overlay: self.overlay.clone(),
      pages: self.pages,
    }
  }

  /// See BucketWriteLocked.begin_transaction for why this is not on Ctx.
  /// WARNING: Don't await whtin this function. This must be called while holding lock (correctness), but awaited outside (performance). See Journal for more details.
  pub fn commit_transaction(&self, txn: Transaction) -> Option<SignalFuture<()>> {
    self.journal.commit_transaction(txn)
  }

  fn bucket_id_for_key(&self, key: &[u8]) -> u64 {
    hash64(key) >> (64 - self.bucket_count_pow2)
  }

  pub fn get_locker_for_key<'b, 'k>(&'b self, key: &'k [u8]) -> BucketLocker<'b, 'k> {
    let bucket_id = self.bucket_id_for_key(key);
    let locker = self.bucket_locks.get(bucket_id);
    BucketLocker {
      locker,
      bucket: LockedBucket {
        bucket_id,
        buckets: self,
        key,
      },
    }
  }
}
