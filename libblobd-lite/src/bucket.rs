use crate::allocator::Allocations;
use crate::allocator::pages::MIN_PAGE_SIZE_POW2;
use crate::allocator::pages::Pages;
use crate::device::IDevice;
use crate::journal::IJournal;
use crate::object::OBJECT_KEY_LEN_MAX;
use crate::object::OBJECT_OFF;
use crate::object::ObjectMeta;
use crate::object::ObjectState;
use crate::overlay::Overlay;
use futures::Stream;
use futures::StreamExt;
use itertools::Itertools;
use off64::int::create_u40_be;
use off64::u16;
use off64::usz;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;
use tracing::debug;
use twox_hash::xxh3::hash64;
use write_journal::Transaction;

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
pub(crate) struct ReadableLockedBucket<'b, 'k> {
  bucket_id: u64,
  buckets: &'b Buckets,
  key_len: u16,
  key: &'k [u8],
}

impl<'b, 'k> ReadableLockedBucket<'b, 'k> {
  pub fn iter_with_fields(&self, fields_to_fetch: u64) -> impl Stream<Item = FoundObject> + Unpin {
    Box::pin(async_stream::stream! {
      let mut dev_offset = self.get_head().await;
      let mut prev_dev_offset = None;
      while dev_offset > 0 {
        // SAFETY: We're holding a read lock, so the linked list cannot be in an invalid/intermediate state, and no objects in it can be deallocated while this lock is held.
        let raw = self.buckets.dev.read_at(dev_offset, fields_to_fetch).await;
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
    })
  }

  pub fn iter(&self) -> impl Stream<Item = FoundObject> {
    self.iter_with_fields(OBJECT_OFF.with_key_len(OBJECT_KEY_LEN_MAX).lpages())
  }

  pub async fn find_object(
    &self,
    expected_state: ObjectState,
    expected_id: Option<u64>,
  ) -> Option<FoundObject> {
    while let Some(o) = self.iter().next().await {
      if o.state() == expected_state && expected_id.is_none_or(|r| r == o.id()) {
        return Some(o);
      };
    }
    None
  }

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

pub(crate) struct BucketReadLocked<'b, 'k, 'l> {
  state: ReadableLockedBucket<'b, 'k>,
  // We just hold this value, we don't use it.
  #[allow(unused)]
  lock: RwLockReadGuard<'l, ()>,
}

impl<'b, 'k, 'l> Deref for BucketReadLocked<'b, 'k, 'l> {
  type Target = ReadableLockedBucket<'b, 'k>;

  fn deref(&self) -> &Self::Target {
    &self.state
  }
}

// This struct's methods take `&mut self` to ensure write lock, even though it's not necessary.
pub(crate) struct BucketWriteLocked<'b, 'k, 'l> {
  state: ReadableLockedBucket<'b, 'k>,
  // We just hold this value, we don't use it.
  #[allow(unused)]
  lock: RwLockWriteGuard<'l, ()>,
}

impl<'b, 'k, 'l> BucketWriteLocked<'b, 'k, 'l> {
  // Use this (instead of directly `journal.begin_transaction()`) to ensure transactions are started after acquiring lock, to avoid race conditions where later transaction actually mutates state earlier than an earlier transaction, corrupting state.
  pub fn begin_transaction(&mut self) -> Transaction {
    self.buckets.journal.begin_transaction()
  }

  pub fn update_head(&mut self, txn: &mut Transaction, dev_offset: u64) {
    self
      .buckets
      .overlay
      .set_bucket_head(self.bucket_id, dev_offset);
    txn.write(
      self.buckets.dev_offset + BUCKETS_OFFSETOF_BUCKET(self.bucket_id),
      create_u40_be(dev_offset >> MIN_PAGE_SIZE_POW2).to_vec(),
    );
  }

  pub async fn delete_object(
    &mut self,
    txn: &mut Transaction,
    to_free: &mut Allocations,
    obj: FoundObject,
  ) {
    let dev = &self.buckets.dev;
    let overlay = &self.buckets.overlay;
    let pages = self.buckets.pages;
    let new_next = obj.next_node_dev_offset().unwrap_or(0);

    // Detach from bucket.
    match obj.prev_dev_offset {
      Some(prev_inode_dev_offset) => {
        // Update next pointer of previous inode.
        overlay.set_object_next(obj.id(), new_next);
        dev
          .write_u48_be_at(
            prev_inode_dev_offset + OBJECT_OFF.next_node_dev_offset(),
            new_next,
          )
          .await;
      }
      None => {
        // Update bucket head.
        self.update_head(txn, new_next);
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
  }
}

impl<'b, 'k, 'l> Deref for BucketWriteLocked<'b, 'k, 'l> {
  type Target = ReadableLockedBucket<'b, 'k>;

  fn deref(&self) -> &Self::Target {
    &self.state
  }
}

pub(crate) struct Buckets {
  bucket_count_pow2: u8,
  bucket_lock_count_pow2: u8,
  bucket_locks: Vec<RwLock<()>>,
  dev_offset: u64,
  dev: Arc<dyn IDevice>,
  journal: Arc<dyn IJournal>,
  pages: Pages,
  overlay: Arc<Overlay>,
}

impl Buckets {
  pub fn new(
    dev: Arc<dyn IDevice>,
    journal: Arc<dyn IJournal>,
    pages: Pages,
    overlay: Arc<Overlay>,
    dev_offset: u64,
    bucket_count_pow2: u8,
    bucket_lock_count_pow2: u8,
  ) -> Buckets {
    let bucket_locks = (0..1usize << bucket_lock_count_pow2)
      .map(|_| RwLock::new(()))
      .collect_vec();
    debug!(
      bucket_count = 1 << bucket_count_pow2,
      bucket_lock_count = 1 << bucket_lock_count_pow2,
      "buckets loaded"
    );
    Buckets {
      bucket_count_pow2,
      bucket_lock_count_pow2,
      bucket_locks,
      dev_offset,
      dev,
      journal,
      overlay,
      pages,
    }
  }

  pub async fn format_device(dev: &Arc<dyn IDevice>, dev_offset: u64, bucket_count_pow2: u8) {
    let raw = vec![0u8; usz!(BUCKETS_SIZE(1 << bucket_count_pow2))];
    dev.write_at(dev_offset, &raw).await;
  }

  pub fn obj(&self, raw: Vec<u8>) -> ObjectMeta {
    ObjectMeta {
      raw,
      overlay: self.overlay.clone(),
      pages: self.pages,
    }
  }

  /// See BucketWriteLocked.begin_transaction for why this is not on Ctx.
  pub async fn commit_transaction(&self, txn: Transaction) {
    self.journal.commit_transaction(txn).await;
  }

  fn bucket_id_for_key(&self, key: &[u8]) -> u64 {
    hash64(key) >> (64 - self.bucket_count_pow2)
  }

  fn bucket_lock_id_for_bucket_id(&self, bkt_id: u64) -> usize {
    usz!(bkt_id >> (self.bucket_count_pow2 - self.bucket_lock_count_pow2))
  }

  fn build_readable_locked_bucket<'b, 'k>(&'b self, key: &'k [u8]) -> ReadableLockedBucket<'b, 'k> {
    let bucket_id = self.bucket_id_for_key(key);
    ReadableLockedBucket {
      bucket_id,
      buckets: self,
      key,
      key_len: u16!(key.len()),
    }
  }

  pub async fn get_bucket_for_key<'b, 'k>(&'b self, key: &'k [u8]) -> BucketReadLocked<'b, 'k, 'b> {
    let state = self.build_readable_locked_bucket(key);
    let lock = self.bucket_locks[self.bucket_lock_id_for_bucket_id(state.bucket_id)]
      .read()
      .await;
    BucketReadLocked { state, lock }
  }

  pub async fn get_bucket_mut_for_key<'b, 'k>(
    &'b self,
    key: &'k [u8],
  ) -> BucketWriteLocked<'b, 'k, 'b> {
    let state = self.build_readable_locked_bucket(key);
    let lock = self.bucket_locks[self.bucket_lock_id_for_bucket_id(state.bucket_id)]
      .write()
      .await;
    BucketWriteLocked { state, lock }
  }
}
