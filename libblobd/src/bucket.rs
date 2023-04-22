use crate::ctx::State;
use crate::object::OBJECT_KEY_LEN_MAX;
use crate::object::OBJECT_OFF;
use crate::page::ObjectPageHeader;
use crate::page::ObjectState;
use crate::page::Pages;
use crate::page::MIN_PAGE_SIZE_POW2;
use crate::stream::StreamEvent;
use crate::stream::StreamEventType;
use itertools::Itertools;
use off64::int::create_u40_be;
use off64::int::Off64ReadInt;
use off64::u16;
use off64::usz;
use off64::Off64Read;
use seekable_async_file::SeekableAsyncFile;
use std::ops::Deref;
use std::sync::Arc;
use tokio::join;
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;
use tokio::sync::RwLockWriteGuard;
use tracing::debug;
use twox_hash::xxh3::hash64;
use write_journal::Transaction;
use write_journal::WriteJournal;

/**

BUCKET
======

Since we hash keys, we expect keys to have no correlation to their buckets. As such, we will likely jump between random buckets on each key lookup, and if they are not loaded into memory, we'll end up continuously paging in and out of disk, ruining any performance gain of using a hash map structure.

The limit on the amount of buckets is somewhat arbitrary, but provides reasonable constraints that we can target and optimise for.

Because we allow deletion of objects and aim for immediate freeing of space, we must use locks on each bucket, as deleting requires detaching the inode, which means modifying non-atomic heap data. If we modified the memory anyway, other readers/writers may jump to arbitrary positions and possibly leak sensitive data or crash. Also, simultaneous creations/deletions on the same bucket would cause race conditions. Creating an object also requires a lock; it's possible to do an atomic CAS on the in-memory bucket head, but then the bucket would point to an inode with an uninitialised or zero "next" link.

Structure
---------

u8 count_log2_between_12_and_40_inclusive
u40[] dev_offset_rshift8_or_zero

**/

pub(crate) const BUCKETS_OFFSETOF_COUNT_LOG2: u64 = 0;

pub(crate) fn BUCKETS_OFFSETOF_BUCKET(bkt_id: u64) -> u64 {
  BUCKETS_OFFSETOF_COUNT_LOG2 + (bkt_id * 5)
}

pub(crate) fn BUCKETS_SIZE(bkt_cnt: u64) -> u64 {
  BUCKETS_OFFSETOF_BUCKET(bkt_cnt)
}

pub(crate) struct FoundObject {
  pub prev_dev_offset: Option<u64>,
  pub next_dev_offset: Option<u64>,
  pub dev_offset: u64,
  pub id: u64,
  pub size: u64,
}

// This type exists to make sure methods are called only when holding appropriate lock.
pub(crate) struct ReadableLockedBucket<'b, 'k> {
  bucket_id: u64,
  buckets: &'b Buckets,
  key_len: u16,
  key: &'k [u8],
}

impl<'b, 'k> ReadableLockedBucket<'b, 'k> {
  pub fn bucket_id(&self) -> u64 {
    self.bucket_id
  }

  pub async fn find_object(&self, expected_id: Option<u64>) -> Option<FoundObject> {
    let mut dev_offset = self.get_head().await;
    let mut prev_dev_offset = None;
    while dev_offset > 0 {
      let (hdr, raw) = join! {
        // SAFETY: We're holding a read lock, so the linked list cannot be in an invalid/intermediate state, and all elements should be committed objects.
        self.buckets.pages.read_page_header::<ObjectPageHeader>(dev_offset),
        self.buckets.dev.read_at(dev_offset, OBJECT_OFF.with_key_len(OBJECT_KEY_LEN_MAX).lpages()),
      };
      debug_assert_eq!(hdr.state, ObjectState::Committed);
      debug_assert_eq!(hdr.deleted_sec, None);
      let object_id = raw.read_u64_be_at(OBJECT_OFF.id());
      if (expected_id.is_none() || expected_id.unwrap() == object_id)
        && raw.read_u16_be_at(OBJECT_OFF.key_len()) == self.key_len
        && raw.read_at(OBJECT_OFF.key(), self.key_len.into()) == self.key
      {
        return Some(FoundObject {
          dev_offset,
          next_dev_offset: Some(hdr.next).filter(|o| *o > 0),
          id: object_id,
          prev_dev_offset,
          size: raw.read_u40_be_at(OBJECT_OFF.size()),
        });
      };
      prev_dev_offset = Some(dev_offset);
      dev_offset = hdr.next;
    }
    None
  }

  pub async fn get_head(&self) -> u64 {
    self
      .buckets
      .journal
      .read_with_overlay(
        self.buckets.dev_offset + BUCKETS_OFFSETOF_BUCKET(self.bucket_id),
        5,
      )
      .await
      .read_u40_be_at(0)
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
  pub async fn move_object_to_deleted_list_if_exists(
    &mut self,
    txn: &mut Transaction,
    // TODO This is a workaround for the borrow checker, as it won't let us borrow both `deleted_list` and `stream` in `State` mutably.
    state: &mut State,
    id: Option<u64>,
  ) -> Option<()> {
    let Some(FoundObject {
      prev_dev_offset: prev_obj,
      next_dev_offset: next_obj,
      dev_offset: obj_dev_offset,
      id: object_id,
      ..
    }) = self.find_object(id).await else {
      return None;
    };

    // Detach from bucket.
    match prev_obj {
      Some(prev_inode_dev_offset) => {
        // Update next pointer of previous inode.
        self
          .buckets
          .pages
          .update_page_header::<ObjectPageHeader>(txn, prev_inode_dev_offset, |p| {
            debug_assert_eq!(p.state, ObjectState::Committed);
            debug_assert_eq!(p.deleted_sec, None);
            p.next = next_obj.unwrap_or(0);
          })
          .await;
      }
      None => {
        // Update bucket head.
        self.mutate_head(txn, next_obj.unwrap_or(0));
      }
    };

    // Attach to deleted list.
    state.deleted_list.attach(txn, obj_dev_offset).await;

    // Create event.
    state.stream.create_event(txn, StreamEvent {
      typ: StreamEventType::ObjectDelete,
      bucket_id: self.state.bucket_id,
      object_id,
    });

    Some(())
  }

  pub fn mutate_head(&mut self, txn: &mut Transaction, dev_offset: u64) {
    txn.write_with_overlay(
      self.buckets.dev_offset + BUCKETS_OFFSETOF_BUCKET(self.bucket_id),
      create_u40_be(dev_offset >> MIN_PAGE_SIZE_POW2).to_vec(),
    );
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
  dev: SeekableAsyncFile,
  journal: Arc<WriteJournal>,
  pages: Arc<Pages>,
}

impl Buckets {
  pub async fn load_from_device(
    dev: SeekableAsyncFile,
    journal: Arc<WriteJournal>,
    pages: Arc<Pages>,
    dev_offset: u64,
    bucket_lock_count_pow2: u8,
  ) -> Buckets {
    let bucket_count_pow2 = dev.read_at(dev_offset, 1).await[0];
    let bucket_locks = (0..1 << bucket_lock_count_pow2)
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
      pages,
    }
  }

  pub async fn format_device(dev: &SeekableAsyncFile, dev_offset: u64, bucket_count_pow2: u8) {
    let mut raw = vec![0u8; usz!(BUCKETS_SIZE(1 << bucket_count_pow2))];
    raw[usz!(BUCKETS_OFFSETOF_COUNT_LOG2)] = bucket_count_pow2;
    dev.write_at(dev_offset, raw).await;
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

  pub async fn get_bucket_for_key<'b, 'k>(&'b self, key: &'k [u8]) -> BucketReadLocked<'b, 'k, '_> {
    let state = self.build_readable_locked_bucket(key);
    let lock = self.bucket_locks[self.bucket_lock_id_for_bucket_id(state.bucket_id)]
      .read()
      .await;
    BucketReadLocked { state, lock }
  }

  pub async fn get_bucket_mut_for_key<'b, 'k>(
    &'b self,
    key: &'k [u8],
  ) -> BucketWriteLocked<'b, 'k, '_> {
    let state = self.build_readable_locked_bucket(key);
    let lock = self.bucket_locks[self.bucket_lock_id_for_bucket_id(state.bucket_id)]
      .write()
      .await;
    BucketWriteLocked { state, lock }
  }
}
