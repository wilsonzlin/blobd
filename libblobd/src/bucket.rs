use crate::ctx::State;
use crate::inode::INODE_OFF;
use crate::inode::INO_KEY_LEN_MAX;
use crate::page::ActiveInodePageHeader;
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

pub(crate) struct FoundInode {
  pub prev_dev_offset: Option<u64>,
  pub next_dev_offset: Option<u64>,
  pub dev_offset: u64,
  pub size: u64,
  pub object_id: u64,
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

  pub async fn find_inode(&self, expected_object_id: Option<u64>) -> Option<FoundInode> {
    let buckets = self.buckets;
    let mut dev_offset = buckets
      .journal
      .read_with_overlay(
        buckets.dev_offset + BUCKETS_OFFSETOF_BUCKET(self.bucket_id),
        5,
      )
      .await
      .read_u48_be_at(0);
    let mut prev_dev_offset = None;
    while dev_offset > 0 {
      let (hdr, raw) = join! {
        buckets.pages.read_page_header::<ActiveInodePageHeader>(dev_offset),
        buckets.dev.read_at(dev_offset, INODE_OFF.with_key_len(INO_KEY_LEN_MAX).lpage_segments()),
      };
      // It's impossible for this to be any other type, as we hold write lock when changing a bucket's linked list.
      let hdr = hdr.unwrap();
      let next_dev_offset = hdr.next;
      let object_id = raw.read_u64_be_at(INODE_OFF.object_id());
      if (expected_object_id.is_none() || expected_object_id.unwrap() == object_id)
        && raw.read_u16_be_at(INODE_OFF.key_len()) == self.key_len
        && raw.read_at(INODE_OFF.key(), self.key_len.into()) == self.key
      {
        return Some(FoundInode {
          dev_offset,
          next_dev_offset: Some(next_dev_offset).filter(|o| *o > 0),
          object_id,
          prev_dev_offset,
          size: raw.read_u40_be_at(INODE_OFF.size()),
        });
      };
      prev_dev_offset = Some(dev_offset);
      dev_offset = next_dev_offset;
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
  lock: RwLockWriteGuard<'l, ()>,
}

impl<'b, 'k, 'l> BucketWriteLocked<'b, 'k, 'l> {
  pub async fn move_object_to_deleted_list_if_exists(
    &mut self,
    txn: &mut Transaction,
    // TODO This is a workaround for the borrow checker, as it won't let us borrow both `deleted_list` and `stream` in `State` mutably.
    state: &mut State,
  ) -> Option<()> {
    let buckets = self.state.buckets;
    let key = self.state.key;
    let key_len = self.state.key_len;
    let Some(FoundInode {
      prev_dev_offset: prev_inode,
      next_dev_offset: next_inode,
      dev_offset: inode_dev_offset,
      object_id,
      ..
    }) = self.find_inode(None).await else {
      return None;
    };

    match prev_inode {
      Some(prev_inode_dev_offset) => {
        // Update next pointer of previous inode.
        self
          .buckets
          .pages
          .update_page_header::<ActiveInodePageHeader>(txn, prev_inode_dev_offset, |p| {
            p.next = next_inode.unwrap_or(0)
          })
          .await;
      }
      None => {
        // Update bucket head.
        self.mutate_head(txn, next_inode.unwrap_or(0));
      }
    };

    state.deleted_list.attach(txn, inode_dev_offset).await;

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
  journal: WriteJournal,
  pages: Arc<Pages>,
}

impl Buckets {
  pub async fn load_from_device(
    dev: SeekableAsyncFile,
    journal: WriteJournal,
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

  pub async fn format_device(dev: &SeekableAsyncFile, dev_offset: u64, bucket_count: u64) {
    let mut raw = vec![0u8; usz!(BUCKETS_SIZE(bucket_count))];
    raw[usz!(BUCKETS_OFFSETOF_COUNT_LOG2)] = bucket_count.ilog2() as u8;
    dev.write_at(dev_offset, raw).await;
  }

  fn bucket_id_for_key(&self, key: &[u8]) -> u64 {
    hash64(key) >> (64 - self.bucket_count_pow2)
  }

  fn bucket_lock_id_for_bucket_id(&self, bkt_id: u64) -> usize {
    usz!(bkt_id >> (self.bucket_count_pow2 - self.bucket_lock_count_pow2))
  }

  fn build_readable_locked_bucket(&self, key: &[u8]) -> ReadableLockedBucket<'_, '_> {
    let bucket_id = self.bucket_id_for_key(key);
    ReadableLockedBucket {
      bucket_id,
      buckets: self,
      key,
      key_len: u16!(key.len()),
    }
  }

  pub async fn get_bucket_for_key(&self, key: &[u8]) -> BucketReadLocked<'_, '_, '_> {
    let state = self.build_readable_locked_bucket(key);
    let lock = self.bucket_locks[self.bucket_lock_id_for_bucket_id(state.bucket_id)]
      .read()
      .await;
    BucketReadLocked { state, lock }
  }

  pub async fn get_bucket_mut_for_key(&self, key: &[u8]) -> BucketWriteLocked<'_, '_, '_> {
    let state = self.build_readable_locked_bucket(key);
    let lock = self.bucket_locks[self.bucket_lock_id_for_bucket_id(state.bucket_id)]
      .write()
      .await;
    BucketWriteLocked { state, lock }
  }
}
