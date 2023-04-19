use crate::inode::InodeOffset;
use crate::inode::INO_KEY_LEN_MAX;
use crate::page::ActiveInodePageHeader;
use crate::page::Pages;
use crate::page::MIN_PAGE_SIZE_POW2;
use itertools::Itertools;
use off64::create_u40_be;
use off64::usz;
use off64::Off64Int;
use off64::Off64Slice;
use seekable_async_file::SeekableAsyncFile;
use std::sync::Arc;
use tokio::join;
use tokio::sync::RwLock;
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
u40[] dev_offset_rshifted_by_8_or_zero

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

pub(crate) struct Buckets {
  buckets: Vec<RwLock<()>>,
  dev_offset: u64,
  dev: SeekableAsyncFile,
  journal: WriteJournal,
  key_mask: u64,
  pages: Arc<Pages>,
}

impl Buckets {
  pub fn load_from_device(
    dev: SeekableAsyncFile,
    journal: WriteJournal,
    pages: Arc<Pages>,
    dev_offset: u64,
    bucket_lock_count: u64,
  ) -> Buckets {
    let count = 1u64 << dev.read_at_sync(dev_offset, 1)[0];
    let key_mask = count - 1;
    let buckets = (0..bucket_lock_count)
      .map(|_| RwLock::new(()))
      .collect_vec();
    debug!(bucket_count = count, "buckets loaded");
    Buckets {
      buckets,
      dev_offset,
      dev,
      journal,
      key_mask,
      pages,
    }
  }

  pub async fn format_device(dev: &SeekableAsyncFile, dev_offset: u64, bucket_count: u64) {
    let mut raw = vec![0u8; usz!(BUCKETS_SIZE(bucket_count))];
    raw[usz!(BUCKETS_OFFSETOF_COUNT_LOG2)] = bucket_count.ilog2() as u8;
    dev.write_at(dev_offset, raw).await;
  }

  pub fn bucket_id_for_key(&self, key: &[u8]) -> u64 {
    hash64(key) & self.key_mask
  }

  pub fn get_bucket_lock(&self, id: u64) -> &RwLock<()> {
    &self.buckets[usz!(id)]
  }

  pub async fn get_bucket_head(&self, id: u64) -> u64 {
    self
      .journal
      .read_with_overlay(self.dev_offset + BUCKETS_OFFSETOF_BUCKET(id), 5)
      .await
      .read_u40_be_at(0)
      << MIN_PAGE_SIZE_POW2
  }

  pub fn mutate_bucket_head(&self, txn: &mut Transaction, bucket_id: u64, dev_offset: u64) {
    txn.write_with_overlay(
      self.dev_offset + BUCKETS_OFFSETOF_BUCKET(bucket_id),
      create_u40_be(dev_offset >> MIN_PAGE_SIZE_POW2).to_vec(),
    );
  }

  pub async fn find_inode_in_bucket(
    &self,
    bucket_id: u64,
    key: &[u8],
    key_len: u16,
    expected_object_id: Option<u64>,
  ) -> Option<FoundInode> {
    let mut dev_offset = self
      .journal
      .read_with_overlay(self.dev_offset + BUCKETS_OFFSETOF_BUCKET(bucket_id), 5)
      .await
      .read_u48_be_at(0);
    let mut prev_dev_offset = None;
    while dev_offset > 0 {
      let (hdr, raw) = join! {
        self.pages.read_page_header::<ActiveInodePageHeader>(dev_offset),
        self.dev.read_at(dev_offset, InodeOffset::with_key_len(INO_KEY_LEN_MAX).lpage_segments()),
      };
      let next_dev_offset = hdr.next;
      let object_id = raw.read_u64_be_at(InodeOffset::object_id());
      if (expected_object_id.is_none() || expected_object_id.unwrap() == object_id)
        && raw.read_u16_be_at(InodeOffset::key_len()) == key_len
        && raw.read_slice_at(InodeOffset::key(), key_len.into()) == key
      {
        return Some(FoundInode {
          dev_offset,
          next_dev_offset: Some(next_dev_offset).filter(|o| *o > 0),
          object_id,
          prev_dev_offset,
          size: raw.read_u40_be_at(InodeOffset::size()),
        });
      };
      prev_dev_offset = Some(dev_offset);
      dev_offset = next_dev_offset;
    }
    None
  }
}
