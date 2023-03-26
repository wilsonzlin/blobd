use itertools::Itertools;
use off64::{usz, Off64};
use seekable_async_file::SeekableAsyncFile;
use tokio::sync::RwLock;
use twox_hash::xxh3::hash64;

use crate::inode::{InodeState, INO_OFFSETOF_STATE, INO_OFFSETOF_KEY, INO_OFFSETOF_OBJ_ID, INO_OFFSETOF_KEY_LEN, INO_OFFSETOF_NEXT_INODE_DEV_OFFSET};

/**

BUCKET
======

Since we hash keys, we expect keys to have no correlation to their buckets. As such, we will likely jump between random buckets on each key lookup, and if they are not loaded into memory, we'll end up continuously paging in and out of disk, ruining any performance gain of using a hash map structure.

The limit on the amount of buckets is somewhat arbitrary, but provides reasonable constraints that we can target and optimise for.

Because we allow deletion of objects and aim for immediate freeing of space, we must use locks on each bucket, as deleting requires detaching the inode, which means modifying non-atomic heap data. If we modified the memory anyway, other readers/writers may jump to arbitrary positions and possibly leak sensitive data or crash. Also, simultaneous creations/deletions on the same bucket would cause race conditions. Creating an object also requires a lock; it's possible to do an atomic CAS on the in-memory bucket head, but then the bucket would point to an inode with an uninitialised or zero "next" link.

Structure
---------

u8 count_log2_between_12_and_40_inclusive
u48[] dev_offset_or_zero

**/

#[allow(non_snake_case)]
pub fn BUCKETS_OFFSETOF_BUCKET(bkt_id: u64) -> u64 { 1 + bkt_id * 6 }

#[allow(non_snake_case)]
pub fn BUCKETS_RESERVED_SPACE(bkt_cnt: u64) -> u64 { BUCKETS_OFFSETOF_BUCKET(bkt_cnt) }

pub struct Bucket {
  // This is an in-memory value that increments on every write on this RwLock-ed Bucket. This allows us to cache the inode offset and metadata when reading an object between response stream chunks, instead of looking up the key every single time in case the object was deleted in the meantime. This should be reasonably optimal given one bucket should equal one object under optimal hashing and load.
  pub version: u64,
}

pub struct FoundInode {
  pub dev_offset: u64,
  pub object_id: u64,
}

impl Bucket {
  // This needs to be non-async as it is called from non-async contexts.
  pub fn find_inode(
    &self,
    buckets: &Buckets,
    bucket_id: u64,
    key: &[u8],
    key_len: u16,
    expected_state: InodeState,
    expected_object_id: Option<u64>,
  ) -> Option<FoundInode> {
    let Buckets { dev, dev_offset, .. } = buckets;
    let mut dev_offset = dev.read_at_sync(dev_offset + BUCKETS_OFFSETOF_BUCKET(bucket_id), 6).read_u48_be_at(0);
    while dev_offset > 0 {
      let base = INO_OFFSETOF_STATE;
      let raw = dev.read_at_sync(dev_offset + base, INO_OFFSETOF_KEY - base);
      let object_id = raw.read_u64_be_at(INO_OFFSETOF_OBJ_ID - base);
      if raw[usz!(INO_OFFSETOF_STATE - base)] == expected_state as u8
      && (expected_object_id.is_none() || expected_object_id.unwrap() == object_id)
      && raw.read_u16_be_at(INO_OFFSETOF_KEY_LEN) == key_len
      // mmap region should already be in page cache, so no need to use async.
      && dev.read_at_sync(dev_offset + INO_OFFSETOF_KEY, key_len.into()) == key
      {
        return Some(FoundInode { dev_offset, object_id });
      };
      dev_offset = raw.read_u48_be_at(INO_OFFSETOF_NEXT_INODE_DEV_OFFSET - base);
    };
    None
  }
}

pub struct Buckets {
  dev: SeekableAsyncFile,
  dev_offset: u64,
  key_mask: u64,
  buckets: Vec<RwLock<Bucket>>,
}

impl Buckets {
  pub fn load_from_device(dev: SeekableAsyncFile, dev_offset: u64) -> Buckets {
    let count = 1u64 << dev.read_at_sync(dev_offset, 1)[0];
    let key_mask = count - 1;
    let buckets = (0..count).map(|_| RwLock::new(Bucket{version: 0})).collect_vec();
    Buckets { buckets, dev, dev_offset, key_mask }
  }

  pub fn bucket_id_for_key(&self, key: &[u8]) -> u64 {
    hash64(key) & self.key_mask
  }

  pub fn get_bucket(&self, id: u64) -> &RwLock<Bucket> {
    &self.buckets[usz!(id)]
  }
}
