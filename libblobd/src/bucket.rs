use crate::inode::INO_OFFSETOF_KEY;
use crate::inode::INO_OFFSETOF_KEY_LEN;
use crate::inode::INO_OFFSETOF_NEXT_INODE_DEV_OFFSET;
use crate::inode::INO_OFFSETOF_OBJ_ID;
use itertools::Itertools;
use off64::create_u48_be;
use off64::usz;
use off64::Off64Int;
use seekable_async_file::SeekableAsyncFile;
use tokio::sync::RwLock;
use tracing::debug;
use twox_hash::xxh3::hash64;

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

pub(crate) const BUCKETS_OFFSETOF_COUNT_LOG2: u64 = 0;

#[allow(non_snake_case)]
pub(crate) fn BUCKETS_OFFSETOF_BUCKET(bkt_id: u64) -> u64 {
  BUCKETS_OFFSETOF_COUNT_LOG2 + (bkt_id * 6)
}

#[allow(non_snake_case)]
pub(crate) fn BUCKETS_SIZE(bkt_cnt: u64) -> u64 {
  BUCKETS_OFFSETOF_BUCKET(bkt_cnt)
}

pub(crate) struct Bucket {
  // This is an in-memory value that increments on every write on this RwLock-ed Bucket. This allows us to cache the inode offset and metadata when reading an object between response stream chunks, instead of looking up the key every single time in case the object was deleted in the meantime. This should be reasonably optimal given one bucket should equal one object under optimal hashing and load.
  pub version: u64,
}

pub(crate) struct FoundInode {
  pub prev_dev_offset: Option<u64>,
  pub next_dev_offset: Option<u64>,
  pub dev_offset: u64,
  pub object_id: u64,
}

impl Bucket {
  pub async fn find_inode(
    &self,
    buckets: &Buckets,
    bucket_id: u64,
    key: &[u8],
    key_len: u16,
    expected_object_id: Option<u64>,
  ) -> Option<FoundInode> {
    let Buckets {
      dev, dev_offset, ..
    } = buckets;
    let mut dev_offset = dev
      .read_at(dev_offset + BUCKETS_OFFSETOF_BUCKET(bucket_id), 6)
      .await
      .read_u48_be_at(0);
    let mut prev_dev_offset = None;
    while dev_offset > 0 {
      let base = INO_OFFSETOF_OBJ_ID;
      let raw = dev
        .read_at(dev_offset + base, INO_OFFSETOF_KEY - base)
        .await;
      let next_dev_offset = raw.read_u48_be_at(INO_OFFSETOF_NEXT_INODE_DEV_OFFSET - base);
      let object_id = raw.read_u64_be_at(INO_OFFSETOF_OBJ_ID - base);
      if (expected_object_id.is_none() || expected_object_id.unwrap() == object_id)
      && raw.read_u16_be_at(INO_OFFSETOF_KEY_LEN - base) == key_len
      // mmap region should already be in page cache, so no need to use async.
      && dev.read_at_sync(dev_offset + INO_OFFSETOF_KEY, key_len.into()) == key
      {
        return Some(FoundInode {
          prev_dev_offset,
          next_dev_offset: Some(next_dev_offset).filter(|o| *o > 0),
          dev_offset,
          object_id,
        });
      };
      prev_dev_offset = Some(dev_offset);
      dev_offset = next_dev_offset;
    }
    None
  }
}

pub(crate) struct Buckets {
  dev: SeekableAsyncFile,
  dev_offset: u64,
  key_mask: u64,
  buckets: Vec<RwLock<Bucket>>,
}

impl Buckets {
  pub fn load_from_device(dev: SeekableAsyncFile, dev_offset: u64) -> Buckets {
    let count = 1u64 << dev.read_at_sync(dev_offset, 1)[0];
    let key_mask = count - 1;
    let buckets = (0..count)
      .map(|_| RwLock::new(Bucket { version: 0 }))
      .collect_vec();
    debug!(bucket_count = count, "buckets loaded");
    Buckets {
      buckets,
      dev,
      dev_offset,
      key_mask,
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

  pub fn get_bucket(&self, id: u64) -> &RwLock<Bucket> {
    &self.buckets[usz!(id)]
  }

  pub async fn get_bucket_head(&self, id: u64) -> u64 {
    self
      .dev
      .read_at(self.dev_offset + BUCKETS_OFFSETOF_BUCKET(id), 6)
      .await
      .read_u48_be_at(0)
  }

  pub fn mutate_bucket_head(
    &self,
    mutation_writes: &mut Vec<(u64, Vec<u8>)>,
    bucket_id: u64,
    dev_offset: u64,
  ) {
    mutation_writes.push((
      self.dev_offset + BUCKETS_OFFSETOF_BUCKET(bucket_id),
      create_u48_be(dev_offset).to_vec(),
    ));
  }
}
