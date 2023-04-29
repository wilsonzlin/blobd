use crate::allocator::Allocator;
use crate::journal::Transaction;
use crate::object::offset::OBJECT_KEY_LEN_MAX;
use crate::object::offset::OBJECT_OFF;
use crate::object::AutoLifecycleObject;
use crate::object::ObjectState;
use crate::persisted_collection::dashmap::PersistedDashMapReader;
use crate::persisted_collection::dashmap::PersistedDashMapWriter;
use crate::persisted_collection::serialisable::Serialisable;
use crate::persisted_collection::serialisable::SerialisedLen;
use crate::state::ObjectsPendingDrop;
use crate::uring::Uring;
use dashmap::mapref::one::RefMut;
use off64::int::Off64ReadInt;
use off64::u16;
use off64::u64;
use off64::u8;
use off64::usz;
use off64::Off64Read;
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;
use std::ops::Deref;
use std::sync::Arc;
use tracing::debug;
use twox_hash::xxh3::hash64;

/*

In memory, bucket lists are stored in an immutable Arc, which continues the lockless seamless object state transitions and lifetimes design (see src/object/mod.rs). This way, it's safe to modify the list without a lock, because mutations only come from the serial state worker, but concurrent readers (e.g. read and write requests) can still concurrently read the list from any thread.

*/

#[derive(Clone, Default)]
pub(crate) struct Bucket {
  objects: Arc<Vec<AutoLifecycleObject>>,
}

impl Serialisable for Bucket {
  type DeserialiseArgs = Arc<ObjectsPendingDrop>;

  const FIXED_SERIALISED_LEN: Option<SerialisedLen> = None;

  fn serialised_len(&self) -> SerialisedLen {
    // Represent length using u8.
    1 + SerialisedLen::try_from(self.objects.len()).unwrap()
      * AutoLifecycleObject::FIXED_SERIALISED_LEN.unwrap()
  }

  fn serialise(&self, out: &mut [u8]) {
    out[0] = u8!(self.objects.len());
    let obj_len = usz!(AutoLifecycleObject::FIXED_SERIALISED_LEN.unwrap());
    for (i, obj) in self.objects.iter().enumerate() {
      let start = 1 + usz!(i) * obj_len;
      let end = start + obj_len;
      obj.serialise(&mut out[start..end]);
    }
  }

  fn deserialise(objects_pending_drop: &Self::DeserialiseArgs, raw: &[u8]) -> Self {
    let len = raw[0];
    let obj_len = u64!(AutoLifecycleObject::FIXED_SERIALISED_LEN.unwrap());
    let mut objects = Vec::new();
    for i in 0..len {
      objects.push(AutoLifecycleObject::deserialise(
        &(objects_pending_drop.clone(), ObjectState::Committed),
        raw.read_at(1 + u64!(i) * obj_len, obj_len),
      ));
    }
    Self {
      objects: Arc::new(objects),
    }
  }
}

impl Bucket {
  pub async fn find_object(
    &self,
    dev: Uring,
    key: &[u8],
    expected_id: Option<u64>,
  ) -> Option<AutoLifecycleObject> {
    for o in self.objects.iter() {
      if expected_id.is_some() && o.id != expected_id.unwrap() {
        continue;
      };
      // SAFETY: Objects aren't reaped until AutoLifecycleObject::drop, and we're holding on to an indirect reference to this object.
      let raw = dev
        .read(
          o.dev_offset,
          OBJECT_OFF.with_key_len(OBJECT_KEY_LEN_MAX).lpages(),
        )
        .await;
      if raw.read_u16_le_at(OBJECT_OFF.key_len()) == u16!(key.len())
        && raw.read_at(OBJECT_OFF.key(), u64!(key.len())) == key
      {
        return Some(o.clone());
      };
    }
    None
  }

  pub async fn find_object_with_id_and_read_key(&self, dev: Uring, id: u64) -> Option<Vec<u8>> {
    let o = self.objects.iter().find(|o| o.id == id)?;
    // SAFETY: Objects aren't reaped until AutoLifecycleObject::drop, and we're holding on to an indirect reference to this object.
    let raw = dev
      .read(
        o.dev_offset,
        OBJECT_OFF.with_key_len(OBJECT_KEY_LEN_MAX).lpages(),
      )
      .await;
    let key_len = raw.read_u16_le_at(OBJECT_OFF.key_len());
    // TODO Avoid heap allocation.
    let key = raw.read_at(OBJECT_OFF.key(), u64!(key_len)).to_vec();
    Some(key)
  }

  /// Returns any removed object.
  // Since this should only be called from serial state worker, we don't need to worry about locking or race conditions when updating `self.objects`. For example, no one else can insert or remove objects into/from `self.objects` while this function is executing.
  pub fn update_list(
    &mut self,
    obj_to_prepend: Option<AutoLifecycleObject>,
    obj_id_to_remove: Option<u64>,
  ) -> Option<AutoLifecycleObject> {
    let mut new_objects = Vec::new();
    if let Some(obj) = obj_to_prepend {
      new_objects.push(obj);
    };
    let mut removed = None;
    for obj in self.objects.iter() {
      if Some(obj.id) == obj_id_to_remove {
        assert!(removed.is_none());
        removed = Some(obj.clone());
      } else {
        new_objects.push(obj.clone());
      };
    }
    self.objects = Arc::new(new_objects);
    removed
  }
}

type PersistedBucketsReader = PersistedDashMapReader<u64, Bucket>;
type PersistedBucketsWriter = PersistedDashMapWriter<u64, Bucket>;

/// This can be cheaply cloned.
#[derive(Clone)]
pub(crate) struct BucketsReader {
  buckets: Arc<PersistedBucketsReader>,
}

impl BucketsReader {
  pub fn bucket_id_for_key(&self, key: &[u8]) -> u64 {
    hash64(key)
  }

  pub fn get_bucket_by_id(&self, id: u64) -> Option<Bucket> {
    Some(self.buckets.get(id)?.clone())
  }

  pub fn get_bucket_for_key(&self, key: &[u8]) -> Option<Bucket> {
    self.get_bucket_by_id(self.bucket_id_for_key(key))
  }
}

pub(crate) struct BucketsWriter {
  reader: BucketsReader,
  writer: PersistedBucketsWriter,
}

impl Deref for BucketsWriter {
  type Target = BucketsReader;

  fn deref(&self) -> &Self::Target {
    &self.reader
  }
}

impl BucketsWriter {
  pub async fn load_from_device(
    dev: Uring,
    first_page_dev_offset: u64,
    spage_size: u64,
    objects_pending_drop: &Arc<ObjectsPendingDrop>,
  ) -> (BucketsReader, BucketsWriter) {
    let (inner_reader, inner_writer) = PersistedBucketsWriter::load_from_device(
      dev,
      first_page_dev_offset,
      spage_size,
      &(),
      &objects_pending_drop,
    )
    .await;
    debug!(bucket_count = inner_reader.len(), "buckets loaded");
    let reader = BucketsReader {
      buckets: inner_reader,
    };
    (reader.clone(), BucketsWriter {
      reader,
      writer: inner_writer,
    })
  }

  pub async fn format_device(dev: Uring, first_page_dev_offset: u64, spage_size: u64) {
    PersistedBucketsWriter::format_device(dev, first_page_dev_offset, spage_size).await;
  }

  pub fn commit(&mut self, txn: &mut Transaction, alloc: &mut Allocator) {
    self.writer.commit(txn, alloc);
  }

  pub fn get_or_create_bucket_mut_by_id(
    &mut self,
    id: u64,
  ) -> RefMut<u64, Bucket, BuildHasherDefault<FxHasher>> {
    self.writer.get_mut_or_insert_default(id)
  }
}
