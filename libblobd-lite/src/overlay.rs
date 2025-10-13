use crate::object::ObjectState;
use dashmap::DashMap;
use std::sync::Arc;

#[must_use]
pub enum OverlayEntry {
  BucketHead { bucket_id: u64, value: u64 },
  ObjectState { object_id: u64, value: ObjectState },
  ObjectNext { object_id: u64, value: u64 },
}

pub struct Overlay {
  bucket_heads: DashMap<u64, u64>,
  object_states: DashMap<u64, ObjectState>,
  object_nexts: DashMap<u64, u64>,
}

impl Overlay {
  pub fn new() -> Overlay {
    Overlay {
      bucket_heads: DashMap::new(),
      object_states: DashMap::new(),
      object_nexts: DashMap::new(),
    }
  }

  pub fn get_bucket_head(&self, bucket_id: u64) -> Option<u64> {
    self.bucket_heads.get(&bucket_id).map(|e| e.value().clone())
  }

  pub fn set_bucket_head(&self, bucket_id: u64, value: u64) -> OverlayEntry {
    self.bucket_heads.insert(bucket_id, value);
    OverlayEntry::BucketHead { bucket_id, value }
  }

  pub fn get_object_state(&self, object_id: u64) -> Option<ObjectState> {
    self
      .object_states
      .get(&object_id)
      .map(|e| e.value().clone())
  }

  pub fn set_object_state(&self, object_id: u64, value: ObjectState) -> OverlayEntry {
    self.object_states.insert(object_id, value);
    OverlayEntry::ObjectState { object_id, value }
  }

  pub fn get_object_next(&self, object_id: u64) -> Option<u64> {
    self.object_nexts.get(&object_id).map(|e| e.value().clone())
  }

  pub fn set_object_next(&self, object_id: u64, value: u64) -> OverlayEntry {
    self.object_nexts.insert(object_id, value);
    OverlayEntry::ObjectNext { object_id, value }
  }
}
