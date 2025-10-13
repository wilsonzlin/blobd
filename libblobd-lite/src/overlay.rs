use crate::object::ObjectState;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// This provides temporary in-memory state values that should be prioritized over values on disk.
/// Our state (e.g. index) is entirely disk resident. This means to query and update it, we read from and write to values on disk.
/// However, we use the journal for writes, for safety reasons. This means that state changes aren't actually immediately visible.
/// Nor would they be safe to read, since they may not be written atomically (even to the page cache e.g. cross-page-boundary writes).
/// Even if we could use page cache with atomic immediately-visible writes, we would never directly write (even to page cache), since that page could get flushed by the kernel before we flush the journal, bypassing journal and corrupting disk state.
/// The alternative would be to wait for the journal to commit, but that would be too slow.
/// So this overlay should be checked, whenever reading specific fields that have an overlay.
/// We can therefore immediately make changes visible and make progress with subsequent requests affecting the same state, instead of waiting for the journal to commit.
/// To prevent memory leaks, these entries must be evicted once the journal does commit.
/// This means `OverlayTicket` must be *carefully* passed to `evict` only AFTER awaiting journal commit.
pub(crate) struct Overlay {
  next_serial: AtomicU64,
  bucket_heads: DashMap<u64, Value<u64>>,
  object_states: DashMap<u128, Value<ObjectState>>,
  object_nexts: DashMap<u128, Value<u64>>,
}

struct Value<T> {
  serial: u64,
  value: T,
}

// We use serial rather than just comparing value Eq as that may have slight race conditions.
#[must_use]
pub(crate) enum OverlayTicket {
  BucketHead { bucket_id: u64, serial: u64 },
  ObjectState { object_id: u128, serial: u64 },
  ObjectNext { object_id: u128, serial: u64 },
}

impl Overlay {
  pub fn new() -> Overlay {
    Overlay {
      next_serial: AtomicU64::new(0),
      bucket_heads: DashMap::new(),
      object_states: DashMap::new(),
      object_nexts: DashMap::new(),
    }
  }

  fn next_serial(&self) -> u64 {
    self.next_serial.fetch_add(1, Ordering::Relaxed)
  }

  pub fn evict(&self, entry: OverlayTicket) {
    match entry {
      OverlayTicket::BucketHead { bucket_id, serial } => {
        if let Entry::Occupied(entry) = self.bucket_heads.entry(bucket_id)
          && entry.get().serial == serial
        {
          entry.remove();
        }
      }
      OverlayTicket::ObjectState { object_id, serial } => {
        if let Entry::Occupied(entry) = self.object_states.entry(object_id)
          && entry.get().serial == serial
        {
          entry.remove();
        }
      }
      OverlayTicket::ObjectNext { object_id, serial } => {
        if let Entry::Occupied(entry) = self.object_nexts.entry(object_id)
          && entry.get().serial == serial
        {
          entry.remove();
        }
      }
    }
  }

  pub fn get_bucket_head(&self, bucket_id: u64) -> Option<u64> {
    self.bucket_heads.get(&bucket_id).map(|e| e.value().value)
  }

  pub fn set_bucket_head(&self, bucket_id: u64, value: u64) -> OverlayTicket {
    let serial = self.next_serial();
    self.bucket_heads.insert(bucket_id, Value { serial, value });
    OverlayTicket::BucketHead { bucket_id, serial }
  }

  pub fn get_object_state(&self, object_id: u128) -> Option<ObjectState> {
    self.object_states.get(&object_id).map(|e| e.value().value)
  }

  pub fn set_object_state(&self, object_id: u128, value: ObjectState) -> OverlayTicket {
    let serial = self.next_serial();
    self
      .object_states
      .insert(object_id, Value { serial, value });
    OverlayTicket::ObjectState { object_id, serial }
  }

  pub fn get_object_next(&self, object_id: u128) -> Option<u64> {
    self.object_nexts.get(&object_id).map(|e| e.value().value)
  }

  pub fn set_object_next(&self, object_id: u128, value: u64) -> OverlayTicket {
    let serial = self.next_serial();
    self.object_nexts.insert(object_id, Value { serial, value });
    OverlayTicket::ObjectNext { object_id, serial }
  }
}
