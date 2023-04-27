use crate::allocator::Allocator;
use crate::journal::Transaction;
use crate::object::AutoLifecycleObject;
use crate::object::ObjectMetadata;
use crate::object::ObjectState;
use crate::persisted_collection::btree::PersistedBTreeMap;
use crate::state::ObjectsPendingDrop;
use crate::uring::Uring;
use crate::util::get_now_sec;
use std::sync::Arc;
use std::time::Duration;

// Map from object ID to bucket ID. It just happens so that object IDs are also chronological, so this map allows removing objects when they're committed and also popping chronologically.
type PersistedList = PersistedBTreeMap<u64, AutoLifecycleObject>;

type LockedList = Arc<parking_lot::RwLock<PersistedList>>;

/// This can be cheaply cloned.
#[derive(Clone)]
pub(crate) struct IncompleteListReader {
  list: LockedList,
}

impl IncompleteListReader {
  pub fn contains(&self, object_id: u64) -> bool {
    self.list.read().contains(object_id)
  }

  pub fn get(&self, object_id: u64) -> Option<AutoLifecycleObject> {
    self.list.read().get(object_id).cloned()
  }
}

pub(crate) struct IncompleteListWriter {
  list: LockedList,
  objects_pending_drop: Arc<ObjectsPendingDrop>,
  reap_objects_after_secs: u64,
}

impl IncompleteListWriter {
  pub async fn load_from_device(
    dev: Uring,
    first_page_dev_offset: u64,
    spage_size: u64,
    reap_objects_after_secs: u64,
    objects_pending_drop: Arc<ObjectsPendingDrop>,
  ) -> (IncompleteListReader, IncompleteListWriter) {
    let list = PersistedList::load_from_device(
      dev,
      first_page_dev_offset,
      spage_size,
      &(),
      &(objects_pending_drop.clone(), ObjectState::Incomplete),
    )
    .await;
    let locked_list = Arc::new(parking_lot::RwLock::new(list));

    let reader = IncompleteListReader {
      list: locked_list.clone(),
    };
    let writer = IncompleteListWriter {
      list: locked_list.clone(),
      objects_pending_drop,
      reap_objects_after_secs,
    };
    (reader, writer)
  }

  pub async fn format_device(dev: Uring, first_page_dev_offset: u64, spage_size: u64) {
    PersistedList::format_device(dev, first_page_dev_offset, spage_size).await;
  }

  pub fn commit(&mut self, txn: &mut Transaction, alloc: &mut Allocator) {
    self.list.write().commit(txn, alloc);
  }

  pub fn insert(&mut self, metadata: ObjectMetadata) {
    let None = self.list.write().insert(metadata.id, AutoLifecycleObject::for_newly_created_object(self.objects_pending_drop.clone(), metadata, ObjectState::Incomplete)) else {
      unreachable!();
    };
  }

  pub fn remove(&mut self, object_id: u64) -> Option<AutoLifecycleObject> {
    self.list.write().remove(object_id)
  }

  // Returns optimal amount of time to wait before calling again, if applicable.
  pub fn reap(&mut self) -> Option<Duration> {
    let now = get_now_sec();
    let mut lock = self.list.write();
    // TODO Popping will mark as dirty, even if we immediately insert it back. However, the Rust borrow checker gets in the way if we try to borrow then remove.
    while let Some((obj_id, obj)) = lock.pop_first() {
      let expires_at = obj.created_sec + self.reap_objects_after_secs;
      if now < expires_at {
        lock.insert(obj_id, obj);
        return Some(Duration::from_secs(expires_at - now));
      };
      obj.delete();
    }
    None
  }
}
