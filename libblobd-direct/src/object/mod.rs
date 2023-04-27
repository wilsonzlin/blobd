use crate::op::OpError;
use crate::op::OpResult;
use crate::persisted_collection::serialisable::Serialisable;
use crate::persisted_collection::serialisable::SerialisedLen;
use crate::state::ObjectsPendingDrop;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use std::ops::Deref;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

pub mod id;
pub mod layout;
pub mod offset;
pub mod reap;
pub mod tail;

/*

LOCKLESS SEAMLESS OBJECT STATE TRANSITIONS AND LIFETIMES
========================================================

- Reap incomplete objects on expiry simply by dropping from the incomplete list. 100% thread safe, because incomplete list is part of state which is always mutated sequentially.
- Transition object state WHILE it's being read from/written to, without holding any locks, not even while reading from/writing to disk.
- Don't require locks on bucket lists, not even to append, remove, or asynchronously find an object.

*/

#[derive(PartialEq, Eq, Clone, Copy, Debug, FromPrimitive)]
#[repr(u8)]
pub(crate) enum ObjectState {
  Incomplete,
  Committed,
  Deleted,
}

// It's not invalid to use (i.e. read/write object data/metadata from/to the device) while holding an Arc reference to this (i.e. while holding am `AutoLifecycleObject`), but check the state regularly and stop whatever you're doing if the state has changed.
struct AutoLifecycleObjectInner {
  state: AtomicU8,
  objects_pending_drop: Arc<ObjectsPendingDrop>,
  metadata: ObjectMetadata,
}

impl Drop for AutoLifecycleObjectInner {
  fn drop(&mut self) {
    assert_eq!(
      self.state.load(std::sync::atomic::Ordering::Relaxed),
      ObjectState::Deleted as u8
    );
    self.objects_pending_drop.remove(&self.metadata.id).unwrap();
  }
}

pub(crate) struct AutoLifecycleObjectLock(Arc<AutoLifecycleObjectInner>);

pub(crate) struct AutoLifecycleObjectWeak {
  inner: Weak<AutoLifecycleObjectInner>,
}

impl AutoLifecycleObjectWeak {
  pub fn lock_if_still_valid(
    &self,
    expected_state: ObjectState,
  ) -> OpResult<AutoLifecycleObjectLock> {
    let Some(strong) = self.inner.upgrade() else {
      return Err(OpError::ObjectNotFound);
    };
    if ObjectState::from_u8(strong.state.load(Ordering::Relaxed)).unwrap() != expected_state {
      return Err(OpError::ObjectNotFound);
    };
    Ok(AutoLifecycleObjectLock(strong))
  }
}

#[derive(Clone)]
#[must_use] // Require explicit `let _` binding to indicate dropping.
pub(crate) struct AutoLifecycleObject {
  inner: Arc<AutoLifecycleObjectInner>,
}

impl AutoLifecycleObject {
  pub fn for_newly_created_object(
    objects_pending_drop: Arc<ObjectsPendingDrop>,
    metadata: ObjectMetadata,
    state: ObjectState,
  ) -> Self {
    Self {
      inner: Arc::new(AutoLifecycleObjectInner {
        state: AtomicU8::new(state as u8),
        objects_pending_drop,
        metadata,
      }),
    }
  }

  pub fn into_weak(self) -> AutoLifecycleObjectWeak {
    AutoLifecycleObjectWeak {
      inner: Arc::downgrade(&self.inner),
    }
  }

  /// This doesn't reap the object immediately; instead, once no one is using it anymore, the object will be reaped.
  /// Taking ownership of `self` isn't technically necessary, but implies that the object shouldn't be used anymore. It also drops the current reference so it can get one step closer to actually reaping.
  pub fn delete(self) {
    self
      .inner
      .state
      .store(ObjectState::Deleted as u8, Ordering::Relaxed);
    let None = self.inner.objects_pending_drop.insert(self.id, self.inner.metadata.clone()) else {
      unreachable!();
    };
  }
}

impl Deref for AutoLifecycleObject {
  type Target = ObjectMetadata;

  fn deref(&self) -> &Self::Target {
    &self.inner.metadata
  }
}

impl Serialisable for AutoLifecycleObject {
  type DeserialiseArgs = (Arc<ObjectsPendingDrop>, ObjectState);

  const FIXED_SERIALISED_LEN: Option<SerialisedLen> = ObjectMetadata::FIXED_SERIALISED_LEN;

  fn serialised_len(&self) -> SerialisedLen {
    self.inner.metadata.serialised_len()
  }

  fn serialise(&self, out: &mut [u8]) {
    self.inner.metadata.serialise(out)
  }

  fn deserialise((objects_pending_drop, state): &Self::DeserialiseArgs, raw: &[u8]) -> Self {
    let metadata = ObjectMetadata::deserialise(&(), raw);
    AutoLifecycleObject::for_newly_created_object(
      objects_pending_drop.clone(),
      metadata,
      state.clone(),
    )
  }
}

#[derive(Clone)]
pub(crate) struct ObjectMetadata {
  pub id: u64,
  pub dev_offset: u64,
  // Store size in the bucket so that we don't need a read of the metadata just to calculate where a data page device offset field is.
  pub size: u64,
  // Stored as u48.
  pub created_sec: u64,
}

const OFFSETOF_ID: u64 = 0;
const OFFSETOF_DEV_OFFSET_RSHIFT8: u64 = OFFSETOF_ID + 8;
const OFFSETOF_SIZE: u64 = OFFSETOF_DEV_OFFSET_RSHIFT8 + 5;
const OFFSETOF_CREATED_SEC: u64 = OFFSETOF_SIZE + 5;
const SIZE: u64 = OFFSETOF_CREATED_SEC + 6;

impl Serialisable for ObjectMetadata {
  type DeserialiseArgs = ();

  const FIXED_SERIALISED_LEN: Option<SerialisedLen> = Some(SIZE as SerialisedLen);

  fn serialised_len(&self) -> SerialisedLen {
    Self::FIXED_SERIALISED_LEN.unwrap()
  }

  fn serialise(&self, out: &mut [u8]) {
    out.write_u64_le_at(OFFSETOF_ID, self.id);
    out.write_u40_le_at(OFFSETOF_DEV_OFFSET_RSHIFT8, self.dev_offset >> 8);
    out.write_u40_le_at(OFFSETOF_SIZE, self.size);
    out.write_u48_le_at(OFFSETOF_CREATED_SEC, self.created_sec);
  }

  fn deserialise(_args: &Self::DeserialiseArgs, raw: &[u8]) -> Self {
    let id = raw.read_u64_le_at(OFFSETOF_ID);
    let dev_offset = raw.read_u40_le_at(OFFSETOF_DEV_OFFSET_RSHIFT8) << 8;
    let size = raw.read_u40_le_at(OFFSETOF_SIZE);
    let created_sec = raw.read_u48_le_at(OFFSETOF_CREATED_SEC);
    Self {
      id,
      dev_offset,
      size,
      created_sec,
    }
  }
}
