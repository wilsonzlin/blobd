use self::tail::TailPageSizes;
use crate::op::OpError;
use crate::pages::Pages;
use crate::util::ceil_pow2;
use crate::util::div_mod_pow2;
use chrono::serde::ts_microseconds;
use chrono::DateTime;
use chrono::Utc;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u32;
use off64::u8;
use off64::usz;
use serde::Deserialize;
use serde::Serialize;
use std::ops::Deref;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tinybuf::TinyBuf;
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;

pub mod tail;

#[derive(PartialEq, Eq, Clone, Copy, Debug, FromPrimitive)]
#[repr(u8)]
pub(crate) enum ObjectState {
  // These are used when scanning the device and aren't actually object states.
  // `_EndOfBundleTuples` must be zero so that a zero filled device is already in that state.
  _EndOfBundleTuples = 0,
  // These are also used in-memory and are actually object states.
  Incomplete,
  Committed,
}

pub(crate) struct ObjectLayout {
  pub lpage_count: u32,
  pub tail_page_sizes_pow2: TailPageSizes,
}

pub(crate) fn calc_object_layout(pages: &Pages, object_size: u64) -> ObjectLayout {
  let (lpage_count, tail_size) = div_mod_pow2(object_size, pages.lpage_size_pow2);
  let lpage_count = u32!(lpage_count);
  let mut rem = ceil_pow2(tail_size, pages.spage_size_pow2);
  let mut tail_page_sizes_pow2 = TailPageSizes::new();
  loop {
    let pos = rem.leading_zeros();
    if pos == 64 {
      break;
    };
    let pow2 = u8!(63 - pos);
    tail_page_sizes_pow2.push(pow2);
    rem &= !(1 << pow2);
  }
  ObjectLayout {
    lpage_count,
    tail_page_sizes_pow2,
  }
}

const TUPLE_OFFSETOF_STATE: u64 = 0;
const TUPLE_OFFSETOF_ID: u64 = TUPLE_OFFSETOF_STATE + 1;
const TUPLE_OFFSETOF_METADATA_DEV_OFFSET: u64 = TUPLE_OFFSETOF_ID + 8;
const TUPLE_OFFSETOF_METADATA_PAGE_SIZE_POW2: u64 = TUPLE_OFFSETOF_METADATA_DEV_OFFSET + 6;
pub(crate) const OBJECT_TUPLE_SERIALISED_LEN: u64 = TUPLE_OFFSETOF_METADATA_PAGE_SIZE_POW2 + 1;

#[derive(Clone, Debug)]
pub(crate) struct ObjectTuple {
  pub state: ObjectState,
  pub id: u64,
  pub metadata_dev_offset: u64,
  pub metadata_page_size_pow2: u8,
}

impl ObjectTuple {
  pub fn serialise(&self, out: &mut [u8]) {
    assert_eq!(out.len(), usz!(OBJECT_TUPLE_SERIALISED_LEN));
    out[usz!(TUPLE_OFFSETOF_STATE)] = self.state as u8;
    out.write_u64_le_at(TUPLE_OFFSETOF_ID, self.id);
    out.write_u48_le_at(TUPLE_OFFSETOF_METADATA_DEV_OFFSET, self.metadata_dev_offset);
    out[usz!(TUPLE_OFFSETOF_METADATA_PAGE_SIZE_POW2)] = self.metadata_page_size_pow2;
  }

  pub fn deserialise(raw: &[u8]) -> Self {
    assert_eq!(raw.len(), usz!(OBJECT_TUPLE_SERIALISED_LEN));
    Self {
      state: ObjectState::from_u8(raw[usz!(TUPLE_OFFSETOF_STATE)]).unwrap(),
      id: raw.read_u64_le_at(TUPLE_OFFSETOF_ID),
      metadata_dev_offset: raw.read_u48_le_at(TUPLE_OFFSETOF_METADATA_DEV_OFFSET),
      metadata_page_size_pow2: raw[usz!(TUPLE_OFFSETOF_METADATA_PAGE_SIZE_POW2)],
    }
  }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ObjectMetadata {
  pub size: u64,
  #[serde(with = "ts_microseconds")]
  pub created: DateTime<Utc>,
  pub key: TinyBuf,
  pub lpage_dev_offsets: Vec<u64>,
  pub tail_page_dev_offsets: Vec<u64>,
}

struct ObjectInner {
  id: u64,
  state: AtomicU8,
  lock: RwLock<()>,
  metadata: ObjectMetadata,
}

#[derive(Clone)]
pub(crate) struct Object {
  inner: Arc<ObjectInner>,
}

impl Object {
  pub fn new(id: u64, state: ObjectState, metadata: ObjectMetadata) -> Self {
    Self {
      inner: Arc::new(ObjectInner {
        id,
        lock: RwLock::new(()),
        metadata,
        state: AtomicU8::new(state as u8),
      }),
    }
  }

  pub fn id(&self) -> u64 {
    self.inner.id
  }

  pub fn get_state(&self) -> ObjectState {
    ObjectState::from_u8(self.inner.state.load(Ordering::Relaxed)).unwrap()
  }

  pub async fn update_state_then_ensure_no_writers(&self, new_state: ObjectState) {
    // Update state BEFORE acquiring lock.
    self.inner.state.store(new_state as u8, Ordering::Relaxed);
    let lock = self.inner.lock.write().await;
    drop(lock);
  }

  pub async fn lock_for_writing_if_still_valid(
    &self,
    expected_state: ObjectState,
  ) -> Result<RwLockReadGuard<()>, OpError> {
    // Acquire lock BEFORE checking state.
    let lock = self.inner.lock.read().await;
    if self.get_state() != expected_state {
      return Err(OpError::ObjectNotFound);
    };
    Ok(lock)
  }
}

impl Deref for Object {
  type Target = ObjectMetadata;

  fn deref(&self) -> &Self::Target {
    &self.inner.metadata
  }
}
