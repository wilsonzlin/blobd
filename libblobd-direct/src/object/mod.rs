use self::layout::calc_object_layout;
use self::offset::ObjectMetadataOffsets;
use self::tail::TailPageSizes;
use crate::op::OpError;
use crate::pages::Pages;
use bufpool::buf::Buf;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::int::Off64ReadInt;
use off64::usz;
use off64::Off64Read;
use std::cmp::min;
use std::ops::Deref;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::RwLockReadGuard;

pub mod id;
pub mod layout;
pub mod offset;
pub mod tail;

#[derive(Clone)]
pub(crate) struct ObjectMetadata {
  dev_offset: u64,
  raw: Buf,
  // If we don't store/cache this, we'll need to recalculate all ObjectMetadataOffsets fields each time we read some fields.
  off: ObjectMetadataOffsets,
  tail_page_sizes: TailPageSizes,
}

impl ObjectMetadata {
  pub fn new(
    dev_offset: u64,
    raw: Buf,
    off: ObjectMetadataOffsets,
    tail_page_sizes: TailPageSizes,
  ) -> Self {
    Self {
      dev_offset,
      off,
      raw,
      tail_page_sizes,
    }
  }

  pub fn load_from_raw(pages: &Pages, dev_offset: u64, raw: Buf) -> Self {
    let mut off = ObjectMetadataOffsets {
      assoc_data_len: 0,
      key_len: 0,
      lpage_count: 0,
      tail_page_count: 0,
    };
    off.key_len = raw.read_u16_le_at(off.key_len());
    let size = raw.read_u40_le_at(off.size());
    let layout = calc_object_layout(pages, size);
    off.lpage_count = layout.lpage_count;
    off.tail_page_count = layout.tail_page_sizes_pow2.len();
    off.assoc_data_len = raw.read_u16_le_at(off.assoc_data_len());
    Self::new(dev_offset, raw, off, layout.tail_page_sizes_pow2)
  }

  // NOTE: This is not the same as `1 << self.metadata_page_size()` as that's rounded up to nearest spage.
  pub fn metadata_size(&self) -> u64 {
    self.off._total_size()
  }

  pub fn dev_offset(&self) -> u64 {
    self.dev_offset
  }

  pub fn lpage_count(&self) -> u32 {
    self.off.lpage_count
  }

  pub fn tail_page_count(&self) -> u8 {
    self.off.tail_page_count
  }

  pub fn tail_page_sizes(&self) -> TailPageSizes {
    self.tail_page_sizes
  }

  pub fn build_raw_data_with_new_object_state(&self, pages: &Pages, new_state: ObjectState) -> Buf {
    let mut buf = pages.allocate_with_zeros(pages.spage_size());
    let len = min(self.raw.len(), usz!(pages.spage_size()));
    buf[..len].copy_from_slice(&self.raw[..len]);
    buf[usz!(self.off.state())] = new_state as u8;
    buf
  }

  /*

  FIELDS
  ======

  */
  pub fn metadata_page_size_pow2(&self) -> u8 {
    // NOTE: This is not the same as `self.metadata_size().ilog2()` as it's rounded up to nearest spage.
    self.raw[usz!(self.off.page_size_pow2())]
  }

  pub fn id(&self) -> u64 {
    self.raw.read_u64_le_at(self.off.id())
  }

  pub fn size(&self) -> u64 {
    self.raw.read_u40_le_at(self.off.size())
  }

  pub fn created_sec(&self) -> u64 {
    self.raw.read_u48_le_at(self.off.created_sec())
  }

  pub fn key_len(&self) -> u16 {
    self.raw.read_u16_le_at(self.off.key_len())
  }

  pub fn key(&self) -> &[u8] {
    self.raw.read_at(self.off.key(), self.key_len().into())
  }

  pub fn lpage_dev_offset(&self, idx: u32) -> u64 {
    self.raw.read_u48_le_at(self.off.lpage(idx))
  }

  pub fn tail_page_dev_offset(&self, idx: u8) -> u64 {
    self.raw.read_u48_le_at(self.off.tail_page(idx))
  }
}

#[derive(PartialEq, Eq, Clone, Copy, Debug, FromPrimitive)]
#[repr(u8)]
pub(crate) enum ObjectState {
  // These are used when scanning the device and aren't actually object states.
  // `_EndOfObjects` must be zero so that a zero filled device is already in that state.
  _EndOfObjects = 0,
  _Empty,
  // These are also used in-memory and are actually object states.
  Incomplete,
  Committed,
}

struct AutoLifecycleObjectInner {
  state: AtomicU8,
  lock: RwLock<()>,
  metadata: ObjectMetadata,
}

#[derive(Clone)]
pub(crate) struct AutoLifecycleObject {
  inner: Arc<AutoLifecycleObjectInner>,
}

impl AutoLifecycleObject {
  pub fn new(metadata: ObjectMetadata, state: ObjectState) -> Self {
    Self {
      inner: Arc::new(AutoLifecycleObjectInner {
        state: AtomicU8::new(state as u8),
        lock: RwLock::new(()),
        metadata,
      }),
    }
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

impl Deref for AutoLifecycleObject {
  type Target = ObjectMetadata;

  fn deref(&self) -> &Self::Target {
    &self.inner.metadata
  }
}
