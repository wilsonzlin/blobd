use crate::backing_store::BoundedStore;
use crate::journal::Transaction;
use crate::pages::Pages;
use crate::ring_buf::BufOrSlice;
use crate::ring_buf::RingBuf;
use crate::ring_buf::RingBufItem;
use bufpool::BUFPOOL;
use itertools::Itertools;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u16;
use off64::u64;
use off64::usz;
use off64::Off64Read;
use off64::Off64WriteMut;
use rustc_hash::FxHashSet;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use struct_name::StructName;
use struct_name_macro::StructName;
use tinybuf::TinyBuf;
use tracing::trace;

/**

STREAM
======

Instead of reading and writing directly from/to mmap, we simply load into memory. This avoids subtle race conditions and handling complexity, as we don't have ability to atomically read/write directly from mmap, and we need to ensure strictly sequential event IDs, careful handling of buffer wrapping, not reading and writing simultaneously (which is hard as all writes are external via the journal), etc. It's not much memory anyway and the storage layout is no better optimised than an in-memory Vec.

Events aren't valid until the transaction for the changes they represent has been committed, but the event is written to the device in the same transaction. Therefore, the in-memory data structure is separate from the device data, and the event is separately inserted into the in-memory structure *after* the journal transaction has committed.

We must have strictly sequential event IDs, and we must assign and store them:
- If a replicating client asks for events past N, and N+1 does not exist but N+2 does, we must know that it's because N+1 has been erased, and not just possibly due to holes in event IDs.
- If we don't assign one to each event or store them, when a client asks for N, we won't know if the event at (N % STREAM_EVENT_CAP) is actually N or some other multiple.

The value of `virtual_head` represents the head position in the ring buffer, as well as that entry's event ID. This saves us from storing a sequential event ID with every event.

**/

const STATE_OFFSETOF_HEAD_EVENT_ID: u64 = 0;
const STATE_OFFSETOF_VIRTUAL_HEAD: u64 = STATE_OFFSETOF_HEAD_EVENT_ID + 8;
const STATE_OFFSETOF_VIRTUAL_TAIL: u64 = STATE_OFFSETOF_VIRTUAL_HEAD + 8;

const EVENT_OFFSETOF_TYPE: u64 = 0;
const EVENT_OFFSETOF_TIMESTAMP_MS: u64 = EVENT_OFFSETOF_TYPE + 1;
const EVENT_OFFSETOF_OBJECT_ID: u64 = EVENT_OFFSETOF_TIMESTAMP_MS + 7;
const EVENT_OFFSETOF_KEY_LEN: u64 = EVENT_OFFSETOF_OBJECT_ID + 8;
const EVENT_OFFSETOF_KEY: u64 = EVENT_OFFSETOF_KEY_LEN + 2;
fn EVENT_SIZE(key_len: u16) -> u64 {
  EVENT_OFFSETOF_KEY + u64::from(key_len)
}

#[derive(PartialEq, Eq, Clone, Copy, Debug, FromPrimitive)]
#[repr(u8)]
pub enum StreamEventType {
  // Out of abundance of caution, don't use zero, as that may inadvertently accept a zeroed device.
  ObjectCommit = 1,
  ObjectDelete,
}

#[derive(Clone, Debug)]
pub struct StreamEventOwned {
  pub typ: StreamEventType,
  pub timestamp_ms: u64,
  pub object_id: u64,
  pub key: TinyBuf,
}

pub struct StreamEvent<'a> {
  raw: BufOrSlice<'a>,
}

impl<'a> StreamEvent<'a> {
  pub fn typ(&self) -> StreamEventType {
    StreamEventType::from_u8(self.raw[usz!(EVENT_OFFSETOF_TYPE)]).unwrap()
  }

  pub fn timestamp_ms(&self) -> u64 {
    self.raw.read_u56_le_at(EVENT_OFFSETOF_TIMESTAMP_MS)
  }

  pub fn object_id(&self) -> u64 {
    self.raw.read_u64_le_at(EVENT_OFFSETOF_OBJECT_ID)
  }

  pub fn key(&self) -> &[u8] {
    self.raw.read_at(
      EVENT_OFFSETOF_KEY,
      self.raw.read_u16_le_at(EVENT_OFFSETOF_KEY_LEN).into(),
    )
  }

  pub fn to_owned(&self) -> StreamEventOwned {
    StreamEventOwned {
      typ: self.typ(),
      timestamp_ms: self.timestamp_ms(),
      object_id: self.object_id(),
      key: TinyBuf::from_slice(self.key()),
    }
  }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, StructName)]
pub struct StreamEventExpiredError;

impl Display for StreamEventExpiredError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(Self::struct_name())
  }
}

impl Error for StreamEventExpiredError {}

pub type StreamEventId = u64;

pub(crate) struct Stream {
  dev: BoundedStore,
  pages: Pages,
  ring: RingBuf,
  // We use two as we need to drain when returning from `commit`, but there could be multiple `commit` calls before `make_available`.
  pending_commit: FxHashSet<StreamEventId>,
  pending_flush: FxHashSet<StreamEventId>,

  head_id: StreamEventId,
  offsets: VecDeque<RingBufItem>,
}

impl Stream {
  pub async fn load_from_device(dev: BoundedStore, pages: Pages) -> Self {
    let raw = dev.read_at(0, dev.len()).await;
    let (state, data) = raw.split_at(usz!(pages.spage_size()));
    let head_id = state.read_u64_le_at(STATE_OFFSETOF_HEAD_EVENT_ID);
    let virtual_head = state.read_u64_le_at(STATE_OFFSETOF_VIRTUAL_HEAD);
    let virtual_tail = state.read_u64_le_at(STATE_OFFSETOF_VIRTUAL_TAIL);
    let ring = RingBuf::load(usz!(pages.spage_size()), usz!(virtual_tail), data.to_vec());

    let mut offsets = VecDeque::new();
    let mut virtual_next = virtual_head;
    while virtual_next < virtual_tail {
      let raw = ring
        .get(RingBufItem {
          virtual_offset: usz!(virtual_next),
          len: usz!(EVENT_SIZE(0)),
        })
        .unwrap();
      let key_len = raw.read_u16_le_at(EVENT_OFFSETOF_KEY_LEN);
      let event_size = EVENT_SIZE(key_len);
      offsets.push_back(RingBufItem {
        virtual_offset: usz!(virtual_next),
        len: usz!(event_size),
      });
      virtual_next += event_size;
    }

    Self {
      dev,
      pages,
      pending_commit: Default::default(),
      pending_flush: Default::default(),
      ring,

      head_id,
      offsets,
    }
  }

  pub async fn format_device(dev: BoundedStore, pages: &Pages) {
    let mut state = pages.allocate_uninitialised(pages.spage_size());
    state.write_u64_le_at(STATE_OFFSETOF_HEAD_EVENT_ID, 0);
    state.write_u64_le_at(STATE_OFFSETOF_VIRTUAL_HEAD, 0);
    state.write_u64_le_at(STATE_OFFSETOF_VIRTUAL_TAIL, 0);
    dev.write_at(0, state).await;
  }

  pub fn get_event(
    &self,
    id: StreamEventId,
  ) -> Result<Option<StreamEvent>, StreamEventExpiredError> {
    if self.pending_commit.contains(&id) || self.pending_flush.contains(&id) {
      return Ok(None);
    };

    if id < self.head_id {
      return Err(StreamEventExpiredError);
    };

    let raw = self
      .ring
      .get(self.offsets[usz!(id - self.head_id)])
      .unwrap();

    let e = StreamEvent { raw };

    Ok(Some(e))
  }

  /// This event will be written to the device, but won't be available/visible (i.e. retrievable via `get_event`) until it's been durably written to the device.
  pub fn add_event_pending_commit(&mut self, e: StreamEventOwned) {
    let id = self.head_id + u64!(self.offsets.len());
    let mut raw = BUFPOOL.allocate_with_zeros(usz!(EVENT_SIZE(u16!(e.key.len()))));
    raw[usz!(EVENT_OFFSETOF_TYPE)] = e.typ as u8;
    raw.write_u56_le_at(EVENT_OFFSETOF_TIMESTAMP_MS, e.timestamp_ms);
    raw.write_u64_le_at(EVENT_OFFSETOF_OBJECT_ID, e.object_id);
    raw.write_u16_le_at(EVENT_OFFSETOF_KEY_LEN, u16!(e.key.len()));
    raw.write_at(EVENT_OFFSETOF_KEY, &e.key);
    let virtual_item = self.ring.push(&raw);
    let new_virtual_head =
      (virtual_item.virtual_offset + virtual_item.len).saturating_sub(self.ring.capacity());
    // Drop any events from the head if they are now totally **or partially** cut off.
    while self
      .offsets
      .front()
      .filter(|o| o.virtual_offset < new_virtual_head)
      .is_some()
    {
      self.head_id += 1;
      self.offsets.pop_front().unwrap();
    }
    self.offsets.push_back(virtual_item);
    assert!(self.pending_commit.insert(id));
  }

  /// Provide the return value to `make_available` once durably written to the device.
  pub fn commit(&mut self, txn: &mut Transaction) -> Vec<StreamEventId> {
    let Some(data_pages_to_commit) = self.ring.commit() else {
      // Nothing is dirty, so do not rewrite state.
      return Vec::new();
    };
    trace!("committing stream state and data");
    for (offset, data_slice) in data_pages_to_commit {
      assert_eq!(data_slice.len(), usz!(self.pages.spage_size()));
      let data = self.pages.allocate_from_data(data_slice);
      // The first page is reserved for metadata, so all data pages are shifted down by one page.
      self
        .dev
        .record_in_transaction(txn, u64!(offset) + self.pages.spage_size(), data);
    }

    let mut state = self.pages.allocate_uninitialised(self.pages.spage_size());
    state.write_u64_le_at(STATE_OFFSETOF_HEAD_EVENT_ID, u64!(self.head_id));
    state.write_u64_le_at(
      STATE_OFFSETOF_VIRTUAL_HEAD,
      u64!(self.offsets.front().unwrap().virtual_offset),
    );
    state.write_u64_le_at(STATE_OFFSETOF_VIRTUAL_TAIL, u64!(self.ring.virtual_tail()));
    self.dev.record_in_transaction(txn, 0, state);

    let ids = self.pending_commit.drain().collect_vec();
    self.pending_flush.extend(ids.iter().cloned());
    ids
  }

  pub fn make_available(&mut self, pending_flush: &[StreamEventId]) {
    for id in pending_flush {
      assert!(self.pending_flush.remove(id));
    }
  }
}
