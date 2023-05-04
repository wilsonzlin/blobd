use crate::journal::Transaction;
use crate::pages::Pages;
use crate::ring_buf::BufOrSlice;
use crate::ring_buf::RingBuf;
use crate::ring_buf::RingBufItem;
use crate::uring::UringBounded;
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
use std::cmp::min;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use struct_name::StructName;
use struct_name_macro::StructName;
use tinybuf::TinyBuf;

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

const STREVT_OFFSETOF_TYPE: u64 = 0;
const STREVT_OFFSETOF_TIMESTAMP_MS: u64 = STREVT_OFFSETOF_TYPE + 1;
const STREVT_OFFSETOF_OBJECT_ID: u64 = STREVT_OFFSETOF_TIMESTAMP_MS + 7;
const STREVT_OFFSETOF_KEY_LEN: u64 = STREVT_OFFSETOF_OBJECT_ID + 8;
const STREVT_OFFSETOF_KEY: u64 = STREVT_OFFSETOF_KEY_LEN + 2;
fn STREVT_SIZE(key_len: u16) -> u64 {
  STREVT_OFFSETOF_KEY + u64::from(key_len)
}

#[derive(PartialEq, Eq, Clone, Copy, Debug, FromPrimitive)]
#[repr(u8)]
pub enum StreamEventType {
  // WARNING: Values must start from 1, so that empty slots are automatically detected since formatting fills storage with zero.
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
    StreamEventType::from_u8(self.raw[usz!(STREVT_OFFSETOF_TYPE)]).unwrap()
  }

  pub fn timestamp_ms(&self) -> u64 {
    self.raw.read_u56_le_at(STREVT_OFFSETOF_TIMESTAMP_MS)
  }

  pub fn object_id(&self) -> u64 {
    self.raw.read_u64_le_at(STREVT_OFFSETOF_OBJECT_ID)
  }

  pub fn key(&self) -> &[u8] {
    self.raw.read_at(
      STREVT_OFFSETOF_KEY,
      self.raw.read_u16_le_at(STREVT_OFFSETOF_KEY_LEN).into(),
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

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct StreamEventId(RingBufItem);

pub(crate) struct Stream {
  dev: UringBounded,
  pages: Pages,
  ring: RingBuf,
  pending: FxHashSet<StreamEventId>,
}

impl Stream {
  pub async fn load_from_device(dev: UringBounded, pages: Pages) -> Self {
    let raw = dev.read(0, dev.len()).await;
    let (meta, data) = raw.split_at(usz!(pages.spage_size()));
    let virtual_tail = meta.read_u64_le_at(0);
    let ring = RingBuf::load(usz!(pages.spage_size()), usz!(virtual_tail), data.to_vec());

    Self {
      dev,
      pages,
      pending: Default::default(),
      ring,
    }
  }

  pub async fn format_device(dev: UringBounded, pages: &Pages) {
    const BUFSIZE: u64 = 1024 * 1024 * 1024 * 1;
    for offset in (0..dev.len()).step_by(usz!(BUFSIZE)) {
      let len = min(dev.len() - offset, BUFSIZE);
      dev.write(offset, pages.slow_allocate_with_zeros(len)).await;
    }
  }

  pub fn get_event(
    &self,
    id: StreamEventId,
  ) -> Result<Option<StreamEvent>, StreamEventExpiredError> {
    if self.pending.contains(&id) {
      return Ok(None);
    };

    let Some(raw) = self.ring.get(id.0) else {
      return Err(StreamEventExpiredError);
    };

    let e = StreamEvent { raw };

    Ok(Some(e))
  }

  /// This event will be written to the device, but won't be available/visible (i.e. retrievable via `get_event`) until it's been durably written to the device.
  pub fn add_event_pending_commit(&mut self, e: StreamEventOwned) {
    let mut raw = BUFPOOL.allocate_with_zeros(usz!(STREVT_SIZE(u16!(e.key.len()))));
    raw[usz!(STREVT_OFFSETOF_TYPE)] = e.typ as u8;
    raw.write_u56_le_at(STREVT_OFFSETOF_TIMESTAMP_MS, e.timestamp_ms);
    raw.write_u64_le_at(STREVT_OFFSETOF_OBJECT_ID, e.object_id);
    raw.write_u16_le_at(STREVT_OFFSETOF_KEY_LEN, u16!(e.key.len()));
    raw.write_at(STREVT_OFFSETOF_KEY, &e.key);
    self.ring.push(&raw);
  }

  /// Provide the return value to `make_available` once durably written to the device.
  pub fn commit(&mut self, txn: &mut Transaction) -> Vec<StreamEventId> {
    let mut meta = self.pages.allocate_uninitialised(self.pages.spage_size());
    meta.write_u64_le_at(0, u64!(self.ring.get_virtual_tail()));
    self.dev.record_in_transaction(txn, 0, meta);

    for (offset, data_slice) in self.ring.commit() {
      assert_eq!(data_slice.len(), usz!(self.pages.spage_size()));
      let data = self.pages.allocate_from_data(data_slice);
      // The first page is reserved for metadata, so all data pages are shifted down by one page.
      self
        .dev
        .record_in_transaction(txn, u64!(offset) + self.pages.spage_size(), data);
    }
    self.pending.drain().collect_vec()
  }

  pub fn make_available(&mut self, pending: &[StreamEventId]) {
    for id in pending {
      assert!(self.pending.remove(id));
    }
  }
}
