use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::int::create_u64_be;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::usz;
use off64::Off64Read;
use seekable_async_file::SeekableAsyncFile;
use std::collections::BTreeMap;
use tracing::debug;
use write_journal::Transaction;

/**

STREAM
======

Instead of reading and writing directly from/to storage, we simply load into memory. This avoids subtle race conditions and handling complexity, as we don't have ability to atomically read/write directly from mmap, and we need to ensure strictly sequential event IDs, careful handling of buffer wrapping, not reading and writing simultaneously (which is hard as all writes are external via the journal), etc. It's not much memory anyway and the storage layout is no better optimised than an in-memory Vec.

We must have strictly sequential event IDs, and we must assign and store them:
- If a replicating client asks for events past N, and N+1 does not exist but N+2 does, we must know that it's because N+1 has been erased, and not just possibly due to holes in event IDs.
- If we don't assign one to each event or store them, when a client asks for N, we won't know if the event at (N % STREAM_EVENT_CAP) is actually N or some other multiple.

The value of `virtual_head` represents the head position in the ring buffer, as well as that entry's event ID. This saves us from storing a sequential event ID with every event.

Structure
---------

u64 virtual_head
{
  u8 type
  u48 bucket_id
  u64 object_id
}[STREAM_EVENT_CAP] events_ring_buffer

**/

pub(crate) const STREVT_OFFSETOF_TYPE: u64 = 0;
pub(crate) const STREVT_OFFSETOF_BUCKET_ID: u64 = STREVT_OFFSETOF_TYPE + 1;
pub(crate) const STREVT_OFFSETOF_OBJECT_ID: u64 = STREVT_OFFSETOF_BUCKET_ID + 6;
pub(crate) const STREVT_SIZE: u64 = STREVT_OFFSETOF_OBJECT_ID + 8;

pub(crate) const STREAM_OFFSETOF_VIRTUAL_HEAD: u64 = 0;
pub(crate) const STREAM_OFFSETOF_EVENTS: u64 = STREAM_OFFSETOF_VIRTUAL_HEAD + 8;
pub(crate) fn STREAM_OFFSETOF_EVENT(event_id: u64) -> u64 {
  STREAM_OFFSETOF_EVENTS + (STREVT_SIZE * (event_id % STREAM_EVENT_CAP))
}
// Ensure this is large enough such that a replica can be created without a high chance of the stream wrapping before the storage is replicated. An event is only 15 bytes, so we can be gracious here.
pub(crate) const STREAM_EVENT_CAP: u64 = 8_000_000;
pub(crate) const STREAM_SIZE: u64 = STREAM_OFFSETOF_EVENTS + (STREAM_EVENT_CAP * STREVT_SIZE);

#[derive(PartialEq, Eq, Clone, Copy, FromPrimitive)]
#[repr(u8)]
pub(crate) enum StreamEventType {
  // Special marker when the stream is not fully filled.
  // WARNING: This must be zero, so that empty slots are automatically detected as this since formatting fills storage with zero.
  EndOfEvents,
  ObjectCommit,
  ObjectDelete,
}

pub(crate) struct StreamEvent {
  pub typ: StreamEventType,
  pub bucket_id: u64,
  pub object_id: u64,
}

pub(crate) struct Stream {
  dev_offset: u64,
  virtual_head: u64,
  events: BTreeMap<u64, StreamEvent>,
}

impl Stream {
  pub async fn load_from_device(dev: &SeekableAsyncFile, dev_offset: u64) -> Stream {
    let raw_all = dev.read_at(dev_offset, STREAM_SIZE).await;
    let virtual_head = raw_all.read_u64_be_at(STREAM_OFFSETOF_VIRTUAL_HEAD);
    let mut events = BTreeMap::new();
    for i in 0..STREAM_EVENT_CAP {
      let event_id = virtual_head + i;
      let raw = raw_all.read_at(STREAM_OFFSETOF_EVENT(event_id), STREVT_SIZE);
      let typ = StreamEventType::from_u8(raw[usz!(STREVT_OFFSETOF_TYPE)]).unwrap();
      if typ == StreamEventType::EndOfEvents {
        break;
      };
      let bucket_id = raw.read_u48_be_at(STREVT_OFFSETOF_BUCKET_ID);
      let object_id = raw.read_u64_be_at(STREVT_OFFSETOF_OBJECT_ID);
      events.insert(event_id, StreamEvent {
        bucket_id,
        object_id,
        typ,
      });
    }
    debug!(event_count = events.len(), virtual_head, "stream loaded");
    Stream {
      dev_offset,
      virtual_head,
      events,
    }
  }

  pub async fn format_device(dev: &SeekableAsyncFile, dev_offset: u64) {
    dev.write_at(dev_offset, vec![0u8; usz!(STREAM_SIZE)]).await;
  }

  pub fn create_event(&mut self, txn: &mut Transaction, e: StreamEvent) {
    let event_id = self.virtual_head;
    self.virtual_head += 1;

    // New head.
    txn.write(
      self.dev_offset + STREAM_OFFSETOF_VIRTUAL_HEAD,
      create_u64_be(self.virtual_head).to_vec(),
    );
    // New event.
    let mut raw = vec![0u8; usz!(STREVT_SIZE)];
    raw[usz!(STREVT_OFFSETOF_TYPE)] = e.typ as u8;
    raw.write_u48_be_at(STREVT_OFFSETOF_BUCKET_ID, e.bucket_id);
    raw.write_u64_be_at(STREVT_OFFSETOF_OBJECT_ID, e.object_id);
    txn.write(self.dev_offset + STREAM_OFFSETOF_EVENT(event_id), raw);

    self.events.insert(event_id, e);
    if self.events.len() > usz!(STREAM_EVENT_CAP) {
      self.events.pop_first().unwrap();
    };
  }
}
