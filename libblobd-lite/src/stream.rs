#[cfg(test)]
use crate::test_util::device::TestSeekableAsyncFile as SeekableAsyncFile;
#[cfg(test)]
use crate::test_util::journal::TestTransaction as Transaction;
use dashmap::DashMap;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::int::create_u64_be;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::usz;
use off64::Off64Read;
use rustc_hash::FxHasher;
#[cfg(not(test))]
use seekable_async_file::SeekableAsyncFile;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::hash::BuildHasherDefault;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use struct_name::StructName;
use struct_name_macro::StructName;
use tracing::debug;
use tracing::warn;
#[cfg(not(test))]
use write_journal::Transaction;

/**

STREAM
======

Instead of reading and writing directly from/to mmap, we simply load into memory. This avoids subtle race conditions and handling complexity, as we don't have ability to atomically read/write directly from mmap, and we need to ensure strictly sequential event IDs, careful handling of buffer wrapping, not reading and writing simultaneously (which is hard as all writes are external via the journal), etc. It's not much memory anyway and the storage layout is no better optimised than an in-memory Vec.

Events aren't valid until the transaction for the changes they represent has been committed, but the event is written to the device in the same transaction. Therefore, the in-memory data structure is separate from the device data, and the event is separately inserted into the in-memory structure *after* the journal transaction has committed.

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

#[derive(PartialEq, Eq, Clone, Copy, Debug, FromPrimitive)]
#[repr(u8)]
pub enum StreamEventType {
  // WARNING: Values must start from 1, so that empty slots are automatically detected since formatting fills storage with zero.
  ObjectCommit = 1,
  ObjectDelete,
}

#[derive(Clone, Debug)]
pub struct StreamEvent {
  pub typ: StreamEventType,
  pub bucket_id: u64,
  pub object_id: u64,
}

pub(crate) struct StreamInMemory {
  virtual_head: AtomicU64,
  events: DashMap<u64, StreamEvent, BuildHasherDefault<FxHasher>>,
}

pub(crate) struct Stream {
  dev_offset: u64,
  in_memory: Arc<StreamInMemory>,
}

#[must_use]
pub(crate) struct CreatedStreamEvent {
  id: u64,
  event: StreamEvent,
}

impl Stream {
  pub async fn load_from_device(
    dev: &SeekableAsyncFile,
    dev_offset: u64,
  ) -> (Stream, Arc<StreamInMemory>) {
    let raw_all = dev.read_at(dev_offset, STREAM_SIZE).await;
    let virtual_head = raw_all.read_u64_be_at(STREAM_OFFSETOF_VIRTUAL_HEAD);
    let events = DashMap::<u64, StreamEvent, BuildHasherDefault<FxHasher>>::default();
    for i in 0..STREAM_EVENT_CAP {
      let event_id = virtual_head + i;
      let raw = raw_all.read_at(STREAM_OFFSETOF_EVENT(event_id), STREVT_SIZE);
      let typ_raw = raw[usz!(STREVT_OFFSETOF_TYPE)];
      if typ_raw == 0 {
        break;
      };
      let typ = StreamEventType::from_u8(typ_raw).unwrap();
      let bucket_id = raw.read_u48_be_at(STREVT_OFFSETOF_BUCKET_ID);
      let object_id = raw.read_u64_be_at(STREVT_OFFSETOF_OBJECT_ID);
      events.insert(event_id, StreamEvent {
        bucket_id,
        object_id,
        typ,
      });
    }
    debug!(event_count = events.len(), virtual_head, "stream loaded");
    let in_memory = Arc::new(StreamInMemory {
      events,
      virtual_head: AtomicU64::new(virtual_head),
    });
    (
      Stream {
        dev_offset,
        in_memory: in_memory.clone(),
      },
      in_memory,
    )
  }

  pub async fn format_device(dev: &SeekableAsyncFile, dev_offset: u64) {
    dev.write_at(dev_offset, vec![0u8; usz!(STREAM_SIZE)]).await;
  }

  /// Returns the event ID. This won't add to the in-memory list, so remember to do that after the transaction has committed.
  pub fn create_event_on_device(
    &self,
    txn: &mut Transaction,
    e: StreamEvent,
  ) -> CreatedStreamEvent {
    let event_id = self.in_memory.virtual_head.fetch_add(1, Ordering::Relaxed);
    if event_id > STREAM_EVENT_CAP {
      // This may not exist.
      self.in_memory.events.remove(&(event_id - STREAM_EVENT_CAP));
    };

    // New head.
    txn.write(
      self.dev_offset + STREAM_OFFSETOF_VIRTUAL_HEAD,
      create_u64_be(event_id + 1),
    );
    // New event.
    let mut raw = vec![0u8; usz!(STREVT_SIZE)];
    raw[usz!(STREVT_OFFSETOF_TYPE)] = e.typ as u8;
    raw.write_u48_be_at(STREVT_OFFSETOF_BUCKET_ID, e.bucket_id);
    raw.write_u64_be_at(STREVT_OFFSETOF_OBJECT_ID, e.object_id);
    txn.write(self.dev_offset + STREAM_OFFSETOF_EVENT(event_id), raw);

    CreatedStreamEvent {
      id: event_id,
      event: e,
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

impl StreamInMemory {
  pub fn add_event_to_in_memory_list(&self, c: CreatedStreamEvent) {
    let virtual_head = self.virtual_head.load(Ordering::Relaxed);
    if virtual_head >= STREAM_EVENT_CAP && virtual_head - STREAM_EVENT_CAP > c.id {
      warn!("event stream is rotating too quickly, recently created event has already expired");
      return;
    };
    let None = self.events.insert(c.id, c.event) else {
      unreachable!();
    };
  }

  pub fn get_event(&self, id: u64) -> Result<Option<StreamEvent>, StreamEventExpiredError> {
    if id >= STREAM_EVENT_CAP && self.virtual_head.load(Ordering::Relaxed) > id - STREAM_EVENT_CAP {
      return Err(StreamEventExpiredError);
    };
    Ok(self.events.get(&id).map(|e| e.clone()))
  }
}
