use crate::journal::Transaction;
use crate::uring::Uring;
use bufpool::buf::Buf;
use bufpool::BUFPOOL;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u16;
use off64::u64;
use off64::usz;
use off64::Off64Read;
use off64::Off64WriteMut;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::mem::replace;
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

Structure
---------

## Metadata page:

u64 virtual_tail_page // The page number of the most recently written/updated page.
u64 virtual_tail_id // The amount of events written/created over all time (only increasing).

## Data page:

{
  u8 type
  u56 timestamp_ms
  u64 object_id
  u16 key_len
  u8[] key
}[] events

**/

const METADATA_OFFSETOF_VIRTUAL_TAIL_PAGE: u64 = 0;
const METADATA_OFFSETOF_VIRTUAL_LEN: u64 = METADATA_OFFSETOF_VIRTUAL_TAIL_PAGE + 8;

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
pub struct StreamEvent {
  pub typ: StreamEventType,
  pub timestamp_ms: u64,
  pub object_id: u64,
  pub key: TinyBuf,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, StructName)]
pub struct StreamEventExpiredError;

impl Display for StreamEventExpiredError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(Self::struct_name())
  }
}

impl Error for StreamEventExpiredError {}

#[derive(Default, Debug)]
pub(crate) struct CommittedEvents {
  pop_old_last_page: bool,
  new_pages: Vec<Buf>,
  new_positions: Vec<(u64, u64)>,
}

pub(crate) struct Stream {
  metadata_dev_offset: u64,
  data_dev_offset: u64,
  data_page_cap: u64,
  spage_size: u64,

  virtual_head_page: u64,
  pages: VecDeque<Buf>,
  virtual_head_id: u64,
  // (virtual_page_number, offset_in_page).
  positions: VecDeque<(u64, u64)>,

  pending: VecDeque<StreamEvent>,
}

impl Stream {
  pub async fn load_from_device(
    dev: Uring,
    metadata_dev_offset: u64,
    data_dev_offset: u64,
    data_page_cap: u64,
    spage_size: u64,
  ) -> Self {
    assert!(data_page_cap >= 1);

    let raw_meta = dev.read(metadata_dev_offset, spage_size).await;
    let virtual_tail_page = raw_meta.read_u64_le_at(METADATA_OFFSETOF_VIRTUAL_TAIL_PAGE);
    let virtual_len = raw_meta.read_u64_le_at(METADATA_OFFSETOF_VIRTUAL_LEN);

    let mut pages = VecDeque::new();
    let mut positions = VecDeque::new();

    let virtual_head_page = virtual_tail_page.saturating_sub(data_page_cap - 1);
    for vp in virtual_head_page..=virtual_tail_page {
      let physical_dev_offset = data_dev_offset + (vp % data_page_cap) * spage_size;
      let raw = dev.read(physical_dev_offset, spage_size).await;
      let mut offset_within_page = 0;
      while offset_within_page < u64!(raw.len()) {
        let typ_raw = raw[usz!(offset_within_page + STREVT_OFFSETOF_TYPE)];
        if typ_raw == 0 {
          break;
        };
        let key_len = raw.read_u16_le_at(offset_within_page + STREVT_OFFSETOF_KEY_LEN);
        positions.push_back((vp, offset_within_page));
        offset_within_page += STREVT_SIZE(key_len);
      }
      pages.push_back(raw);
    }
    let virtual_head_id = virtual_len - u64!(positions.len());

    Self {
      data_dev_offset,
      data_page_cap,
      metadata_dev_offset,
      pages,
      pending: Default::default(),
      positions,
      spage_size,
      virtual_head_id,
      virtual_head_page,
    }
  }

  pub async fn format_device(
    dev: Uring,
    metadata_dev_offset: u64,
    data_dev_offset: u64,
    _data_page_cap: u64,
    spage_size: u64,
  ) {
    dev
      .write(
        metadata_dev_offset,
        BUFPOOL.allocate_with_zeros(usz!(spage_size)),
      )
      .await;
    dev
      .write(
        data_dev_offset,
        BUFPOOL.allocate_with_zeros(usz!(spage_size)),
      )
      .await;
  }

  pub fn get_event(&self, id: u64) -> Result<Option<StreamEvent>, StreamEventExpiredError> {
    if id < self.virtual_head_id {
      return Err(StreamEventExpiredError);
    };
    let idx = id - self.virtual_head_id;
    let Some((virtual_page_number, offset_in_page)) = self.positions.get(usz!(idx)) else {
      return Ok(None);
    };
    // The page must exist.
    let page = self
      .pages
      .get(usz!(virtual_page_number - self.virtual_head_page))
      .unwrap();
    let typ = StreamEventType::from_u8(page[usz!(offset_in_page + STREVT_OFFSETOF_TYPE)]).unwrap();
    let timestamp_ms = page.read_u56_le_at(offset_in_page + STREVT_OFFSETOF_TIMESTAMP_MS);
    let object_id = page.read_u64_le_at(offset_in_page + STREVT_OFFSETOF_OBJECT_ID);
    let key_len = page.read_u16_le_at(offset_in_page + STREVT_OFFSETOF_KEY_LEN);
    let key =
      TinyBuf::from_slice(page.read_at(offset_in_page + STREVT_OFFSETOF_KEY, key_len.into()));
    Ok(Some(StreamEvent {
      typ,
      timestamp_ms,
      object_id,
      key,
    }))
  }

  // This event will be written to the device, but won't be available/visible (i.e. retrievable via `get_event`) until it's been written to the device and synced.
  pub fn add_event_pending_commit(&mut self, e: StreamEvent) {
    self.pending.push_back(e);
  }

  /// How to use: call `commit` to generate data to write to device using journal and return an opaque value. After it's been written and synced, call `sync` on the returned value to make pending events available.
  /// WARNING: `sync` must be called in the same order as `commit`, consuming the `commit` return values in order. However, it is safe to call `commit` multiple times before or in between `sync` calls, so long as the previous requirement is upheld.
  pub fn commit(&mut self, txn: &mut Transaction) -> CommittedEvents {
    let mut committed = CommittedEvents::default();
    if self.pending.is_empty() {
      return committed;
    };

    // TODO Don't pop old last page if not enough free space for first pending event.
    let (pop_old_last_page, mut buf, mut virtual_page_number, mut offset_within_page) =
      match self.pages.back() {
        Some(buf) => {
          let virtual_page_number = self.virtual_head_page + u64!(self.pages.len()) - 1;
          let last_pos = self.positions.back().unwrap();
          assert_eq!(last_pos.0, virtual_page_number);
          let offset_within_page = last_pos.1;
          assert!(offset_within_page > 0);
          (true, buf.clone(), virtual_page_number, offset_within_page)
        }
        None => {
          let buf = BUFPOOL.allocate_with_zeros(usz!(self.spage_size));
          (false, buf, self.virtual_head_page, 0)
        }
      };
    committed.pop_old_last_page = pop_old_last_page;

    for e in self.pending.drain(..) {
      if offset_within_page + STREVT_SIZE(u16!(e.key.len())) > self.spage_size {
        // We've run out of space on the current page.
        let old_buf = replace(&mut buf, BUFPOOL.allocate_with_zeros(usz!(self.spage_size)));
        txn.record(
          self.data_dev_offset + (virtual_page_number % self.data_page_cap) * self.spage_size,
          old_buf.clone(),
        );
        committed.new_pages.push(old_buf);
        virtual_page_number += 1;
        offset_within_page = 0;
      }
      committed
        .new_positions
        .push((virtual_page_number, offset_within_page));
      buf[usz!(offset_within_page + STREVT_OFFSETOF_TYPE)] = e.typ as u8;
      buf.write_u56_le_at(
        offset_within_page + STREVT_OFFSETOF_TIMESTAMP_MS,
        e.timestamp_ms,
      );
      buf.write_u64_le_at(offset_within_page + STREVT_OFFSETOF_OBJECT_ID, e.object_id);
      buf.write_u16_le_at(
        offset_within_page + STREVT_OFFSETOF_KEY_LEN,
        u16!(e.key.len()),
      );
      buf.write_at(offset_within_page + STREVT_OFFSETOF_KEY, e.key.as_slice());
      offset_within_page += STREVT_SIZE(u16!(e.key.len()));
    }
    // Commit last page.
    txn.record(
      self.data_dev_offset + (virtual_page_number % self.data_page_cap) * self.spage_size,
      buf.clone(),
    );
    committed.new_pages.push(buf);

    // Commit metadata.
    let mut raw_meta = BUFPOOL.allocate_with_zeros(usz!(self.spage_size));
    raw_meta.write_u64_le_at(METADATA_OFFSETOF_VIRTUAL_TAIL_PAGE, virtual_page_number);
    raw_meta.write_u64_le_at(
      METADATA_OFFSETOF_VIRTUAL_LEN,
      self.virtual_head_id + u64!(self.positions.len() + committed.new_positions.len()),
    );
    txn.record(self.metadata_dev_offset, raw_meta);

    committed
  }

  pub fn sync(&mut self, committed: CommittedEvents) {
    if committed.pop_old_last_page {
      self.pages.pop_back();
    };
    for e in committed.new_pages {
      self.pages.push_back(e);
    }
    for e in committed.new_positions {
      self.positions.push_back(e);
    }
    while u64!(self.pages.len()) > self.data_page_cap {
      let virtual_page_number = self.virtual_head_page;
      self.pages.pop_front().unwrap();
      self.virtual_head_page += 1;
      while self
        .positions
        .front()
        .filter(|p| p.0 == virtual_page_number)
        .is_some()
      {
        self.positions.pop_front().unwrap();
        self.virtual_head_id += 1;
      }
    }
  }
}
