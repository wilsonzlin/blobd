use crate::uring::Uring;
use crate::util::ceil_pow2;
use crate::util::div_pow2;
use crate::util::mod_pow2;
use bufpool::buf::Buf;
use bufpool::BUFPOOL;
use futures::StreamExt;
use off64::int::create_u64_le;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u32;
use off64::u64;
use off64::u8;
use off64::usz;
use off64::Off64Read;
use off64::Off64WriteMut;
use std::collections::BTreeMap;
use std::mem::take;
use tokio::spawn;
use tracing::warn;

// Little more than a wrapper over a hash map, but provides semantic type and method names and does some basic assertions.
#[derive(Debug)]
pub(crate) struct Transaction {
  spage_size_pow2: u8,
  spages: BTreeMap<u64, Buf>,
}

impl Transaction {
  pub fn new(spage_size_pow2: u8) -> Self {
    Self {
      spage_size_pow2,
      spages: Default::default(),
    }
  }

  pub fn is_empty(&self) -> bool {
    self.spages.is_empty()
  }

  pub fn record(&mut self, dev_offset: u64, data: Buf) {
    assert!(data.len() >= (1 << self.spage_size_pow2));
    assert_eq!(mod_pow2(u64!(data.len()), self.spage_size_pow2), 0);
    if let Some((prev_dev_offset, prev_data)) = self.spages.range(..dev_offset).rev().next() {
      assert!(prev_dev_offset + u64!(prev_data.len()) <= dev_offset);
    };
    if let Some((next_dev_offset, _)) = self.spages.range(dev_offset..).next() {
      assert!(*next_dev_offset >= dev_offset + u64!(data.len()));
    };
    let None = self.spages.insert(dev_offset, data) else {
      panic!("conflicting data to write at offset {dev_offset}");
    };
  }

  pub fn drain_all(&mut self) -> impl Iterator<Item = (u64, Buf)> {
    take(&mut self.spages).into_iter()
  }
}

const JOURNAL_METADATA_OFFSETOF_HASH: u64 = 0;
// We must store the raw byte count, rounded up to the next spage, as otherwise we won't know how much data to read from the device.
// For simplicity, this value includes the reserved metadata as well as all record metadata entries.
const JOURNAL_METADATA_OFFSETOF_PAGE_COUNT: u64 = JOURNAL_METADATA_OFFSETOF_HASH + 32;
const JOURNAL_METADATA_OFFSETOF_RECORD_COUNT: u64 = JOURNAL_METADATA_OFFSETOF_PAGE_COUNT + 4;
const JOURNAL_METADATA_RESERVED_SIZE: u64 = JOURNAL_METADATA_OFFSETOF_RECORD_COUNT + 4;

pub(crate) struct Journal {
  dev: Uring,
  state_dev_offset: u64,
  // Amount of bytes on the device reserved for the journal.
  state_size: u64,
  spage_size_pow2: u8,
}

impl Journal {
  pub fn new(dev: Uring, state_dev_offset: u64, state_size: u64, spage_size_pow2: u8) -> Self {
    Self {
      dev,
      state_dev_offset,
      state_size,
      spage_size_pow2,
    }
  }

  // Do not erase corrupt journal, in case user wants to investigate.
  // Do not being writing/recovering until fully checked that journal is not corrupt.
  pub async fn recover(&self) {
    let spage_size = 1 << self.spage_size_pow2;
    let mut raw = self.dev.read(self.state_dev_offset, spage_size).await;
    let journal_page_count = u64!(raw.read_u32_le_at(JOURNAL_METADATA_OFFSETOF_PAGE_COUNT));
    if journal_page_count > 1 {
      if journal_page_count * spage_size > self.state_size {
        // Corrupt journal.
        warn!("journal is corrupt, invalid size");
        return;
      };
      raw.append(
        &mut self
          .dev
          .read(
            self.state_dev_offset + spage_size,
            (journal_page_count - 1) * spage_size,
          )
          .await,
      );
    };
    let record_count = u64!(raw.read_u32_le_at(JOURNAL_METADATA_OFFSETOF_RECORD_COUNT));
    let metadata_paged_size = ceil_pow2(
      JOURNAL_METADATA_RESERVED_SIZE + (6 * record_count),
      self.spage_size_pow2,
    );

    let mut hasher = blake3::Hasher::new();

    let mut metadata_offset = JOURNAL_METADATA_RESERVED_SIZE;
    let mut data_offset = metadata_paged_size;
    let mut to_write = Vec::new();
    for _ in 0..record_count {
      let record_dev_offset = raw.read_u40_le_at(metadata_offset) << 8;
      let record_data_len = u64!(raw[usz!(metadata_offset + 5)]) * spage_size;
      metadata_offset += 6;

      let data = raw.read_at(data_offset, record_data_len);
      data_offset += record_data_len;
      to_write.push((record_dev_offset, BUFPOOL.allocate_from_data::<&[u8]>(data)));

      hasher.update(&create_u64_le(record_dev_offset));
      hasher.update(&create_u64_le(record_data_len));
      hasher.update(data);
    }
    hasher.update(&create_u64_le(journal_page_count));
    hasher.update(&create_u64_le(record_count));
    if hasher.finalize().as_bytes() != raw.read_at(JOURNAL_METADATA_OFFSETOF_HASH, 32) {
      warn!("journal is corrupt, invalid hash");
      return;
    };

    futures::stream::iter(to_write)
      .for_each_concurrent(None, |(dev_offset, buf)| async move {
        self.dev.write(dev_offset, buf).await;
      })
      .await;
    self.dev.sync().await;

    // WARNING: Make sure to sync writes BEFORE erasing journal.
    self
      .dev
      .write(self.state_dev_offset, self.generate_blank_state())
      .await;
    self.dev.sync().await;
  }

  fn generate_blank_state(&self) -> Buf {
    let mut raw = BUFPOOL.allocate_with_zeros(usz!(1 << self.spage_size_pow2));

    let mut hasher = blake3::Hasher::new();
    hasher.update(&create_u64_le(0));
    let hash = hasher.finalize();
    raw.write_at(JOURNAL_METADATA_OFFSETOF_HASH, hash.as_bytes());

    raw
  }

  pub async fn format_device(&self) {
    self
      .dev
      .write(self.state_dev_offset, self.generate_blank_state())
      .await;
  }

  pub async fn commit(&self, txn_records: Vec<(u64, Buf)>) {
    assert!(!txn_records.is_empty());

    // WARNING: We must fit all records in one journal write, and cannot have iterations, as otherwise it's no longer atomic.

    // For each record, we need 5 bytes for the device offset (right-shifted by 8), and 1 byte for log2(data length).
    let metadata_len = JOURNAL_METADATA_RESERVED_SIZE + (6 * u64!(txn_records.len()));
    let metadata_paged_size = ceil_pow2(metadata_len, self.spage_size_pow2);
    let mut metadata = BUFPOOL.allocate_with_zeros(usz!(metadata_paged_size));
    let mut free = self.state_size - metadata_paged_size;

    let mut hasher = blake3::Hasher::new();

    let mut record_writes = Vec::new();
    let mut metadata_offset = JOURNAL_METADATA_RESERVED_SIZE;
    for (dev_offset, data) in txn_records {
      let data_len = u64!(data.len());
      assert!(free >= data_len);

      hasher.update(&create_u64_le(dev_offset));
      hasher.update(&create_u64_le(data_len));
      hasher.update(&data);

      assert_eq!(mod_pow2(data_len, self.spage_size_pow2), 0);
      metadata.write_u40_le_at(metadata_offset, dev_offset >> 8);
      metadata[usz!(metadata_offset + 5)] = u8!(div_pow2(data_len, self.spage_size_pow2));
      metadata_offset += 6;

      record_writes.push(spawn({
        let dev = self.dev.clone();
        let write_dev_offset = self.state_dev_offset + (self.state_size - free);
        async move { (dev_offset, dev.write(write_dev_offset, data).await) }
      }));
      free -= data_len;
    }
    assert!(
      !record_writes.is_empty(),
      "journal is stuck, not enough space"
    );

    let journal_page_count = ceil_pow2(self.state_size - free, self.spage_size_pow2);
    hasher.update(&create_u64_le(journal_page_count));
    hasher.update(&create_u64_le(u64!(record_writes.len())));
    metadata.write_u32_le_at(
      JOURNAL_METADATA_OFFSETOF_PAGE_COUNT,
      u32!(journal_page_count),
    );
    metadata.write_u32_le_at(
      JOURNAL_METADATA_OFFSETOF_RECORD_COUNT,
      u32!(record_writes.len()),
    );
    let hash = hasher.finalize();
    metadata.write_at(JOURNAL_METADATA_OFFSETOF_HASH, hash.as_bytes());

    self.dev.write(self.state_dev_offset, metadata).await;
    let mut to_flush = Vec::new();
    for w in record_writes {
      let (dev_offset, buf) = w.await.unwrap();
      to_flush.push((dev_offset, buf));
    }
    self.dev.sync().await;

    let mut flush_writes = Vec::new();
    for (dev_offset, buf) in to_flush {
      flush_writes.push(spawn({
        let dev = self.dev.clone();
        async move {
          dev.write(dev_offset, buf).await;
        }
      }));
    }
    for w in flush_writes {
      w.await.unwrap();
    }
    self.dev.sync().await;

    self
      .dev
      .write(self.state_dev_offset, self.generate_blank_state())
      .await;
    self.dev.sync().await;
  }
}
