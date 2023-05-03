use crate::pages::Pages;
use crate::uring::UringBounded;
use crate::util::div_pow2;
use crate::util::mod_pow2;
use bufpool_fixed::buf::FixedBuf;
use futures::stream::iter;
use futures::StreamExt;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u64;
use off64::u8;
use off64::usz;
use off64::Off64Read;
use off64::Off64WriteMut;
use std::collections::BTreeMap;
use std::mem::take;
use tokio::task::spawn_blocking;
use tracing::warn;

/*

For simplicity, we buffer the entire journal in memory, and perform one single write instead of writing spages.
- This allows easier reading and writing of individual fields and records, as we don't need to worry about page boundaries.
- This also makes it possible to hash in one go at the end, which means we can send it to a separate thread and not block an async thread. Normally, there are many small hash calculations, so `spawn_blocking` is difficult to use and has some overhead (as each individual hash is not that slow).

*/

// Little more than a wrapper over a hash map, but provides semantic type and method names and does some basic assertions.
#[derive(Debug)]
pub(crate) struct Transaction {
  spage_size_pow2: u8,
  spages: BTreeMap<u64, FixedBuf>,
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

  pub fn record(&mut self, dev_offset: u64, data: FixedBuf) {
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

  pub fn drain_all(&mut self) -> impl Iterator<Item = (u64, FixedBuf)> {
    take(&mut self.spages).into_iter()
  }
}

// For simplicity:
// - The byte count includes the reserved size, so there's no need to do additional subtle calculations.
// - The hashed data includes the reserved data, where the hash field only contains zeros. This makes it simple to calculate; it's simply all data from start of journal (offset 0) until the byte count.
const JOURNAL_METADATA_OFFSETOF_HASH: u64 = 0;
const JOURNAL_METADATA_OFFSETOF_BYTE_COUNT: u64 = JOURNAL_METADATA_OFFSETOF_HASH + 32;
const JOURNAL_METADATA_RESERVED_SIZE: u64 = JOURNAL_METADATA_OFFSETOF_BYTE_COUNT + 8;

pub(crate) struct Journal {
  dev: UringBounded,
  // Must be a multiple of spage for direct I/O writing to work.
  state_dev_offset: u64,
  // Must be a multiple of spage for direct I/O writing to work.
  state_size: u64,
  pages: Pages,
}

impl Journal {
  pub fn new(dev: UringBounded, state_dev_offset: u64, state_size: u64, pages: Pages) -> Self {
    Self {
      dev,
      pages,
      state_dev_offset,
      state_size,
    }
  }

  // Do not erase corrupt journal, in case user wants to investigate.
  // Do not being writing/recovering until fully checked that journal is not corrupt.
  pub async fn recover(&self) {
    let mut raw = self.dev.read(self.state_dev_offset, self.state_size).await;
    let byte_count = raw.read_u64_le_at(JOURNAL_METADATA_OFFSETOF_BYTE_COUNT);
    let recorded_hash = raw.read_at(JOURNAL_METADATA_OFFSETOF_HASH, 32).to_vec();
    raw.write_at(JOURNAL_METADATA_OFFSETOF_HASH, &[0u8; 32]);
    let (expected_hash, raw) = spawn_blocking(move || (blake3::hash(&raw), raw))
      .await
      .unwrap();
    if expected_hash.as_bytes() != recorded_hash.as_slice() {
      warn!("journal is corrupt, invalid hash");
      return;
    };

    let mut next = JOURNAL_METADATA_RESERVED_SIZE;
    let mut to_write = Vec::new();
    while next < byte_count {
      let record_dev_offset = raw.read_u40_le_at(next) << 8;
      let record_data_len = u64!(raw[usz!(next + 5)]) * self.pages.spage_size();
      next += 6;

      let mut record_data = self.pages.allocate_with_zeros(record_data_len);
      record_data.copy_from_slice(raw.read_at(next, record_data_len));
      next += record_data_len;
      to_write.push((record_dev_offset, record_data));
    }

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

  fn generate_blank_state(&self) -> FixedBuf {
    let mut raw = self
      .pages
      .allocate_with_zeros(JOURNAL_METADATA_RESERVED_SIZE);
    raw.write_u64_le_at(
      JOURNAL_METADATA_OFFSETOF_BYTE_COUNT,
      JOURNAL_METADATA_RESERVED_SIZE,
    );
    let hash = blake3::hash(&raw);
    raw.write_at(JOURNAL_METADATA_OFFSETOF_HASH, hash.as_bytes());
    raw
  }

  pub async fn format_device(&self) {
    self
      .dev
      .write(self.state_dev_offset, self.generate_blank_state())
      .await;
  }

  pub async fn commit(&self, txn_records: Vec<(u64, FixedBuf)>) {
    assert!(!txn_records.is_empty());

    // WARNING: We must fit all records in one journal write, and cannot have iterations, as otherwise it's no longer atomic.

    let mut jnl = self.pages.allocate_with_zeros(self.state_size);

    let mut next = JOURNAL_METADATA_RESERVED_SIZE;
    for (dev_offset, data) in txn_records.iter() {
      let data_len = u64!(data.len());
      assert_eq!(mod_pow2(data_len, self.pages.spage_size_pow2), 0);

      jnl.write_u40_le_at(next, dev_offset >> 8);
      jnl[usz!(next + 5)] = u8!(div_pow2(data_len, self.pages.spage_size_pow2));
      next += 6;

      jnl.write_at(next, &data);
      next += data_len;
    }

    jnl.write_u64_le_at(JOURNAL_METADATA_OFFSETOF_BYTE_COUNT, next);

    let (hash, mut jnl) = spawn_blocking(move || (blake3::hash(&jnl), jnl))
      .await
      .unwrap();
    jnl.write_at(JOURNAL_METADATA_OFFSETOF_HASH, hash.as_bytes());

    self.dev.write(self.state_dev_offset, jnl).await;
    self.dev.sync().await;

    iter(txn_records)
      .for_each_concurrent(None, |(dev_offset, buf)| async move {
        self.dev.write(dev_offset, buf).await;
      })
      .await;
    self.dev.sync().await;

    self
      .dev
      .write(self.state_dev_offset, self.generate_blank_state())
      .await;
    self.dev.sync().await;
  }
}
