use crate::uring::Uring;
use crate::util::ceil_pow2;
use crate::util::div_pow2;
use crate::util::mod_pow2;
use bufpool::buf::Buf;
use bufpool::BUFPOOL;
use off64::int::create_u64_le;
use off64::int::Off64WriteMutInt;
use off64::u32;
use off64::u64;
use off64::u8;
use off64::usz;
use off64::Off64WriteMut;
use std::collections::BTreeMap;
use std::collections::VecDeque;
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
const JOURNAL_METADATA_OFFSETOF_RECORD_COUNT: u64 = JOURNAL_METADATA_OFFSETOF_HASH + 32;
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

  pub async fn recover(&self) {
    todo!();
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

  pub async fn commit(&self, mut txn_records: VecDeque<(u64, Buf)>) {
    assert!(!txn_records.is_empty());

    let mut iterations = 0;
    while !txn_records.is_empty() {
      iterations += 1;

      // For each record, we need 5 bytes for the device offset (right-shifted by 8), and 1 byte for log2(data length).
      // NOTE: This isn't technically optimal as not all records will fit, but that's a bin packing problem, and reducing/increasing metadata length also affects how many records can fit.
      let metadata_len = JOURNAL_METADATA_RESERVED_SIZE + (6 * u64!(txn_records.len()));
      let metadata_paged_size = ceil_pow2(metadata_len, self.spage_size_pow2);
      let mut metadata = BUFPOOL.allocate_with_zeros(usz!(metadata_paged_size));
      let mut free = self.state_size - metadata_paged_size;

      let mut hasher = blake3::Hasher::new();

      let mut record_writes = Vec::new();
      let mut metadata_offset = JOURNAL_METADATA_RESERVED_SIZE;
      while txn_records
        .front()
        .filter(|r| u64!(r.1.len()) >= free)
        .is_some()
      {
        let (dev_offset, data) = txn_records.pop_front().unwrap();
        let data_len = u64!(data.len());

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

      hasher.update(&create_u64_le(u64!(record_writes.len())));
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

    // If our journal is not big enough, we have to wait on writes from previous iterations, even though there's no logical blocking necessary.
    if iterations > 1 {
      warn!(
        iterations,
        "it took more than one journal iteration to commit"
      );
    };
  }
}
