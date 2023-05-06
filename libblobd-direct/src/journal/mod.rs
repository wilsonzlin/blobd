use crate::backing_store::PartitionStore;
use crate::pages::Pages;
use crate::util::div_pow2;
use crate::util::mod_pow2;
use bufpool::buf::Buf;
use futures::stream::iter;
use futures::StreamExt;
use off64::int::create_u40_le;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u64;
use off64::u8;
use off64::usz;
use off64::Off64Read;
use off64::Off64WriteMut;
use std::io::Write;
use tokio::task::spawn_blocking;
use tracing::debug;
use tracing::trace;
use tracing::warn;

/*

For simplicity, we buffer the entire journal in memory, and perform one single write instead of writing spages.
- This allows easier reading and writing of individual fields and records, as we don't need to worry about page boundaries.
- This also makes it possible to hash in one go at the end, which means we can send it to a separate thread and not block an async thread. Normally, there are many small hash calculations, so `spawn_blocking` is difficult to use and has some overhead (as each individual hash is not that slow).

*/

// Little more than a wrapper, but provides semantic type and method names and does some basic assertions.
#[derive(Debug)]
pub(crate) struct Transaction {
  spage_size_pow2: u8,
  // We no longer use a map, as overwrites in the same transaction are rare and intentional (e.g. updating state of object rewrites only first spage) in the current new architecture.
  spages: Vec<(u64, Buf)>,
}

impl Transaction {
  pub fn new(spage_size_pow2: u8) -> Self {
    Self {
      spage_size_pow2,
      spages: Vec::new(),
    }
  }

  pub fn is_empty(&self) -> bool {
    self.spages.is_empty()
  }

  pub fn record(&mut self, dev_offset: u64, data: Buf) {
    trace!(dev_offset, len = data.len(), "adding transaction record");
    assert!(data.len() >= (1 << self.spage_size_pow2));
    assert_eq!(mod_pow2(u64!(data.len()), self.spage_size_pow2), 0);
    self.spages.push((dev_offset, data));
  }

  pub fn drain_all(&mut self) -> impl Iterator<Item = (u64, Buf)> + '_ {
    self.spages.drain(..)
  }
}

// For simplicity:
// - The byte count includes the reserved size, so there's no need to do additional subtle calculations.
// - The hashed data includes the reserved data, where the hash field only contains zeros. This makes it simple to calculate; it's simply all data from start of journal (offset 0) until the byte count.
const JOURNAL_METADATA_OFFSETOF_HASH: u64 = 0;
const JOURNAL_METADATA_OFFSETOF_BYTE_COUNT: u64 = JOURNAL_METADATA_OFFSETOF_HASH + 32;
const JOURNAL_METADATA_RESERVED_SIZE: u64 = JOURNAL_METADATA_OFFSETOF_BYTE_COUNT + 8;

pub(crate) struct Journal {
  disabled: bool,
  dev: PartitionStore,
  // Must be a multiple of spage for direct I/O writing to work.
  state_dev_offset: u64,
  // Must be a multiple of spage for direct I/O writing to work.
  state_size: u64,
  pages: Pages,
}

impl Journal {
  pub fn new(dev: PartitionStore, state_dev_offset: u64, state_size: u64, pages: Pages) -> Self {
    Self {
      disabled: false,
      dev,
      pages,
      state_dev_offset,
      state_size,
    }
  }

  pub fn dangerously_disable_journal(&mut self) {
    self.disabled = true;
    warn!("journal is now disabled, corruption will likely occur on exit");
  }

  // Do not erase corrupt journal, in case user wants to investigate.
  // Do not being writing/recovering until fully checked that journal is not corrupt.
  pub async fn recover(&self) {
    debug!("checking journal");
    let mut raw = self
      .dev
      .read_at(self.state_dev_offset, self.state_size)
      .await;
    let byte_count = raw.read_u64_le_at(JOURNAL_METADATA_OFFSETOF_BYTE_COUNT);
    let recorded_hash = raw.read_at(JOURNAL_METADATA_OFFSETOF_HASH, 32).to_vec();
    raw.write_at(JOURNAL_METADATA_OFFSETOF_HASH, &[0u8; 32]);
    let (expected_hash, raw) =
      spawn_blocking(move || (blake3::hash(&raw[..usz!(byte_count)]), raw))
        .await
        .unwrap();
    if expected_hash.as_bytes() != recorded_hash.as_slice() {
      warn!(
        expected_hash = format!("{:x?}", expected_hash.as_bytes()),
        recorded_hash = format!("{:x?}", recorded_hash.as_slice()),
        "journal is corrupt, invalid hash",
      );
      return;
    };

    let mut next = JOURNAL_METADATA_RESERVED_SIZE;
    let mut to_write = Vec::new();
    while next < byte_count {
      let record_dev_offset = raw.read_u40_le_at(next) << 8;
      let record_data_len = u64!(raw[usz!(next + 5)]) * self.pages.spage_size();
      next += 6;

      let record_data = self
        .pages
        .allocate_from_data(raw.read_at(next, record_data_len));
      next += record_data_len;
      to_write.push((record_dev_offset, record_data));
    }
    debug!(records = to_write.len(), "recovering from journal");

    futures::stream::iter(to_write)
      .for_each_concurrent(None, |(dev_offset, buf)| async move {
        self.dev.write_at(dev_offset, buf).await;
      })
      .await;
    self.dev.sync().await;

    // WARNING: Make sure to sync writes BEFORE erasing journal.
    self
      .dev
      .write_at(self.state_dev_offset, self.generate_blank_state())
      .await;
    self.dev.sync().await;
    debug!("journal recovered");
  }

  fn generate_blank_state(&self) -> Buf {
    let mut raw = self.pages.allocate_uninitialised(self.pages.spage_size());
    raw.write_at(JOURNAL_METADATA_OFFSETOF_HASH, &[0u8; 32]);
    raw.write_u64_le_at(
      JOURNAL_METADATA_OFFSETOF_BYTE_COUNT,
      JOURNAL_METADATA_RESERVED_SIZE,
    );
    let hash = blake3::hash(&raw[..usz!(JOURNAL_METADATA_RESERVED_SIZE)]);
    raw.write_at(JOURNAL_METADATA_OFFSETOF_HASH, hash.as_bytes());
    raw
  }

  pub async fn format_device(&self) {
    self
      .dev
      .write_at(self.state_dev_offset, self.generate_blank_state())
      .await;
  }

  pub async fn commit(&self, txn_records: Vec<(u64, Buf)>) {
    assert!(!txn_records.is_empty());

    if !self.disabled {
      // WARNING: We must fit all records in one journal write, and cannot have iterations, as otherwise it's no longer atomic.
      trace!(records = txn_records.len(), "beginning commit");

      // Don't use `allocate_with_zeros`, as it takes a really long time to allocate then fill a huge slice of memory (especially in this hot path, as a lot of writes have to go through the journal), and it's unnecessary.
      let mut jnl = self.pages.allocate(self.state_size);
      jnl.extend_from_slice(&[0u8; JOURNAL_METADATA_RESERVED_SIZE as usize]);

      for (dev_offset, data) in txn_records.iter() {
        let data_len = u64!(data.len());
        assert_eq!(mod_pow2(data_len, self.pages.spage_size_pow2), 0);

        jnl.write(&create_u40_le(dev_offset >> 8)).unwrap();
        jnl.push(u8!(div_pow2(data_len, self.pages.spage_size_pow2)));
        jnl.write(data).unwrap();
      }

      let jnl_byte_count = u64!(jnl.len());
      jnl.write_u64_le_at(JOURNAL_METADATA_OFFSETOF_BYTE_COUNT, jnl_byte_count);

      trace!(bytes = jnl_byte_count, "hashing journal");
      let (hash, mut jnl) = spawn_blocking(move || (blake3::hash(&jnl), jnl))
        .await
        .unwrap();
      jnl.write_at(JOURNAL_METADATA_OFFSETOF_HASH, hash.as_bytes());

      trace!(bytes = jnl_byte_count, "writing journal");
      self.dev.write_at(self.state_dev_offset, jnl).await;
      self.dev.sync().await;
    };

    trace!(records = txn_records.len(), "writing records");
    iter(txn_records)
      .for_each_concurrent(None, |(dev_offset, buf)| async move {
        self.dev.write_at(dev_offset, buf).await;
      })
      .await;
    self.dev.sync().await;

    if !self.disabled {
      trace!("erasing journal");
      self
        .dev
        .write_at(self.state_dev_offset, self.generate_blank_state())
        .await;
      self.dev.sync().await;
      trace!("journal committed");
    };
  }
}
