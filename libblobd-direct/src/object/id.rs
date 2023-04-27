use crate::journal::Transaction;
use crate::uring::Uring;
use bufpool::BUFPOOL;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::usz;
use tracing::debug;

pub(crate) struct ObjectIdSerial {
  dev_offset: u64,
  next: u64,
  spage_size: u64,
  dirty: bool,
}

impl ObjectIdSerial {
  pub async fn load_from_device(dev: Uring, dev_offset: u64, spage_size: u64) -> Self {
    let raw = dev.read(dev_offset, spage_size).await;
    let next = raw.read_u64_le_at(0);
    debug!(next_id = next, "object ID serial loaded");
    Self {
      dev_offset,
      next,
      spage_size,
      dirty: false,
    }
  }

  pub async fn format_device(dev: Uring, dev_offset: u64, spage_size: u64) {
    dev
      .write(dev_offset, BUFPOOL.allocate_with_zeros(usz!(spage_size)))
      .await;
  }

  pub fn next(&mut self) -> u64 {
    let id = self.next;
    self.next += 1;
    self.dirty = true;
    id
  }

  pub fn commit(&mut self, txn: &mut Transaction) {
    if !self.dirty {
      return;
    };
    self.dirty = false;

    let mut raw = BUFPOOL.allocate_with_zeros(usz!(self.spage_size));
    raw.write_u64_le_at(0, self.next);
    txn.record(self.dev_offset, raw);
  }
}
