use std::{ops::{Deref, DerefMut}, collections::BTreeMap};

use seekable_async_file::SeekableAsyncFile;
use signal_future::{SignalFutureController, SignalFuture};
use tokio::sync::Mutex;
use write_journal::{WriteJournal, AtomicWriteGroup};

use crate::{free_list::FreeList, bucket::Buckets};

// Since updated state to the free list on storage must be reflected in order, and we don't want to lock free list for entire write + fdatasync, we don't append to journal directly, but take a serial, and require that journal writes are sequentialised.
pub struct ChangeSerial {
  num: u64,
  used: bool,
}

// If this is dropped without usage, then we need to detect it, as otherwise we'll get stuck waiting for it forever.
impl Drop for ChangeSerial {
  fn drop(&mut self) {
    if !self.used {
      panic!("change serial dropped without being used");
    };
  }
}

pub struct FreeListWithChangeTracker {
  free_list: FreeList,
  next_change_serial: u64,
}

impl FreeListWithChangeTracker {
  pub fn generate_change_serial(&mut self) -> ChangeSerial {
    let serial = self.next_change_serial;
    self.next_change_serial += 1;
    ChangeSerial { num:serial, used: false}
  }
}

impl Deref for FreeListWithChangeTracker {
  type Target = FreeList;

  fn deref(&self) -> &Self::Target {
    &self.free_list
  }
}

impl DerefMut for FreeListWithChangeTracker {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.free_list
  }
}

pub struct SequentialisedJournal {
  journal: WriteJournal,
  next_serial: u64,
  pending: BTreeMap<u64, (AtomicWriteGroup, SignalFutureController)>,
}

impl SequentialisedJournal {
  pub async fn write(&mut self, mut serial: ChangeSerial, write: AtomicWriteGroup) {
    assert!(!serial.used);
    serial.used = true;
    let (fut, fut_ctl) = SignalFuture::new();
    self.pending.insert(serial.num, (write, fut_ctl));
    while let Some(e) = self.pending.first_entry() {
      if *e.key() == self.next_serial {
        self.next_serial += 1;
        let (write, fut_ctl) = e.remove();
        self.journal.write_with_custom_signal(write, fut_ctl).await;
      };
    };
    fut.await;
  }
}

pub struct Ctx {
  pub buckets: Buckets,
  pub device: SeekableAsyncFile,
  pub journal: Mutex<SequentialisedJournal>,
  pub free_list: Mutex<FreeListWithChangeTracker>,
  pub free_list_dev_offset: u64,
}
