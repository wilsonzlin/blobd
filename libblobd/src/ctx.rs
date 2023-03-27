use crate::bucket::Buckets;
use crate::free_list::FreeList;
use crate::object_id::ObjectIdSerial;
use crate::stream::Stream;
use seekable_async_file::SeekableAsyncFile;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use write_journal::AtomicWriteGroup;
use write_journal::WriteJournal;

// Since updated state to the free list on storage must be reflected in order, and we don't want to lock free list for entire write + fdatasync, we don't append to journal directly, but take a serial, and require that journal writes are sequentialised. This also sequentialises writes to object ID serial, stream, etc., which they rely on.
pub(crate) struct ChangeSerial {
  num: u64,
}

pub(crate) struct FreeListWithChangeTracker {
  free_list: FreeList,
  next_change_serial: u64,
}

impl FreeListWithChangeTracker {
  pub fn new(free_list: FreeList) -> Self {
    Self {
      free_list,
      next_change_serial: 0,
    }
  }

  pub fn generate_change_serial(&mut self) -> ChangeSerial {
    let serial = self.next_change_serial;
    self.next_change_serial += 1;
    ChangeSerial { num: serial }
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

pub(crate) struct SequentialisedJournal {
  journal: Arc<WriteJournal>,
  next_serial: u64,
  pending: BTreeMap<u64, (AtomicWriteGroup, SignalFutureController)>,
}

impl SequentialisedJournal {
  pub fn new(journal: Arc<WriteJournal>) -> Self {
    Self {
      journal,
      next_serial: 0,
      pending: BTreeMap::new(),
    }
  }

  pub async fn write(&mut self, serial: ChangeSerial, write: AtomicWriteGroup) {
    let (fut, fut_ctl) = SignalFuture::new();
    self.pending.insert(serial.num, (write, fut_ctl));
    while let Some(e) = self.pending.first_entry() {
      if *e.key() != self.next_serial {
        break;
      };
      self.next_serial += 1;
      let (write, fut_ctl) = e.remove();
      let journal = self.journal.clone();
      spawn(async move {
        journal.write_with_custom_signal(write, fut_ctl).await;
      });
    }
    fut.await;
  }
}

pub(crate) struct Ctx {
  pub buckets: Buckets,
  pub device: SeekableAsyncFile,
  pub free_list: Mutex<FreeListWithChangeTracker>,
  pub journal: Mutex<SequentialisedJournal>,
  pub object_id_serial: ObjectIdSerial,
  pub stream: RwLock<Stream>,
}
