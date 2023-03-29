use crate::bucket::Buckets;
use crate::free_list::FreeList;
use crate::object_id::ObjectIdSerial;
use crate::stream::Stream;
use dashmap::DashMap;
use rustc_hash::FxHasher;
use seekable_async_file::SeekableAsyncFile;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::hash::BuildHasherDefault;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time::sleep;
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

#[derive(Clone)]
pub(crate) struct SequentialisedJournal {
  journal: Arc<WriteJournal>,
  pending:
    Arc<DashMap<u64, (AtomicWriteGroup, SignalFutureController), BuildHasherDefault<FxHasher>>>,
}

impl SequentialisedJournal {
  pub fn new(journal: Arc<WriteJournal>) -> Self {
    Self {
      journal,
      pending: Default::default(),
    }
  }

  pub async fn write(&self, serial: ChangeSerial, write: AtomicWriteGroup) {
    let (fut, fut_ctl) = SignalFuture::new();
    let None = self.pending.insert(serial.num, (write, fut_ctl)) else {
      unreachable!();
    };
    fut.await;
  }

  // We must use a background loop. If we try to commit pending and await future inside `write`, we'll deadlock because the caller holds lock on `journal` (it needs one because `pending` would not be wrapped with Mutex) but `write` will wait forever for some previous serial entry to be written.
  pub async fn start_commit_background_loop(&self) {
    let mut next_serial = 0;
    loop {
      sleep(Duration::from_micros(200)).await;

      let mut writes = Vec::new();
      while let Some(e) = self.pending.remove(&next_serial) {
        next_serial += 1;
        writes.push(e.1);
      }
      if !writes.is_empty() {
        // This await is just to acquire the journal internal queue lock, not actually wait for a full journal commit.
        self.journal.write_many_with_custom_signal(writes).await;
      };
    }
  }
}

pub(crate) struct Ctx {
  pub buckets: Buckets,
  pub device: SeekableAsyncFile,
  pub free_list: Mutex<FreeListWithChangeTracker>,
  pub journal: SequentialisedJournal,
  pub object_id_serial: ObjectIdSerial,
  pub stream: RwLock<Stream>,
}
