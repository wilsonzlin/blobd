use crate::device::IDevice;
use crate::journal::IJournal;
use crate::journal::merge::WriteMerger;
use async_trait::async_trait;
use bitcode::Decode;
use bitcode::Encode;
use futures::StreamExt;
use futures::stream::iter;
use off64::Off64WriteMut;
use off64::int::Off64WriteMutInt;
use off64::u32;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::mem::take;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::Instant;
use tokio::time::sleep;

static BLANK_STATE: Lazy<Vec<u8>> = Lazy::new(|| {
  let mut raw = vec![0u8; 32 + 4];
  let hash = blake3::hash(&[]);
  raw.write_at(0, hash.as_bytes());
  raw.write_u32_be_at(32, 0u32);
  raw
});

struct PendingJournal {
  // Always reserved start 32 bytes for Blake3 hash and 4 bytes for u32 length.
  buf: Vec<u8>,
  hasher: blake3::Hasher,
  entries: WriteMerger,
  signals: Vec<SignalFutureController<()>>,
  earliest_entry: Option<Instant>,
}

impl Default for PendingJournal {
  fn default() -> Self {
    Self {
      buf: vec![0u8; 32 + 4],
      hasher: blake3::Hasher::new(),
      entries: WriteMerger::new(),
      signals: vec![],
      earliest_entry: None,
    }
  }
}

struct Flusher {
  dev: Arc<dyn IDevice>,
  dev_offset: u64,
  receiver: UnboundedReceiver<PendingJournal>,
  in_flush_queue: Arc<AtomicUsize>,
}

impl Flusher {
  pub async fn start_loop(&mut self) {
    while let Some(pending) = self.receiver.recv().await {
      self.dev.write_at(self.dev_offset, &pending.buf).await;

      // Keep semantic order without slowing down to serial writes.
      iter(pending.entries.into_merged())
        .for_each_concurrent(None, async |(offset, data)| {
          self.dev.write_at(offset, &data).await;
        })
        .await;

      // We actually cannot ACK immediately after writing to journal, even though they are safely persisted to disk (even if only in journal), because when we ACK, overlays will be cleared, but they actually haven't been written to disk yet (and so cannot be read from yet).
      for signal in pending.signals {
        signal.signal(());
      }

      self.dev.write_at(self.dev_offset, &BLANK_STATE).await;

      self.in_flush_queue.fetch_sub(1, Relaxed);
    }
  }
}

struct JournalState {
  pending: Mutex<PendingJournal>,
  flush_sender: UnboundedSender<PendingJournal>,
  in_flush_queue: Arc<AtomicUsize>,
  stop_signal: AtomicBool,
}

impl JournalState {
  fn send_pending(&self, mut to_flush: PendingJournal) {
    let len = to_flush.buf.len();
    let hash = to_flush.hasher.finalize();
    to_flush.buf.write_at(0, hash.as_bytes());
    to_flush.buf.write_u32_le_at(32, u32!(len));
    self.flush_sender.send(to_flush).unwrap();
    self.in_flush_queue.fetch_add(1, Relaxed);
  }

  // This is for when the system has so low throughput that size threshold based flushing doesn't naturally happen because there aren't enough requests.
  // This would be high latency (1 ms) but it doesn't matter anyway because the system is low throughput.
  // This does not block/delay normal high throughput operations.
  async fn start_time_threshold_loop(&self) {
    let max_wait = Duration::from_millis(1);
    while !self.stop_signal.load(Relaxed) {
      sleep(max_wait).await;
      let mut pending = self.pending.lock();
      let Some(earliest_entry) = pending.earliest_entry else {
        continue;
      };
      if earliest_entry.elapsed() < max_wait {
        continue;
      };
      self.send_pending(take(&mut *pending));
    }
  }
}

pub(crate) struct Journal {
  dev: Arc<dyn IDevice>,
  dev_offset: u64,
  cap: usize,
  state: Arc<JournalState>,
}

impl Drop for Journal {
  fn drop(&mut self) {
    self.state.stop_signal.store(true, Relaxed);
  }
}

impl Journal {
  pub fn new(dev: Arc<dyn IDevice>, dev_offset: u64, cap: usize) -> Self {
    let (tx, rx) = unbounded_channel();
    let in_flush_queue = Arc::new(AtomicUsize::new(0));
    spawn({
      let dev = dev.clone();
      let in_flush_queue = in_flush_queue.clone();
      async move {
        Flusher {
          dev,
          dev_offset,
          receiver: rx,
          in_flush_queue,
        }
        .start_loop()
        .await;
      }
    });
    let state = Arc::new(JournalState {
      pending: Mutex::new(PendingJournal::default()),
      flush_sender: tx,
      in_flush_queue,
      stop_signal: AtomicBool::new(false),
    });
    spawn({
      let state = state.clone();
      async move {
        state.start_time_threshold_loop().await;
      }
    });
    Self {
      dev,
      dev_offset,
      cap,
      state,
    }
  }
}

#[derive(Decode, Encode)]
pub(crate) struct Write {
  pub offset: u64,
  pub data: Vec<u8>,
}

#[derive(Default)]
pub(crate) struct Transaction {
  writes: Vec<Write>,
  writes_raw: Vec<u8>,
}

impl Transaction {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn into_writes(self) -> Vec<Write> {
    self.writes
  }

  pub fn write(&mut self, offset: u64, data: Vec<u8>) {
    if data.is_empty() {
      return;
    }
    let write = Write { offset, data };
    self.writes_raw.extend(bitcode::encode(&write));
    self.writes.push(write);
  }
}

#[async_trait]
impl IJournal for Journal {
  async fn format_device(&self) {
    self.dev.write_at(self.dev_offset, &BLANK_STATE).await;
  }

  async fn recover(&self) {
    // TODO
  }

  fn begin_transaction(&self) -> Transaction {
    Transaction::new()
  }

  fn commit_transaction(&self, txn: Transaction) -> Option<SignalFuture<()>> {
    if txn.writes.is_empty() {
      return None;
    }
    let (signal, signal_ctl) = SignalFuture::<()>::new();
    {
      let mut pending = self.state.pending.lock();
      let would_overflow = pending.buf.len() + txn.writes_raw.len() > self.cap;
      // We should be continuously flushing because there's only one serial flusher and I/Os are faster than sleeping.
      // But if it's already flushing, there's no point in sending an unnecessarily small flush in backlog.
      // So we only enqueue when smaller than cap if flusher is idle and journal has at least minimum block device write size.
      let flusher_is_idle_and_journal_meets_min_write =
        self.state.in_flush_queue.load(Relaxed) == 0 && pending.buf.len() >= 512;
      if would_overflow || flusher_is_idle_and_journal_meets_min_write {
        self.state.send_pending(take(&mut *pending));
      }
      pending.hasher.update(&txn.writes_raw);
      pending.buf.extend(txn.writes_raw);
      for Write { offset, data } in txn.writes {
        pending.entries.insert(offset, data);
      }
      pending.signals.push(signal_ctl);
      pending.earliest_entry.get_or_insert_with(|| Instant::now());
    };
    Some(signal)
  }
}
