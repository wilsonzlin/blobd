use crate::backing_store::BoundedStore;
use crate::metrics::BlobdMetrics;
use crate::object::get_bundle_index_for_key;
use crate::object::load_bundle_from_device;
use crate::object::ObjectTupleData;
use crate::object::ObjectTupleKey;
use crate::pages::Pages;
use crate::util::ceil_pow2;
use crate::util::mod_pow2;
use crossbeam_channel::RecvTimeoutError;
use dashmap::DashMap;
use itertools::Itertools;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u64;
use serde::Deserialize;
use serde::Serialize;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tinybuf::TinyBuf;
use tokio::spawn;

const OFFSETOF_VIRTUAL_HEAD: u64 = 0;
const OFFSETOF_VIRTUAL_TAIL: u64 = OFFSETOF_VIRTUAL_HEAD + 8;

const PERSISTED_ENTRY_TAG_PADDING: u8 = 0xc1; // This byte is never used by MessagePack.
const PERSISTED_ENTRY_TAG_WRAPAROUND: u8 = 0xc0; // `nil` in MessagePack.

pub(crate) enum BundleTask {
  Upsert {
    key: ObjectTupleKey,
    data: ObjectTupleData,
    signal: SignalFutureController<()>,
  },
  Delete {
    key: ObjectTupleKey,
    signal: SignalFutureController<()>,
  },
}

// WARNING: Do not reorder variant struct fields, as rmp_serde doesn't store field names.
#[derive(Serialize, Deserialize)]
enum LogBufferPersistedEntry {
  #[serde(rename = "0")]
  Upsert {
    key: ObjectTupleKey,
    data: ObjectTupleData,
  },
  #[serde(rename = "1")]
  Delete { key: ObjectTupleKey },
}

enum LogBufferOverlayEntry {
  Deleted {},
  Exists { data: ObjectTupleData },
}

struct CompletedFlushesBacklogEntry {
  new_virtual_tail_to_write: u64,
  started: Instant,
  tasks: Vec<BundleTask>,
}

pub(crate) struct LogBuffer {
  bundle_count: u64,
  bundles_dev: BoundedStore,
  overlay: Arc<DashMap<ObjectTupleKey, LogBufferOverlayEntry>>,
  pages: Pages,
  // std::sync::mpsc::Sender is not Send.
  sender: crossbeam_channel::Sender<(BundleTask, Vec<u8>)>,
}

impl LogBuffer {
  pub async fn format_device(state_dev: &BoundedStore, pages: &Pages) {
    state_dev
      .write_at(0, pages.slow_allocate_with_zeros(pages.spage_size()))
      .await;
  }

  // TODO Commit loop.
  // TODO Load existing log entries.
  pub async fn load_from_device(
    bundles_dev: BoundedStore,
    data_dev: BoundedStore,
    state_dev: BoundedStore,
    pages: Pages,
    metrics: BlobdMetrics,
    bundle_count: u64,
  ) -> Self {
    let handle = tokio::runtime::Handle::current();

    let state_raw = state_dev.read_at(0, pages.spage_size()).await;
    let virtual_head = state_raw.read_u64_le_at(OFFSETOF_VIRTUAL_HEAD);
    let virtual_tail = state_raw.read_u64_le_at(OFFSETOF_VIRTUAL_TAIL);

    // TODO Regularly shrink_to_fit if capacity is excessively high.
    let overlay: Arc<DashMap<ObjectTupleKey, LogBufferOverlayEntry>> = Default::default();

    // This separate background future and async channel exists so that completed flushes can continue to be enqueued while this is writing the log state asynchronously.
    let (completer_send, mut completer_recv) =
      tokio::sync::mpsc::unbounded_channel::<(u64, CompletedFlushesBacklogEntry)>();
    spawn({
      let metrics = metrics.clone();
      let overlay = overlay.clone();
      let pages = pages.clone();

      let mut backlog: BTreeMap<u64, CompletedFlushesBacklogEntry> = Default::default();
      let mut next_flush_id: u64 = 0;
      async move {
        while let Some((flush_id, ent)) = completer_recv.recv().await {
          assert!(backlog.insert(flush_id, ent).is_none());
          // TODO Tune and allow configuring hyperparameter.
          while let Ok(Some((flush_id, ent))) =
            tokio::time::timeout(Duration::from_micros(10), completer_recv.recv()).await
          {
            assert!(backlog.insert(flush_id, ent).is_none());
          }

          let mut seq_ents = Vec::new();
          while backlog
            .first_key_value()
            .filter(|(id, _)| **id == next_flush_id)
            .is_some()
          {
            let (_, ent) = backlog.pop_first().unwrap();
            seq_ents.push(ent);
            next_flush_id += 1;
          }
          if !seq_ents.is_empty() {
            let mut state_buf = pages.allocate_uninitialised(pages.spage_size());
            state_buf.write_u64_le_at(OFFSETOF_VIRTUAL_HEAD, virtual_head);
            state_buf.write_u64_le_at(
              OFFSETOF_VIRTUAL_TAIL,
              seq_ents.last().unwrap().new_virtual_tail_to_write,
            );
            state_dev.write_at(0, state_buf).await;
            metrics.0.log_buffer_flush_state_count.fetch_add(1, Relaxed);
            for ent in seq_ents {
              for msg in ent.tasks {
                match msg {
                  BundleTask::Delete { key, signal } => {
                    metrics
                      .0
                      .log_buffer_delete_entry_count
                      .fetch_add(1, Relaxed);
                    overlay.insert(key, LogBufferOverlayEntry::Deleted {});
                    signal.signal(());
                  }
                  BundleTask::Upsert { key, data, signal } => {
                    metrics.0.log_buffer_write_entry_count.fetch_add(1, Relaxed);
                    metrics
                      .0
                      .log_buffer_write_entry_data_bytes
                      .fetch_add(u64!(data.len()), Relaxed);
                    overlay.insert(key, LogBufferOverlayEntry::Exists { data });
                    signal.signal(());
                  }
                };
              }
              metrics
                .0
                .log_buffer_flush_total_us
                .fetch_add(u64!(ent.started.elapsed().as_micros()), Relaxed);
            }
          };
        }
      }
    });

    let (sender, receiver) = crossbeam_channel::unbounded::<(BundleTask, Vec<u8>)>();
    thread::spawn({
      let pages = pages.clone();
      move || {
        let mut virtual_tail = virtual_tail;
        // TODO Tune and allow configuring hyperparameter.
        const MAX_BUF_LEN: u64 = 128 * 1024 * 1024;
        let mut buf = Vec::new();
        let mut pending_log_flush = Vec::new();
        let mut next_flush_id = 0;
        let mut last_flush_time = Instant::now();
        let mut disconnected = false;
        while !disconnected {
          // TODO Tune and allow configuring hyperparameter.
          match receiver.recv_timeout(Duration::from_micros(100)) {
            Ok((task, mut log_buffer_entry_serialised)) => {
              buf.append(&mut log_buffer_entry_serialised);
              pending_log_flush.push(task);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
              disconnected = true;
            }
          };
          let now = Instant::now();
          let buf_len = u64!(buf.len());
          // TODO Tune and allow configuring duration hyperparameter. This doesn't have to match the `recv_timeout` as they handle different scenarios: the `recv_timeout` determines how often to check in again and see if a flush is necessary, while this determines when a flush is necessary.
          if disconnected
            || buf_len >= MAX_BUF_LEN
            || u64!(now.duration_since(last_flush_time).as_micros()) > 10_000
          {
            // We need padding:
            // - to avoid double writing the last spage between flushes
            // - to allow parallel flushes (without padding, flushes will likely overlap and clobber each other in the last spage)
            if mod_pow2(buf_len, pages.spage_size_pow2) > 0 {
              buf.push(PERSISTED_ENTRY_TAG_PADDING);
            };
            metrics
              .0
              .log_buffer_flush_entry_count
              .fetch_add(u64!(pending_log_flush.len()), Relaxed);
            metrics
              .0
              .log_buffer_flush_data_bytes
              .fetch_add(buf_len, Relaxed);
            metrics.0.log_buffer_flush_count.fetch_add(1, Relaxed);
            if buf_len <= 1024 * 4 {
              metrics
                .0
                .log_buffer_flush_leq_4k_count
                .fetch_add(1, Relaxed);
            } else if buf_len <= 1024 * 64 {
              metrics
                .0
                .log_buffer_flush_leq_64k_count
                .fetch_add(1, Relaxed);
            } else if buf_len <= 1024 * 1024 * 1 {
              metrics
                .0
                .log_buffer_flush_leq_1m_count
                .fetch_add(1, Relaxed);
            } else if buf_len <= 1024 * 1024 * 8 {
              metrics
                .0
                .log_buffer_flush_leq_8m_count
                .fetch_add(1, Relaxed);
            };
            let mut buf_padded =
              pages.allocate_uninitialised(ceil_pow2(buf_len, pages.spage_size_pow2));
            metrics
              .0
              .log_buffer_flush_padding_bytes
              .fetch_add(u64!(buf_padded.len() - buf.len()), Relaxed);
            buf_padded[..buf.len()].copy_from_slice(&buf);
            buf.clear();

            let flush_id = next_flush_id;
            next_flush_id += 1;
            // TODO Wrapping across physical boundaries, ensuring there's enough space, inserting marker if remaining physical space is skipped for wraparound.
            let physical_offset = virtual_tail;
            virtual_tail += u64!(buf_padded.len());

            let new_virtual_tail_to_write = virtual_tail;
            let pending_log_flush = pending_log_flush.drain(..).collect_vec();
            last_flush_time = now;
            handle.spawn({
              let completer_send = completer_send.clone();
              let data_dev = data_dev.clone();
              let metrics = metrics.clone();
              async move {
                let flush_write_started = Instant::now();
                data_dev.write_at(physical_offset, buf_padded).await;
                metrics
                  .0
                  .log_buffer_flush_write_us
                  .fetch_add(u64!(flush_write_started.elapsed().as_micros()), Relaxed);
                assert!(completer_send
                  .send((flush_id, CompletedFlushesBacklogEntry {
                    new_virtual_tail_to_write,
                    started: now,
                    tasks: pending_log_flush,
                  }))
                  .is_ok());
              }
            });
          };
        }
      }
    });

    Self {
      bundle_count,
      bundles_dev,
      overlay,
      pages,
      sender,
    }
  }

  pub async fn read_tuple(&self, key_raw: TinyBuf) -> Option<ObjectTupleData> {
    let hash = blake3::hash(&key_raw);
    let bundle_idx = get_bundle_index_for_key(&hash, self.bundle_count);
    let key = ObjectTupleKey::from_raw_and_hash(key_raw, hash);
    // We're taking a bold step here and not using any in-memory bundle cache, because each read of a random spage is extremely fast on NVMe devices and (assuming good hashing and bucket load factor) we should very rarely re-read a bundle unless we re-read the same key (which we don't need to optimise for).
    // We don't even need to acquire some read lock, because even if the log commits just as we're about to read or reading, the bundle spage read should still be atomic (i.e. either state before or after our commit), and technically either state is legal and correct. For a similar reason, it's safe to just read an `self.overlay` entry.
    // WARNING: We must never return a value that has not persisted to the log or bundle yet, even if in memory, as that gives misleading confirmation of durable persistence.
    match self.overlay.get(&key) {
      Some(e) => match &*e {
        LogBufferOverlayEntry::Deleted {} => None,
        LogBufferOverlayEntry::Exists { data } => Some(data.clone()),
      },
      None => {
        // TODO OPTIMISATION: Avoid initial Vec allocation.
        load_bundle_from_device(&self.bundles_dev, &self.pages, bundle_idx)
          .await
          .into_iter()
          .find(|t| t.key == key)
          .map(|t| t.data)
      }
    }
  }

  pub async fn upsert_tuple(&self, key: TinyBuf, data: ObjectTupleData) {
    let key = ObjectTupleKey::from_raw(key);
    let (fut, signal) = SignalFuture::new();
    // Serialise outside of flush thread to parallelise.
    let mut log_entry = Vec::new();
    LogBufferPersistedEntry::Upsert {
      key: key.clone(),
      data: data.clone(),
    }
    .serialize(&mut rmp_serde::Serializer::new(&mut log_entry))
    .unwrap();
    self
      .sender
      .send((BundleTask::Upsert { key, data, signal }, log_entry))
      .unwrap();
    fut.await;
  }

  pub async fn delete_tuple(&self, key: TinyBuf) {
    let key = ObjectTupleKey::from_raw(key);
    let (fut, signal) = SignalFuture::new();
    // Serialise outside of flush thread to parallelise.
    let mut log_entry = Vec::new();
    LogBufferPersistedEntry::Delete { key: key.clone() }
      .serialize(&mut rmp_serde::Serializer::new(&mut log_entry))
      .unwrap();
    self
      .sender
      .send((BundleTask::Delete { key, signal }, log_entry))
      .unwrap();
    fut.await;
  }
}
