use crate::backing_store::BoundedStore;
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
use off64::u64;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tinybuf::TinyBuf;

const OFFSETOF_VIRTUAL_HEAD: u64 = 0;
const OFFSETOF_VIRTUAL_TAIL: u64 = OFFSETOF_VIRTUAL_HEAD + 8;

const PERSISTED_ENTRY_TAG_PADDING: u8 = 0xc1; // This byte is never used by MessagePack.

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

#[derive(Serialize, Deserialize)]
enum LogBufferPersistedEntry {
  Upsert {
    key: ObjectTupleKey,
    data: ObjectTupleData,
  },
  Delete {
    key: ObjectTupleKey,
  },
}

enum LogBufferOverlayEntry {
  Deleted {},
  Exists { data: ObjectTupleData },
}

#[derive(Default)]
struct CompletedFlushes {
  backlog: BTreeMap<u64, Vec<BundleTask>>,
  next_id: u64,
}

pub(crate) struct LogBuffer {
  bundle_count: u64,
  bundles_dev: BoundedStore,
  overlay: Arc<DashMap<ObjectTupleKey, LogBufferOverlayEntry>>,
  pages: Pages,
  // std::sync::mpsc::Sender is not Send.
  sender: crossbeam_channel::Sender<BundleTask>,
}

impl LogBuffer {
  // TODO Commit loop.
  // TODO Load existing log entries.
  pub async fn load_from_device(
    bundles_dev: BoundedStore,
    data_dev: BoundedStore,
    state_dev: BoundedStore,
    pages: Pages,
    bundle_count: u64,
  ) -> Self {
    let handle = tokio::runtime::Handle::current();

    let state_raw = state_dev.read_at(0, pages.spage_size()).await;
    let virtual_head = state_raw.read_u64_le_at(OFFSETOF_VIRTUAL_HEAD);
    let virtual_tail = state_raw.read_u64_le_at(OFFSETOF_VIRTUAL_TAIL);

    // TODO Regularly shrink_to_fit if capacity is excessively high.
    let overlay: Arc<DashMap<ObjectTupleKey, LogBufferOverlayEntry>> = Default::default();

    let (sender, receiver) = crossbeam_channel::unbounded::<BundleTask>();
    std::thread::spawn({
      let overlay = overlay.clone();
      let pages = pages.clone();
      move || {
        let mut virtual_tail = virtual_tail;
        // TODO Tune and allow configuring hyperparameter.
        const MAX_BUF_LEN: u64 = 128 * 1024 * 1024;
        let mut buf = Vec::new();
        let mut pending_log_flush = Vec::new();
        let mut next_flush_id = 0;
        let completed_flushes: Arc<Mutex<CompletedFlushes>> = Default::default();
        let mut last_flush_time = Instant::now();
        loop {
          // TODO Tune and allow configuring hyperparameter.
          match receiver.recv_timeout(std::time::Duration::from_micros(10)) {
            Ok(msg) => {
              match &msg {
                BundleTask::Upsert { key, data, .. } => {
                  LogBufferPersistedEntry::Upsert {
                    key: key.clone(),
                    data: data.clone(),
                  }
                  .serialize(&mut rmp_serde::Serializer::new(&mut buf))
                  .unwrap();
                }
                BundleTask::Delete { key, .. } => {
                  LogBufferPersistedEntry::Delete { key: key.clone() }
                    .serialize(&mut rmp_serde::Serializer::new(&mut buf))
                    .unwrap();
                }
              };
              pending_log_flush.push(msg);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
          };
          let now = Instant::now();
          let buf_len = u64!(buf.len());
          // TODO Tune and allow configuring duration hyperparameter. This doesn't have to match the `recv_timeout` as they handle different scenarios: the `recv_timeout` determines how often to check in again and see if a flush is necessary, while this determines when a flush is necessary.
          if buf_len >= MAX_BUF_LEN || u64!(now.duration_since(last_flush_time).as_micros()) > 100 {
            // We need padding:
            // - to avoid double writing the last spage between flushes
            // - to allow parallel flushes (without padding, flushes will likely overlap and clobber each other in the last spage)
            if mod_pow2(buf_len, pages.spage_size_pow2) > 0 {
              buf.push(PERSISTED_ENTRY_TAG_PADDING);
            };
            let mut buf_padded =
              pages.allocate_uninitialised(ceil_pow2(buf_len, pages.spage_size_pow2));
            buf_padded[..buf.len()].copy_from_slice(&buf);
            buf.clear();
            let flush_id = next_flush_id;
            next_flush_id += 1;
            // TODO Wrapping across physical boundaries.
            let physical_offset = virtual_tail;
            virtual_tail += u64!(buf_padded.len());
            let pending_log_flush = pending_log_flush.drain(..).collect_vec();
            last_flush_time = now;
            handle.spawn({
              let completed_flushes = completed_flushes.clone();
              let heap_dev = data_dev.clone();
              let overlay = overlay.clone();
              async move {
                heap_dev.write_at(physical_offset, buf_padded).await;
                {
                  let mut completed_flushes = completed_flushes.lock();
                  // TODO Persist new tail.
                  assert!(completed_flushes
                    .backlog
                    .insert(flush_id, pending_log_flush)
                    .is_none());
                  while completed_flushes
                    .backlog
                    .first_key_value()
                    .filter(|(id, _)| **id == completed_flushes.next_id)
                    .is_some()
                  {
                    let (_, msgs) = completed_flushes.backlog.pop_first().unwrap();
                    for msg in msgs {
                      match msg {
                        BundleTask::Delete { key, signal } => {
                          overlay.insert(key, LogBufferOverlayEntry::Deleted {});
                          signal.signal(());
                        }
                        BundleTask::Upsert { key, data, signal } => {
                          overlay.insert(key, LogBufferOverlayEntry::Exists { data });
                          signal.signal(());
                        }
                      };
                    }
                    completed_flushes.next_id += 1;
                  }
                };
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
    self
      .sender
      .send(BundleTask::Upsert { key, data, signal })
      .unwrap();
    fut.await;
  }

  pub async fn delete_tuple(&self, key: TinyBuf) {
    let key = ObjectTupleKey::from_raw(key);
    let (fut, signal) = SignalFuture::new();
    self
      .sender
      .send(BundleTask::Delete { key, signal })
      .unwrap();
    fut.await;
  }
}
