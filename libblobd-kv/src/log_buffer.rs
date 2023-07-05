use crate::allocator::Allocator;
use crate::backing_store::BackingStore;
use crate::backing_store::BoundedStore;
use crate::metrics::BlobdMetrics;
use crate::object::get_bundle_index_for_key;
use crate::object::serialise_bundle;
use crate::object::BundleDeserialiser;
use crate::object::ObjectTupleData;
use crate::object::ObjectTupleKey;
use crate::object::OBJECT_TUPLE_DATA_LEN_INLINE_THRESHOLD;
use crate::op::write_object::allocate_object_on_heap;
use crate::pages::Pages;
use crate::util::ceil_pow2;
use crate::util::mod_pow2;
use crate::util::ByteConsumer;
use ahash::AHashMap;
use crossbeam_channel::RecvTimeoutError;
use dashmap::DashMap;
use futures::stream::iter;
use futures::StreamExt;
use futures::TryFutureExt;
use itertools::Itertools;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::u32;
use off64::u64;
use off64::usz;
use off64::Off64WriteMut;
use parking_lot::Mutex;
use parking_lot::RwLock;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::mem::take;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tinybuf::TinyBuf;
use tokio::runtime::Handle;
use tokio::spawn;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tokio::time::timeout;
use tracing::info;
use tracing::warn;

/*

Data layout:

{
  ({
    u8 == 1 upsert
    u8[] key_serialised
    u8[] data_serialised
  } | {
    u8 == 2 delete
    u8[] key_serialised
  } | {
    u8 == 3 padding_marker
  } | {
    u8 == 4 wraparound_marker
  })[] persisted_entries
}[] flushes

*/

const OFFSETOF_VIRTUAL_HEAD: u64 = 0;
const OFFSETOF_VIRTUAL_TAIL: u64 = OFFSETOF_VIRTUAL_HEAD + 8;

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

// We intentionally don't use structs here, instead using this enum as just the raw u8 tag, as otherwise it makes it hard to try and serialise without owning (i.e. a lot of memcpy) while deserialising with ownership (since it's almost always from raw device read buffers that will be discarded).
#[derive(Clone, Copy, PartialEq, Eq, Debug, FromPrimitive)]
#[repr(u8)]
enum LogBufferPersistedEntry {
  Upsert = 1,
  Delete,
  Padding, // When parsing, this means to skip to the next spage as the rest of the current spage is junk.
  Wraparound, // When parsing, this means to skip to the next virtual offset where the physical offset is zero as the rest of the physical log buffer is junk.
}

#[derive(Clone)]
enum LogBufferOverlayEntry {
  Deleted {},
  Upserted { data: ObjectTupleData },
}

struct CompletedFlushesBacklogEntry {
  new_virtual_tail_to_write: u64,
  started: Instant,
  tasks: Vec<BundleTask>,
}

// If we just have standard HashMap and hold a read lock, then read performance quickly becomes single-threaded; DashMap is much faster for this reason. However, we still need some way to quickly switch between maps for commits. Therefore, we use a light lock and quickly clone the inner Arc<DashMap> to try and get the best of both worlds.
// WARNING: Do not use FxHasher, as it makes the maps poorly distributed and causes high CPU usage from many key comparisons.
#[derive(Clone, Default)]
struct Overlay {
  committing: Option<Arc<DashMap<ObjectTupleKey, LogBufferOverlayEntry, ahash::RandomState>>>,
  uncommitted: Arc<DashMap<ObjectTupleKey, LogBufferOverlayEntry, ahash::RandomState>>,
}

struct LogBufferState {
  head: u64,
  tail: u64,
}

impl LogBufferState {
  pub async fn flush(&self, state_dev: &BoundedStore, pages: &Pages, metrics: &BlobdMetrics) {
    let mut state_buf = pages.allocate_uninitialised(pages.spage_size());
    state_buf.write_u64_le_at(OFFSETOF_VIRTUAL_HEAD, self.head);
    state_buf.write_u64_le_at(OFFSETOF_VIRTUAL_TAIL, self.tail);
    state_dev.write_at(0, state_buf).await;
    metrics.0.log_buffer_virtual_head.store(self.head, Relaxed);
    metrics.0.log_buffer_virtual_tail.store(self.tail, Relaxed);
    metrics.0.log_buffer_flush_state_count.fetch_add(1, Relaxed);
  }
}

pub(crate) struct LogBuffer {
  bundle_count: u64,
  bundles_dev: BoundedStore,
  commit_threshold: u64,
  currently_committing: Arc<AtomicBool>,
  data_dev: BoundedStore,
  dev: Arc<dyn BackingStore>,
  heap_allocator: Arc<Mutex<Allocator>>,
  metrics: BlobdMetrics,
  overlay: Arc<RwLock<Overlay>>,
  pages: Pages,
  state_dev: BoundedStore,
  virtual_pointers: Arc<tokio::sync::RwLock<LogBufferState>>,
  // std::sync::mpsc::Sender is not Send.
  sender: crossbeam_channel::Sender<BundleTask>,
  receiver: Mutex<Option<crossbeam_channel::Receiver<BundleTask>>>, // This will be taken and left as None after calling `start`.
}

impl LogBuffer {
  pub async fn format_device(state_dev: &BoundedStore, pages: &Pages) {
    state_dev
      .write_at(0, pages.slow_allocate_with_zeros(pages.spage_size()))
      .await;
  }

  pub async fn load_from_device(
    dev: Arc<dyn BackingStore>,
    bundles_dev: BoundedStore,
    data_dev: BoundedStore,
    state_dev: BoundedStore,
    heap_allocator: Arc<Mutex<Allocator>>,
    pages: Pages,
    metrics: BlobdMetrics,
    bundle_count: u64,
    commit_threshold: u64,
  ) -> Self {
    let currently_committing = Arc::new(AtomicBool::new(false));

    let state_raw = state_dev.read_at(0, pages.spage_size()).await;
    let init_virtual_head = state_raw.read_u64_le_at(OFFSETOF_VIRTUAL_HEAD);
    let init_virtual_tail = state_raw.read_u64_le_at(OFFSETOF_VIRTUAL_TAIL);
    assert_eq!(mod_pow2(init_virtual_head, pages.spage_size_pow2), 0);
    assert_eq!(mod_pow2(init_virtual_tail, pages.spage_size_pow2), 0);
    assert!(init_virtual_head <= init_virtual_tail);
    metrics
      .0
      .log_buffer_virtual_head
      .store(init_virtual_head, Relaxed);
    metrics
      .0
      .log_buffer_virtual_tail
      .store(init_virtual_tail, Relaxed);
    info!(
      virtual_head = init_virtual_head,
      virtual_tail = init_virtual_tail,
      "loading log buffer"
    );

    // If this is async-locked, it means someone is writing to the log buffer state.
    let virtual_pointers = Arc::new(tokio::sync::RwLock::new(LogBufferState {
      head: init_virtual_head,
      tail: init_virtual_tail,
    }));

    // TODO Regularly shrink_to_fit if capacity is excessively high.
    let overlay: Arc<RwLock<Overlay>> = {
      let overlay = Overlay::default();
      // TODO Allow configuring.
      const BUFSIZE: u64 = 1024 * 1024 * 128;
      let mut buf_vbase = init_virtual_head;
      let mut buf = Vec::new();
      let mut buf_drained: usize = 0;
      let vend = init_virtual_tail;
      loop {
        let vnext = buf_vbase + u64!(buf_drained);
        if vnext == vend {
          break;
        }
        // Use a loop as we may be near the physical end and so will need more than one read.
        loop {
          assert!(buf_drained <= buf.len());
          let avail = u64!(buf.len() - buf_drained);
          let buf_vafter = vnext + avail;
          assert_eq!(mod_pow2(buf_vafter, pages.spage_size_pow2), 0);
          assert!(buf_vafter <= vend);
          if avail >= BUFSIZE || buf_vafter == vend {
            break;
          }
          // The buffer's running dry and there's still more to read. We don't know how long each serialised entry is, so we need to have enough buffered to ensure that any deserialisation error is because of corruption/bugs and not because unexpected EOF.
          let physical_offset = buf_vafter % data_dev.len();
          let to_read = BUFSIZE
            .min(vend - buf_vafter)
            .min(data_dev.len() - physical_offset);
          let rd = data_dev.read_at(physical_offset, to_read).await;
          buf_vbase += u64!(buf_drained);
          buf.drain(..buf_drained);
          buf_drained = 0;
          buf.extend_from_slice(&rd);
        }
        // Use `buf_drained` instead of draining a few bytes from `buf` on each iteration, causing reallocation each time.
        let mut rd = ByteConsumer::new(&buf[buf_drained..]);
        let to_skip = match LogBufferPersistedEntry::from_u8(rd.consume(1)[0]).unwrap() {
          LogBufferPersistedEntry::Upsert => {
            let key = ObjectTupleKey::deserialise(&mut rd);
            let data = ObjectTupleData::deserialise(&mut rd);
            metrics.0.log_buffer_write_entry_count.fetch_add(1, Relaxed);
            metrics
              .0
              .log_buffer_write_entry_data_bytes
              .fetch_add(u64!(data.len()), Relaxed);
            overlay
              .uncommitted
              .insert(key, LogBufferOverlayEntry::Upserted { data });
            rd.consumed()
          }
          LogBufferPersistedEntry::Delete => {
            let key = ObjectTupleKey::deserialise(&mut rd);
            metrics
              .0
              .log_buffer_delete_entry_count
              .fetch_add(1, Relaxed);
            overlay
              .uncommitted
              .insert(key, LogBufferOverlayEntry::Deleted {});
            rd.consumed()
          }
          LogBufferPersistedEntry::Padding => {
            let to_skip = pages.spage_size() - mod_pow2(vnext, pages.spage_size_pow2);
            assert!(to_skip > 0 && to_skip < pages.spage_size());
            usz!(to_skip)
          }
          LogBufferPersistedEntry::Wraparound => {
            let to_skip = data_dev.len() - (vnext % data_dev.len());
            assert!(to_skip > 0 && to_skip < data_dev.len());
            usz!(to_skip)
          }
        };
        buf_drained += to_skip;
      }
      info!(entries = overlay.uncommitted.len(), "loaded log buffer");
      Arc::new(RwLock::new(overlay))
    };

    let (sender, receiver) = crossbeam_channel::unbounded::<BundleTask>();

    Self {
      bundle_count,
      bundles_dev,
      commit_threshold,
      currently_committing,
      data_dev,
      dev,
      heap_allocator,
      metrics,
      overlay,
      pages,
      receiver: Mutex::new(Some(receiver)),
      sender,
      state_dev,
      virtual_pointers,
    }
  }

  /// WARNING: This must only be called once.
  pub async fn start_background_threads(&self) {
    let handle = Handle::current();
    let init_virtual_tail = self.virtual_pointers.try_read().unwrap().tail;
    let receiver = self.receiver.lock().take().unwrap();
    let bundle_count = self.bundle_count;
    let commit_threshold = self.commit_threshold;

    // This separate background future and async channel exists so that completed flushes can continue to be enqueued while this is writing the log state asynchronously.
    let (completer_send, mut completer_recv) =
      tokio::sync::mpsc::unbounded_channel::<(u64, CompletedFlushesBacklogEntry)>();
    spawn({
      let bundles_dev = self.bundles_dev.clone();
      let currently_committing = self.currently_committing.clone();
      let dev = self.dev.clone();
      let heap_allocator = self.heap_allocator.clone();
      let metrics = self.metrics.clone();
      let overlay = self.overlay.clone();
      let pages = self.pages.clone();
      let state_dev = self.state_dev.clone();
      let virtual_pointers = self.virtual_pointers.clone();
      async move {
        let mut backlog: AHashMap<u64, CompletedFlushesBacklogEntry> = Default::default();
        let mut next_flush_id: u64 = 0;
        loop {
          // Check if we need to do a commit. We should always be able to read-lock `virtual_pointers` because the only other thread that could write-lock (other than this future loop) is the commit future, which we checked isn't running.
          if !currently_committing.load(Relaxed)
            && virtual_pointers
              .try_read()
              .map(|v| v.tail.checked_sub(v.head).unwrap() >= commit_threshold)
              .unwrap()
          {
            currently_committing.store(true, Relaxed);
            // Only we (the current spawned future) can update the overlay entries and virtual head/tail, so even though virtual head/tail are not locked as part of `overlay`, they are always in sync and this is consistent and correct.
            // NOTE: To keep the previous consistency and correctness guarantees, do this outside of the following `spawn`.
            let log_entries_to_commit = {
              let mut overlay = overlay.write();
              assert!(overlay.committing.is_none());
              // It's safe to take this and interpret it as a frozen map because the only thread that modifies this Arc-shared map is us and we are currently not writing to it.
              let entry_map = take(&mut overlay.uncommitted);
              overlay.committing = Some(entry_map.clone());
              entry_map
            };
            let commit_up_to_tail = virtual_pointers.try_read().unwrap().tail;
            info!(
              entries = log_entries_to_commit.len(),
              up_to_virtual_tail = commit_up_to_tail,
              "log buffer commit: starting"
            );
            metrics.0.log_buffer_commit_count.fetch_add(1, Relaxed);
            metrics
              .0
              .log_buffer_commit_entry_count
              .fetch_add(u64!(log_entries_to_commit.len()), Relaxed);
            spawn({
              let bundles_dev = bundles_dev.clone();
              let currently_committing = currently_committing.clone();
              let dev = dev.clone();
              let heap_allocator = heap_allocator.clone();
              let metrics = metrics.clone();
              let overlay = overlay.clone();
              let pages = pages.clone();
              let state_dev = state_dev.clone();
              let virtual_pointers = virtual_pointers.clone();
              async move {
                let grouping_started = Instant::now();
                let (heap_writes, by_bundle_idx) = spawn_blocking({
                  let heap_allocator = heap_allocator.clone();
                  let metrics = metrics.clone();
                  let pages = pages.clone();
                  move || {
                    // Lock once instead of every time we need it, which will probably be many given that almost all objects will need to be moved to the heap.
                    let mut allocator = heap_allocator.lock();
                    let mut heap_writes = Vec::new();
                    let by_bundle_idx = log_entries_to_commit
                      .iter()
                      .map(|e| {
                        let key = e.key().clone();
                        let entry = match e.value() {
                          LogBufferOverlayEntry::Upserted {
                            data: ObjectTupleData::Inline(data),
                          } if data.len() > OBJECT_TUPLE_DATA_LEN_INLINE_THRESHOLD => {
                            // We're now writing to the tuples area which has a much smaller treshold for inline data than the log buffer, so we need to rewrite the object.
                            // We do this now in a spawn_blocking so we don't block async threads; each individual entry doesn't take too much CPU time to update the allocator but it adds up when there are potentially millions of entries.
                            // TODO Handle ENOSPC.
                            let size = u32!(data.len());
                            let (dev_offset, size_on_dev) =
                              allocate_object_on_heap(&mut allocator, &pages, &metrics, size)
                                .unwrap();
                            let mut buf = pages.allocate_uninitialised(size_on_dev);
                            buf.write_at(0, data);
                            heap_writes.push((dev_offset, buf));
                            metrics
                              .0
                              .log_buffer_commit_object_heap_move_bytes
                              .fetch_add(u64!(data.len()), Relaxed);
                            metrics
                              .0
                              .log_buffer_commit_object_heap_move_count
                              .fetch_add(1, Relaxed);
                            LogBufferOverlayEntry::Upserted {
                              data: ObjectTupleData::Heap { size, dev_offset },
                            }
                          }
                          e => e.clone(),
                        };
                        (key, entry)
                      })
                      .into_group_map_by(|(key, _data)| {
                        get_bundle_index_for_key(&key.hash(), bundle_count)
                      });
                    (heap_writes, by_bundle_idx)
                  }
                })
                .await
                .unwrap();
                metrics
                  .0
                  .log_buffer_commit_object_grouping_us
                  .fetch_add(u64!(grouping_started.elapsed().as_micros()), Relaxed);
                metrics
                  .0
                  .log_buffer_commit_bundle_count
                  .fetch_add(u64!(by_bundle_idx.len()), Relaxed);
                info!(
                  bundle_count = by_bundle_idx.len(),
                  "log buffer commit: entries grouped"
                );

                // Move objects to heap first before updating bundles.
                let heap_writes_started = Instant::now();
                // TODO Opportunities for coalescing?
                iter(heap_writes)
                  .for_each_concurrent(None, |(dev_offset, buf)| {
                    let dev = dev.clone();
                    async move {
                      dev.write_at(dev_offset, buf).await;
                    }
                  })
                  .await;
                metrics
                  .0
                  .log_buffer_commit_object_heap_move_write_us
                  .fetch_add(u64!(heap_writes_started.elapsed().as_micros()), Relaxed);
                info!("log buffer commit: objects moved to heap");

                // Read then update then write tuple bundles.
                let bundles_update_started = Instant::now();
                // TODO Allow configuring concurrrency level. Do not set too high as otherwise we'll run out of system memory, because all futures will spawn.
                iter(by_bundle_idx)
                  .for_each_concurrent(Some(1_000_000), |(bundle_idx, overlay_entries)| {
                    let bundles_dev = bundles_dev.clone();
                    let dev = dev.clone();
                    let heap_allocator = heap_allocator.clone();
                    let metrics = metrics.clone();
                    let pages = pages.clone();
                    // If we don't spawn, `for_each_concurrent` essentially becomes one future that processes every single bundle and entry, and while each individual bundle doesn't take that much CPU time, in aggregate it adds up to a lot and we'd be blocking future threads and slowing down the entire system.
                    spawn(async move {
                      let read_started = Instant::now();
                      let bundle_raw = bundles_dev
                        .read_at(bundle_idx * pages.spage_size(), pages.spage_size())
                        .await;
                      metrics
                        .0
                        .log_buffer_commit_bundle_read_us
                        .fetch_add(u64!(read_started.elapsed().as_micros()), Relaxed);

                      let mut bundle =
                        BundleDeserialiser::new(bundle_raw).collect::<AHashMap<_, _>>();
                      for (key, ent) in overlay_entries {
                        let existing_object_to_delete = match ent {
                          LogBufferOverlayEntry::Deleted {} => bundle.remove(&key),
                          LogBufferOverlayEntry::Upserted { data } => {
                            // We've already ensured that data is no larger than `OBJECT_TUPLE_DATA_LEN_INLINE_THRESHOLD` if inline.
                            // We'll subtract one again when handling `existing_object_to_delete` if there's an existing object.
                            metrics.0.object_count.fetch_add(1, Relaxed);
                            bundle.insert(key, data)
                          }
                        };
                        if let Some(deleted) = existing_object_to_delete {
                          metrics.0.object_count.fetch_sub(1, Relaxed);
                          // TODO There is still a race condition here where someone is about to read this object but we release its space and some other new object gets allocated it and writes some other data, causing junk to be read.
                          if let ObjectTupleData::Heap { size, dev_offset } = deleted {
                            heap_allocator.lock().release(dev_offset, size);
                            metrics
                              .0
                              .heap_object_data_bytes
                              .fetch_sub(size.into(), Relaxed);
                          };
                        };
                      }
                      // TODO Better error/panic message on overflow.
                      let new_bundle = serialise_bundle(&pages, bundle);
                      let write_started = Instant::now();
                      dev
                        .write_at(bundle_idx * pages.spage_size(), new_bundle)
                        .await;
                      metrics
                        .0
                        .log_buffer_commit_bundle_committed_count
                        .fetch_add(1, Relaxed);
                      metrics
                        .0
                        .log_buffer_commit_bundle_write_us
                        .fetch_add(u64!(write_started.elapsed().as_micros()), Relaxed);
                    })
                    .unwrap_or_else(|_| ())
                  })
                  .await;
                metrics
                  .0
                  .log_buffer_commit_bundles_update_us
                  .fetch_add(u64!(bundles_update_started.elapsed().as_micros()), Relaxed);
                info!("log buffer commit: bundles updated");
                {
                  let mut v = virtual_pointers.write().await;
                  v.head = commit_up_to_tail;
                  v.flush(&state_dev, &pages, &metrics).await;
                };
                assert!(overlay.write().committing.take().is_some());
                currently_committing.store(false, Relaxed);
                info!("log buffer commit: completed");
              }
            });
          }

          let Some((flush_id, ent)) = completer_recv.recv().await else {
            break;
          };
          assert!(backlog.insert(flush_id, ent).is_none());
          // TODO Tune and allow configuring hyperparameter.
          while let Ok(Some((flush_id, ent))) =
            timeout(Duration::from_micros(10), completer_recv.recv()).await
          {
            assert!(backlog.insert(flush_id, ent).is_none());
          }

          let mut flushed_entries = Vec::new();
          while let Some(ent) = backlog.remove(&next_flush_id) {
            flushed_entries.push(ent);
            next_flush_id += 1;
          }
          if !flushed_entries.is_empty() {
            let new_virtual_tail = flushed_entries.last().unwrap().new_virtual_tail_to_write;
            {
              let mut v = virtual_pointers.write().await;
              v.tail = new_virtual_tail;
              v.flush(&state_dev, &pages, &metrics).await;
            };
            let overlay = overlay.read().clone();
            let metrics = metrics.clone();
            spawn_blocking(move || {
              for ent in flushed_entries {
                for msg in ent.tasks {
                  match msg {
                    BundleTask::Delete { key, signal } => {
                      metrics
                        .0
                        .log_buffer_delete_entry_count
                        .fetch_add(1, Relaxed);
                      overlay
                        .uncommitted
                        .insert(key, LogBufferOverlayEntry::Deleted {});
                      signal.signal(());
                    }
                    BundleTask::Upsert { key, data, signal } => {
                      metrics.0.log_buffer_write_entry_count.fetch_add(1, Relaxed);
                      metrics
                        .0
                        .log_buffer_write_entry_data_bytes
                        .fetch_add(u64!(data.len()), Relaxed);
                      overlay
                        .uncommitted
                        .insert(key, LogBufferOverlayEntry::Upserted { data });
                      signal.signal(());
                    }
                  };
                }
                metrics
                  .0
                  .log_buffer_flush_total_us
                  .fetch_add(u64!(ent.started.elapsed().as_micros()), Relaxed);
              }
            })
            .await
            .unwrap();
          };
        }
      }
    });

    thread::spawn({
      let data_dev = self.data_dev.clone();
      let metrics = self.metrics.clone();
      let pages = self.pages.clone();
      let virtual_pointers = self.virtual_pointers.clone();
      move || {
        let mut virtual_tail = init_virtual_tail;
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
            Ok(task) => {
              match &task {
                BundleTask::Upsert { key, data, .. } => {
                  buf.push(LogBufferPersistedEntry::Upsert as u8);
                  key.serialise(&mut buf);
                  data.serialise(&mut buf);
                }
                BundleTask::Delete { key, .. } => {
                  buf.push(LogBufferPersistedEntry::Delete as u8);
                  key.serialise(&mut buf);
                }
              };
              pending_log_flush.push(task);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
              disconnected = true;
            }
          };
          if pending_log_flush.is_empty() {
            continue;
          };
          let now = Instant::now();
          let mut buf_len = u64!(buf.len());
          // TODO Tune and allow configuring duration hyperparameter. This doesn't have to match the `recv_timeout` as they handle different scenarios: the `recv_timeout` determines how often to check in again and see if a flush is necessary, while this determines when a flush is necessary.
          if disconnected
            || buf_len >= MAX_BUF_LEN
            || u64!(now.duration_since(last_flush_time).as_micros()) > 10_000
          {
            // We need padding:
            // - to avoid double writing the last spage between flushes
            // - to allow parallel flushes (without padding, flushes will likely overlap and clobber each other in the last spage)
            if mod_pow2(buf_len, pages.spage_size_pow2) > 0 {
              buf.push(LogBufferPersistedEntry::Padding as u8);
              buf_len += 1;
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
              metrics.0.log_buffer_flush_4k_count.fetch_add(1, Relaxed);
            } else if buf_len <= 1024 * 64 {
              metrics.0.log_buffer_flush_64k_count.fetch_add(1, Relaxed);
            } else if buf_len <= 1024 * 1024 * 1 {
              metrics.0.log_buffer_flush_1m_count.fetch_add(1, Relaxed);
            } else if buf_len <= 1024 * 1024 * 8 {
              metrics.0.log_buffer_flush_8m_count.fetch_add(1, Relaxed);
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
            assert_eq!(mod_pow2(virtual_tail, pages.spage_size_pow2), 0);
            let mut physical_offset = virtual_tail % data_dev.len();
            let write_wraparound_at = if physical_offset + u64!(buf_padded.len()) > data_dev.len() {
              // We don't have enough room at the end, so wraparound.
              // TODO This is not efficient use of space if `buf_padded` is large, as some entries could probably still fit.
              warn!("log buffer wrapped");
              let write_wraparound_at = physical_offset;
              virtual_tail += data_dev.len() - physical_offset;
              physical_offset = 0;
              assert_eq!(virtual_tail % data_dev.len(), physical_offset);
              Some(write_wraparound_at)
            } else {
              None
            };
            virtual_tail += u64!(buf_padded.len());
            // Check this AFTER possible wraparound. This is always safe as `head` can only increase, so we can only have false positives, not false negatives.
            if virtual_tail - virtual_pointers.blocking_read().head >= data_dev.len() {
              // TODO Better handling.
              panic!("out of log buffer space");
            };

            let new_virtual_tail_to_write = virtual_tail;
            let pending_log_flush = pending_log_flush.drain(..).collect_vec();
            last_flush_time = now;
            handle.spawn({
              let completer_send = completer_send.clone();
              let data_dev = data_dev.clone();
              let metrics = metrics.clone();
              let pages = pages.clone();
              async move {
                let flush_write_started = Instant::now();
                if let Some(write_wraparound_at) = write_wraparound_at {
                  let mut raw = pages.allocate_uninitialised(pages.spage_size());
                  raw[0] = LogBufferPersistedEntry::Wraparound as u8;
                  data_dev.write_at(write_wraparound_at, raw).await;
                }
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
  }

  pub async fn wait_for_any_current_commit(&self) {
    while self.currently_committing.load(Relaxed) {
      sleep(Duration::from_millis(1)).await;
    }
  }

  pub async fn read_tuple(&self, key_raw: TinyBuf) -> Option<ObjectTupleData> {
    let hash = blake3::hash(&key_raw);
    let bundle_idx = get_bundle_index_for_key(hash.as_bytes(), self.bundle_count);
    let key = ObjectTupleKey::from_raw_and_hash(key_raw, hash);
    // We're taking a bold step here and not using any in-memory bundle cache, because each read of a random spage is extremely fast on NVMe devices and (assuming good hashing and bucket load factor) we should very rarely re-read a bundle unless we re-read the same key (which we don't need to optimise for).
    // We don't even need to acquire some read lock, because even if the log commits just as we're about to read or reading, the bundle spage read should still be atomic (i.e. either state before or after our commit), and technically either state is legal and correct. For a similar reason, it's safe to just read any `self.overlay` map entry; all entries represent legal persisted state.
    // WARNING: We must never return a value that has not persisted to the log or bundle yet, even if in memory, as that gives misleading confirmation of durable persistence.
    let overlay = self.overlay.read().clone();
    match overlay
      .uncommitted
      .get(&key)
      .map(|e| e.clone())
      .or_else(|| {
        overlay
          .committing
          .as_ref()
          .and_then(|m| m.get(&key).map(|e| e.clone()))
      }) {
      Some(e) => match e {
        LogBufferOverlayEntry::Deleted {} => None,
        LogBufferOverlayEntry::Upserted { data } => Some(data),
      },
      None => BundleDeserialiser::new(
        self
          .bundles_dev
          .read_at(
            bundle_idx * self.pages.spage_size(),
            self.pages.spage_size(),
          )
          .await,
      )
      .find(|(tuple_key, _data)| tuple_key == &key)
      .map(|(_, data)| data),
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
