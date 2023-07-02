use super::BackingStore;
use crate::metrics::BlobdMetrics;
use crate::pages::Pages;
use async_trait::async_trait;
use bufpool::buf::Buf;
use dashmap::DashMap;
use io_uring::cqueue::Entry as CEntry;
use io_uring::opcode;
use io_uring::squeue::Entry as SEntry;
use io_uring::types;
use io_uring::IoUring;
use off64::u32;
use off64::u64;
use off64::usz;
use rustc_hash::FxHasher;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::VecDeque;
use std::fmt;
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::io;
use std::os::fd::AsRawFd;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::thread;
use strum::Display;
use tokio::time::Instant;
use tracing::trace;

fn assert_result_is_ok(req: &Request, res: i32) -> u32 {
  if res < 0 {
    panic!(
      "{:?} failed with {:?}",
      req,
      io::Error::from_raw_os_error(-res)
    );
  };
  u32!(res)
}

struct ReadRequest {
  out_buf: Buf,
  offset: u64,
  len: u32,
}

struct WriteRequest {
  offset: u64,
  data: Buf,
}

#[derive(Display)]
enum Request {
  Read {
    req: ReadRequest,
    res: SignalFutureController<Buf>,
  },
  Write {
    req: WriteRequest,
    res: SignalFutureController<Buf>,
  },
  Sync {
    res: SignalFutureController<()>,
  },
}

impl fmt::Debug for Request {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Read { req, .. } => write!(f, "read {} len {}", req.offset, req.len),
      Self::Write { req, .. } => write!(f, "write {} len {}", req.offset, req.data.len()),
      Self::Sync { .. } => write!(f, "sync"),
    }
  }
}

// For now, we just use one ring, with one submitter on one thread and one receiver on another thread. While there are possibly faster ways, like one ring per thread and using one thread for both submitting and receiving (to avoid any locking), the actual I/O should be the bottleneck so we can just stick with this for now.
/// This can be cheaply cloned.
#[derive(Clone)]
pub(crate) struct UringBackingStore {
  pages: Pages,
  // We don't use std::sync::mpsc::Sender as it is not Sync, so it's really complicated to use from any async function.
  sender: crossbeam_channel::Sender<Request>,
}

/// For advanced users only. Some of these may cause EINVAL, or worsen performance.
#[derive(Clone, Default, Debug)]
pub(crate) struct UringCfg {
  pub coop_taskrun: bool,
  pub defer_taskrun: bool,
  pub iopoll: bool,
  /// This requires CAP_SYS_NICE.
  pub sqpoll: Option<u32>,
}

impl UringBackingStore {
  /// `offset` must be a multiple of the underlying device's sector size.
  pub fn new(file: File, pages: Pages, metrics: BlobdMetrics, cfg: UringCfg) -> Self {
    let (sender, receiver) = crossbeam_channel::unbounded::<Request>();
    let pending: Arc<DashMap<u64, (Request, Instant), BuildHasherDefault<FxHasher>>> =
      Default::default();
    let ring = {
      let mut builder = IoUring::<SEntry, CEntry>::builder();
      builder.setup_clamp();
      if cfg.coop_taskrun {
        builder.setup_coop_taskrun();
      };
      if cfg.defer_taskrun {
        builder.setup_defer_taskrun();
      };
      if cfg.iopoll {
        builder.setup_iopoll();
      }
      if let Some(sqpoll) = cfg.sqpoll {
        builder.setup_sqpoll(sqpoll);
      };
      builder.build(134217728).unwrap()
    };
    ring
      .submitter()
      .register_files(&[file.as_raw_fd()])
      .unwrap();
    let ring = Arc::new(ring);
    // Submission thread.
    thread::spawn({
      let metrics = metrics.clone();
      let pending = pending.clone();
      let ring = ring.clone();
      // This is outside the loop to avoid reallocation each time.
      let mut msgbuf = VecDeque::new();
      move || {
        let mut submission = unsafe { ring.submission_shared() };
        let mut next_id = 0;
        // If this loop exits, it means we've dropped the `UringBackingStore` and can safely stop.
        while let Ok(init_msg) = receiver.recv() {
          // Process multiple messages at once to avoid too many io_uring submits.
          msgbuf.push_back(init_msg);
          while let Ok(msg) = receiver.try_recv() {
            msgbuf.push_back(msg);
          }
          // How the io_uring submission queue work:
          // - The buffer is shared between the kernel and userspace.
          // - There are atomic head and tail indices that allow them to be shared mutably between kernel and userspace safely.
          // - The Rust library we're using abstracts over this by caching the head and tail as local values. Once we've made our inserts, we update the atomic tail and then tell the kernel to consume some of the queue. When we update the atomic tail, we also check the atomic head and update our local cached value; some entries may have been consumed by the kernel in some other thread since we last checked and we may actually have more free space than we thought.
          while let Some(msg) = msgbuf.pop_front() {
            let id = next_id;
            next_id += 1;
            trace!(id, typ = msg.to_string(), "submitting request");
            let submission_entry = match &msg {
              Request::Read { req, .. } => {
                metrics.0.uring_read_request_count.fetch_add(1, Relaxed);
                // Using `as_mut_ptr` would require a mutable borrow.
                let ptr = req.out_buf.as_ptr() as *mut u8;
                opcode::Read::new(types::Fixed(0), ptr, req.len)
                  .offset(req.offset)
                  .build()
                  .user_data(id)
              }
              Request::Write { req, .. } => {
                metrics.0.uring_write_request_count.fetch_add(1, Relaxed);
                // Using `as_mut_ptr` would require a mutable borrow.
                let ptr = req.data.as_ptr() as *mut u8;
                // This takes ownership of `buf` which will ensure it doesn't get dropped while waiting for io_uring.
                let len = u32!(req.data.len());
                opcode::Write::new(types::Fixed(0), ptr, len)
                  .offset(req.offset)
                  .build()
                  .user_data(id)
              }
              Request::Sync { .. } => {
                metrics.0.uring_sync_request_count.fetch_add(1, Relaxed);
                opcode::Fsync::new(types::Fixed(0)).build().user_data(id)
              }
            };
            // Insert before submitting.
            pending.insert(id, (msg, Instant::now()));
            if submission.is_full() {
              submission.sync();
              metrics.0.uring_submission_count.fetch_add(1, Relaxed);
              ring.submit_and_wait(1).unwrap();
            }
            unsafe {
              // This call only has one error: queue is full. It should never happen because we just checked that it's not.
              submission.push(&submission_entry).unwrap();
            };
          }
          submission.sync();
          metrics.0.uring_submission_count.fetch_add(1, Relaxed);
          // This is still necessary even with sqpoll, as our kernel thread may have gone to sleep.
          ring.submit().unwrap();
        }
      }
    });

    // Completion thread.
    thread::spawn({
      let metrics = metrics.clone();
      let pending = pending.clone();
      let ring = ring.clone();
      move || {
        let mut completion = unsafe { ring.completion_shared() };
        // TODO Stop this loop if `UringBackingStore` has been dropped.
        loop {
          let Some(e) = completion.next() else {
            metrics.0.uring_submission_count.fetch_add(1, Relaxed);
            ring.submit_and_wait(1).unwrap();
            completion.sync();
            continue;
          };
          let id = e.user_data();
          let (req, started) = pending.remove(&id).unwrap().1;
          let exec_us = u64!(started.elapsed().as_micros());
          trace!(id, typ = req.to_string(), exec_us, "completing request");
          let rv = assert_result_is_ok(&req, e.result());
          match req {
            Request::Read { req, res } => {
              // We may have read fewer bytes.
              assert_eq!(usz!(rv), req.out_buf.len());
              metrics
                .0
                .uring_read_request_bytes
                .fetch_add(u64!(rv), Relaxed);
              metrics.0.uring_read_request_us.fetch_add(exec_us, Relaxed);
              res.signal(req.out_buf);
            }
            Request::Write { req, res } => {
              // Assert that all requested bytes to write were written.
              assert_eq!(rv, u32!(req.data.len()));
              metrics
                .0
                .uring_write_request_bytes
                .fetch_add(u64!(rv), Relaxed);
              metrics.0.uring_write_request_us.fetch_add(exec_us, Relaxed);
              res.signal(req.data);
            }
            Request::Sync { res } => {
              metrics.0.uring_sync_request_us.fetch_add(exec_us, Relaxed);
              res.signal(());
            }
          }
        }
      }
    });

    Self { pages, sender }
  }
}

// Sources:
// - Example: https://github1s.com/tokio-rs/io-uring/blob/HEAD/examples/tcp_echo.rs
// - liburing docs: https://unixism.net/loti/ref-liburing/completion.html
// - Quick high-level overview: https://man.archlinux.org/man/io_uring.7.en
// - io_uring walkthrough: https://unixism.net/2020/04/io-uring-by-example-part-1-introduction/
// - Multithreading:
//   - https://github.com/axboe/liburing/issues/109#issuecomment-1114213402
//   - https://github.com/axboe/liburing/issues/109#issuecomment-1166378978
//   - https://github.com/axboe/liburing/issues/109#issuecomment-614911522
//   - https://github.com/axboe/liburing/issues/125
//   - https://github.com/axboe/liburing/issues/127
//   - https://github.com/axboe/liburing/issues/129
//   - https://github.com/axboe/liburing/issues/571#issuecomment-1106480309
// - Kernel poller: https://unixism.net/loti/tutorial/sq_poll.html
#[async_trait]
impl BackingStore for UringBackingStore {
  /// `offset` and `len` must be multiples of the underlying device's sector size.
  async fn read_at(&self, offset: u64, len: u64) -> Buf {
    let out_buf = self.pages.allocate_uninitialised(len);
    let (fut, fut_ctl) = SignalFuture::new();
    self
      .sender
      .send(Request::Read {
        req: ReadRequest {
          out_buf,
          offset,
          len: u32!(len),
        },
        res: fut_ctl,
      })
      .unwrap();
    fut.await
  }

  /// `offset` and `data.len()` must be multiples of the underlying device's sector size.
  /// Returns the original `data` so that it can be reused, if desired.
  async fn write_at(&self, offset: u64, data: Buf) -> Buf {
    let (fut, fut_ctl) = SignalFuture::new();
    self
      .sender
      .send(Request::Write {
        req: WriteRequest { offset, data },
        res: fut_ctl,
      })
      .unwrap();
    fut.await
  }

  /// Even when using direct I/O, `fsync` is still necessary, as it ensures the device itself has flushed any internal caches.
  async fn sync(&self) {
    let (fut, fut_ctl) = SignalFuture::new();
    self.sender.send(Request::Sync { res: fut_ctl }).unwrap();
    fut.await
  }
}
