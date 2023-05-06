use super::BackingStore;
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
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::io;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::thread;
use strum::Display;
use tokio::time::Instant;
use tracing::trace;

fn assert_result_is_ok(res: i32) -> u32 {
  if res < 0 {
    panic!("{:?}", io::Error::from_raw_os_error(-res));
  };
  u32!(res)
}

struct ReadRequest {
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

#[derive(Display)]
enum Pending {
  Read {
    buf: Buf,
    res: SignalFutureController<Buf>,
  },
  Write {
    // This takes ownership of `buf` to prevent it from being dropped while io_uring is working.
    // This will also be returned, so that it can be reused.
    buf: Buf,
    len: u32,
    res: SignalFutureController<Buf>,
  },
  Sync {
    res: SignalFutureController<()>,
  },
}

// For now, we just use one ring, with one submitter on one thread and one receiver on another thread. While there are possibly faster ways, like one ring per thread and using one thread for both submitting and receiving (to avoid any locking), the actual I/O should be the bottleneck so we can just stick with this for now.
/// This can be cheaply cloned.
#[derive(Clone)]
pub(crate) struct UringBackingStore {
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
  pub fn new(file: File, pages: Pages, cfg: UringCfg) -> Self {
    let (sender, receiver) = crossbeam_channel::unbounded::<Request>();
    let pending: Arc<DashMap<u64, (Pending, Instant), BuildHasherDefault<FxHasher>>> =
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
      let pages = pages.clone();
      let pending = pending.clone();
      let ring = ring.clone();
      // This is outside the loop to avoid reallocation each time.
      let mut msgbuf = VecDeque::new();
      move || {
        let mut submission = unsafe { ring.submission_shared() };
        let mut next_id = 0;
        loop {
          // Process multiple messages at once to avoid too many io_uring submits.
          msgbuf.push_back(receiver.recv().unwrap());
          while let Ok(msg) = receiver.try_recv() {
            msgbuf.push_back(msg);
          }
          for msg in msgbuf.drain(..) {
            let id = next_id;
            next_id += 1;
            trace!(id, typ = msg.to_string(), "submitting request");
            let (pending_entry, submission_entry) = match msg {
              Request::Read { req, res } => {
                let buf = pages.allocate_uninitialised(u64!(req.len));
                // Using `as_mut_ptr` would require a mutable borrow.
                let ptr = buf.as_ptr() as *mut u8;
                // This takes ownership of `buf` which will ensure it doesn't get dropped while waiting for io_uring.
                (
                  Pending::Read { buf, res },
                  opcode::Read::new(types::Fixed(0), ptr, req.len)
                    .offset(req.offset)
                    .build()
                    .user_data(id),
                )
              }
              Request::Write { req, res } => {
                // Using `as_mut_ptr` would require a mutable borrow.
                let ptr = req.data.as_ptr() as *mut u8;
                // This takes ownership of `buf` which will ensure it doesn't get dropped while waiting for io_uring.
                let len = u32!(req.data.len());
                (
                  Pending::Write {
                    res,
                    len,
                    buf: req.data,
                  },
                  opcode::Write::new(types::Fixed(0), ptr, len)
                    .offset(req.offset)
                    .build()
                    .user_data(id),
                )
              }
              Request::Sync { res } => (
                Pending::Sync { res },
                opcode::Fsync::new(types::Fixed(0)).build().user_data(id),
              ),
            };
            // Insert before submitting.
            pending.insert(id, (pending_entry, Instant::now()));
            unsafe {
              submission.push(&submission_entry).unwrap();
            };
          }
          submission.sync();
          // This is still necessary even with sqpoll, as our kernel thread may have gone to sleep.
          ring.submit().unwrap();
        }
      }
    });

    // Completion thread.
    thread::spawn({
      let pending = pending.clone();
      let ring = ring.clone();
      move || {
        let mut completion = unsafe { ring.completion_shared() };
        loop {
          let Some(e) = completion.next() else {
            ring.submit_and_wait(1).unwrap();
            completion.sync();
            continue;
          };
          let id = e.user_data();
          let (p, started) = pending.remove(&id).unwrap().1;
          trace!(
            id,
            typ = p.to_string(),
            exec_us = started.elapsed().as_micros(),
            "completing request"
          );
          let rv = assert_result_is_ok(e.result());
          match p {
            Pending::Read { buf, res } => {
              // We may have read fewer bytes.
              assert_eq!(usz!(rv), buf.len());
              res.signal(buf);
            }
            Pending::Write { res, len, buf } => {
              // Assert that all requested bytes to write were written.
              assert_eq!(rv, len);
              res.signal(buf);
            }
            Pending::Sync { res } => {
              res.signal(());
            }
          }
        }
      }
    });

    Self { sender }
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
    let (fut, fut_ctl) = SignalFuture::new();
    self
      .sender
      .send(Request::Read {
        req: ReadRequest {
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
    assert!(offset + u64!(data.len()) <= self.len);
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
