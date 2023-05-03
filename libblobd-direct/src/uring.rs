use crate::journal::Transaction;
use crate::pages::Pages;
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
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::io;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::thread;
use strum::Display;

struct ReadRequest {
  abs_offset: u64,
  len: u32,
}

struct WriteRequest {
  abs_offset: u64,
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
pub(crate) struct UringBounded {
  // We don't use std::sync::mpsc::Sender as it is not Sync, so it's really complicated to use from any async function.
  sender: crossbeam_channel::Sender<Request>,
  offset: u64,
  len: u64,
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
impl UringBounded {
  /// `offset` must be a multiple of the underlying device's sector size.
  pub fn new(file: File, offset: u64, len: u64, pages: Pages) -> Self {
    let (sender, receiver) = crossbeam_channel::unbounded::<Request>();
    let pending: Arc<DashMap<u64, Pending, BuildHasherDefault<FxHasher>>> = Default::default();
    let ring = IoUring::<SEntry, CEntry>::builder()
      // TODO These cause EINVAL, investigate further.
      // .setup_coop_taskrun()
      // .setup_defer_taskrun()
      // .setup_iopoll()
      .setup_clamp()
      .setup_single_issuer()
      .setup_sqpoll(3000) // NOTE: This requires CAP_SYS_NICE.
      .build(134217728)
      .unwrap();
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
      move || {
        let mut submission = unsafe { ring.submission_shared() };
        let mut next_id = 0;
        for msg in receiver {
          let id = next_id;
          next_id += 1;
          let entry = match msg {
            Request::Read { req, res } => {
              let buf = pages.allocate_with_zeros(u64!(req.len));
              // Using `as_mut_ptr` would require a mutable borrow.
              let ptr = buf.as_ptr() as *mut u8;
              // Insert before submitting. This takes ownership of `buf` which will ensure it doesn't get dropped while waiting for io_uring.
              pending.insert(id, Pending::Read { buf, res });
              opcode::Read::new(types::Fixed(0), ptr, req.len)
                .offset(req.abs_offset)
                .build()
                .user_data(id)
            }
            Request::Write { req, res } => {
              // Using `as_mut_ptr` would require a mutable borrow.
              let ptr = req.data.as_ptr() as *mut u8;
              // Insert before submitting. This takes ownership of `buf` which will ensure it doesn't get dropped while waiting for io_uring.
              let len = u32!(req.data.len());
              pending.insert(id, Pending::Write {
                res,
                len,
                buf: req.data,
              });
              opcode::Write::new(types::Fixed(0), ptr, len)
                .offset(req.abs_offset)
                .build()
                .user_data(id)
            }
            Request::Sync { res } => {
              // Insert before submitting.
              pending.insert(id, Pending::Sync { res });
              opcode::Fsync::new(types::Fixed(0)).build().user_data(id)
            }
          };
          unsafe {
            submission.push(&entry).unwrap();
          };
          submission.sync();
          // In case our kernel thread has gone to sleep.
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
          let p = pending.remove(&id).unwrap().1;
          match p {
            Pending::Read { buf, res } => {
              if e.result() < 0 {
                panic!("{:?}", io::Error::from_raw_os_error(-e.result()));
              };
              // We may have read fewer bytes.
              assert_eq!(usz!(e.result()), buf.len());
              res.signal(buf);
            }
            Pending::Write { res, len, buf } => {
              if e.result() < 0 {
                panic!("{:?}", io::Error::from_raw_os_error(-e.result()));
              };
              // Assert that all requested bytes to write were written.
              assert_eq!(len, u32!(e.result()));
              res.signal(buf);
            }
            Pending::Sync { res } => {
              if e.result() < 0 {
                panic!("{:?}", io::Error::from_raw_os_error(-e.result()));
              };
              res.signal(());
            }
          }
        }
      }
    });

    Self {
      sender,
      offset,
      len,
    }
  }

  /// Length of the underlying file range. Note that this may not be the same as the actual file length.
  pub fn len(&self) -> u64 {
    self.len
  }

  /// `offset` must be a multiple of the underlying device's sector size.
  pub fn bounded(&self, offset: u64, len: u64) -> UringBounded {
    Self {
      sender: self.sender.clone(),
      offset: self.offset + offset,
      len,
    }
  }

  fn assert_in_bounds(&self, offset: u64, len: u64) {
    assert!(
      offset + len <= self.len,
      "attempted to read/write at {} with length {}, but device only has length {}",
      self.offset + offset,
      len,
      self.offset + self.len
    );
  }

  /// `offset` and `len` must be multiples of the underlying device's sector size.
  pub async fn read(&self, offset: u64, len: u64) -> Buf {
    self.assert_in_bounds(offset, len);
    let (fut, fut_ctl) = SignalFuture::new();
    self
      .sender
      .send(Request::Read {
        req: ReadRequest {
          abs_offset: self.offset + offset,
          len: u32!(len),
        },
        res: fut_ctl,
      })
      .unwrap();
    fut.await
  }

  /// `offset` and `data.len()` must be multiples of the underlying device's sector size.
  /// Returns the original `data` so that it can be reused, if desired.
  pub async fn write(&self, offset: u64, data: Buf) -> Buf {
    self.assert_in_bounds(offset, u64!(data.len()));
    let (fut, fut_ctl) = SignalFuture::new();
    self
      .sender
      .send(Request::Write {
        req: WriteRequest {
          abs_offset: self.offset + offset,
          data,
        },
        res: fut_ctl,
      })
      .unwrap();
    fut.await
  }

  pub fn record_in_transaction(&self, txn: &mut Transaction, offset: u64, data: Buf) {
    self.assert_in_bounds(offset, u64!(data.len()));
    txn.record(self.offset + offset, data);
  }

  /// Even when using direct I/O, `fsync` is still necessary, as it ensures the device itself has flushed any internal caches.
  pub async fn sync(&self) {
    let (fut, fut_ctl) = SignalFuture::new();
    self.sender.send(Request::Sync { res: fut_ctl }).unwrap();
    fut.await
  }
}
