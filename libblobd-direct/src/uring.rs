use bufpool::buf::Buf;
use bufpool::BUFPOOL;
use dashmap::DashMap;
use io_uring::cqueue::Entry as CEntry;
use io_uring::opcode;
use io_uring::squeue::Entry as SEntry;
use io_uring::types;
use io_uring::IoUring;
use off64::u32;
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
  /// Must be a multiple of 4096.
  offset: u64,
  /// Must be a multiple of 4096.
  len: u32,
}

struct WriteRequest {
  /// Must be a multiple of 4096.
  offset: u64,
  /// Must be a multiple of 4096.
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
pub(crate) struct Uring {
  // We don't use std::sync::mpsc::Sender as it is not Sync, so it's really complicated to use from any async function.
  sender: crossbeam_channel::Sender<Request>,
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
impl Uring {
  pub fn new(file: File) -> Self {
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
              assert_eq!(req.offset % 4096, 0);
              assert_eq!(req.len % 4096, 0);
              let buf = BUFPOOL.allocate(usz!(req.len));
              // Using `as_mut_ptr` would require a mutable borrow.
              let ptr = buf.as_ptr() as *mut u8;
              // Insert before submitting. This takes ownership of `buf` which will ensure it doesn't get dropped while waiting for io_uring.
              pending.insert(id, Pending::Read { buf, res });
              opcode::Read::new(types::Fixed(0), ptr, req.len)
                .offset(req.offset)
                .build()
                .user_data(id)
            }
            Request::Write { req, res } => {
              assert_eq!(req.offset % 4096, 0);
              assert_eq!(req.data.len() % 4096, 0);
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
                .offset(req.offset)
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
            Pending::Read { mut buf, res } => {
              if e.result() < 0 {
                panic!("{:?}", io::Error::from_raw_os_error(-e.result()));
              };
              unsafe { buf.set_len(usz!(e.result())) };
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

    Self { sender }
  }

  pub async fn read(&self, offset: u64, len: u64) -> Buf {
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

  /// Returns the original `data` so that it can be reused, if desired.
  pub async fn write(&self, offset: u64, data: Buf) -> Buf {
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
  pub async fn sync(&self) {
    let (fut, fut_ctl) = SignalFuture::new();
    self.sender.send(Request::Sync { res: fut_ctl }).unwrap();
    fut.await
  }
}
