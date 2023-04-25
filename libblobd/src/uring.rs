use crate::bufpool::Buf;
use crate::bufpool::BufPool;
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
use tracing::info;

pub(crate) struct UringReadRequest {
  /// This is wrapped in `Arc` to ensure the file descriptor lives until the response.
  pub file: Arc<File>,
  /// Must be a multiple of 4096.
  pub offset: u64,
  /// Must be a multiple of 4096.
  pub len: u32,
}

pub(crate) struct UringReadResponse {
  pub data: Buf,
}

pub(crate) struct UringWriteRequest {
  /// This is wrapped in `Arc` to ensure the file descriptor lives until the response.
  pub file: Arc<File>,
  /// Must be a multiple of 4096.
  pub offset: u64,
  /// Must be a multiple of 4096.
  pub data: Buf,
}

pub(crate) struct UringWriteResponse {}

#[derive(Display)]
enum UringRequest {
  Read {
    req: UringReadRequest,
    res: SignalFutureController<io::Result<UringReadResponse>>,
  },
  Write {
    req: UringWriteRequest,
    res: SignalFutureController<io::Result<UringWriteResponse>>,
  },
}

#[derive(Display)]
enum Pending {
  Read {
    buf: Buf,
    res: SignalFutureController<io::Result<UringReadResponse>>,
  },
  Write {
    res: SignalFutureController<io::Result<UringWriteResponse>>,
  },
}

// For now, we just use one ring, with one submitter on one thread and one receiver on another thread. While there are possibly faster ways, like one ring per thread and using one thread for both submitting and receiving (to avoid any locking), the actual I/O should be the bottleneck so we can just stick with this for now.
pub(crate) struct Uring {
  sender: std::sync::mpsc::Sender<UringRequest>,
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
  pub fn new(bufpool: BufPool) -> Self {
    let (sender, receiver) = std::sync::mpsc::channel::<UringRequest>();
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
    let ring = Arc::new(ring);
    // Submission thread.
    thread::spawn({
      let pending = pending.clone();
      let ring = ring.clone();
      move || {
        let mut submission = unsafe { ring.submission_shared() };
        info!("submission thread started");
        let mut next_id = 0;
        for msg in receiver {
          let id = next_id;
          next_id += 1;
          info!(id, typ = msg.to_string(), "submitting");
          let entry = match msg {
            UringRequest::Read { req, res } => {
              assert_eq!(req.offset % 4096, 0);
              assert_eq!(req.len % 4096, 0);
              let buf = bufpool.allocate(usz!(req.len));
              let ptr = buf.get_raw_ptr();
              // Insert before submitting.
              pending.insert(id, Pending::Read { buf, res });
              opcode::Read::new(types::Fd(req.file.as_raw_fd()), ptr, req.len)
                .offset(req.offset)
                .build()
                .user_data(id)
            }
            UringRequest::Write { req, res } => {
              assert_eq!(req.offset % 4096, 0);
              assert_eq!(req.data.len() % 4096, 0);
              let ptr = req.data.get_raw_ptr();
              // Insert before submitting.
              pending.insert(id, Pending::Write { res });
              opcode::Write::new(types::Fd(req.file.as_raw_fd()), ptr, u32!(req.data.len()))
                .offset(req.offset)
                .build()
                .user_data(id)
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
        info!("completion thread started");
        loop {
          let Some(e) = completion.next() else {
            ring.submit_and_wait(1).unwrap();
            completion.sync();
            continue;
          };
          let id = e.user_data();
          let p = pending.remove(&id).unwrap().1;
          info!(id, typ = p.to_string(), "completing");
          match p {
            Pending::Read { mut buf, res } => {
              let val = if e.result() >= 0 {
                buf.truncate(usz!(e.result()));
                Ok(UringReadResponse { data: buf })
              } else {
                Err(io::Error::from_raw_os_error(-e.result()))
              };
              res.signal(val);
            }
            Pending::Write { res } => {
              let val = if e.result() >= 0 {
                Ok(UringWriteResponse {})
              } else {
                Err(io::Error::from_raw_os_error(-e.result()))
              };
              res.signal(val);
            }
          }
        }
      }
    });

    Self { sender }
  }

  pub async fn read(&self, req: UringReadRequest) -> io::Result<UringReadResponse> {
    let (fut, fut_ctl) = SignalFuture::new();
    self
      .sender
      .send(UringRequest::Read { req, res: fut_ctl })
      .unwrap();
    fut.await
  }

  pub async fn write(&self, req: UringWriteRequest) -> io::Result<UringWriteResponse> {
    let (fut, fut_ctl) = SignalFuture::new();
    self
      .sender
      .send(UringRequest::Write { req, res: fut_ctl })
      .unwrap();
    fut.await
  }
}
