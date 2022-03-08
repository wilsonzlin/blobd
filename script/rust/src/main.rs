use clap::Parser;
use futures::future;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

pub fn read_u64(v: &[u8]) -> u64 {
  let (l, _) = v.split_at(8);
  u64::from_be_bytes(l.try_into().expect("failed to read u64"))
}

#[derive(Debug, FromPrimitive)]
pub enum TurbostoreError {
  NotEnoughArgs = 1,
  KeyTooLong = 2,
  TooManyArgs = 3,
  NotFound = 4,
  InvalidStart = 5,
  InvalidEnd = 6,
}

impl TurbostoreError {
  pub fn from_primitive(v: u8) -> TurbostoreError {
    FromPrimitive::from_u8(v).expect("unknown error")
  }
}

impl fmt::Display for TurbostoreError {
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    fmt::Debug::fmt(self, f)
  }
}

impl Error for TurbostoreError {}

fn args_commit_object(key: &[u8], obj_no: u64) -> Vec<u8> {
  let raw_len = 1 + key.len() + 8;
  let mut payload = Vec::<u8>::with_capacity(1 + 1 + raw_len);
  payload.push(5);
  payload.push(raw_len as u8);
  payload.push(key.len() as u8);
  payload.extend_from_slice(key);
  payload.extend_from_slice(&obj_no.to_be_bytes());
  return payload;
}

fn args_create_object(key: &[u8], size: u64) -> Vec<u8> {
  let raw_len = 1 + key.len() + 8;
  let mut payload = Vec::<u8>::with_capacity(1 + 1 + raw_len);
  payload.push(1);
  payload.push(raw_len as u8);
  payload.push(key.len() as u8);
  payload.extend_from_slice(key);
  payload.extend_from_slice(&size.to_be_bytes());
  return payload;
}

fn args_read_object(key: &[u8], start: u64, end: u64) -> Vec<u8> {
  let raw_len = 1 + key.len() + 8 + 8;
  let mut payload = Vec::<u8>::with_capacity(1 + 1 + raw_len);
  payload.push(1);
  payload.push(raw_len as u8);
  payload.push(key.len() as u8);
  payload.extend_from_slice(key);
  payload.extend_from_slice(&start.to_be_bytes());
  payload.extend_from_slice(&end.to_be_bytes());
  return payload;
}

fn args_write_object(key: &[u8], obj_no: u64, start: u64) -> Vec<u8> {
  let raw_len = 1 + key.len() + 8 + 8;
  let mut payload = Vec::<u8>::with_capacity(1 + 1 + raw_len);
  payload.push(4);
  payload.push(raw_len as u8);
  payload.push(key.len() as u8);
  payload.extend_from_slice(key);
  payload.extend_from_slice(&obj_no.to_be_bytes());
  payload.extend_from_slice(&start.to_be_bytes());
  return payload;
}

#[derive(Parser, Debug)]
#[clap()]
struct Cli {
  #[clap(long)]
  concurrency: usize,

  #[clap(long)]
  upload: bool,

  #[clap(long)]
  read: bool,

  #[clap(long)]
  count: usize,

  #[clap(long)]
  size: usize,
}

async fn keep_writing(dest: &mut UnixStream, data: &[u8]) -> Result<(), Box<dyn Error>> {
  dest.write_all(data).await?;
  Ok(())
}

async fn keep_reading(src: &mut UnixStream, len: usize) -> Result<Vec<u8>, Box<dyn Error>> {
  let mut out = vec![0; len];
  src.read_exact(&mut out).await?;
  Ok(out)
}

struct WorkerState {
  next: AtomicUsize,
  total: usize,
  data: Vec<u8>,
}

async fn upload_worker(worker_state: Arc<WorkerState>) -> Result<(), Box<dyn Error>> {
  let mut conn_wrk = UnixStream::connect("/tmp/turbostore.sock").await?;
  let mut conn_mgr = UnixStream::connect("/tmp/turbostore-manager.sock").await?;

  loop {
    let no = worker_state.next.fetch_add(1, Ordering::Relaxed);
    if no >= worker_state.total {
      break;
    };
    let data_len = worker_state.data.len() as u64;
    let key = format!("/random/data/{}", no);
    let key = key.as_bytes();

    // create_object.
    keep_writing(&mut conn_mgr, &args_create_object(key, data_len)).await?;
    let create_res_raw = keep_reading(&mut conn_mgr, 9).await?;
    if create_res_raw[0] != 0 {
      return Err(Box::new(TurbostoreError::from_primitive(create_res_raw[0])));
    };
    let obj_no = read_u64(&create_res_raw[1..]);

    // write_object.
    keep_writing(&mut conn_wrk, &args_write_object(key, obj_no, 0)).await?;
    keep_writing(&mut conn_wrk, &worker_state.data).await?;
    let write_res_raw = keep_reading(&mut conn_wrk, 1).await?;
    if write_res_raw[0] != 0 {
      return Err(Box::new(TurbostoreError::from_primitive(write_res_raw[0])));
    };

    // commit_object.
    keep_writing(&mut conn_mgr, &args_commit_object(key, obj_no)).await?;
    let commit_res_raw = keep_reading(&mut conn_mgr, 1).await?;
    if commit_res_raw[0] != 0 {
      return Err(Box::new(TurbostoreError::from_primitive(commit_res_raw[0])));
    };
  };

  Ok(())
}

async fn read_worker(worker_state: Arc<WorkerState>) -> Result<(), Box<dyn Error>> {
  let mut conn_wrk = UnixStream::connect("/tmp/turbostore.sock").await?;

  loop {
    let no = worker_state.next.fetch_add(1, Ordering::Relaxed);
    if no >= worker_state.total {
      break;
    };
    let key = format!("/random/data/{}", no);
    let key = key.as_bytes();

    // read_object.
    keep_writing(&mut conn_wrk, &args_read_object(key, 0, 0)).await?;
    let read_res_raw = keep_reading(&mut conn_wrk, 17).await?;
    if read_res_raw[0] != 0 {
      return Err(Box::new(TurbostoreError::from_primitive(read_res_raw[0])));
    };
    let actual_start = read_u64(&read_res_raw[1..]);
    let actual_len = read_u64(&read_res_raw[9..]);
    let read_data = keep_reading(&mut conn_wrk, actual_len as usize).await?;
    if actual_start != 0 || read_data != worker_state.data {
      panic!("Invalid data");
    };
  };

  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  let args = Cli::parse();

  let mut data = "DEADBEEF".repeat((args.size / 8) + (args.size % 8));
  data.truncate(args.size);

  let worker_state = Arc::new(WorkerState {
    next: AtomicUsize::new(0),
    data: data.into_bytes(),
    total: args.count,
  });
  let started = if args.read {
    if args.upload {
      panic!("Invalid mode argument");
    };
    let mut workers = vec![];
    for _ in 0..args.concurrency {
      let worker_state = worker_state.clone();
      workers.push(
        tokio::spawn(async move {
          upload_worker(worker_state).await.expect("failed");
        })
      );
    };
    let started = Instant::now();
    future::try_join_all(workers).await?;
    started
  } else {
    if !args.upload {
      panic!("Invalid mode argument");
    };
    let mut workers = vec![];
    for _ in 0..args.concurrency {
      let worker_state = worker_state.clone();
      workers.push(
        tokio::spawn(async move {
          read_worker(worker_state).await.expect("failed");
        })
      );
    };
    let started = Instant::now();
    future::try_join_all(workers).await?;
    started
  };

  let elapsed = started.elapsed().as_secs_f64();
  println!("Effective time: {} seconds", elapsed);
  println!("Effective processing rate: {} per second", args.count as f64 / elapsed);
  println!("Effective bandwidth: {} MiB/s", (args.size * args.count) as f64 / 1024.0 / 1024.0 / elapsed);

  Ok(())
}
