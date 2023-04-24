use clap::Parser;
use futures::stream::iter;
use futures::stream::once;
use futures::StreamExt;
use libblobd::op::commit_object::OpCommitObjectInput;
use libblobd::op::create_object::OpCreateObjectInput;
use libblobd::op::delete_object::OpDeleteObjectInput;
use libblobd::op::inspect_object::OpInspectObjectInput;
use libblobd::op::read_object::OpReadObjectInput;
use libblobd::op::write_object::OpWriteObjectInput;
use libblobd::BlobdCfg;
use libblobd::BlobdLoader;
use off64::int::create_u64_be;
use off64::u8;
use off64::usz;
use seekable_async_file::get_file_len_via_seek;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::SeekableAsyncFileMetrics;
use std::cmp::min;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tinybuf::TinyBuf;
use tokio::join;
use tokio::spawn;
use tokio::time::Instant;
use tracing::info;

const EMPTY_POOL: [u8; 16_777_216] = [0u8; 16_777_216];

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
  /// Path to the device or regular file to use as the underlying storage.
  #[arg(long)]
  device: PathBuf,

  // How many buckets to allocate. Defaults to 131,072.
  #[arg(long, default_value_t = 131_072)]
  buckets: u64,

  /// Objects to create. Defaults to 100,000.
  #[arg(long, default_value_t = 100_000)]
  objects: u64,

  /// Object size. Defaults to 150 MiB.
  #[arg(long, default_value_t = 1024 * 1024 * 150)]
  object_size: u64,

  /// Read size. Defaults to 4 MiB.
  #[arg(long, default_value_t = 1024 * 1024 * 4)]
  read_size: u64,

  /// Read stream buffer size. Defaults to 16 KiB.
  #[arg(long, default_value_t = 1024 * 16)]
  read_stream_buffer_size: u64,

  /// Concurrency level. Defaults to 64.
  #[arg(long, default_value_t = 64)]
  threads: usize,

  /// Lpage size. Defaults to 16 MiB.
  #[arg(long, default_value_t = 1024 * 1024 * 16)]
  lpage_size: u64,

  /// Spage size. Defaults to 512 bytes.
  #[arg(long, default_value_t = 512)]
  spage_size: u64,
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt::init();

  let cli = Cli::parse();

  assert!(cli.buckets.is_power_of_two());
  let bucket_count_log2: u8 = cli.buckets.ilog2().try_into().unwrap();

  let device_size = get_file_len_via_seek(&cli.device).await.unwrap();

  let io_metrics = Arc::new(SeekableAsyncFileMetrics::default());
  let device = SeekableAsyncFile::open(
    &cli.device,
    device_size,
    io_metrics,
    Duration::from_micros(200),
    0,
  )
  .await;

  let blobd = BlobdLoader::new(device.clone(), device_size, BlobdCfg {
    bucket_count_log2,
    bucket_lock_count_log2: bucket_count_log2,
    reap_objects_after_secs: 60 * 60 * 24 * 7,
    lpage_size_pow2: u8!(cli.lpage_size.ilog2()),
    spage_size_pow2: u8!(cli.spage_size.ilog2()),
    versioning: false,
  });
  blobd.format().await;
  info!("formatted device");
  let blobd = blobd.load().await;
  info!("loaded device");

  spawn({
    let blobd = blobd.clone();
    let device = device.clone();
    async move {
      join! {
        blobd.start(),
        device.start_delayed_data_sync_background_loop(),
      };
    }
  });

  let now = Instant::now();
  let incomplete_tokens = Arc::new(parking_lot::Mutex::new(Vec::new()));
  iter(0..cli.objects)
    .for_each_concurrent(Some(cli.threads), |i| {
      let blobd = blobd.clone();
      let incomplete_tokens = incomplete_tokens.clone();
      async move {
        let res = blobd
          .create_object(OpCreateObjectInput {
            key: create_u64_be(i).into(),
            size: cli.object_size,
            assoc_data: TinyBuf::empty(),
          })
          .await
          .unwrap();
        incomplete_tokens.lock().push(res.token);
      }
    })
    .await;
  let create_exec_secs = now.elapsed().as_secs_f64();
  info!(
    create_exec_secs,
    create_ops_per_second = (cli.objects as f64) / create_exec_secs,
    "completed all create ops",
  );

  let now = Instant::now();
  iter(incomplete_tokens.lock().as_slice())
    .for_each_concurrent(Some(cli.threads), |incomplete_token| {
      let blobd = blobd.clone();
      async move {
        for offset in (0..cli.object_size).step_by(usz!(cli.lpage_size)) {
          let data_len = min(cli.object_size - offset, cli.lpage_size);
          blobd
            .write_object(OpWriteObjectInput {
              offset,
              incomplete_token: incomplete_token.clone(),
              data_len,
              data_stream: once(async { Ok(TinyBuf::from_static(&EMPTY_POOL[..usz!(data_len)])) })
                .boxed(),
            })
            .await
            .unwrap();
        }
      }
    })
    .await;
  let write_exec_secs = now.elapsed().as_secs_f64();
  info!(
    write_exec_secs,
    write_ops_per_second = (cli.objects as f64) / write_exec_secs,
    "completed all write ops",
  );

  let now = Instant::now();
  iter(incomplete_tokens.lock().as_slice())
    .for_each_concurrent(Some(cli.threads), |incomplete_token| {
      let blobd = blobd.clone();
      async move {
        blobd
          .commit_object(OpCommitObjectInput {
            incomplete_token: incomplete_token.clone(),
          })
          .await
          .unwrap();
      }
    })
    .await;
  let commit_exec_secs = now.elapsed().as_secs_f64();
  info!(
    commit_exec_secs,
    commit_ops_per_second = (cli.objects as f64) / commit_exec_secs,
    "completed all commit ops",
  );

  let now = Instant::now();
  iter(0..cli.objects)
    .for_each_concurrent(Some(cli.threads), |i| {
      let blobd = blobd.clone();
      async move {
        blobd
          .inspect_object(OpInspectObjectInput {
            key: create_u64_be(i).into(),
            id: None,
          })
          .await
          .unwrap();
      }
    })
    .await;
  let inspect_exec_secs = now.elapsed().as_secs_f64();
  info!(
    inspect_exec_secs,
    inspect_ops_per_second = (cli.objects as f64) / inspect_exec_secs,
    "completed all inspect ops",
  );

  let now = Instant::now();
  iter(0..cli.objects)
    .for_each_concurrent(Some(cli.threads), |i| {
      let blobd = blobd.clone();
      async move {
        for start in (0..cli.object_size).step_by(usz!(cli.read_size)) {
          blobd
            .read_object(OpReadObjectInput {
              key: create_u64_be(i).into(),
              id: None,
              start,
              end: Some(min(cli.object_size, start + cli.read_size)),
              stream_buffer_size: cli.read_stream_buffer_size,
            })
            .await
            .unwrap();
        }
      }
    })
    .await;
  let read_exec_secs = now.elapsed().as_secs_f64();
  info!(
    read_exec_secs,
    read_ops_per_second = (cli.objects as f64) / read_exec_secs,
    "completed all read ops",
  );

  let now = Instant::now();
  iter(0..cli.objects)
    .for_each_concurrent(Some(cli.threads), |i| {
      let blobd = blobd.clone();
      async move {
        blobd
          .delete_object(OpDeleteObjectInput {
            key: create_u64_be(i).into(),
            id: None,
          })
          .await
          .unwrap();
      }
    })
    .await;
  let delete_exec_secs = now.elapsed().as_secs_f64();
  info!(
    delete_exec_secs,
    delete_ops_per_second = (cli.objects as f64) / delete_exec_secs,
    "completed all delete ops",
  );

  info!("all done");
}
