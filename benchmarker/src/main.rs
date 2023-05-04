use blobd_universal_client::direct::Direct;
use blobd_universal_client::lite::Lite;
use blobd_universal_client::BlobdProvider;
use blobd_universal_client::CommitObjectInput;
use blobd_universal_client::CreateObjectInput;
use blobd_universal_client::DeleteObjectInput;
use blobd_universal_client::InitCfg;
use blobd_universal_client::InspectObjectInput;
use blobd_universal_client::ReadObjectInput;
use blobd_universal_client::WriteObjectInput;
use clap::Parser;
use clap::ValueEnum;
use futures::stream::iter;
use futures::StreamExt;
use off64::int::create_u64_be;
use off64::usz;
use seekable_async_file::get_file_len_via_seek;
use std::cmp::min;
use std::path::PathBuf;
use std::sync::Arc;
use tinybuf::TinyBuf;
use tokio::time::Instant;
use tracing::info;

/*

# Benchmarker

This is both similar and almost the opposite of the stochastic stress tester: this is designed to try and find the maximum possible performance, leveraging any advantage possible. When reviewing the output results, keep in mind:

- The reads may be extremely fast, because the writes caused all the mmap pages to be cached.
- The compiler may be optimising away reads because the returned data isn't being used.
- It's not a realistic workload:
  - All ops of the same type are performed at once instead of being interspersed.
  - Objects are created, written, committed, inspected, read, and deleted in the exact same order for every op.
  - The object size is identical for all objects.
  - The data being written/read is full of zeros.
- No correctness checks are done.

Despite these limitations, the benchmarker can be useful to find hotspots and slow code (when profiling), high-level op performance, and upper limit of possible performance.

*/

const EMPTY_POOL: [u8; 16_777_216] = [0u8; 16_777_216];

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
enum TargetType {
  Direct,
  Lite,
}

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Cli {
  #[arg(long)]
  target: TargetType,

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

  let device_size = get_file_len_via_seek(&cli.device).await.unwrap();

  let init_cfg = InitCfg {
    bucket_count: cli.buckets,
    device_size,
    lpage_size: cli.lpage_size,
    object_count: cli.objects,
    spage_size: cli.spage_size,
    device: cli.device,
  };

  let blobd: Arc<dyn BlobdProvider> = match cli.target {
    TargetType::Direct => Arc::new(Direct::start(init_cfg).await),
    TargetType::Lite => Arc::new(Lite::start(init_cfg).await),
  };

  let now = Instant::now();
  let incomplete_tokens = Arc::new(parking_lot::Mutex::new(Vec::new()));
  iter(0..cli.objects)
    .for_each_concurrent(Some(cli.threads), |i| {
      let blobd = blobd.clone();
      let incomplete_tokens = incomplete_tokens.clone();
      async move {
        let res = blobd
          .create_object(CreateObjectInput {
            key: create_u64_be(i).into(),
            size: cli.object_size,
            assoc_data: TinyBuf::empty(),
          })
          .await;
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
            .write_object(WriteObjectInput {
              offset,
              incomplete_token: incomplete_token.clone(),
              data: &EMPTY_POOL[..usz!(data_len)],
            })
            .await;
        }
      }
    })
    .await;
  let write_exec_secs = now.elapsed().as_secs_f64();
  info!(
    write_exec_secs,
    write_ops_per_second = cli.objects as f64 / write_exec_secs,
    write_mib_per_second =
      (cli.objects * cli.object_size) as f64 / write_exec_secs / 1024.0 / 1024.0,
    "completed all write ops",
  );

  let now = Instant::now();
  iter(incomplete_tokens.lock().as_slice())
    .for_each_concurrent(Some(cli.threads), |incomplete_token| {
      let blobd = blobd.clone();
      async move {
        blobd
          .commit_object(CommitObjectInput {
            incomplete_token: incomplete_token.clone(),
          })
          .await;
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
          .inspect_object(InspectObjectInput {
            key: create_u64_be(i).into(),
            id: None,
          })
          .await;
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
          let res = blobd
            .read_object(ReadObjectInput {
              key: create_u64_be(i).into(),
              id: None,
              start,
              end: Some(min(cli.object_size, start + cli.read_size)),
              stream_buffer_size: cli.read_stream_buffer_size,
            })
            .await;
          let page_count = res.data_stream.count().await;
          // Do something with `page_count` so that the compiler doesn't just drop it, and then possibly drop the stream too.
          assert!(page_count > 0);
        }
      }
    })
    .await;
  let read_exec_secs = now.elapsed().as_secs_f64();
  info!(
    read_exec_secs,
    read_ops_per_second = cli.objects as f64 / read_exec_secs,
    read_mib_per_second = (cli.objects * cli.object_size) as f64 / read_exec_secs / 1024.0 / 1024.0,
    "completed all read ops",
  );

  let now = Instant::now();
  iter(0..cli.objects)
    .for_each_concurrent(Some(cli.threads), |i| {
      let blobd = blobd.clone();
      async move {
        blobd
          .delete_object(DeleteObjectInput {
            key: create_u64_be(i).into(),
            id: None,
          })
          .await;
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
