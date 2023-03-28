use clap::Parser;
use futures::stream::once;
use futures::StreamExt;
use futures::TryStreamExt;
use indicatif::MultiProgress;
use indicatif::ProgressBar;
use libblobd::op::commit_object::OpCommitObjectInput;
use libblobd::op::create_object::OpCreateObjectInput;
use libblobd::op::read_object::OpReadObjectInput;
use libblobd::op::write_object::OpWriteObjectInput;
use libblobd::tile::TILE_SIZE_U64;
use libblobd::BlobdLoader;
use rand::thread_rng;
use rand::Rng;
use rand::RngCore;
use seekable_async_file::get_file_len_via_seek;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::SeekableAsyncFileMetrics;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use stochastic_queue::stochastic_channel;
use tokio::join;
use tokio::spawn;
use tokio::task::spawn_blocking;
use twox_hash::xxh3::hash64_with_seed;

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

  /// Maximum size of a created object. Defaults to 150 MiB.
  #[arg(long, default_value_t = 1024 * 1024 * 150)]
  maximum_object_size: u64,

  /// Concurrency level. Defaults to 64.
  #[arg(long, default_value_t = 64)]
  threads: u64,

  /// Size of random bytes pool. Defaults to 1 GiB.
  #[arg(long, default_value_t = 1024 * 1024 * 1024)]
  pool_size: u64,
}

#[derive(Clone)]
struct Pool {
  data: Arc<Vec<u8>>,
}

impl Pool {
  fn new(size: u64) -> Self {
    let mut data = vec![0u8; usize::try_from(size).unwrap()];
    thread_rng().fill_bytes(&mut data);
    Self {
      data: Arc::new(data),
    }
  }

  fn get(&self, offset: u64, len: u64) -> Vec<u8> {
    let start: usize = offset.try_into().unwrap();
    let end: usize = (offset + len).try_into().unwrap();
    self.data[start..end].to_vec()
  }
}

enum Task {
  Create {
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
  },
  Write {
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
    object_id: u64,
    inode_dev_offset: u64,
    chunk_offset: u64,
  },
  Commit {
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
    object_id: u64,
    inode_dev_offset: u64,
  },
  Read {
    key_len: u64,
    key_offset: u64,
    data_len: u64,
    data_offset: u64,
    chunk_offset: u64,
  },
}

#[tokio::main]
async fn main() {
  let cli = Cli::parse();

  assert!(cli.buckets.is_power_of_two());
  let bkt_cnt_log2: u8 = cli.buckets.ilog2().try_into().unwrap();

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

  let blobd = BlobdLoader::new(device.clone(), device_size, bkt_cnt_log2);
  blobd.format().await;
  let blobd = blobd.load().await;

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

  let pool = Pool::new(cli.pool_size);
  let key_len_seed = thread_rng().next_u64();
  let key_offset_seed = thread_rng().next_u64();
  let data_len_seed = thread_rng().next_u64();
  let data_offset_seed = thread_rng().next_u64();

  let mp = MultiProgress::new();
  let pb_create = mp.add(ProgressBar::new(cli.objects));
  let pb_write = mp.add(ProgressBar::new(cli.objects));
  let pb_commit = mp.add(ProgressBar::new(cli.objects));
  let pb_read = mp.add(ProgressBar::new(cli.objects));

  let (tasks_sender, tasks_receiver) = stochastic_channel::<Task>();
  {
    let tasks_sender = tasks_sender.clone();
    spawn_blocking(move || {
      for i in 0..cli.objects {
        let key_len = (hash64_with_seed(&i.to_be_bytes(), key_len_seed) % 65535) + 1;
        let key_offset =
          hash64_with_seed(&i.to_be_bytes(), key_offset_seed) % (cli.pool_size - key_len);
        let data_len = hash64_with_seed(&i.to_be_bytes(), data_len_seed) % cli.maximum_object_size;
        let data_offset =
          hash64_with_seed(&i.to_be_bytes(), data_offset_seed) % (cli.pool_size - data_len);
        tasks_sender
          .send(Task::Create {
            key_len,
            key_offset,
            data_len,
            data_offset,
          })
          .unwrap();
      }
    });
  };
  let mut threads = Vec::new();
  for _ in 0..cli.threads {
    let blobd = blobd.clone();
    let pool = pool.clone();
    let tasks_sender = tasks_sender.clone();
    let tasks_receiver = tasks_receiver.clone();
    let pb_create = pb_create.clone();
    let pb_write = pb_write.clone();
    let pb_commit = pb_commit.clone();
    let pb_read = pb_read.clone();
    threads.push(spawn(async move {
      for t in tasks_receiver {
        match t {
          Task::Create {
            key_len,
            key_offset,
            data_len,
            data_offset,
          } => {
            let res = blobd
              .create_object(OpCreateObjectInput {
                key: pool.get(key_offset, key_len),
                size: data_len,
              })
              .await
              .unwrap();
            pb_create.inc(1);
            tasks_sender
              .send(Task::Write {
                key_len,
                key_offset,
                data_len,
                data_offset,
                object_id: res.object_id,
                inode_dev_offset: res.inode_dev_offset,
                chunk_offset: 0,
              })
              .unwrap();
          }
          Task::Write {
            key_len,
            key_offset,
            data_len,
            data_offset,
            chunk_offset,
            object_id,
            inode_dev_offset,
          } => {
            let new_chunk_offset = chunk_offset + TILE_SIZE_U64;
            blobd
              .write_object(OpWriteObjectInput {
                data_len,
                data_stream: once(async {
                  Ok(pool.get(
                    data_offset + chunk_offset,
                    if new_chunk_offset < data_len {
                      TILE_SIZE_U64
                    } else {
                      data_len - chunk_offset
                    },
                  ))
                })
                .boxed(),
                inode_dev_offset,
                object_id,
                offset: chunk_offset,
              })
              .await
              .unwrap();
            tasks_sender
              .send(if new_chunk_offset < data_len {
                Task::Write {
                  key_len,
                  key_offset,
                  data_len,
                  data_offset,
                  object_id,
                  inode_dev_offset,
                  chunk_offset: new_chunk_offset,
                }
              } else {
                pb_write.inc(1);
                Task::Commit {
                  key_len,
                  key_offset,
                  data_len,
                  data_offset,
                  object_id,
                  inode_dev_offset,
                }
              })
              .unwrap();
          }
          Task::Commit {
            key_len,
            key_offset,
            data_len,
            data_offset,
            object_id,
            inode_dev_offset,
          } => {
            blobd
              .commit_object(OpCommitObjectInput {
                inode_dev_offset,
                object_id,
                key: pool.get(key_offset, key_len),
              })
              .await
              .unwrap();
            pb_commit.inc(1);
            tasks_sender
              .send(Task::Read {
                key_len,
                key_offset,
                data_len,
                data_offset,
                chunk_offset: 0,
              })
              .unwrap();
          }
          Task::Read {
            key_len,
            key_offset,
            data_len,
            data_offset,
            mut chunk_offset,
          } => {
            // Read a random amount to test various cases stochastically.
            let end = thread_rng().gen_range(chunk_offset..data_len);
            let mut res = blobd
              .read_object(OpReadObjectInput {
                end: Some(end),
                start: Some(chunk_offset),
                key: pool.get(key_offset, key_len),
                stream_buffer_size: 1024 * 16,
              })
              .await
              .unwrap();
            while let Some(chunk) = res.data_stream.try_next().await.unwrap() {
              let chunk_len: u64 = chunk.len().try_into().unwrap();
              assert_eq!(chunk, pool.get(data_offset + chunk_offset, chunk_len));
              chunk_offset += chunk_len;
              assert!(chunk_offset <= end);
            }
            assert_eq!(chunk_offset, end);
            if end < data_len {
              tasks_sender
                .send(Task::Read {
                  key_len,
                  key_offset,
                  data_len,
                  data_offset,
                  chunk_offset,
                })
                .unwrap();
            } else {
              pb_read.inc(1);
            };
          }
        };
      }
    }));
  }
  drop(tasks_sender);
  drop(tasks_receiver);
  for t in threads {
    t.await.unwrap();
  }
}
