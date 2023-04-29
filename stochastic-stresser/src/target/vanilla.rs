use super::InitCfg;
use super::Target;
use super::TargetCommitObjectInput;
use super::TargetCommitObjectOutput;
use super::TargetCreateObjectInput;
use super::TargetCreateObjectOutput;
use super::TargetDeleteObjectInput;
use super::TargetInspectObjectInput;
use super::TargetInspectObjectOutput;
use super::TargetReadObjectInput;
use super::TargetReadObjectOutput;
use super::TargetWriteObjectInput;
use async_trait::async_trait;
use futures::stream::once;
use futures::StreamExt;
use libblobd::incomplete_token::IncompleteToken;
use libblobd::op::commit_object::OpCommitObjectInput;
use libblobd::op::create_object::OpCreateObjectInput;
use libblobd::op::delete_object::OpDeleteObjectInput;
use libblobd::op::inspect_object::OpInspectObjectInput;
use libblobd::op::read_object::OpReadObjectInput;
use libblobd::op::write_object::OpWriteObjectInput;
use libblobd::Blobd;
use libblobd::BlobdCfg;
use libblobd::BlobdLoader;
use off64::u64;
use off64::u8;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::SeekableAsyncFileMetrics;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::join;
use tokio::spawn;
use tokio::time::sleep;
use tracing::info;

pub struct Vanilla {
  blobd: Blobd,
}

impl Vanilla {
  pub async fn start(cfg: InitCfg, completed: Arc<AtomicU64>) -> Self {
    assert!(cfg.bucket_count.is_power_of_two());
    let bucket_count_log2: u8 = cfg.bucket_count.ilog2().try_into().unwrap();

    let io_metrics = Arc::new(SeekableAsyncFileMetrics::default());
    let device = SeekableAsyncFile::open(
      &cfg.device,
      cfg.device_size,
      io_metrics,
      Duration::from_micros(200),
      0,
    )
    .await;

    let blobd = BlobdLoader::new(device.clone(), cfg.device_size, BlobdCfg {
      bucket_count_log2,
      bucket_lock_count_log2: bucket_count_log2,
      reap_objects_after_secs: 60 * 60 * 24 * 7,
      lpage_size_pow2: u8!(cfg.lpage_size.ilog2()),
      spage_size_pow2: u8!(cfg.spage_size.ilog2()),
      // We must enable versioning as some objects will have duplicate keys, and then their derived tasks won't work unless they were the last to commit.
      versioning: true,
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

    // Background loop to regularly prinit out metrics and progress.
    spawn({
      let blobd = blobd.clone();
      let completed = completed.clone();
      async move {
        loop {
          sleep(Duration::from_secs(5)).await;
          let completed = completed.load(Ordering::Relaxed);
          info!(
            completed,
            allocated_block_count = blobd.metrics().allocated_block_count(),
            allocated_page_count = blobd.metrics().allocated_page_count(),
            deleted_object_count = blobd.metrics().deleted_object_count(),
            incomplete_object_count = blobd.metrics().incomplete_object_count(),
            object_count = blobd.metrics().object_count(),
            object_data_bytes = blobd.metrics().object_data_bytes(),
            object_metadata_bytes = blobd.metrics().object_metadata_bytes(),
            used_bytes = blobd.metrics().used_bytes(),
            "progress",
          );
          if completed == cfg.object_count {
            break;
          };
        }
      }
    });

    Self { blobd }
  }
}

#[async_trait]
impl Target for Vanilla {
  async fn create_object(&self, input: TargetCreateObjectInput) -> TargetCreateObjectOutput {
    let res = self
      .blobd
      .create_object(OpCreateObjectInput {
        assoc_data: input.assoc_data,
        key: input.key,
        size: input.size,
      })
      .await
      .unwrap();
    TargetCreateObjectOutput {
      token: Arc::new(res.token),
    }
  }

  async fn write_object<'a>(&'a self, input: TargetWriteObjectInput<'a>) {
    self
      .blobd
      .write_object(OpWriteObjectInput {
        data_len: u64!(input.data.len()),
        data_stream: once(async { Ok(input.data) }).boxed(),
        incomplete_token: *input
          .incomplete_token
          .downcast::<IncompleteToken>()
          .unwrap(),
        offset: input.offset,
      })
      .await
      .unwrap();
  }

  async fn commit_object(&self, input: TargetCommitObjectInput) -> TargetCommitObjectOutput {
    let res = self
      .blobd
      .commit_object(OpCommitObjectInput {
        incomplete_token: *input
          .incomplete_token
          .downcast::<IncompleteToken>()
          .unwrap(),
      })
      .await
      .unwrap();
    TargetCommitObjectOutput {
      object_id: res.object_id,
    }
  }

  async fn inspect_object(&self, input: TargetInspectObjectInput) -> TargetInspectObjectOutput {
    let res = self
      .blobd
      .inspect_object(OpInspectObjectInput {
        id: input.id,
        key: input.key,
      })
      .await
      .unwrap();
    TargetInspectObjectOutput {
      id: res.id,
      size: res.size,
    }
  }

  async fn read_object(&self, input: TargetReadObjectInput) -> TargetReadObjectOutput {
    let res = self
      .blobd
      .read_object(OpReadObjectInput {
        end: input.end,
        id: input.id,
        key: input.key,
        start: input.start,
        stream_buffer_size: input.stream_buffer_size,
      })
      .await
      .unwrap();
    TargetReadObjectOutput {
      data_stream: res
        .data_stream
        .map(|chunk| {
          let boxed: Box<dyn AsRef<[u8]>> = Box::new(chunk.unwrap());
          boxed
        })
        .boxed(),
    }
  }

  async fn delete_object(&self, input: TargetDeleteObjectInput) {
    self
      .blobd
      .delete_object(OpDeleteObjectInput {
        id: input.id,
        key: input.key,
      })
      .await
      .unwrap();
  }
}
