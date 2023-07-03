use super::CommitObjectInput;
use super::CommitObjectOutput;
use super::CreateObjectInput;
use super::CreateObjectOutput;
use super::DeleteObjectInput;
use super::InitCfg;
use super::InspectObjectInput;
use super::InspectObjectOutput;
use super::ReadObjectInput;
use super::ReadObjectOutput;
use super::WriteObjectInput;
use crate::BlobdProvider;
use async_trait::async_trait;
use futures::stream::once;
use futures::StreamExt;
use libblobd_lite::incomplete_token::IncompleteToken;
use libblobd_lite::op::commit_object::OpCommitObjectInput;
use libblobd_lite::op::create_object::OpCreateObjectInput;
use libblobd_lite::op::delete_object::OpDeleteObjectInput;
use libblobd_lite::op::inspect_object::OpInspectObjectInput;
use libblobd_lite::op::read_object::OpReadObjectInput;
use libblobd_lite::op::write_object::OpWriteObjectInput;
use libblobd_lite::Blobd;
use libblobd_lite::BlobdCfg;
use libblobd_lite::BlobdLoader;
use off64::u64;
use off64::u8;
use seekable_async_file::SeekableAsyncFile;
use seekable_async_file::SeekableAsyncFileMetrics;
use std::sync::Arc;
use std::time::Duration;
use tinybuf::TinyBuf;
use tokio::join;
use tokio::spawn;
use tracing::info;

pub struct Lite {
  blobd: Blobd,
}

impl Lite {
  pub async fn start(cfg: InitCfg) -> Self {
    assert!(cfg.bucket_count.is_power_of_two());
    let bucket_count_log2: u8 = cfg.bucket_count.ilog2().try_into().unwrap();

    assert_eq!(cfg.partitions.len(), 1);
    let device_cfg = &cfg.partitions[0];
    assert_eq!(device_cfg.offset, 0);

    let io_metrics = Arc::new(SeekableAsyncFileMetrics::default());
    let device = SeekableAsyncFile::open(
      &device_cfg.path,
      device_cfg.len,
      io_metrics,
      Duration::from_micros(200),
      0,
    )
    .await;

    let blobd = BlobdLoader::new(device.clone(), device_cfg.len, BlobdCfg {
      bucket_count_log2,
      bucket_lock_count_log2: bucket_count_log2,
      reap_objects_after_secs: 60 * 60 * 24 * 7,
      lpage_size_pow2: u8!(cfg.lpage_size.ilog2()),
      spage_size_pow2: u8!(cfg.spage_size.ilog2()),
      versioning: false,
    });
    if !cfg.do_not_format_device {
      blobd.format().await;
      info!("formatted device");
    };
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

    Self { blobd }
  }
}

#[async_trait]
impl BlobdProvider for Lite {
  #[rustfmt::skip]
  fn metrics(&self) -> Vec<(&'static str, u64)> {
    let metrics = self.blobd.metrics();
    vec![
      ("allocated_block_count", metrics.allocated_block_count()),
      ("allocated_page_count", metrics.allocated_page_count()),
      ("deleted_object_count", metrics.deleted_object_count()),
      ("incomplete_object_count", metrics.incomplete_object_count()),
      ("object_count", metrics.object_count()),
      ("object_data_bytes", metrics.object_data_bytes()),
      ("object_metadata_bytes", metrics.object_metadata_bytes()),
      ("used_bytes", metrics.used_bytes()),
    ]
  }

  async fn create_object(&self, input: CreateObjectInput) -> CreateObjectOutput {
    let res = self
      .blobd
      .create_object(OpCreateObjectInput {
        assoc_data: TinyBuf::empty(),
        key: input.key,
        size: input.size,
      })
      .await
      .unwrap();
    CreateObjectOutput {
      token: Arc::new(res.token),
    }
  }

  async fn write_object<'a>(&'a self, input: WriteObjectInput<'a>) {
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

  async fn commit_object(&self, input: CommitObjectInput) -> CommitObjectOutput {
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
    CommitObjectOutput {
      object_id: res.object_id,
    }
  }

  async fn inspect_object(&self, input: InspectObjectInput) -> InspectObjectOutput {
    let res = self
      .blobd
      .inspect_object(OpInspectObjectInput {
        id: input.id,
        key: input.key,
      })
      .await
      .unwrap();
    InspectObjectOutput {
      id: res.id,
      size: res.size,
    }
  }

  async fn read_object(&self, input: ReadObjectInput) -> ReadObjectOutput {
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
    ReadObjectOutput {
      data_stream: res.data_stream.map(|chunk| chunk.unwrap().to_vec()).boxed(),
    }
  }

  async fn delete_object(&self, input: DeleteObjectInput) {
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
