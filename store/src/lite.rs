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
use crate::Store;
use async_trait::async_trait;
use futures::stream::once;
use futures::StreamExt;
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
use std::sync::Arc;
use tracing::info;

pub struct BlobdLiteIncompleteToken {
  pub object_id: u128,
}

pub struct BlobdLiteStore {
  blobd: Blobd,
}

impl BlobdLiteStore {
  pub async fn start(cfg: InitCfg) -> Self {
    assert!(cfg.bucket_count.is_power_of_two());
    let bucket_count_log2: u8 = cfg.bucket_count.ilog2().try_into().unwrap();

    assert_eq!(cfg.partitions.len(), 1);
    let device_cfg = &cfg.partitions[0];
    assert_eq!(device_cfg.offset, 0);

    let blobd = BlobdLoader::open(&device_cfg.path, device_cfg.len, BlobdCfg {
      bucket_count_log2,
      reap_incomplete_objects_after_secs: 60 * 60 * 24 * 7,
      lpage_size_pow2: u8!(cfg.lpage_size.ilog2()),
      spage_size_pow2: u8!(cfg.spage_size.ilog2()),
      versioning: false,
    })
    .await;
    if !cfg.do_not_format_device {
      blobd.format().await;
      info!("formatted device");
    };
    let blobd = blobd.load().await;
    info!("loaded device");

    Self { blobd }
  }
}

#[async_trait]
impl Store for BlobdLiteStore {
  #[rustfmt::skip]
  fn metrics(&self) -> Vec<(&'static str, u64)> {
    let metrics = self.blobd.metrics();
    vec![
      ("allocated_bytes", metrics.allocated_bytes()),
      ("incomplete_object_count", metrics.incomplete_object_count()),
      ("object_count", metrics.object_count()),
      ("object_data_bytes", metrics.object_data_bytes()),
      ("object_metadata_bytes", metrics.object_metadata_bytes()),
      ("bucket_lock_read_acq_ns", metrics.bucket_lock_read_acq_ns()),
      ("bucket_lock_read_held_ns", metrics.bucket_lock_read_held_ns()),
      ("bucket_lock_write_acq_ns", metrics.bucket_lock_write_acq_ns()),
      ("bucket_lock_write_held_ns", metrics.bucket_lock_write_held_ns()),
    ]
  }

  fn write_chunk_size(&self) -> u64 {
    1u64 << self.blobd.cfg().lpage_size_pow2
  }

  async fn wait_for_end(&self) {}

  async fn create_object(&self, input: CreateObjectInput) -> CreateObjectOutput {
    let res = self
      .blobd
      .create_object(OpCreateObjectInput {
        assoc_data: Vec::new(),
        key: input.key,
        size: input.size,
      })
      .await
      .unwrap();
    CreateObjectOutput {
      token: Arc::new(BlobdLiteIncompleteToken {
        object_id: res.object_id,
      }),
    }
  }

  async fn write_object<'a>(&'a self, input: WriteObjectInput<'a>) {
    self
      .blobd
      .write_object(OpWriteObjectInput {
        data_len: u64!(input.data.len()),
        data_stream: once(async { Ok(input.data) }).boxed(),
        key: input.key,
        object_id: input
          .incomplete_token
          .downcast::<BlobdLiteIncompleteToken>()
          .unwrap()
          .object_id,
        offset: input.offset,
      })
      .await
      .unwrap();
  }

  async fn commit_object(&self, input: CommitObjectInput) -> CommitObjectOutput {
    let object_id = input
      .incomplete_token
      .downcast::<BlobdLiteIncompleteToken>()
      .unwrap()
      .object_id;
    self
      .blobd
      .commit_object(OpCommitObjectInput {
        key: input.key,
        object_id,
      })
      .await
      .unwrap();
    CommitObjectOutput {
      object_id: Some(object_id.to_string()),
    }
  }

  async fn inspect_object(&self, input: InspectObjectInput) -> InspectObjectOutput {
    let res = self
      .blobd
      .inspect_object(OpInspectObjectInput {
        id: input.id.map(|id| id.parse().unwrap()),
        key: input.key,
      })
      .await
      .unwrap();
    InspectObjectOutput {
      id: Some(res.id.to_string()),
      size: res.size,
    }
  }

  async fn read_object(&self, input: ReadObjectInput) -> ReadObjectOutput {
    let res = self
      .blobd
      .read_object(OpReadObjectInput {
        end: input.end,
        id: input.id.map(|id| id.parse().unwrap()),
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
        id: input.id.map(|id| id.parse().unwrap()),
        key: input.key,
      })
      .await
      .unwrap();
  }
}
