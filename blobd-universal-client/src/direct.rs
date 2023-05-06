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
use libblobd_direct::incomplete_token::IncompleteToken;
use libblobd_direct::op::commit_object::OpCommitObjectInput;
use libblobd_direct::op::create_object::OpCreateObjectInput;
use libblobd_direct::op::delete_object::OpDeleteObjectInput;
use libblobd_direct::op::inspect_object::OpInspectObjectInput;
use libblobd_direct::op::read_object::OpReadObjectInput;
use libblobd_direct::op::write_object::OpWriteObjectInput;
use libblobd_direct::Blobd;
use libblobd_direct::BlobdCfg;
use libblobd_direct::BlobdCfgBackingStore;
use libblobd_direct::BlobdCfgPartition;
use libblobd_direct::BlobdLoader;
use off64::u64;
use off64::u8;
use std::sync::Arc;
use tracing::info;

pub struct Direct {
  blobd: Blobd,
}

impl Direct {
  pub async fn start(cfg: InitCfg) -> Self {
    let part_count = u64!(num_cpus::get());
    let part_len = cfg.device_size / part_count / cfg.lpage_size * cfg.lpage_size;
    // Don't use step_by as `part_len` will likely be slightly less than `device_size / part_count` so there will end up being more partitions than wanted.
    let partitions = (0..part_count)
      .map(|part_no| BlobdCfgPartition {
        path: cfg.device.clone(),
        len: part_len,
        offset: part_no * part_len,
      })
      .collect();
    let blobd = BlobdLoader::new(partitions, BlobdCfg {
      #[cfg(not(target_os = "linux"))]
      backing_store: BlobdCfgBackingStore::File,
      #[cfg(target_os = "linux")]
      backing_store: BlobdCfgBackingStore::Uring,
      dangerously_disable_journal: false,
      journal_size_min: (1024 * 1024 * 512) / part_count,
      object_metadata_reserved_space: (1024 * 1024 * 1024 * 4) / part_count,
      event_stream_size: (1024 * 1024 * 1024 * 1) / part_count,
      expire_incomplete_objects_after_secs: 60 * 60 * 24 * 7,
      lpage_size_pow2: u8!(cfg.lpage_size.ilog2()),
      spage_size_pow2: u8!(cfg.spage_size.ilog2()),
      uring_coop_taskrun: false,
      uring_defer_taskrun: false,
      uring_iopoll: false,
      uring_sqpoll: None,
    });
    blobd.format().await;
    info!("formatted device");
    let blobd = blobd.load_and_start().await;
    info!("loaded device");

    Self { blobd }
  }
}

#[async_trait]
impl BlobdProvider for Direct {
  fn metrics(&self) -> Vec<(&'static str, u64)> {
    let m = self.blobd.metrics();
    vec![
      ("object_data_bytes", m.object_data_bytes()),
      ("object_metadata_bytes", m.object_metadata_bytes()),
      ("used_bytes", m.used_bytes()),
    ]
  }

  async fn create_object(&self, input: CreateObjectInput) -> CreateObjectOutput {
    let res = self
      .blobd
      .create_object(OpCreateObjectInput {
        assoc_data: input.assoc_data,
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
      })
      .await
      .unwrap();
    ReadObjectOutput {
      data_stream: res
        .data_stream
        .map(|chunk| {
          let boxed: Box<dyn AsRef<[u8]>> = Box::new(chunk.unwrap());
          boxed
        })
        .boxed(),
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
