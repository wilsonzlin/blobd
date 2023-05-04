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
use libblobd_direct::BlobdCfgPartition;
use libblobd_direct::BlobdLoader;
use off64::u64;
use off64::u8;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;
use tracing::info;

pub struct Direct {
  blobd: Blobd,
}

impl Direct {
  pub async fn start(cfg: InitCfg, completed: Arc<AtomicU64>) -> Self {
    let blobd = BlobdLoader::new(BlobdCfg {
      journal_size_min: 1024 * 1024 * 512,
      object_metadata_reserved_space: 1024 * 1024 * 1024 * 4,
      partitions: vec![BlobdCfgPartition {
        path: cfg.device,
        len: cfg.device_size,
        offset: 0,
      }],
      event_stream_size: 1024 * 1024 * 1024 * 1,
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
    let blobd = blobd.load().await;
    info!("loaded device");

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
impl BlobdProvider for Direct {
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
