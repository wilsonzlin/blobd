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
use libblobd_kv::op::delete_object::OpDeleteObjectInput;
use libblobd_kv::op::read_object::OpReadObjectInput;
use libblobd_kv::op::write_object::OpWriteObjectInput;
use libblobd_kv::Blobd;
use libblobd_kv::BlobdCfg;
use libblobd_kv::BlobdCfgBackingStore;
use libblobd_kv::BlobdLoader;
use off64::u8;
use std::sync::Arc;
use tracing::info;

pub struct Kv {
  blobd: Blobd,
}

impl Kv {
  pub async fn start(cfg: InitCfg) -> Self {
    assert_eq!(cfg.partitions.len(), 1);
    let device_cfg = &cfg.partitions[0];
    assert_eq!(device_cfg.offset, 0);

    let blobd = BlobdLoader::new(BlobdCfg {
      #[cfg(not(target_os = "linux"))]
      backing_store: BlobdCfgBackingStore::File,
      #[cfg(target_os = "linux")]
      backing_store: BlobdCfgBackingStore::Uring,
      device_len: device_cfg.len,
      device_path: device_cfg.path.clone(),
      object_tuples_area_reserved_space: cfg.object_count * 512,
      spage_size_pow2: u8!(cfg.spage_size.ilog2()),
      #[cfg(target_os = "linux")]
      uring_coop_taskrun: false,
      #[cfg(target_os = "linux")]
      uring_defer_taskrun: false,
      #[cfg(target_os = "linux")]
      uring_iopoll: false,
      #[cfg(target_os = "linux")]
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
impl BlobdProvider for Kv {
  async fn create_object(&self, _input: CreateObjectInput) -> CreateObjectOutput {
    CreateObjectOutput {
      token: Arc::new(()),
    }
  }

  async fn write_object<'a>(&'a self, input: WriteObjectInput<'a>) {
    assert_eq!(input.offset, 0);
    self
      .blobd
      .write_object(OpWriteObjectInput {
        key: input.key,
        data: input.data.to_vec(),
        if_not_exists: false,
      })
      .await
      .unwrap();
  }

  async fn commit_object(&self, _input: CommitObjectInput) -> CommitObjectOutput {
    CommitObjectOutput { object_id: 0 }
  }

  async fn inspect_object(&self, input: InspectObjectInput) -> InspectObjectOutput {
    let res = self
      .blobd
      .read_object(OpReadObjectInput {
        end: Some(0),
        key: input.key,
        start: 0,
      })
      .await
      .unwrap();
    InspectObjectOutput {
      id: 0,
      size: res.object_size,
    }
  }

  async fn read_object(&self, input: ReadObjectInput) -> ReadObjectOutput {
    let res = self
      .blobd
      .read_object(OpReadObjectInput {
        end: input.end,
        key: input.key,
        start: input.start,
      })
      .await
      .unwrap();
    ReadObjectOutput {
      data_stream: once(async move { res.data }).boxed(),
    }
  }

  async fn delete_object(&self, input: DeleteObjectInput) {
    self
      .blobd
      .delete_object(OpDeleteObjectInput { key: input.key })
      .await
      .unwrap();
  }
}
