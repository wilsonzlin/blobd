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

fn div_ceil(v: u64, d: u64) -> u64 {
  let (div, rem) = (v / d, v % d);
  div + if rem > 0 { 1 } else { 0 }
}

impl Direct {
  pub async fn start(cfg: InitCfg) -> Self {
    let part_count = u64!(cfg.partitions.len());
    let blobd = BlobdLoader::new(
      cfg
        .partitions
        .into_iter()
        .map(|p| BlobdCfgPartition {
          path: p.path,
          offset: p.offset,
          len: p.len,
        })
        .collect(),
      BlobdCfg {
        #[cfg(not(target_os = "linux"))]
        backing_store: BlobdCfgBackingStore::File,
        #[cfg(target_os = "linux")]
        backing_store: BlobdCfgBackingStore::Uring,
        expire_incomplete_objects_after_secs: 60 * 60 * 24 * 7,
        lpage_size_pow2: u8!(cfg.lpage_size.ilog2()),
        // Each tuple requires around 20 bytes.
        object_tuples_area_reserved_space: div_ceil(cfg.object_count * 20, part_count),
        spage_size_pow2: u8!(cfg.spage_size.ilog2()),
        #[cfg(target_os = "linux")]
        uring_coop_taskrun: false,
        #[cfg(target_os = "linux")]
        uring_defer_taskrun: false,
        #[cfg(target_os = "linux")]
        uring_iopoll: false,
        #[cfg(target_os = "linux")]
        uring_sqpoll: None,
      },
    );
    if !cfg.do_not_format_device {
      blobd.format().await;
      info!("formatted device");
    };
    let blobd = blobd.load_and_start().await;
    info!("loaded device");

    Self { blobd }
  }
}

#[async_trait]
impl BlobdProvider for Direct {
  #[rustfmt::skip]
  fn metrics(&self) -> Vec<(&'static str, u64)> {
    let metrics = self.blobd.metrics();
    vec![
      ("commit_op_count", metrics.commit_op_count()),
      ("create_op_count", metrics.create_op_count()),
      ("delete_op_count", metrics.delete_op_count()),
      ("inspect_op_count", metrics.inspect_op_count()),

      ("read_op_count", metrics.read_op_count()),
      ("read_op_bytes_requested", metrics.read_op_bytes_requested()),
      ("read_op_bytes_sent", metrics.read_op_bytes_sent()),
      ("read_op_bytes_discarded", metrics.read_op_bytes_discarded()),

      ("write_op_count", metrics.write_op_count()),
      ("write_op_bytes_requested", metrics.write_op_bytes_requested()),
      ("write_op_bytes_written", metrics.write_op_bytes_written()),

      ("allocated_bytes", metrics.allocated_bytes()),
      ("object_metadata_bytes", metrics.object_metadata_bytes()),
      ("object_data_bytes", metrics.object_data_bytes()),

      ("incomplete_object_count", metrics.incomplete_object_count()),
      ("committed_object_count", metrics.committed_object_count()),
    ]
  }

  async fn wait_for_end(&self) {}

  async fn create_object(&self, input: CreateObjectInput) -> CreateObjectOutput {
    let res = self
      .blobd
      .create_object(OpCreateObjectInput {
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
        if_not_exists: false,
      })
      .await
      .unwrap();
    CommitObjectOutput {
      object_id: res.object_id.unwrap(),
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
