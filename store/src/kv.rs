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
use libblobd_kv::op::delete_object::OpDeleteObjectInput;
use libblobd_kv::op::read_object::OpReadObjectInput;
use libblobd_kv::op::write_object::OpWriteObjectInput;
use libblobd_kv::Blobd;
use libblobd_kv::BlobdCfg;
use libblobd_kv::BlobdCfgBackingStore;
use libblobd_kv::BlobdLoader;
use off64::u8;
use off64::usz;
use std::sync::Arc;
use tracing::info;

pub struct BlobdKVStore {
  blobd: Blobd,
}

impl BlobdKVStore {
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
      log_buffer_commit_threshold: cfg.log_buffer_size / 2, // TODO
      log_buffer_size: cfg.log_buffer_size,
      log_entry_data_len_inline_threshold: usz!(cfg.lpage_size), // TODO
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
impl Store for BlobdKVStore {
  #[rustfmt::skip]
  fn metrics(&self) -> Vec<(&'static str, u64)> {
    let metrics = self.blobd.metrics();
    vec![
      ("delete_op_count", metrics.delete_op_count()),

      ("read_op_count", metrics.read_op_count()),
      ("read_op_bytes_sent", metrics.read_op_bytes_sent()),
      ("read_op_bytes_discarded", metrics.read_op_bytes_discarded()),

      ("write_op_count", metrics.write_op_count()),
      ("write_op_bytes_persisted", metrics.write_op_bytes_persisted()),
      ("write_op_bytes_padding", metrics.write_op_bytes_padding()),

      ("allocated_bytes", metrics.allocated_bytes()),
      ("heap_object_data_bytes", metrics.heap_object_data_bytes()),
      ("object_count", metrics.object_count()),

      ("log_buffer_write_entry_count", metrics.log_buffer_write_entry_count()),
      ("log_buffer_write_entry_data_bytes", metrics.log_buffer_write_entry_data_bytes()),
      ("log_buffer_delete_entry_count", metrics.log_buffer_delete_entry_count()),
      ("log_buffer_virtual_head", metrics.log_buffer_virtual_head()),
      ("log_buffer_virtual_tail", metrics.log_buffer_virtual_tail()),

      ("log_buffer_commit_count", metrics.log_buffer_commit_count()),
      ("log_buffer_commit_entry_count", metrics.log_buffer_commit_entry_count()),
      ("log_buffer_commit_object_grouping_us", metrics.log_buffer_commit_object_grouping_us()),
      ("log_buffer_commit_object_heap_move_count", metrics.log_buffer_commit_object_heap_move_count()),
      ("log_buffer_commit_object_heap_move_bytes", metrics.log_buffer_commit_object_heap_move_bytes()),
      ("log_buffer_commit_object_heap_move_write_us", metrics.log_buffer_commit_object_heap_move_write_us()),
      ("log_buffer_commit_bundle_committed_count", metrics.log_buffer_commit_bundle_committed_count()),
      ("log_buffer_commit_bundle_count", metrics.log_buffer_commit_bundle_count()),
      ("log_buffer_commit_bundle_read_us", metrics.log_buffer_commit_bundle_read_us()),
      ("log_buffer_commit_bundle_write_us", metrics.log_buffer_commit_bundle_write_us()),
      ("log_buffer_commit_bundles_update_us", metrics.log_buffer_commit_bundles_update_us()),

      ("log_buffer_flush_entry_count", metrics.log_buffer_flush_entry_count()),
      ("log_buffer_flush_count", metrics.log_buffer_flush_count()),
      ("log_buffer_flush_4k_count", metrics.log_buffer_flush_4k_count()),
      ("log_buffer_flush_64k_count", metrics.log_buffer_flush_64k_count()),
      ("log_buffer_flush_1m_count", metrics.log_buffer_flush_1m_count()),
      ("log_buffer_flush_8m_count", metrics.log_buffer_flush_8m_count()),
      ("log_buffer_flush_data_bytes", metrics.log_buffer_flush_data_bytes()),
      ("log_buffer_flush_padding_bytes", metrics.log_buffer_flush_padding_bytes()),
      ("log_buffer_flush_write_us", metrics.log_buffer_flush_write_us()),
      ("log_buffer_flush_total_us", metrics.log_buffer_flush_total_us()),
      ("log_buffer_flush_state_count", metrics.log_buffer_flush_state_count()),
    ]
  }

  fn write_chunk_size(&self) -> u64 {
    u64::MAX
  }

  async fn wait_for_end(&self) {
    self.blobd.wait_for_any_current_log_buffer_commit().await;
  }

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
      })
      .await
      .unwrap();
  }

  async fn commit_object(&self, _input: CommitObjectInput) -> CommitObjectOutput {
    CommitObjectOutput { object_id: None }
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
      id: None,
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
