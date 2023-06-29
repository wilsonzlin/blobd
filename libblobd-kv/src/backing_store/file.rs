use super::BackingStore;
use crate::pages::Pages;
use async_trait::async_trait;
use bufpool::buf::Buf;
use off64::usz;
use std::fs::File;
use std::os::unix::prelude::FileExt;
use std::sync::Arc;
use tokio::task::spawn_blocking;

#[derive(Clone)]
pub(crate) struct FileBackingStore {
  file: Arc<File>,
  pages: Pages,
}

impl FileBackingStore {
  /// `offset` must be a multiple of the underlying device's sector size.
  pub fn new(file: File, pages: Pages) -> Self {
    Self {
      file: Arc::new(file),
      pages,
    }
  }
}

#[async_trait]
impl BackingStore for FileBackingStore {
  async fn read_at(&self, offset: u64, len: u64) -> Buf {
    let mut buf = self.pages.allocate_uninitialised(len);
    spawn_blocking({
      let file = self.file.clone();
      move || {
        let n = file.read_at(&mut buf, offset).unwrap();
        assert_eq!(n, usz!(len));
        buf
      }
    })
    .await
    .unwrap()
  }

  async fn write_at(&self, offset: u64, data: Buf) -> Buf {
    spawn_blocking({
      let file = self.file.clone();
      move || {
        let n = file.write_at(&data, offset).unwrap();
        assert_eq!(n, data.len());
        data
      }
    })
    .await
    .unwrap()
  }

  async fn sync(&self) {
    spawn_blocking({
      let file = self.file.clone();
      move || {
        file.sync_data().unwrap();
      }
    })
    .await
    .unwrap();
  }
}
