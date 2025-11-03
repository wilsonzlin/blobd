use crate::device::IDevice;
use async_trait::async_trait;
use off64::Off64AsyncRead;
use off64::Off64AsyncWrite;
use off64::int::Off64AsyncReadInt;
use off64::int::Off64AsyncWriteInt;
use off64::usz;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::task::spawn_blocking;

#[derive(Clone)]
pub struct DsyncFile {
  file: Arc<std::fs::File>,
}

impl DsyncFile {
  pub async fn open(path: &Path) -> Self {
    // We use DSYNC for more efficient fsync. All our writes must be immediately fsync'd — we don't use kernel buffer as intermediate storage, nor split writes (and smaller intermediate writes always go through the journal) — but fsync isn't granular (and sync_file_range is not portable). Delaying sync is even more pointless.
    Self {
      file: OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DSYNC)
        .open(path)
        .await
        .unwrap()
        .into_std()
        .await
        .into(),
    }
  }
}

#[async_trait]
impl<'a> Off64AsyncRead<'a, Vec<u8>> for DsyncFile {
  async fn read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    let file = self.file.clone();
    spawn_blocking(move || {
      let mut buf = vec![0u8; usz!(len)];
      file.read_exact_at(&mut buf, offset).unwrap();
      buf
    })
    .await
    .unwrap()
  }
}
#[async_trait]
impl<'a> Off64AsyncReadInt<'a, Vec<u8>> for DsyncFile {}

#[async_trait]
impl Off64AsyncWrite for DsyncFile {
  async fn write_at(&self, offset: u64, value: &[u8]) {
    let file = self.file.clone();
    let value = value.to_vec();
    spawn_blocking(move || {
      file.write_all_at(&value, offset).unwrap();
    })
    .await
    .unwrap();
  }
}
#[async_trait]
impl Off64AsyncWriteInt for DsyncFile {}

#[async_trait]
impl IDevice for DsyncFile {}
