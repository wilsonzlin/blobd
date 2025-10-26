use crate::CommitObjectInput;
use crate::CommitObjectOutput;
use crate::CreateObjectInput;
use crate::CreateObjectOutput;
use crate::DeleteObjectInput;
use crate::InspectObjectInput;
use crate::InspectObjectOutput;
use crate::ReadObjectInput;
use crate::ReadObjectOutput;
use crate::Store;
use crate::WriteObjectInput;
use async_trait::async_trait;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use futures::StreamExt;
use tokio::fs::OpenOptions;
use std::io::SeekFrom;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::create_dir_all;
use tokio::fs::metadata;
use tokio::fs::remove_file;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::spawn;
use tokio::task::spawn_blocking;
use tokio_stream::wrappers::UnboundedReceiverStream;

pub struct FileSystemStore {
  prefix: PathBuf,
  tiering: usize,
}

impl FileSystemStore {
  pub fn new(prefix: PathBuf, tiering: usize) -> Self {
    Self {
      prefix,
      tiering,
    }
  }

  fn get_path(&self, key: &[u8]) -> PathBuf {
    // Hash key otherwise all paths may have the same prefix because of the distribution of keys.
    let hash_ascii = BASE64_URL_SAFE_NO_PAD.encode(blake3::hash(key).as_bytes());
    let key_ascii = BASE64_URL_SAFE_NO_PAD.encode(key);
    let mut path = self.prefix.clone();
    for c in hash_ascii.chars().take(self.tiering) {
      path.push(c.to_string());
    }
    path.push(key_ascii);
    path
  }
}

#[async_trait]
impl Store for FileSystemStore {
  fn metrics(&self) -> Vec<(&'static str, u64)> {
    vec![]
  }

  fn write_chunk_size(&self) -> u64 {
    u64::MAX
  }

  async fn wait_for_end(&self) {}

  // It would not be realistic to cache file handles; a real system would not do this. Also, we'd run out of file handles.
  async fn create_object(&self, input: CreateObjectInput) -> CreateObjectOutput {
    let path = self.get_path(&input.key);
    let parent = path.parent().unwrap();
    create_dir_all(parent).await.unwrap();
    // Also fsync the parent directory.
    OpenOptions::new().read(true).custom_flags(libc::O_DIRECTORY).open(parent).await.unwrap().sync_all().await.unwrap();
    File::create(&path).await.unwrap();
    CreateObjectOutput {
      token: Arc::new(()),
    }
  }

  async fn write_object<'a>(&'a self, input: WriteObjectInput<'a>) {
    let path = self.get_path(&input.key);
    let f = OpenOptions::new().write(true).custom_flags(libc::O_DSYNC).open(&path).await.unwrap().into_std().await;
    let data = input.data.to_vec();
    spawn_blocking(move || {
      f.write_all_at(&data, input.offset).unwrap();
    })
    .await
    .unwrap();
  }

  async fn commit_object(&self, _input: CommitObjectInput) -> CommitObjectOutput {
    CommitObjectOutput { object_id: None }
  }

  async fn inspect_object(&self, input: InspectObjectInput) -> InspectObjectOutput {
    let path = self.get_path(&input.key);
    InspectObjectOutput {
      id: None,
      size: metadata(path).await.unwrap().len(),
    }
  }

  async fn read_object(&self, input: ReadObjectInput) -> ReadObjectOutput {
    let path = self.get_path(&input.key);
    let (start, end) = match input.end {
      Some(end) => (input.start, end),
      None => (input.start, metadata(&path).await.unwrap().len()),
    };
    // Use stream as we calculate TTFB based on time to first chunk, which is incorrect if we simply read all at once.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    spawn(async move {
      let mut rem = (end - start) as usize;
      let mut buf = vec![0u8; rem];
      let mut f = File::open(&path).await.unwrap();
      f.seek(SeekFrom::Start(start)).await.unwrap();
      while rem > 0 {
        let n = f.read(&mut buf).await.unwrap();
        rem -= n;
        tx.send(buf[..n].to_vec()).unwrap();
      }
    });
    ReadObjectOutput {
      data_stream: UnboundedReceiverStream::new(rx).boxed(),
    }
  }

  async fn delete_object(&self, input: DeleteObjectInput) {
    let path = self.get_path(&input.key);
    remove_file(&path).await.unwrap();
  }
}
