use crate::CmdUpload;
use crate::Ctx;
use blobd_client_rs::BatchCreateObjectEntry;
use bytes::Bytes;
use futures::channel::mpsc::unbounded;
use futures::Stream;
use futures::StreamExt;
use indicatif::MultiProgress;
use indicatif::ProgressBar;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use tokio::spawn;
use tokio::task::spawn_blocking;
use walkdir::WalkDir;

struct FileStream {
  file: File,
}

impl Stream for FileStream {
  type Item = Result<Bytes, Box<dyn Error + Send + Sync>>;

  fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let mut buf = vec![0u8; 1024 * 64];
    let item = match self.file.read(&mut buf) {
      Ok(n) => {
        if n == 0 {
          None
        } else {
          buf.truncate(n);
          Some(Ok(Bytes::from(buf)))
        }
      }
      Err(err) => Some(Err(Box::<dyn Error + Send + Sync>::from(err))),
    };
    Poll::Ready(item)
  }
}

pub(crate) async fn cmd_upload(ctx: Ctx, cmd: CmdUpload) {
  let mp = MultiProgress::new();
  let walk_progress = mp.add(ProgressBar::new_spinner());
  walk_progress.set_message("Finding files");
  let upload_progress = mp.add(ProgressBar::new_spinner());
  upload_progress.set_message("Uploading");

  // For UX, we walk entire tree to show progress, total size, and total file count. If the user has so many files that this OOMs or takes too long, they should probably use a more advanced tool that supports interruptions and crashes and running for long periods (e.g. memory leaks).
  let dir = cmd.source;
  let prefix = cmd.prefix.unwrap_or_default();
  let (sender, receiver) = unbounded();
  {
    let upload_progress = upload_progress.clone();
    spawn_blocking(move || {
      let mut total_objects = 0;
      let mut total_bytes = 0;
      for ent in WalkDir::new(&dir) {
        walk_progress.set_message(format!("Found {} files", total_objects));
        let ent = match ent {
          Ok(ent) => ent,
          Err(err) => {
            walk_progress.println(format!("⚠️ {}", err));
            continue;
          }
        };
        let size = match ent.metadata() {
          Ok(m) => m.len(),
          Err(err) => {
            walk_progress.println(format!("⚠️ {}", err));
            continue;
          }
        };
        let Some(path) = ent.path().to_str() else {
          walk_progress.println(format!("⚠️ {:?} is a non-Unicode path", ent.path()));
          continue;
        };
        let key = format!("{}{}", &prefix, path).into_bytes();
        let file = match File::open(dir.join(ent.path())) {
          Ok(f) => f,
          Err(err) => {
            walk_progress.println(format!("⚠️ failed to open file {}: {}", path, err));
            continue;
          }
        };
        sender
          .unbounded_send(BatchCreateObjectEntry {
            data_stream: FileStream { file },
            key,
            size,
          })
          .unwrap();
        total_objects += 1;
        total_bytes += size;
      }
      walk_progress.finish_with_message(format!("Will upload {} files", total_objects));
      upload_progress.update(|s| s.set_len(total_bytes));
    });
  };

  let (transfer_counter_sender, transfer_counter_receiver) = unbounded();
  spawn({
    let upload_progress = upload_progress.clone();
    async move {
      if let Err(err) = ctx
        .client
        .batch_create_objects(receiver, transfer_counter_sender)
        .await
      {
        upload_progress.println(format!("❌ {}", err));
      };
    }
  });
  transfer_counter_receiver
    .fold(0, |mut total, add| {
      let add: u64 = add.try_into().unwrap();
      total += add;
      upload_progress.update(|s| s.set_pos(total));
      async move { total }
    })
    .await;
  upload_progress.finish_with_message("✔️ All done");
}
