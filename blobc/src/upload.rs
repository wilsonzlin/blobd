use crate::CmdUpload;
use crate::Ctx;
use blobd_client_rs::BatchCreateObjectEntry;
use bytes::Bytes;
use futures::channel::mpsc::unbounded;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use indicatif::MultiProgress;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use tokio::runtime::Handle;
use tokio::spawn;
use tokio::task::spawn_blocking;
use tokio_sync_read_stream::SyncReadStream;
use walkdir::DirEntry;
use walkdir::WalkDir;

fn process_dir_ent(
  tokio: Handle,
  base_dir: &Path,
  prefix: &str,
  ent: walkdir::Result<DirEntry>,
) -> Result<
  BatchCreateObjectEntry<impl Stream<Item = Result<Bytes, Box<dyn Error + Send + Sync + 'static>>>>,
  Box<dyn Error>,
> {
  let ent = ent?;
  let size = ent.metadata()?.len();
  let path_str = ent
    .path()
    .to_str()
    .ok_or_else(|| Box::<dyn Error>::from(format!("{:?} is a non-Unicode path", ent.path())))?;
  let key = format!("{}{}", &prefix, path_str).into_bytes();
  let file = File::open(base_dir.join(ent.path()))?;
  Ok(BatchCreateObjectEntry {
    data_stream: SyncReadStream::with_tokio_handle(tokio, file)
      .map_ok(|b| Bytes::from(b))
      .map_err(|e| Box::from(e)),
    key,
    size,
  })
}

pub(crate) async fn cmd_upload(ctx: Ctx, cmd: CmdUpload) {
  let tokio = Handle::current();

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
        let obj = match process_dir_ent(tokio.clone(), &dir, &prefix, ent) {
          Ok(o) => o,
          Err(err) => {
            walk_progress.println(format!("⚠️ {}", err));
            continue;
          }
        };
        let size = obj.size;
        sender.unbounded_send(obj).unwrap();
        total_objects += 1;
        total_bytes += size;
      }
      walk_progress.finish_with_message(format!("Will upload {} files", total_objects));
      upload_progress.update(|s| s.set_len(total_bytes));
      upload_progress.set_style(ProgressStyle::with_template("{spinner} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})").unwrap().progress_chars("#>-"));
    });
  };

  let (transfer_counter_sender, transfer_counter_receiver) = unbounded();
  spawn({
    let upload_progress = upload_progress.clone();
    async move {
      let res = ctx
        .client
        .batch_create_objects(receiver, transfer_counter_sender)
        .await;
      upload_progress.finish_with_message(match res {
        Ok(_) => "✔️ All done".to_string(),
        Err(err) => format!("❌ Aborted: {}", err),
      });
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
}
