use crate::CmdPut;
use crate::Ctx;
use futures::StreamExt;
use indicatif::ProgressBar;
use std::fs::File;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_sync_read_stream::SyncReadStream;

pub(crate) async fn cmd_put(ctx: Ctx, cmd: CmdPut) {
  let key = &cmd.destination;
  let file = File::open(&cmd.source).expect("open source file");
  let metadata = file.metadata().expect("get source file metadata");
  let size = metadata.len();
  let pb = ProgressBar::new(size);
  pb.set_message("Creating object");
  let creation = ctx
    .client
    .create_object(key, size)
    .await
    .expect("create object");
  const TILE_SIZE: u64 = 1024 * 1024 * 16;
  let stream = SyncReadStream::with_buffer_size(file, TILE_SIZE.try_into().unwrap());
  pb.set_message("Writing object");
  let write_receipts = Arc::new(Mutex::new(Vec::new()));
  stream
    .enumerate()
    .for_each_concurrent(Some(cmd.concurrency), |(chunk_no, chunk_data)| {
      let write_receipts = write_receipts.clone();
      let chunk_data = chunk_data.expect("read part");
      let client = ctx.client.clone();
      let creation = creation.clone();
      let pb = pb.clone();
      async move {
        let offset = u64::try_from(chunk_no).unwrap() * TILE_SIZE;
        let len: u64 = chunk_data.len().try_into().unwrap();
        let wr = client
          .write_object(key, creation, offset, chunk_data)
          .await
          .expect("upload part");
        let mut receipts = write_receipts.lock().await;
        while receipts.len() <= chunk_no {
          receipts.push(String::new());
        }
        receipts[chunk_no] = wr.write_receipt;
        pb.inc(len);
      }
    })
    .await;
  pb.set_message("Committing object");
  let write_receipts = Arc::try_unwrap(write_receipts).unwrap();
  let write_receipts = write_receipts.into_inner();
  ctx
    .client
    .commit_object(key, creation, write_receipts)
    .await
    .expect("commit object");
  pb.set_message("All done!");
}
