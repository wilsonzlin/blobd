use crate::CmdGet;
use crate::Ctx;
use futures::StreamExt;
use tokio::io::stdout;
use tokio::io::AsyncWriteExt;

pub(crate) async fn cmd_get(ctx: Ctx, cmd: CmdGet) {
  let mut res = ctx
    .client
    .read_object(&cmd.key, cmd.start, cmd.end)
    .await
    .expect("read object");
  while let Some(chunk) = res.next().await {
    let chunk = chunk.expect("read chunk");
    stdout().write_all(&chunk).await.unwrap();
  }
}
