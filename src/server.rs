use crate::ctx::Ctx;
use crate::endpoint::commit_object::endpoint_commit_object;
use crate::endpoint::create_object::endpoint_create_object;
use crate::endpoint::delete_object::endpoint_delete_object;
use crate::endpoint::inspect_object::endpoint_inspect_object;
use crate::endpoint::read_object::endpoint_read_object;
use crate::endpoint::write_object::endpoint_write_object;
use axum::extract::DefaultBodyLimit;
use axum::routing::delete;
use axum::Router;
use axum::Server;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;

pub async fn start_http_server_loop(interface: Ipv4Addr, port: u16, ctx: Arc<Ctx>) {
  let app = Router::new()
    .route(
      "/*",
      delete(endpoint_delete_object)
        .get(endpoint_read_object)
        .head(endpoint_inspect_object)
        .patch(endpoint_write_object)
        .post(endpoint_create_object)
        .put(endpoint_commit_object),
    )
    .layer(DefaultBodyLimit::disable())
    .with_state(ctx.clone());

  let addr = SocketAddr::from((interface, port));

  Server::bind(&addr)
    .serve(app.into_make_service())
    .await
    .unwrap();
}
