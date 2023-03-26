use std::{sync::Arc, net::{Ipv4Addr, SocketAddr}};

use axum::{extract::DefaultBodyLimit, routing::delete, Server, Router};

use crate::{endpoint::{read_object::endpoint_get_object, create_object::endpoint_create_object, commit_object::endpoint_commit_object, delete_object::endpoint_delete_object, write_object::endpoint_write_object}, ctx::Ctx};



pub async fn start_http_server_loop(interface: Ipv4Addr, port: u16, ctx: Arc<Ctx>) {
  let app = Router::new()
    .route("/*", delete(endpoint_delete_object).get(endpoint_get_object).patch(endpoint_write_object).post(endpoint_create_object).put(endpoint_commit_object))
    .layer(DefaultBodyLimit::disable())
    .with_state(ctx.clone());

  let addr = SocketAddr::from((interface, port));

  Server::bind(&addr)
    .serve(app.into_make_service())
    .await
    .unwrap();
}
