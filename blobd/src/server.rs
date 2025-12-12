use crate::endpoint::batch_create_objects::endpoint_batch_create_objects;
use crate::endpoint::commit_object::endpoint_commit_object;
use crate::endpoint::create_object::endpoint_create_object;
use crate::endpoint::delete_object::endpoint_delete_object;
use crate::endpoint::inspect_object::endpoint_inspect_object;
use crate::endpoint::read_object::endpoint_read_object;
use crate::endpoint::write_object::endpoint_write_object;
use crate::endpoint::HttpCtx;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::routing::delete;
use axum::routing::post;
use axum::Router;
use axum::Server;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::Any;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

async fn method_not_allowed() -> StatusCode {
  StatusCode::METHOD_NOT_ALLOWED
}

pub async fn start_http_server_loop(interface: Ipv4Addr, port: u16, ctx: Arc<HttpCtx>) {
  let app = Router::new()
    .route(
      "/",
      post(endpoint_batch_create_objects).fallback(method_not_allowed),
    )
    .fallback(
      delete(endpoint_delete_object)
        .get(endpoint_read_object)
        .head(endpoint_inspect_object)
        .patch(endpoint_write_object)
        .post(endpoint_create_object)
        .put(endpoint_commit_object),
    )
    .layer(DefaultBodyLimit::disable())
    .layer(
      CorsLayer::new()
        .allow_headers(Any)
        .allow_methods(Any)
        .allow_origin(Any)
        .max_age(std::time::Duration::from_secs(60 * 60 * 24)),
    )
    .layer(TraceLayer::new_for_http())
    .with_state(ctx.clone());

  let addr = SocketAddr::from((interface, port));
  info!(interface = interface.to_string(), port, "starting server");

  Server::bind(&addr)
    .tcp_nodelay(true)
    .serve(app.into_make_service())
    .await
    .unwrap();
}
