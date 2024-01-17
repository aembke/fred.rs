use axum::{
  body::Body,
  extract::{Path, State},
  response::{IntoResponse, Response},
  routing::{get, post},
  Router,
};
use bytes::Bytes;
use fred::{clients::RedisPool, prelude::*};
use log::{debug, info};
use std::{env, str};
use tokio::net::TcpListener;

// don't need a lock or an extra `Arc`
#[derive(Clone)]
struct AppState {
  pub pool: RedisPool,
}

#[tokio::main]
async fn main() {
  pretty_env_logger::init();

  let pool_size = env::var("REDIS_POOL_SIZE")
    .ok()
    .and_then(|v| v.parse::<usize>().ok())
    .unwrap_or(8);
  let config =
    RedisConfig::from_url("redis://foo:bar@127.0.0.1:6379").expect("Failed to create redis config from url");
  let pool = Builder::from_config(config)
    .with_performance_config(|config| {
      config.auto_pipeline = true;
    })
    // use exponential backoff, starting at 100 ms and doubling on each failed attempt up to 30 sec
    .set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2))
    .build_pool(pool_size)
    .expect("Failed to create redis pool");

  let _ = pool.connect();
  pool.wait_for_connect().await.expect("Failed to connect to redis");
  info!("Connected to Redis");

  let app = Router::new()
    .route("/:key", get(get_kv).post(set_kv).delete(del_kv))
    .route("/:key/incr", post(incr_kv))
    .with_state(AppState { pool });

  let listener = TcpListener::bind("127.0.0.1:3000")
    .await
    .expect("Failed to bind to port");
  info!("Starting server...");
  axum::serve(listener, app).await.unwrap();
}

async fn get_kv(Path(key): Path<String>, State(state): State<AppState>) -> impl IntoResponse {
  debug!("get {}", key);

  let (code, val) = match state.pool.get::<Option<Bytes>, _>(key).await {
    Ok(Some(val)) => (200, val),
    Ok(None) => (404, "Not found".into()),
    Err(err) if *err.kind() == RedisErrorKind::NotFound => (404, err.to_string().into()),
    Err(err) if err.details().starts_with("WRONGTYPE") => (400, err.to_string().into()),
    Err(err) => (500, err.to_string().into()),
  };

  Response::builder().status(code).body(Body::from(val)).unwrap()
}

async fn set_kv(Path(key): Path<String>, State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
  debug!("set {} {}", key, String::from_utf8_lossy(&body));

  let (code, val) = match state.pool.set::<Bytes, _, _>(key, body, None, None, false).await {
    Ok(val) => (200, val),
    Err(err) if *err.kind() == RedisErrorKind::NotFound => (404, err.to_string().into()),
    Err(err) if err.details().starts_with("WRONGTYPE") => (400, err.to_string().into()),
    Err(err) => (500, err.to_string().into()),
  };

  Response::builder().status(code).body(Body::from(val)).unwrap()
}

async fn del_kv(Path(key): Path<String>, State(state): State<AppState>) -> impl IntoResponse {
  debug!("del {}", key);

  let (code, val) = match state.pool.del::<i64, _>(key).await {
    Ok(val) if val == 0 => (404, "Not Found.".into()),
    Ok(val) => (200, val.to_string()),
    Err(err) if *err.kind() == RedisErrorKind::NotFound => (404, err.to_string()),
    Err(err) if err.details().starts_with("WRONGTYPE") => (400, err.to_string()),
    Err(err) => (500, err.to_string()),
  };

  Response::builder().status(code).body(Body::from(val)).unwrap()
}

async fn incr_kv(Path(key): Path<String>, State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
  let count = str::from_utf8(&body)
    .ok()
    .and_then(|s| s.parse::<i64>().ok())
    .unwrap_or(1);
  debug!("incr {} by {}", key, count);

  let (code, val) = match state.pool.incr_by::<i64, _>(key, count).await {
    Ok(val) => (200, val.to_string()),
    Err(err) if *err.kind() == RedisErrorKind::NotFound => (404, err.to_string()),
    Err(err) if err.details().starts_with("WRONGTYPE") => (400, err.to_string()),
    Err(err) => (500, err.to_string()),
  };

  Response::builder().status(code).body(Body::from(val)).unwrap()
}

// example usage with curl:
// $ curl http://localhost:3000/foo
// Not found
// $ curl -X POST -d '100' http://localhost:3000/foo
// OK
// $ curl -X POST -d '50' http://localhost:3000/foo/incr
// 150
// $ curl -X POST -d '50' http://localhost:3000/foo/incr
// 200
// $ curl -X POST -d '50' http://localhost:3000/foo/incr
// 250
// $ curl http://localhost:3000/foo
// 250
// $ curl -X DELETE http://localhost:3000/foo
// 1
// $ curl http://localhost:3000/foo
// Not found
