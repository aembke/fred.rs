use axum::{
  body::Body,
  extract::{Path, State},
  response::{sse::Event, IntoResponse, Response, Sse},
  routing::{get, post},
  Router,
};
use bytes::Bytes;
use fred::{
  clients::{RedisPool, SubscriberClient},
  prelude::*,
};
use futures::{Stream, StreamExt};
use log::{debug, info};
use std::{env, str, time::Duration};
use tokio::net::TcpListener;
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};

// cloning a client or pool just clones an inner `Arc`.
#[derive(Clone)]
struct AppState {
  pub pool:       RedisPool,
  pub subscriber: SubscriberClient,
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
  let mut builder = Builder::from_config(config);
  builder.with_performance_config(|config| {
      // tune the size of the buffer behind the pubsub broadcast channels
      config.broadcast_channel_capacity = 64;
    })
    .with_connection_config(|config| {
      config.connection_timeout = Duration::from_secs(10);
    })
    // use exponential backoff, starting at 100 ms and doubling on each failed attempt up to 30 sec
    .set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2));

  let pool = builder.build_pool(pool_size).expect("Failed to create redis pool");
  // see the pubsub example for more info
  let subscriber = builder
    .build_subscriber_client()
    .expect("Failed to create subscriber client");

  pool.init().await.expect("Failed to connect to redis");
  subscriber.init().await.expect("Failed to connect to redis");
  info!("Connected to Redis");

  // spawn a task that automatically resubscribes whenever the client reconnects
  subscriber.manage_subscriptions();

  let app = Router::new()
    .route("/:key", get(get_kv).post(set_kv).delete(del_kv))
    .route("/:key/incr", post(incr_kv))
    .route("/:channel/subscribe", get(subscribe))
    .route("/:channel/unsubscribe", post(unsubscribe))
    .route("/:channel/publish", post(publish))
    .with_state(AppState { pool, subscriber });

  let listener = TcpListener::bind("127.0.0.1:3000")
    .await
    .expect("Failed to bind to port");
  info!("Starting server...");
  axum::serve(listener, app).await.unwrap();
}

fn map_error(err: RedisError) -> (u16, Body) {
  let details: Body = err.details().to_string().into();
  let code = if *err.kind() == RedisErrorKind::NotFound {
    404
  } else if err.details().starts_with("WRONGTYPE") {
    400
  } else {
    500
  };

  (code, details)
}

async fn get_kv(Path(key): Path<String>, State(state): State<AppState>) -> impl IntoResponse {
  debug!("get {}", key);

  let (code, val) = match state.pool.get::<Option<Bytes>, _>(key).await {
    Ok(Some(val)) => (200, val.into()),
    Ok(None) => (404, "Not found".into()),
    Err(err) => map_error(err),
  };
  Response::builder().status(code).body(val).unwrap()
}

async fn set_kv(Path(key): Path<String>, State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
  debug!("set {} {}", key, String::from_utf8_lossy(&body));

  let (code, val) = match state.pool.set::<Bytes, _, _>(key, body, None, None, false).await {
    Ok(val) => (200, val.into()),
    Err(err) => map_error(err),
  };
  Response::builder().status(code).body(val).unwrap()
}

async fn del_kv(Path(key): Path<String>, State(state): State<AppState>) -> impl IntoResponse {
  debug!("del {}", key);

  let (code, val) = match state.pool.del::<i64, _>(key).await {
    Ok(0) => (404, "Not Found.".into()),
    Ok(val) => (200, val.to_string().into()),
    Err(err) => map_error(err),
  };
  Response::builder().status(code).body(val).unwrap()
}

async fn incr_kv(Path(key): Path<String>, State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
  let count = str::from_utf8(&body)
    .ok()
    .and_then(|s| s.parse::<i64>().ok())
    .unwrap_or(1);
  debug!("incr {} by {}", key, count);

  let (code, val) = match state.pool.incr_by::<i64, _>(key, count).await {
    Ok(val) => (200, val.to_string().into()),
    Err(err) => map_error(err),
  };
  Response::builder().status(code).body(val).unwrap()
}

async fn subscribe(
  Path(channel): Path<String>,
  State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, BroadcastStreamRecvError>>> {
  debug!("subscribe to {}", channel);
  let rx = BroadcastStream::new(state.subscriber.message_rx());
  state.subscriber.subscribe(channel).await.expect("Failed to subscribe");

  Sse::new(rx.map(|message| {
    message.map(|msg| {
      let contents = msg
        .value
        .convert::<String>()
        .expect("Failed to convert pubsub message to string");
      Event::default().data(contents)
    })
  }))
}

async fn unsubscribe(Path(channel): Path<String>, State(state): State<AppState>) -> impl IntoResponse {
  debug!("unsubscribe {}", channel);

  let (code, val) = match state.subscriber.unsubscribe(channel).await {
    Ok(_) => (200, "OK".into()),
    Err(err) => map_error(err),
  };
  Response::builder().status(code).body(val).unwrap()
}

async fn publish(Path(channel): Path<String>, State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
  debug!("publish {} on {}", String::from_utf8_lossy(&body), channel);

  // or if using RESP3 then we can publish with the same subscriber client. this example assumes RESP2.
  let (code, val) = match state.pool.next().publish::<(), _, _>(channel, body).await {
    Ok(_) => (200, "OK".into()),
    Err(err) => map_error(err),
  };
  Response::builder().status(code).body(val).unwrap()
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

// terminal 1:
// $ curl -N http://127.0.0.1:3000/foo/subscribe
// data: bar
// data: baz

// terminal 2:
// $ curl -X POST -d 'bar' http://127.0.0.1:3000/foo/publish
// OK
// $ curl -X POST -d 'baz' http://127.0.0.1:3000/foo/publish
// OK
