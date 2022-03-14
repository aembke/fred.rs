use fred::prelude::*;
use fred::types::{BackpressureConfig, PerformanceConfig, RespVersion};
use futures::stream::StreamExt;
use std::default::Default;

const DATABASE: u8 = 2;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  // example showing a full kitchen sink configuration
  // use `..Default::default` to fill in defaults wherever needed
  let config = RedisConfig {
    // whether to skip reconnect logic when first connecting
    fail_fast: true,
    // server configuration
    server: ServerConfig::new_centralized("127.0.0.1", 6379),
    // how to handle commands sent while a connection is blocked
    blocking: Blocking::Block,
    // an optional username, if using ACL rules
    username: None,
    // an optional authentication key or password
    password: None,
    // optional TLS settings
    tls: None,
    // whether to enable tracing
    tracing: false,
    // the protocol version to use
    version: RespVersion::RESP2,
    // the database to automatically select after connecting or reconnecting
    database: Some(DATABASE),
    // performance tuning options
    performance: PerformanceConfig {
      // whether or not to automatically pipeline commands
      pipeline: true,
      // the max number of frames to feed into a socket before flushing it
      max_feed_count: 1000,
      // a default timeout to apply to all commands (0 means no timeout)
      default_command_timeout_ms: 0,
      // the amount of time to wait before syncing cluster state after a MOVED or ASK error
      cluster_cache_update_delay_ms: 10,
      // the maximum number of times to retry commands when connections close unexpectedly
      max_command_attempts: 3,
      // backpressure config options
      backpressure: BackpressureConfig {
        // whether to disable automatic backpressure features
        disable_auto_backpressure: false,
        // whether to disable scaling backpressure `sleep` durations based on the number of in-flight commands
        disable_backpressure_scaling: false,
        // the minimum amount of time to `sleep` when applying automatic backpressure
        min_sleep_duration_ms: 100,
        // the max number of in-flight commands before applying backpressure or returning backpressure errors
        max_in_flight_commands: 5000,
      },
    },
  };
  // configure exponential backoff when reconnecting, starting at 100 ms, and doubling each time up to 30 sec.
  let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
  let client = RedisClient::new(config);

  // run a function when the connection closes unexpectedly
  tokio::spawn(client.on_error().for_each(|e| async move {
    println!("Client received connection error: {:?}", e);
  }));
  // run a function whenever the client reconnects
  tokio::spawn(client.on_reconnect().for_each(move |client| async move {
    println!("Client {} reconnected.", client.id());
    Ok(())
  }));

  let _ = client.connect(Some(policy));
  let _ = client.wait_for_connect().await?;

  // declare types on response values
  let foo: Option<String> = client.get("foo").await?;
  println!("Foo: {:?}", foo);

  let _: () = client
    .set("foo", "bar", Some(Expiration::EX(1)), Some(SetOptions::NX), false)
    .await?;

  // or use turbofish. the first type is always the response type.
  println!("Foo: {:?}", client.get::<String, _>("foo").await?);

  // update performance config options as needed
  client.update_perf_config(PerformanceConfig {
    max_command_attempts: 100,
    ..Default::default()
  });

  let _ = client.quit().await?;
  Ok(())
}
