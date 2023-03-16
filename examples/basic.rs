use fred::{
  prelude::*,
  types::{BackpressureConfig, BackpressurePolicy, PerformanceConfig, RespVersion, TlsConfig},
};
use futures::stream::StreamExt;
use std::{default::Default, sync::Arc};

#[cfg(feature = "mocks")]
use fred::mocks::Echo;
#[cfg(feature = "partial-tracing")]
use fred::tracing::Level;
#[cfg(feature = "partial-tracing")]
use fred::types::TracingConfig;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let _ = RedisConfig::from_url("redis://username:password@foo.com:6379/1")?;

  // full configuration with default values
  let config = RedisConfig {
    fail_fast: true,
    server: ServerConfig::new_centralized("127.0.0.1", 6379),
    blocking: Blocking::Block,
    username: None,
    password: None,
    version: RespVersion::RESP2,
    database: None,
    #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
    tls: None,
    #[cfg(feature = "partial-tracing")]
    tracing: TracingConfig {
      enabled:               false,
      default_tracing_level: Level::INFO,
      full_tracing_level:    Level::DEBUG,
    },
    #[cfg(feature = "mocks")]
    mocks: Arc::new(Echo),
  };
  // example showing a full kitchen sink configuration for performance tuning options
  let perf = PerformanceConfig {
    // whether or not to automatically pipeline commands across tasks
    auto_pipeline:                                             true,
    // the max number of frames to feed into a socket before flushing it
    max_feed_count:                                            1000,
    // a default timeout to apply to all commands (0 means no timeout)
    default_command_timeout_ms:                                0,
    // the amount of time to wait before rebuilding the client's cached cluster state after a MOVED error.
    cluster_cache_update_delay_ms:                             10,
    // the maximum number of times to retry commands
    max_command_attempts:                                      3,
    // backpressure config options
    backpressure:                                              BackpressureConfig {
      // whether to disable automatic backpressure features
      disable_auto_backpressure: false,
      // the max number of in-flight commands before applying backpressure or returning backpressure errors
      max_in_flight_commands:    5000,
      // the policy to apply when the max in-flight commands count is reached
      policy:                    BackpressurePolicy::Drain,
    },
    // the amount of time a command can wait in memory without a response before the connection is considered
    // unresponsive
    #[cfg(feature = "check-unresponsive")]
    network_timeout_ms:                                        60_000,
  };

  // configure exponential backoff when reconnecting, starting at 100 ms, and doubling each time up to 30 sec.
  let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
  let client = RedisClient::new(config, Some(perf), Some(policy));

  // spawn tasks that listen for connection close or reconnect events
  let mut error_rx = client.on_error();
  let mut reconnect_rx = client.on_reconnect();

  tokio::spawn(async move {
    while let Some(error) = error_rx.recv().await {
      println!("Client disconnected with error: {:?}", error);
    }
  });
  tokio::spawn(async move {
    while let Some(_) = reconnect_rx.recv().await {
      println!("Client reconnected.");
    }
  });

  let connection_task = client.connect();
  let _ = client.wait_for_connect().await?;

  // convert response types to most common rust types
  let foo: Option<String> = client.get("foo").await?;
  println!("Foo: {:?}", foo);

  let _: () = client
    .set("foo", "bar", Some(Expiration::EX(1)), Some(SetOptions::NX), false)
    .await?;

  // or use turbofish. the first type is always the response type.
  println!("Foo: {:?}", client.get::<String, _>("foo").await?);

  // update performance config options as needed
  let mut perf_config = client.perf_config();
  perf_config.max_command_attempts = 100;
  perf_config.max_feed_count = 1000;
  client.update_perf_config(perf_config);

  // send commands in a pipeline
  let pipeline = client.pipeline();
  let _ = pipeline.incr("bar").await?;
  let _ = pipeline.incr("bar").await?;
  let (first, second): (i64, i64) = pipeline.all().await?;
  assert_eq!((first, second), (1, 2));

  let _ = client.quit().await?;
  let _ = connection_task.await;
  Ok(())
}
