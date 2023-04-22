use fred::{prelude::*, types::RespVersion};

#[cfg(feature = "partial-tracing")]
use fred::tracing::Level;
#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use fred::types::TlsConfig;
#[cfg(feature = "partial-tracing")]
use fred::types::TracingConfig;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  pretty_env_logger::init();

  let _ = RedisConfig::from_url("redis://username:password@foo.com:6379/1")?;
  // full configuration with testing values
  let config = RedisConfig {
    fail_fast: true,
    server: ServerConfig::new_centralized("redis-main", 6379),
    blocking: Blocking::Block,
    username: Some("foo".into()),
    password: Some("bar".into()),
    version: RespVersion::RESP2,
    database: None,
    #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
    tls: None,
    #[cfg(feature = "partial-tracing")]
    tracing: TracingConfig {
      enabled:                                             false,
      default_tracing_level:                               Level::INFO,
      #[cfg(feature = "full-tracing")]
      full_tracing_level:                                  Level::DEBUG,
    },
  };

  // configure exponential backoff when reconnecting, starting at 100 ms, and doubling each time up to 30 sec.
  let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
  let perf = PerformanceConfig::default();
  let client = RedisClient::new(config, Some(perf), Some(policy));

  // spawn tasks that listen for connection close or reconnect events
  let mut error_rx = client.on_error();
  let mut reconnect_rx = client.on_reconnect();

  tokio::spawn(async move {
    while let Ok(error) = error_rx.recv().await {
      println!("Client disconnected with error: {:?}", error);
    }
  });
  tokio::spawn(async move {
    while reconnect_rx.recv().await.is_ok() {
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

  let _ = client.quit().await?;
  let _ = connection_task.await;
  Ok(())
}
