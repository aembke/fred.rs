#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use fred::{prelude::*, types::RespVersion};
#[cfg(feature = "partial-tracing")]
use fred::{tracing::Level, types::TracingConfig};

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  // create a config from a URL
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
    ..Default::default()
  };
  // see the Builder interface for more information
  let _client = Builder::from_config(config).build()?;
  // or use default values
  let client = Builder::default_centralized().build()?;
  let connection_task = client.connect();
  client.wait_for_connect().await?;

  // convert response types to most common rust types
  let foo: Option<String> = client.get("foo").await?;
  println!("Foo: {:?}", foo);

  client
    .set("foo", "bar", Some(Expiration::EX(1)), Some(SetOptions::NX), false)
    .await?;

  // or use turbofish. the first type is always the response type.
  println!("Foo: {:?}", client.get::<Option<String>, _>("foo").await?);

  client.quit().await?;
  // calling quit ends the connection and event listener tasks
  let _ = connection_task.await;
  Ok(())
}
