use fred::{prelude::*, types::RespVersion};

#[cfg(feature = "partial-tracing")]
use fred::tracing::Level;
#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use fred::types::TlsConfig;
#[cfg(feature = "partial-tracing")]
use fred::types::TracingConfig;

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
  };
  // see the Builder interface for more information
  let client = Builder::from_config(config).build()?;
  // or use default values
  let client = Builder::default_centralized().build()?;

  // spawn tasks that listen for connection events
  let error_jh = client.on_error(|error| {
    println!("Client disconnected with error: {:?}", error);
    Ok(())
  });
  let reconnect_jh = client.on_error(|server| {
    println!("Client reconnected to {:?}", server);
    Ok(())
  });
  // or use the broadcast receivers directly
  let _reconnect_rx = client.reconnect_rx();

  let connection_jh = client.connect();
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
  // calling quit ends the connection and event listener tasks
  let _ = connection_jh.await;
  let _ = error_jh.await;
  let _ = reconnect_jh.await;
  Ok(())
}
