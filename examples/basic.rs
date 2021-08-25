use fred::prelude::*;
use futures::stream::StreamExt;
use std::time::Duration;
use tokio::time::sleep;

const DATABASE: u8 = 2;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  // example showing full config options
  let config = RedisConfig {
    // whether to skip reconnect logic when first connecting
    fail_fast: true,
    // server configuration
    server: ServerConfig::new_centralized("127.0.0.1", 6379),
    // whether to automatically pipeline commands
    pipeline: true,
    // how to handle commands sent while a connection is blocked
    blocking: Blocking::Block,
    // an optional username, if using ACL rules
    username: None,
    // an optional authentication key
    password: None,
    // optional TLS settings
    tls: None,
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
    // select the database each time we connect or reconnect
    let _ = client.select(DATABASE).await;
  }));

  let _ = client.connect(Some(policy));
  let _ = client.wait_for_connect().await?;

  println!("Foo: {:?}", client.get("foo").await?);
  let _ = client
    .set("foo", "bar", Some(Expiration::EX(1)), Some(SetOptions::NX), false)
    .await?;
  println!("Foo: {:?}", client.get("foo").await?);

  sleep(Duration::from_millis(1000)).await;
  println!("Foo: {:?}", client.get("foo").await?);

  let _ = client.quit().await?;
  Ok(())
}
