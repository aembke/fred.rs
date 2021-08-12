use fred::prelude::*;
use futures::stream::StreamExt;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let database = 2;
  let config = RedisConfig::Centralized {
    host: "127.0.0.1".into(),
    port: 6379,
    key: Some("your key".into()),
    tls: None,
  };
  let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
  let client = RedisClient::new(config);

  tokio::spawn(client.on_error().for_each(|e| async move {
    println!("Client received connection error: {:?}", e);
  }));
  tokio::spawn(client.on_reconnect().for_each(move |client| async move {
    println!("Client {} reconnected.", client.id());
    // select the database each time we connect or reconnect
    let _ = client.select(database).await;
  }));

  let jh = client.connect(Some(policy), false);
  let _ = client.wait_for_connect().await?;

  println!("Foo: {:?}", client.get("foo").await?);
  let _ = client
    .set("foo", "bar", Some(Expiration::PX(1000)), Some(SetOptions::NX), false)
    .await?;
  println!("Foo: {:?}", client.get("foo").await?);

  sleep(Duration::from_millis(1000)).await;
  println!("Foo: {:?}", client.get("foo").await?);

  let _ = client.quit().await?;
  let _ = jh.await;
  Ok(())
}
