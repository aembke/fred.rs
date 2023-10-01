#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use fred::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  pretty_env_logger::init();

  let publisher_client = RedisClient::default();
  let subscriber_client = RedisClient::default();

  let _ = publisher_client.connect();
  let _ = subscriber_client.connect();
  publisher_client.wait_for_connect().await?;
  subscriber_client.wait_for_connect().await?;

  let subscriber_jh = tokio::spawn(async move {
    loop {
      let (key, value): (String, i64) = match subscriber_client.blpop("foo", 5.0).await.ok() {
        Some(value) => value,
        None => continue,
      };

      println!("BLPOP result on {}: {}", key, value);
    }
  });

  for idx in 0 .. 30 {
    publisher_client.rpush("foo", idx).await?;
    sleep(Duration::from_secs(1)).await;
  }

  subscriber_jh.abort();
  Ok(())
}
