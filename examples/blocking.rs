use fred::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

static COUNT: i64 = 50;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  pretty_env_logger::init();

  let config = RedisConfig::default();
  let publisher_client = RedisClient::new(config.clone(), None, None);
  let subscriber_client = RedisClient::new(config.clone(), None, None);

  let _ = publisher_client.connect();
  let _ = subscriber_client.connect();
  let _ = publisher_client.wait_for_connect().await?;
  let _ = subscriber_client.wait_for_connect().await?;

  let subscriber_jh = tokio::spawn(async move {
    while let Ok((key, value)) = subscriber_client.blpop::<(String, i64), _>("foo", 5.0).await {
      println!("Blocking pop result on {}: {}", key, value);
    }

    Ok::<(), RedisError>(())
  });

  for idx in 0 .. COUNT {
    let _ = publisher_client.rpush("foo", idx).await?;
    sleep(Duration::from_millis(1000)).await;
  }

  let _ = subscriber_jh.abort();
  Ok(())
}
