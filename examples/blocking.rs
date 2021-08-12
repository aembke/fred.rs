use fred::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

static COUNT: usize = 50;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default_centralized();
  let publisher_client = RedisClient::new(config.clone());
  let subscriber_client = RedisClient::new(config.clone());

  let _ = publisher_client.connect(None, false);
  let _ = subscriber_client.connect(None, false);
  let _ = publisher_client.wait_for_connect().await?;
  let _ = subscriber_client.wait_for_connect().await?;

  let subscriber_jh = tokio::spawn(async move {
    loop {
      let result = subscriber_client.blpop("foo", 5.0).await?;
      println!("Blocking pop result: {:?}", result);
    }

    Ok::<_, RedisError>(())
  });

  for _ in 0..COUNT {
    let _ = publisher_client.rpush("foo", "bar").await?;
    sleep(Duration::from_millis(1000)).await;
  }

  let _ = subscriber_jh.abort();
  Ok(())
}
