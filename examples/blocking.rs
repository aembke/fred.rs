use fred::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

static COUNT: i64 = 50;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  pretty_env_logger::init();

  let publisher_client = RedisClient::default();
  let subscriber_client = RedisClient::default();

  let _ = publisher_client.connect();
  let _ = subscriber_client.connect();
  let _ = publisher_client.wait_for_connect().await?;
  let _ = subscriber_client.wait_for_connect().await?;

  #[allow(unreachable_code)]
  let subscriber_jh = tokio::spawn(async move {
    loop {
      let (key, value): (String, i64) = if let Some(result) = subscriber_client.blpop("foo", 5.0).await? {
        result
      } else {
        // retry after a timeout
        continue;
      };

      println!("BLPOP result on {}: {}", key, value);
    }

    Ok::<(), RedisError>(())
  });

  for idx in 0 .. COUNT {
    let _ = publisher_client.rpush("foo", idx).await?;
    sleep(Duration::from_secs(1)).await;
  }

  let _ = subscriber_jh.abort();
  Ok(())
}
