use fred::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

static COUNT: usize = 50;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  pretty_env_logger::init();

  let config = RedisConfig::default();
  let publisher_client = RedisClient::new(config.clone());
  let subscriber_client = RedisClient::new(config.clone());

  let _ = publisher_client.connect(None);
  let _ = subscriber_client.connect(None);
  let _ = publisher_client.wait_for_connect().await?;
  let _ = subscriber_client.wait_for_connect().await?;

  let subscriber_jh = tokio::spawn(async move {
    let result: RedisValue = match subscriber_client.blpop("foo", 5.0).await {
      Ok(r) => {
        println!("HERE {:?}", r);
        r
      }
      Err(e) => {
        println!("{:?}", e);
        RedisValue::Null
      }
    };

    /*while let Ok(RedisValue::String(result)) = subscriber_client.blpop("foo", 5.0).await {
      println!("Blocking pop result: {:?}", result);
    }*/

    Ok::<(), RedisError>(())
  });

  for _ in 0..COUNT {
    let _ = publisher_client.rpush("foo", "bar").await?;
    sleep(Duration::from_millis(1000)).await;
  }

  let _ = subscriber_jh.abort();
  Ok(())
}
