use fred::{
  monitor::{self, Config},
  prelude::*,
};
use futures::stream::StreamExt;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  pretty_env_logger::init();

  let monitor_jh = tokio::spawn(async move {
    let config = Config::default();
    let mut monitor_stream = monitor::run(config).await?;

    while let Some(command) = monitor_stream.next().await {
      // the Display trait prints results in the same format as redis-cli
      println!("{}", command);
    }

    Ok::<(), RedisError>(())
  });

  let config = RedisConfig::default();
  let client = RedisClient::new(config);
  let _ = client.connect(None);
  if let Err(error) = client.wait_for_connect().await {
    println!("Client failed to connect with error: {:?}", error);
  }

  for idx in 0 .. 50 {
    let _ = client.set("foo", idx, Some(Expiration::EX(10)), None, false).await?;
  }
  let _ = client.quit().await?;

  sleep(Duration::from_secs(1)).await;
  monitor_jh.abort();
  Ok(())
}
