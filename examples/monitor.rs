#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use fred::{monitor, prelude::*};
use futures::stream::StreamExt;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let monitor_jh = tokio::spawn(async move {
    let config = RedisConfig::default();
    let mut monitor_stream = monitor::run(config).await?;

    while let Some(command) = monitor_stream.next().await {
      // the Display implementation prints results in the same format as redis-cli
      println!("{}", command);
    }

    Ok::<(), RedisError>(())
  });

  let client = RedisClient::default();
  let _ = client.connect();
  if let Err(error) = client.wait_for_connect().await {
    println!("Client failed to connect with error: {:?}", error);
  }

  for idx in 0 .. 50 {
    let _ = client.set("foo", idx, Some(Expiration::EX(10)), None, false).await?;
  }
  let _ = client.quit().await?;

  // wait a bit for the monitor stream to catch up
  sleep(Duration::from_secs(1)).await;
  monitor_jh.abort();
  Ok(())
}
