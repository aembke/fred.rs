use fred::globals;
use fred::prelude::*;
use futures::stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig::default();
  let client = RedisClient::new(config);

  let jh = client.connect(None);
  if let Err(error) = client.wait_for_connect().await {
    println!("Client failed to connect with error: {:?}", error);
  }

  globals::set_feed_count(500);
  globals::set_cluster_error_cache_delay_ms(100);
  globals::set_min_backpressure_time_ms(20);
  globals::set_default_command_timeout(30_000);
  globals::set_max_command_attempts(5);
  globals::set_backpressure_count(100);

  #[cfg(feature = "blocking-encoding")]
  globals::set_blocking_encode_threshold(10_000_000);

  // do stuff...

  let _ = jh.await;
  Ok(())
}
