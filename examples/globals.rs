use fred::globals;
use fred::prelude::*;
use futures::stream::{StreamExt};


#[tokio::main]
async fn main() -> Result<(), RedisError> {
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
  tokio::spawn(client.on_reconnect().for_each(|client| async move {
    println!("Client {} reconnected.", client.id());
  }));

  let jh = client.connect(Some(policy), false);
  if let Err(error) = client.wait_for_connect().await {
    println!("Client failed to connect with error: {:?}", error);
  }

  globals::set_feed_count(500);
  globals::set_cluster_error_cache_delay_ms(100);
  globals::set_min_backpressure_time_ms(20);
  globals::set_default_command_timeout(30_000);
  globals::set_max_command_attempts(5);

  // do stuff...

  let _ = jh.await;
  Ok(())
}
