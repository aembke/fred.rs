use fred::{globals, prelude::*};

#[cfg(feature = "custom-reconnect-errors")]
use globals::ReconnectError;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  // note: in fred v5 the majority of the performance options were moved from the globals to the `RedisConfig`
  let config = RedisConfig::default();
  let client = RedisClient::new(config);

  let jh = client.connect(None);
  if let Err(error) = client.wait_for_connect().await {
    println!("Client failed to connect with error: {:?}", error);
  }

  globals::set_sentinel_connection_timeout_ms(10_000);
  #[cfg(feature = "blocking-encoding")]
  globals::set_blocking_encode_threshold(10_000_000);
  #[cfg(feature = "custom-reconnect-errors")]
  globals::set_custom_reconnect_errors(vec![
    ReconnectError::ClusterDown,
    ReconnectError::MasterDown,
    ReconnectError::ReadOnly,
  ]);

  // do stuff...

  let _ = jh.await;
  Ok(())
}
