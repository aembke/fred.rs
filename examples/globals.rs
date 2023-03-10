use fred::{globals, prelude::*};

#[cfg(feature = "custom-reconnect-errors")]
use globals::ReconnectError;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  globals::set_sentinel_connection_timeout_ms(10_000);
  #[cfg(feature = "blocking-encoding")]
  globals::set_blocking_encode_threshold(10_000_000);
  #[cfg(feature = "custom-reconnect-errors")]
  globals::set_custom_reconnect_errors(vec![
    ReconnectError::ClusterDown,
    ReconnectError::MasterDown,
    ReconnectError::ReadOnly,
  ]);

  // ...

  Ok(())
}
