#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use fred::{globals, prelude::*};

#[cfg(feature = "custom-reconnect-errors")]
use globals::ReconnectError;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  globals::set_default_broadcast_channel_capacity(64);

  #[cfg(feature = "blocking-encoding")]
  globals::set_blocking_encode_threshold(10_000_000);
  #[cfg(feature = "custom-reconnect-errors")]
  globals::set_custom_reconnect_errors(vec![
    ReconnectError::ClusterDown,
    ReconnectError::MasterDown,
    ReconnectError::ReadOnly,
  ]);
  #[cfg(feature = "check-unresponsive")]
  globals::set_unresponsive_interval_ms(1000);

  // ...

  Ok(())
}
