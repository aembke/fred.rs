use fred::{
  globals::{self, ReconnectError},
  prelude::*,
  types::{InfoKind::Default, PerformanceConfig},
};
use futures::StreamExt;

const DATABASE: u8 = 2;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  // automatically trigger reconnection and retry logic whenever errors are received with the provided prefixes
  globals::set_custom_reconnect_errors(vec![
    ReconnectError::Loading,
    ReconnectError::ClusterDown,
    ReconnectError::ReadOnly,
  ]);

  let config = RedisConfig {
    // apply reconnection logic on the first connection attempt instead of returning initial connection errors to the
    // caller. if you use this feature make sure your server config is correct or you wont see errors until the
    // reconnection policy max attempts value is reached (unless certain logging is enabled).
    fail_fast: false,
    performance: PerformanceConfig {
      // try to write commands up to 20 times. this value is incremented for a command whenever the connection closes
      // while the command is in-flight, or whenever the client receives a MOVED/ASK error in response to the
      // command. in the case of a MOVED/ASK error the client will not try the command again until the hash slot
      // is finished migrating to the destination node.
      max_command_attempts: 20,
      // configure the amount of time to wait when checking the state of migrating hash slots.
      cluster_cache_update_delay_ms: 100,
      // apply a global timeout on commands, if necessary. otherwise the client will attempt to write them until the
      // max attempt count is reached (however long that takes depends on the reconnect policy, etc).
      default_command_timeout_ms: 60_000,
      ..Default::default()
    },
    ..Default::default()
  };
  // configure exponential backoff when reconnecting, starting at 100 ms, and doubling each time up to 30 sec.
  // the max_attempts value here is 0, meaning the client will attempt to reconnect forever. the reconnection
  // attempt count is reset whenever the client successfully reconnects.
  let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
  let client = RedisClient::new(config);

  // run a function when the connection closes unexpectedly
  tokio::spawn(client.on_error().for_each(|e| async move {
    println!("Client received connection error: {:?}", e);
  }));
  // run a function whenever the client reconnects
  tokio::spawn(client.on_reconnect().for_each(move |client| async move {
    println!("Client {} reconnected.", client.id());
    // (re)subscribe to any pubsub channels upon connecting or reconnecting.
    let _ = client.subscribe("foo").await?;
    // it is recommended to use the `database`, `username`, and `password` fields on the `RedisConfig` instead of
    // manually adding SELECT or AUTH/HELLO calls to this block. in-flight commands will retry before commands
    // specified in this block, which may lead to some difficult bugs. however, the client will automatically
    // authenticate and select the correct database first if the associated configuration options are provided.
  }));

  let _ = client.connect(Some(policy));
  let _ = client.wait_for_connect().await?;

  // do stuff...

  let _ = client.quit().await?;
  Ok(())
}
