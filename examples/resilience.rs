use fred::globals::{self, ReconnectError};
use fred::prelude::*;
use futures::StreamExt;

const DATABASE: u8 = 2;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  // try to write commands up to 20 times. this value is incremented for a command whenever the connection closes while the
  // command is in-flight or whenever the client receives a MOVED error in response to the command. in the case of a MOVED
  // error the client will not try the command again until the hash slot is finished migrating to the destination node.
  globals::set_max_command_attempts(20);
  // configure the amount of time to wait when checking the state of migrating hash slots
  globals::set_cluster_error_cache_delay_ms(100);
  // apply a global timeout on commands, if necessary. otherwise the client will attempt to write them until the max attempt
  // count is reached (however long that takes depends on the reconnect policy, etc).
  globals::set_default_command_timeout(60_000);
  // automatically trigger reconnection and retry logic whenever errors are received with the provided prefixes
  globals::set_custom_reconnect_errors(vec![
    ReconnectError::Loading,
    ReconnectError::ClusterDown,
    ReconnectError::ReadOnly,
  ]);

  let config = RedisConfig {
    // apply reconnection logic on the first connection attempt instead of returning initial connection errors to the caller.
    // if you use this feature make sure your server config is correct or you wont see errors until the reconnection policy
    // max attempts value is reached (unless certain logging is enabled).
    fail_fast: false,
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
    // set up db selection logic to run whenever we reconnect
    let _ = client.select(DATABASE).await;
    // if using pubsub features then SUBSCRIBE calls should go here too
    // if it's necessary to change users then AUTH calls should go here too
  }));

  let _ = client.connect(Some(policy));
  let _ = client.wait_for_connect().await?;

  // do stuff...

  let _ = client.quit().await?;
  Ok(())
}
