#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use fred::{
  prelude::*,
  types::{BackpressureConfig, BackpressurePolicy, UnresponsiveConfig},
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = Builder::default_centralized()
    .with_performance_config(|config| {
      config.max_feed_count = 100;
      config.auto_pipeline = true;
      config.broadcast_channel_capacity = 48;
      // allow up to 25000 in-flight commands per connection
      config.backpressure = BackpressureConfig {
        disable_auto_backpressure: false,
        max_in_flight_commands: 25_000,
        policy: BackpressurePolicy::Drain,
      }
    })
    .with_connection_config(|config| {
      config.tcp = TcpConfig {
        nodelay: Some(true),
        ..Default::default()
      };
      config.max_command_attempts = 5;
      config.max_redirections = 5;
      config.internal_command_timeout = Duration::from_secs(2);
      config.connection_timeout = Duration::from_secs(10);
      // check every 3 seconds for connections that have been waiting on a response for more than 10 seconds
      config.unresponsive = UnresponsiveConfig {
        max_timeout: Some(Duration::from_secs(10)),
        interval: Duration::from_secs(3)
      };
      config.auto_client_setname = true;
      config.reconnect_on_auth_error = true;
    })
    // use exponential backoff, starting at 100 ms and doubling on each failed attempt up to 30 sec
    .set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2))
    .build()?;
  let _ = client.connect();
  client.wait_for_connect().await?;

  // run all event listener functions in one task
  let events_task = client.on_any(
    |error| {
      println!("Connection error: {:?}", error);
      Ok(())
    },
    |server| {
      println!("Reconnected to {:?}", server);
      Ok(())
    },
    |changes| {
      println!("Cluster changed: {:?}", changes);
      Ok(())
    },
  );

  // update performance config options
  let mut perf_config = client.perf_config();
  perf_config.max_feed_count = 1000;
  client.update_perf_config(perf_config);

  // overwrite configuration options on individual commands
  let options = Options {
    max_attempts: Some(5),
    max_redirections: Some(5),
    timeout: Some(Duration::from_secs(10)),
    ..Default::default()
  };
  let _: Option<String> = client.with_options(&options).get("foo").await?;

  // apply custom options to a pipeline
  let pipeline = client.pipeline().with_options(&options);
  pipeline.get("foo").await?;
  pipeline.get("bar").await?;
  let (_, _): (Option<i64>, Option<i64>) = pipeline.all().await?;

  // reuse pipelines
  let pipeline = client.pipeline();
  pipeline.incr("foo").await?;
  pipeline.incr("foo").await?;
  assert_eq!(pipeline.last::<i64>().await?, 2);
  assert_eq!(pipeline.last::<i64>().await?, 4);
  assert_eq!(pipeline.last::<i64>().await?, 6);

  // interact with specific cluster nodes
  if client.is_clustered() {
    // discover connections via the active connection map
    let _connections = client.active_connections().await?;
    // or use the cached cluster state from `CLUSTER SLOTS`
    let connections = client
      .cached_cluster_state()
      .map(|state| state.unique_primary_nodes())
      .unwrap_or_default();

    for server in connections.into_iter() {
      let info: String = client.with_cluster_node(&server).client_info().await?;
      println!("Client info for {}: {}", server, info);
    }
  }

  // the `RedisValue` type also works as quick way to discover the type signature of a complicated response:
  println!(
    "{:?}",
    client
      .xreadgroup::<RedisValue, _, _, _, _>("foo", "bar", None, None, false, "baz", ">")
      .await?
  );

  client.quit().await?;
  let _ = events_task.await;
  Ok(())
}
