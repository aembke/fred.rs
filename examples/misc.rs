use fred::{
  prelude::*,
  types::{BackpressureConfig, BackpressurePolicy, PerformanceConfig},
};

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  // full configuration for performance tuning options
  let perf = PerformanceConfig {
    // whether or not to automatically pipeline commands across tasks
    auto_pipeline:                                             true,
    // the max number of frames to feed into a socket before flushing it
    max_feed_count:                                            1000,
    // a default timeout to apply to all commands (0 means no timeout)
    default_command_timeout_ms:                                0,
    // the amount of time to wait before rebuilding the client's cached cluster state after a MOVED error.
    cluster_cache_update_delay_ms:                             10,
    // the maximum number of times to retry commands
    max_command_attempts:                                      3,
    // backpressure config options
    backpressure:                                              BackpressureConfig {
      // whether to disable automatic backpressure features
      disable_auto_backpressure: false,
      // the max number of in-flight commands before applying backpressure or returning backpressure errors
      max_in_flight_commands:    5000,
      // the policy to apply when the max in-flight commands count is reached
      policy:                    BackpressurePolicy::Drain,
    },
    // the amount of time a command can wait in memory without a response before the connection is considered
    // unresponsive
    #[cfg(feature = "check-unresponsive")]
    network_timeout_ms:                                        60_000,
  };
  let config = RedisConfig {
    server: ServerConfig::default_clustered(),
    ..RedisConfig::default()
  };

  let client = RedisClient::new(config, Some(perf), None);
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  // update performance config options
  let mut perf_config = client.perf_config();
  perf_config.max_command_attempts = 100;
  perf_config.max_feed_count = 1000;
  client.update_perf_config(perf_config);

  // interact with specific cluster nodes
  if client.is_clustered() {
    let connections = client.active_connections().await?;

    for server in connections.into_iter() {
      let info: String = client.with_cluster_node(&server).client_info().await?;
      println!("Client info for {}: {}", server, info);
    }
  }

  let _ = client.quit().await?;
  Ok(())
}
