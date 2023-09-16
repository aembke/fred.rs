use fred::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let client = Builder::default_centralized()
    .with_performance_config(|config| {
      config.max_feed_count = 1000;
      config.auto_pipeline = true;
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
    })
    // use exponential backoff, starting at 100 ms and doubling on each failed attempt up to 30 sec
    .set_policy(ReconnectPolicy::new_exponential(0, 100, 30_000, 2))
    .build()?;
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

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
  let _: () = pipeline.get("foo").await?;
  let _: () = pipeline.get("bar").await?;
  let (_, _): (Option<i64>, Option<i64>) = pipeline.all().await?;

  // interact with specific cluster nodes
  if client.is_clustered() {
    let connections = client.active_connections().await?;

    for server in connections.into_iter() {
      let info: String = client.with_cluster_node(&server).client_info().await?;
      println!("Client info for {}: {}", server, info);

      // or combine client wrappers
      let _: () = client.with_cluster_node(&server).with_options(&options).ping().await?;
    }
  }

  let _ = client.quit().await?;
  let _ = events_task.await;
  Ok(())
}
