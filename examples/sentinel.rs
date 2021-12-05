use fred::prelude::*;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig {
    server: ServerConfig::Sentinel {
      // the name of the service, as configured in the sentinel configuration
      service_name: "my-service-name".into(),
      // the known host/port tuples for the sentinel nodes
      // the client will automatically update these if sentinels are added or removed
      hosts: vec![
        ("localhost".into(), 26379),
        ("localhost".into(), 26380),
        ("localhost".into(), 26381),
      ],
      // note: by default sentinel nodes use the same authentication settings as the redis servers, however
      // callers can also use the `sentinel-auth` feature to use different credentials to sentinel nodes
      #[cfg(feature = "sentinel-auth")]
      username: None,
      #[cfg(feature = "sentinel-auth")]
      password: None,
    },
    // sentinels should use the same TLS settings as the Redis servers
    ..Default::default()
  };

  let client = RedisClient::new(config);
  let policy = ReconnectPolicy::default();
  let _ = client.connect(Some(policy));
  let _ = client.wait_for_connect().await?;

  // add a sentinel node...

  // force update the sentinel node list if needed
  let _ = client.update_sentinel_nodes().await?;
  println!("New sentinel nodes: {:?}", client.client_config().server.hosts());

  let _ = client.quit().await?;
  Ok(())
}
