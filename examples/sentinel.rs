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
    },
    // sentinels should use the same TLS and authentication settings as the Redis servers
    ..Default::default()
  };

  let client = RedisClient::new(config);
  let _ = client.connect(Some(policy));
  let _ = client.wait_for_connect().await?;

  // add a sentinel node...

  println!("New sentinel nodes: {:?}", client.client_config().server.hosts());

  let _ = client.quit().await?;
  Ok(())
}
