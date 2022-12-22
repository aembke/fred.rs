use fred::{prelude::*, types::Server};

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig {
    server: ServerConfig::Sentinel {
      // the name of the service, as configured in the sentinel configuration
      service_name:                               "my-service-name".into(),
      // the known host/port tuples for the sentinel nodes
      // the client will automatically update these if sentinels are added or removed
      hosts:                                      vec![
        Server::new("localhost", 26379),
        Server::new("localhost", 26380),
        Server::new("localhost", 26381),
      ],
      // note: by default sentinel nodes use the same authentication settings as the redis servers, however
      // callers can also use the `sentinel-auth` feature to use different credentials to sentinel nodes
      #[cfg(feature = "sentinel-auth")]
      username:                                   None,
      #[cfg(feature = "sentinel-auth")]
      password:                                   None,
    },
    // sentinels should use the same TLS settings as the Redis servers
    ..Default::default()
  };

  let policy = ReconnectPolicy::default();
  let client = RedisClient::new(config, None, Some(policy));
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  // do stuff

  let _ = client.quit().await?;
  Ok(())
}
