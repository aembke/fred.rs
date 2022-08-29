use fred::prelude::*;
use fred::types::TlsConfig;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig {
    // or use Some(TlsConfig::default()) to use default TLS settings
    tls: Some(TlsConfig {
      root_certs: None,
      min_protocol_version: None,
      max_protocol_version: None,
      disable_built_in_roots: false,
      use_sni: true,
    }),
    ..RedisConfig::default()
  };
  let client = RedisClient::new(config);

  let jh = client.connect(None);
  if let Err(error) = client.wait_for_connect().await {
    println!("Client failed to connect with error: {:?}", error);
  }

  // do stuff...

  let _ = jh.await;
  Ok(())
}
