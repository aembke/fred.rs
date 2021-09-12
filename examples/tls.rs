use fred::prelude::*;
use fred::types::TlsConfig;
use futures::stream::StreamExt;

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
  let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
  let client = RedisClient::new(config);

  tokio::spawn(client.on_error().for_each(|e| async move {
    println!("Client received connection error: {:?}", e);
  }));
  tokio::spawn(client.on_reconnect().for_each(|client| async move {
    println!("Client {} reconnected.", client.id());
  }));

  let jh = client.connect(Some(policy));
  if let Err(error) = client.wait_for_connect().await {
    println!("Client failed to connect with error: {:?}", error);
  }

  // do stuff...

  let _ = jh.await;
  Ok(())
}
