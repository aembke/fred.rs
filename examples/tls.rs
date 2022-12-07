use fred::prelude::*;

#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use fred::types::TlsConnector;

#[cfg(feature = "enable-native-tls")]
fn create_tls_config() -> TlsConnector {
  use fred::types::native_tls::TlsConnector as NativeTlsConnector;

  NativeTlsConnector::builder()
    .use_sni(true)
    .danger_accept_invalid_certs(false)
    .danger_accept_invalid_certs(false)
    .build()
    .expect("Failed to create TLS config")
    .into()
}

#[cfg(feature = "enable-rustls")]
fn create_tls_config() -> TlsConnector {
  use tokio_rustls::rustls::{
    client::WantsClientCert,
    ClientConfig as RustlsClientConfig,
    ConfigBuilder,
    RootCertStore,
  };

  RustlsClientConfig::builder()
    .with_safe_defaults()
    .with_root_certificates(RootCertStore::empty())
    .with_no_client_auth()
    .into()
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig {
    // or use `TlsConnector::default_native_tls` or `TlsConnector::default_rustls`
    #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
    tls: Some(create_tls_config().into()),
    ..RedisConfig::default()
  };
  let client = RedisClient::new(config, None, None);

  let jh = client.connect();
  if let Err(error) = client.wait_for_connect().await {
    println!("Client failed to connect with error: {:?}", error);
  }

  // do stuff...

  let _ = jh.await;
  Ok(())
}
