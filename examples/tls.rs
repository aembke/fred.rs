#![allow(clippy::disallowed_names)]
#![allow(clippy::let_underscore_future)]

use fred::prelude::*;

#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use fred::types::TlsConnector;

#[cfg(feature = "enable-native-tls")]
fn create_tls_config() -> TlsConnector {
  use fred::native_tls::TlsConnector as NativeTlsConnector;

  // or use `TlsConnector::default_native_tls()`
  NativeTlsConnector::builder()
    .use_sni(true)
    .danger_accept_invalid_certs(false)
    .danger_accept_invalid_certs(false)
    .build()
    .expect("Failed to create TLS config")
    .into()
}

#[cfg(all(feature = "enable-rustls", not(feature = "enable-native-tls")))]
fn create_tls_config() -> TlsConnector {
  use fred::rustls::{ClientConfig, RootCertStore};

  // or use `TlsConnector::default_rustls()`
  ClientConfig::builder()
    .with_root_certificates(RootCertStore::empty())
    .with_no_client_auth()
    .into()
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig {
    #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
    tls: Some(create_tls_config().into()),
    ..RedisConfig::default()
  };
  let client = Builder::from_config(config).build()?;
  client.init().await?;

  // ...

  client.quit().await?;
  Ok(())
}
