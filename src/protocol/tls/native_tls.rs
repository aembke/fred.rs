use std::fmt;
use crate::error::{RedisError, RedisErrorKind};
use crate::types::RedisConfig;
use native_tls::TlsConnector as NativeTlsConnector;
use parking_lot::RwLock;
use tokio_native_tls::TlsConnector;
use std::net::SocketAddr;
use crate::protocol::tls::{should_disable_cert_verification, should_disable_host_verification};
pub use native_tls::{Certificate, Protocol, Identity};
use crate::protocol::connection::RedisTransport;

/// Configuration for Tls connections with the `native-tls` crate.
///
/// See the [native-tls docs](https://docs.rs/tokio-native-tls/*/tokio_native_tls/native_tls/struct.TlsConnectorBuilder.html) for more information.
#[derive(Clone)]
pub struct TlsConfig {
  pub root_certs: Option<Vec<Certificate>>,
  pub min_protocol_version: Option<Protocol>,
  pub max_protocol_version: Option<Protocol>,
  pub disable_built_in_roots: bool,
  pub use_sni: bool,
  pub identity: Option<Identity>
}

impl Default for TlsConfig {
  fn default() -> Self {
    TlsConfig {
      root_certs: None,
      min_protocol_version: None,
      max_protocol_version: None,
      identity: None,
      disable_built_in_roots: false,
      use_sni: true,
    }
  }
}

impl PartialEq for TlsConfig {
  fn eq(&self, _other: &Self) -> bool {
    true
  }
}

impl Eq for TlsConfig {}

impl fmt::Debug for TlsConfig {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[Tls Config]")
  }
}

pub fn create_tls_connector(config: &RwLock<RedisConfig>) -> Result<TlsConnector, RedisError> {
  let mut builder = NativeTlsConnector::builder();

  if should_disable_cert_verification() {
    builder.danger_accept_invalid_certs(true);
  }
  if should_disable_host_verification() {
    builder.danger_accept_invalid_hostnames(true);
  }

  if let Some(ref config) = config.read().tls {
    if let Some(ref root_certs) = config.root_certs {
      for cert in root_certs.iter() {
        builder.add_root_certificate(cert.clone());
      }
    }
    if !config.use_sni {
      builder.use_sni(false);
    }
    if config.disable_built_in_roots {
      builder.disable_built_in_roots(true);
    }
    if let Some(ref protocol) = config.min_protocol_version {
      builder.min_protocol_version(Some(protocol.clone()));
    }
    if let Some(ref protocol) = config.max_protocol_version {
      builder.max_protocol_version(Some(protocol.clone()));
    }
    if let Some(ref identity) = config.identity {
      builder.identity(identity.clone());
    }
  }

  builder
    .build()
    .map(|t| TlsConnector::from(t))
    .map_err(|e| RedisError::new(RedisErrorKind::Tls, format!("{:?}", e)))
}

pub async fn create_tls_transport(addr: SocketAddr) -> Result<RedisTransport, RedisError> {
  unimplemented!()
}