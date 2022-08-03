use std::net::SocketAddr;
pub use tokio_rustls::rustls::{ClientConfig, SupportedCipherSuite, SupportedKxGroup, SupportedProtocolVersion, RootCertStore, client::ServerCertVerifier};
use super::{should_disable_host_verification, should_disable_cert_verification};
use crate::types::RedisConfig;
use parking_lot::RwLock;
use crate::error::{RedisError, RedisErrorKind};
use tokio_rustls::TlsConnector;
use crate::protocol::connection::RedisTransport;
use std::fmt;
use std::sync::Arc;

pub struct TlsConfig {
  pub cipher_suites: Option<Vec<SupportedCipherSuite>>,
  pub kx_groups: Option<Vec<&'static SupportedKxGroup>>,
  pub protocol_versions: Option<Vec<&'static SupportedProtocolVersion>>,
  pub root_store: Option<RootCertStore>,
  //pub cert_verifier: Option<Arc<dyn ServerC>>

}

impl Default for TlsConfig {
  fn default() -> Self {
    unimplemented!()
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
  let mut builder = tokio_rustls::rustls::ClientConfig::builder();

  if should_disable_cert_verification() {
      unimplemented!()
  }
  if should_disable_host_verification() {
    unimplemented!()
  }

  if let Some(ref config) = config.read().tls {

  }

  unimplemented!()
}

pub async fn create_tls_transport(addr: SocketAddr) -> Result<RedisTransport, RedisError> {
  unimplemented!()
}