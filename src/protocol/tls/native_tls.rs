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

pub use native_tls;
pub type ConfigBuilder = native_tls::TlsConnectorBuilder;

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