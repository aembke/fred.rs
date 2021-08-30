use std::fmt;

#[cfg(feature = "enable-tls")]
use crate::error::{RedisError, RedisErrorKind};
#[cfg(feature = "enable-tls")]
use crate::types::RedisConfig;
#[cfg(feature = "enable-tls")]
use native_tls::{Certificate, Protocol, TlsConnector as NativeTlsConnector};
#[cfg(feature = "enable-tls")]
use parking_lot::RwLock;
#[cfg(feature = "enable-tls")]
use std::env;
#[cfg(feature = "enable-tls")]
use tokio_native_tls::TlsConnector;

#[cfg(feature = "enable-tls")]
pub fn should_disable_cert_verification() -> bool {
  match env::var_os("FRED_DISABLE_CERT_VERIFICATION") {
    Some(s) => match s.into_string() {
      Ok(s) => match s.as_ref() {
        "1" | "true" | "TRUE" => true,
        _ => false,
      },
      Err(_) => false,
    },
    None => false,
  }
}

#[cfg(feature = "enable-tls")]
pub fn should_disable_host_verification() -> bool {
  match env::var_os("FRED_DISABLE_HOST_VERIFICATION") {
    Some(s) => match s.into_string() {
      Ok(s) => match s.as_ref() {
        "1" | "true" | "TRUE" => true,
        _ => false,
      },
      Err(_) => false,
    },
    None => false,
  }
}

/// Configuration for Tls Connections
///
/// See <https://docs.rs/tokio-native-tls/0.3.0/tokio_native_tls/native_tls/struct.TlsConnectorBuilder.html> for more information.
#[cfg(feature = "enable-tls")]
#[derive(Clone)]
pub struct TlsConfig {
  pub root_certs: Option<Vec<Certificate>>,
  pub min_protocol_version: Option<Protocol>,
  pub max_protocol_version: Option<Protocol>,
  pub disable_built_in_roots: bool,
  pub use_sni: bool,
}

#[cfg(feature = "enable-tls")]
impl Default for TlsConfig {
  fn default() -> Self {
    TlsConfig {
      root_certs: None,
      min_protocol_version: None,
      max_protocol_version: None,
      disable_built_in_roots: false,
      use_sni: true,
    }
  }
}

/// Configuration for Tls Connections
///
/// See https://docs.rs/tokio-native-tls/0.3.0/tokio_native_tls/native_tls/struct.TlsConnectorBuilder.html for more information.
#[cfg(not(feature = "enable-tls"))]
#[derive(Clone)]
pub struct TlsConfig;

#[cfg(not(feature = "enable-tls"))]
impl Default for TlsConfig {
  fn default() -> Self {
    TlsConfig
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

#[cfg(feature = "enable-tls")]
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
  }

  builder
    .build()
    .map(|t| TlsConnector::from(t))
    .map_err(|e| RedisError::new(RedisErrorKind::Tls, format!("{:?}", e)))
}
