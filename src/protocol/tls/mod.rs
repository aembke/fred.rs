use crate::error::{RedisError, RedisErrorKind};
use crate::types::RedisConfig;
use parking_lot::RwLock;
use std::convert::TryFrom;
use std::fmt::Formatter;
use std::sync::Arc;
use std::{env, fmt};

#[cfg(feature = "enable-native-tls")]
use tokio_native_tls::native_tls::TlsConnectorBuilder;
#[cfg(feature = "enable-native-tls")]
use tokio_native_tls::TlsConnector as NativeTlsConnector;
#[cfg(feature = "enable-rustls")]
use tokio_rustls::rustls::{client::WantsClientCert, ClientConfig, ConfigBuilder};
#[cfg(feature = "enable-rustls")]
use tokio_rustls::TlsConnector as RustlsConnector;

/// TLS types and reexports from the `native-tls` crate.
#[cfg(feature = "enable-native-tls")]
pub mod native_tls;
/// TLS types and reexports from the `rustls` crate.
#[cfg(feature = "enable-rustls")]
pub mod rustls;

#[cfg_attr(docsrs, doc(cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))))]
#[derive(Clone)]
pub enum TlsConnector {
  #[cfg(feature = "enable-native-tls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
  Native(NativeTlsConnector),
  #[cfg(feature = "enable-rustls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
  Rustls(RustlsConnector),
}

impl fmt::Debug for TlsConnector {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("TlsConnector")
      .field(
        "kind",
        match self {
          #[cfg(feature = "enable-native-tls")]
          TlsConnector::Native(_) => "Native",
          #[cfg(feature = "enable-rustls")]
          TlsConnector::Rustls(_) => "Rustls",
        },
      )
      .finish()
  }
}

#[cfg_attr(docsrs, doc(cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))))]
impl TlsConnector {
  /// Create a default TLS connector from the `native-tls` module.
  ///
  /// The `FRED_DISABLE_CERT_VERIFICATION` and `FRED_DISABLE_HOST_VERIFICATION` environment variables are used here.
  #[cfg(feature = "enable-native-tls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
  pub fn default_native_tls() -> Result<Self, RedisError> {
    let mut builder = TlsConnectorBuilder::builder();

    if should_disable_cert_verification() {
      builder.danger_accept_invalid_certs(true);
    }
    if should_disable_host_verification() {
      builder.danger_accept_invalid_hostnames(true);
    }

    build.try_into()
  }

  /// Create a default TLS connector with the `rustls` module with safe defaults.
  #[cfg(feature = "enable-rustls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
  pub fn default_rustls() -> Result<Self, RedisError> {
    ClientConfig::builder()
      .with_safe_defaults()
      .with_no_client_auth()
      .into()
  }
}

#[cfg(feature = "enable-native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
impl TryFrom<TlsConnectorBuilder> for TlsConnector {
  type Error = RedisError;

  fn try_from(builder: TlsConnectorBuilder) -> Result<Self, Self::Error> {
    let connector = builder
      .build()
      .map(|t| NativeTlsConnector::from(t))
      .map_err(|e| RedisError::new(RedisErrorKind::Tls, format!("{:?}", e)))?;
    Ok(TlsConnector::Native(connector))
  }
}

#[cfg(feature = "enable-native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
impl From<NativeTlsConnector> for TlsConnector {
  fn from(connector: NativeTlsConnector) -> Self {
    TlsConnector::Native(connector)
  }
}

#[cfg(feature = "enable-rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
impl From<ClientConfig> for TlsConnector {
  fn from(config: ClientConfig) -> Self {
    TlsConnector::Rustls(RustlsConnector::from(Arc::new(config)))
  }
}

#[cfg(feature = "enable-rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
impl From<RustlsConnector> for TlsConnector {
  fn from(connector: RustlsConnector) -> Self {
    TlsConnector::Rustls(connector)
  }
}

pub fn should_disable_cert_verification() -> bool {
  match env::var_os("FRED_DISABLE_CERT_VERIFICATION") {
    Some(s) => match s.into_string() {
      Ok(s) => match s.as_ref() {
        "1" | "true" | "TRUE" | "yes" => true,
        _ => false,
      },
      Err(_) => false,
    },
    None => false,
  }
}

pub fn should_disable_host_verification() -> bool {
  match env::var_os("FRED_DISABLE_HOST_VERIFICATION") {
    Some(s) => match s.into_string() {
      Ok(s) => match s.as_ref() {
        "1" | "true" | "TRUE" | "yes" => true,
        _ => false,
      },
      Err(_) => false,
    },
    None => false,
  }
}
