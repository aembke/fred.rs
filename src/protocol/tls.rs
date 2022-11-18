use crate::error::{RedisError, RedisErrorKind};
use std::{
  convert::{TryFrom, TryInto},
  env,
  fmt,
  fmt::Formatter,
  sync::Arc,
};

#[cfg(feature = "enable-native-tls")]
use tokio_native_tls::native_tls::{
  TlsConnector as NativeTlsConnector,
  TlsConnectorBuilder as NativeTlsConnectorBuilder,
};
#[cfg(feature = "enable-native-tls")]
use tokio_native_tls::TlsConnector as TokioNativeTlsConnector;
#[cfg(feature = "enable-rustls")]
use tokio_rustls::rustls::{Certificate, ClientConfig as RustlsClientConfig, RootCertStore};
#[cfg(feature = "enable-rustls")]
use tokio_rustls::TlsConnector as RustlsConnector;

#[cfg(feature = "enable-native-tls")]
pub use tokio_native_tls::native_tls;
#[cfg(feature = "enable-rustls")]
pub use tokio_rustls::rustls;

/// An enum for interacting with various TLS libraries and interfaces.
#[cfg_attr(docsrs, doc(cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))))]
#[derive(Clone)]
pub enum TlsConnector {
  #[cfg(feature = "enable-native-tls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
  Native(TokioNativeTlsConnector),
  #[cfg(feature = "enable-rustls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
  Rustls(RustlsConnector),
}

impl PartialEq for TlsConnector {
  fn eq(&self, _: &Self) -> bool {
    true
  }
}

impl Eq for TlsConnector {}

impl fmt::Debug for TlsConnector {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("TlsConnector")
      .field("kind", match self {
        #[cfg(feature = "enable-native-tls")]
        TlsConnector::Native(_) => &"Native",
        #[cfg(feature = "enable-rustls")]
        TlsConnector::Rustls(_) => &"Rustls",
      })
      .finish()
  }
}

#[cfg_attr(docsrs, doc(cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))))]
impl TlsConnector {
  /// Create a default TLS connector from the `native-tls` module.
  ///
  /// The `FRED_DISABLE_CERT_VERIFICATION` and `FRED_DISABLE_HOST_VERIFICATION` environment variables can be used.
  #[cfg(feature = "enable-native-tls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
  pub fn default_native_tls() -> Result<Self, RedisError> {
    let mut builder = NativeTlsConnector::builder();

    if should_disable_cert_verification() {
      builder.danger_accept_invalid_certs(true);
    }
    if should_disable_host_verification() {
      builder.danger_accept_invalid_hostnames(true);
    }

    builder.try_into()
  }

  /// Create a default TLS connector with the `rustls` module with safe defaults and system certs via [rustls-native-certs](https://github.com/rustls/rustls-native-certs).
  #[cfg(feature = "enable-rustls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
  pub fn default_rustls() -> Result<Self, RedisError> {
    let system_certs = rustls_native_certs::load_native_certs()?;
    let mut cert_store = RootCertStore::empty();
    for system_cert in system_certs.into_iter() {
      let _ = cert_store.add(&Certificate(system_cert.0))?;
    }

    Ok(
      RustlsClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(cert_store)
        .with_no_client_auth()
        .into(),
    )
  }
}

#[cfg(feature = "enable-native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
impl TryFrom<NativeTlsConnectorBuilder> for TlsConnector {
  type Error = RedisError;

  fn try_from(builder: NativeTlsConnectorBuilder) -> Result<Self, Self::Error> {
    let connector = builder
      .build()
      .map(|t| TokioNativeTlsConnector::from(t))
      .map_err(|e| RedisError::new(RedisErrorKind::Tls, format!("{:?}", e)))?;
    Ok(TlsConnector::Native(connector))
  }
}

#[cfg(feature = "enable-native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
impl From<NativeTlsConnector> for TlsConnector {
  fn from(connector: NativeTlsConnector) -> Self {
    TlsConnector::Native(TokioNativeTlsConnector::from(connector))
  }
}

#[cfg(feature = "enable-native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
impl From<TokioNativeTlsConnector> for TlsConnector {
  fn from(connector: TokioNativeTlsConnector) -> Self {
    TlsConnector::Native(connector)
  }
}

#[cfg(feature = "enable-rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
impl From<RustlsClientConfig> for TlsConnector {
  fn from(config: RustlsClientConfig) -> Self {
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
