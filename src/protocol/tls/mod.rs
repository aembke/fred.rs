use std::env;
use parking_lot::RwLock;
use crate::types::RedisConfig;
use crate::error::{RedisError, RedisErrorKind};

#[cfg(feature = "enable-native-tls")]
use tokio_native_tls::TlsConnector as NativeTlsConnector;
#[cfg(feature = "enable-rustls")]
use tokio_rustls::TlsConnector as RustlsConnector;
#[cfg(feature = "enable-native-tls")]
use tokio_native_tls::native_tls::TlsConnectorBuilder;
#[cfg(feature = "enable-rustls")]
use tokio_rustls::rustls::{ConfigBuilder, ClientConfig};

/// TLS types and reexports from the `native-tls` crate.
#[cfg(feature = "enable-native-tls")]
pub mod native_tls;
/// TLS types and reexports from the `rustls` crate.
#[cfg(feature = "enable-rustls")]
pub mod rustls;

///
pub trait TlsConfigBuilder: Clone {

  // TODO see if these can be pass by ref

  ///
  #[cfg(feature = "enable-native-tls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
  fn build(self) -> Result<TlsConnectorBuilder, RedisError> {
    let mut builder = TlsConnectorBuilder::builder();

    if should_disable_cert_verification() {
      builder.danger_accept_invalid_certs(true);
    }
    if should_disable_host_verification() {
      builder.danger_accept_invalid_hostnames(true);
    }

    Ok(builder)
  }

  ///
  #[cfg(feature = "enable-rustls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
  fn build(self) -> Result<ConfigBuilder<ClientConfig>, RedisError> {
    unimplemented!()
  }

}

///
#[derive(Clone)]
pub struct DefaultTlsConfig {}

impl TlsConfigBuilder for DefaultTlsConfig {}






pub(crate) fn should_disable_cert_verification() -> bool {
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

pub(crate) fn should_disable_host_verification() -> bool {
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