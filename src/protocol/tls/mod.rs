use std::env;
use parking_lot::RwLock;
use crate::types::RedisConfig;

#[cfg(feature = "enable-tls")]
use tokio_native_tls::TlsConnector as NativeTlsConnector;
#[cfg(feature = "enable-rustls")]
use tokio_rustls::TlsConnector as RustlsConnector;
use crate::error::RedisError;

/// TLS types and reexports from the `native-tls` crate.
#[cfg(feature = "enable-tls")]
pub mod native_tls;
/// TLS types and reexports from the `rustls` crate.
#[cfg(feature = "enable-rustls")]
pub mod rustls;

// TOOD redo this interface in the next major version
/// Wrapper type for TLS client configuration options.
///
/// Depending on the feature flags used this may be an alias for the following types:
/// * `enable-rustls` - Rustls [TlsConfig](crate::protocol::tls::rustls::TlsConfig)
/// * `enable-tls` - Native TLS [TlsConfig](crate::protocol::tls::native_tls::TlsConfig)
#[cfg(feature = "enable-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-tls")))]
pub type TlsConfig = self::native_tls::TlsConfig;
/// Wrapper type for TLS client configuration options.
///
/// Depending on the feature flags used this may be an alias for the following types:
/// * `enable-rustls` - Rustls [TlsConfig](crate::protocol::tls::rustls::TlsConfig)
/// * `enable-tls` - Native TLS [TlsConfig](crate::protocol::tls::native_tls::TlsConfig)
#[cfg(feature = "enable-rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
pub type TlsConfig = self::rustls::TlsConfig;

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

#[cfg(feature = "enable-tls")]
pub use self::native_tls::{create_tls_connector, create_tls_transport};

#[cfg(feature = "enable-rustls")]
pub use self::rustls::{create_tls_connector, create_tls_transport};