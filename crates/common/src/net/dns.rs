use crate::{
  error::{Error, ErrorKind},
  FredResult,
};
use async_trait::async_trait;
use bytes_utils::Str;
use std::net::{SocketAddr, ToSocketAddrs};
use tracing::trace;

/// Default DNS resolver that uses [to_socket_addrs](std::net::ToSocketAddrs::to_socket_addrs).
#[derive(Clone, Debug)]
pub struct DefaultResolver {
  id: Str,
}

impl DefaultResolver {
  /// Create a new resolver using the system's default DNS resolution.
  pub fn new(id: &Str) -> Self {
    DefaultResolver { id: id.clone() }
  }
}

/// A trait that can be used to override DNS resolution logic.
///
/// Note: currently this requires [async-trait](https://crates.io/crates/async-trait).
#[cfg(feature = "glommio")]
#[async_trait(?Send)]
#[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
pub trait Resolve: 'static {
  /// Resolve a hostname.
  async fn resolve(&self, host: Str, port: u16) -> FredResult<Vec<SocketAddr>>;
}

#[cfg(feature = "glommio")]
#[async_trait(?Send)]
impl Resolve for DefaultResolver {
  async fn resolve(&self, host: Str, port: u16) -> FredResult<Vec<SocketAddr>> {
    let client_id = self.id.clone();

    // glommio users should probably use a non-blocking impl such as hickory-dns
    crate::runtime::spawn(async move {
      let addr = format!("{}:{}", host, port);
      let ips: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();

      if ips.is_empty() {
        Err(Error::new(
          ErrorKind::IO,
          format!("Failed to resolve {}:{}", host, port),
        ))
      } else {
        trace!("{}: Found {} addresses for {}", client_id, ips.len(), addr);
        Ok(ips)
      }
    })
    .await?
  }
}

/// A trait that can be used to override DNS resolution logic.
///
/// Note: currently this requires [async-trait](https://crates.io/crates/async-trait).
#[cfg(not(feature = "glommio"))]
#[async_trait]
#[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
pub trait Resolve: Send + Sync + 'static {
  /// Resolve a hostname.
  async fn resolve(&self, host: Str, port: u16) -> FredResult<Vec<SocketAddr>>;
}

#[cfg(not(feature = "glommio"))]
#[async_trait]
impl Resolve for DefaultResolver {
  async fn resolve(&self, host: Str, port: u16) -> FredResult<Vec<SocketAddr>> {
    let client_id = self.id.clone();

    tokio::task::spawn_blocking(move || {
      let addr = format!("{}:{}", host, port);
      let ips: Vec<SocketAddr> = addr.to_socket_addrs()?.collect();

      if ips.is_empty() {
        Err(Error::new(
          ErrorKind::IO,
          format!("Failed to resolve {}:{}", host, port),
        ))
      } else {
        trace!("{}: Found {} addresses for {}", client_id, ips.len(), addr);
        Ok(ips)
      }
    })
    .await?
  }
}
