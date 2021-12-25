use crate::clients::redis::RedisClient;
use crate::interfaces::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::tls::TlsConfig;
use crate::types::{Blocking, RedisConfig, ServerConfig};
use futures::{Stream, StreamExt};
use redis_protocol::resp3::prelude::RespVersion;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Configuration options for sentinel clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SentinelConfig {
  /// The hostname for the sentinel node.
  ///
  /// Default: `127.0.0.1`
  pub host: String,
  /// The port on which the sentinel node is listening.
  ///
  /// Default: `26379`
  pub port: u16,
  /// An optional ACL username for the client to use when authenticating. If ACL rules are not configured this should be `None`.
  ///
  /// Default: `None`
  pub username: Option<String>,
  /// An optional password for the client to use when authenticating.
  ///
  /// Default: `None`
  pub password: Option<String>,
  /// TLS configuration fields. If `None` the connection will not use TLS.
  ///
  /// Default: `None`
  #[cfg(feature = "enable-tls")]
  #[cfg_attr(docsrs, doc(cfg(feature = "enable-tls")))]
  pub tls: Option<TlsConfig>,
  /// Whether or not to enable tracing for this client.
  ///
  /// Default: `false`
  #[cfg(feature = "partial-tracing")]
  #[cfg_attr(docsrs, doc(cfg(feature = "partial-tracing")))]
  pub tracing: bool,
}

impl Default for SentinelConfig {
  fn default() -> Self {
    SentinelConfig {
      host: "127.0.0.1".into(),
      port: 26379,
      username: None,
      password: None,
      #[cfg(feature = "enable-tls")]
      tls: None,
      #[cfg(feature = "partial-tracing")]
      tracing: false,
    }
  }
}

#[doc(hidden)]
impl From<SentinelConfig> for RedisConfig {
  fn from(config: SentinelConfig) -> Self {
    RedisConfig {
      server: ServerConfig::Centralized {
        host: config.host,
        port: config.port,
      },
      fail_fast: true,
      pipeline: false,
      blocking: Blocking::Block,
      username: config.username,
      password: config.password,
      version: RespVersion::RESP2,
      #[cfg(feature = "enable-tls")]
      tls: config.tls,
      #[cfg(feature = "partial-tracing")]
      tracing: config.tracing,
    }
  }
}

/// A struct for interacting directly with Sentinel nodes.
///
/// This struct **will not** communicate with Redis servers behind the sentinel interface, but rather with the sentinel nodes themselves. Callers should use the [RedisClient](crate::client::RedisClient) interface with a [ServerConfig::Sentinel](crate::types::ServerConfig::Sentinel) for interacting with Redis services behind a sentinel layer.
///
/// See the [sentinel API docs](https://redis.io/topics/sentinel#sentinel-api) for more information.
#[derive(Clone)]
pub struct SentinelClient {
  inner: Arc<RedisClientInner>,
}

impl ClientLike for SentinelClient {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }
}

impl fmt::Display for SentinelClient {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[SentinelClient {}: {}]", self.inner.id, self.state())
  }
}

#[doc(hidden)]
impl<'a> From<&'a Arc<RedisClientInner>> for SentinelClient {
  fn from(inner: &'a Arc<RedisClientInner>) -> Self {
    SentinelClient { inner: inner.clone() }
  }
}

#[doc(hidden)]
impl From<RedisClient> for SentinelClient {
  fn from(client: RedisClient) -> Self {
    SentinelClient { inner: client.inner }
  }
}

impl SentinelInterface for SentinelClient {}
impl MetricsInterface for SentinelClient {}
impl AclInterface for SentinelClient {}
impl PubsubInterface for SentinelClient {}
impl ClientInterface for SentinelClient {}
impl AuthInterface for SentinelClient {}
impl HeartbeatInterface for SentinelClient {}

impl SentinelClient {
  /// Create a new client instance without connecting to the sentinel node.
  pub fn new(config: SentinelConfig) -> SentinelClient {
    SentinelClient {
      inner: RedisClientInner::new(config.into()),
    }
  }

  /// Listen for reconnection notifications.
  ///
  /// This function can be used to receive notifications whenever the client successfully reconnects.
  ///
  /// A reconnection event is also triggered upon first connecting to the server.
  pub fn on_reconnect(&self) -> impl Stream<Item = Self> {
    let (tx, rx) = unbounded_channel();
    self.inner.reconnect_tx.write().push_back(tx);

    UnboundedReceiverStream::new(rx).map(|client| client.into())
  }
}
