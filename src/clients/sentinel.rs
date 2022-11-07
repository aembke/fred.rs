use crate::{
  clients::redis::RedisClient,
  interfaces::*,
  modules::inner::RedisClientInner,
  types::{Blocking, PerformanceConfig, ReconnectPolicy, RedisConfig, ServerConfig},
};
use futures::{Stream, StreamExt};
use redis_protocol::resp3::prelude::RespVersion;
use std::{default::Default, fmt, sync::Arc};
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;

#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use crate::protocol::tls::TlsConnector;

/// Configuration options for sentinel clients.
#[derive(Clone, Debug)]
#[cfg_attr(docsrs, doc(cfg(feature = "sentinel-client")))]
pub struct SentinelConfig {
  /// The hostname for the sentinel node.
  ///
  /// Default: `127.0.0.1`
  pub host:     String,
  /// The port on which the sentinel node is listening.
  ///
  /// Default: `26379`
  pub port:     u16,
  /// An optional ACL username for the client to use when authenticating. If ACL rules are not configured this should
  /// be `None`.
  ///
  /// Default: `None`
  pub username: Option<String>,
  /// An optional password for the client to use when authenticating.
  ///
  /// Default: `None`
  pub password: Option<String>,
  /// TLS configuration fields. If `None` the connection will not use TLS.
  ///
  /// See the `tls` examples on Github for more information.
  ///
  /// Default: `None`
  #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
  #[cfg_attr(docsrs, doc(cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))))]
  pub tls:      Option<TlsConnector>,
  /// Whether or not to enable tracing for this client.
  ///
  /// Default: `false`
  #[cfg(feature = "partial-tracing")]
  #[cfg_attr(docsrs, doc(cfg(feature = "partial-tracing")))]
  pub tracing:  bool,
}

impl Default for SentinelConfig {
  fn default() -> Self {
    SentinelConfig {
      host: "127.0.0.1".into(),
      port: 26379,
      username: None,
      password: None,
      #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
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
      database: None,
      blocking: Blocking::Block,
      username: config.username,
      password: config.password,
      version: RespVersion::RESP2,
      #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
      tls: config.tls,
      #[cfg(feature = "partial-tracing")]
      tracing: config.tracing,
    }
  }
}

/// A struct for interacting directly with Sentinel nodes.
///
/// This struct **will not** communicate with Redis servers behind the sentinel interface, but rather with the
/// sentinel nodes themselves. Callers should use the [RedisClient](crate::clients::RedisClient) interface with a
/// [ServerConfig::Sentinel](crate::types::ServerConfig::Sentinel) for interacting with Redis services behind a
/// sentinel layer.
///
/// See the [sentinel API docs](https://redis.io/topics/sentinel#sentinel-api) for more information.
#[derive(Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "sentinel-client")))]
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
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("SentinelClient")
      .field("id", &self.inner.id)
      .field("state", &self.state())
      .finish()
  }
}

#[doc(hidden)]
impl<'a> From<&'a Arc<RedisClientInner>> for SentinelClient {
  fn from(inner: &'a Arc<RedisClientInner>) -> Self {
    SentinelClient { inner: inner.clone() }
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
  pub fn new(
    config: SentinelConfig,
    perf: Option<PerformanceConfig>,
    policy: Option<ReconnectPolicy>,
  ) -> SentinelClient {
    SentinelClient {
      inner: RedisClientInner::new(config.into(), perf.unwrap_or_default(), policy),
    }
  }
}
