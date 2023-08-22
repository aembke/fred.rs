use crate::{
  interfaces::*,
  modules::inner::RedisClientInner,
  types::{ConnectionConfig, PerformanceConfig, ReconnectPolicy, SentinelConfig},
};
use std::{fmt, sync::Arc};

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

impl fmt::Debug for SentinelClient {
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
  ///
  /// See the [builder](crate::types::Builder) interface for more information.
  pub fn new(
    config: SentinelConfig,
    perf: Option<PerformanceConfig>,
    connection: Option<ConnectionConfig>,
    policy: Option<ReconnectPolicy>,
  ) -> SentinelClient {
    SentinelClient {
      inner: RedisClientInner::new(
        config.into(),
        perf.unwrap_or_default(),
        connection.unwrap_or_default(),
        policy,
      ),
    }
  }
}
