#[cfg(feature = "replicas")]
use crate::clients::Replicas;
#[cfg(feature = "i-tracking")]
use crate::interfaces::TrackingInterface;
use crate::{
  clients::{Pipeline, WithOptions},
  commands,
  error::{RedisError, RedisErrorKind},
  interfaces::*,
  modules::inner::RedisClientInner,
  prelude::ClientLike,
  runtime::RefCount,
  types::*,
};
use bytes_utils::Str;
use futures::Stream;
use std::{fmt, fmt::Formatter};

/// A cheaply cloneable Redis client struct.
#[derive(Clone)]
pub struct RedisClient {
  pub(crate) inner: RefCount<RedisClientInner>,
}

impl Default for RedisClient {
  fn default() -> Self {
    RedisClient::new(RedisConfig::default(), None, None, None)
  }
}

impl fmt::Debug for RedisClient {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("RedisClient")
      .field("id", &self.inner.id)
      .field("state", &self.state())
      .finish()
  }
}

impl fmt::Display for RedisClient {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.inner.id)
  }
}

#[doc(hidden)]
impl<'a> From<&'a RefCount<RedisClientInner>> for RedisClient {
  fn from(inner: &'a RefCount<RedisClientInner>) -> RedisClient {
    RedisClient { inner: inner.clone() }
  }
}

impl ClientLike for RedisClient {
  #[doc(hidden)]
  fn inner(&self) -> &RefCount<RedisClientInner> {
    &self.inner
  }
}

impl EventInterface for RedisClient {}
#[cfg(feature = "i-redis-json")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-redis-json")))]
impl RedisJsonInterface for RedisClient {}
#[cfg(feature = "i-time-series")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-time-series")))]
impl TimeSeriesInterface for RedisClient {}
#[cfg(feature = "i-acl")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-acl")))]
impl AclInterface for RedisClient {}
#[cfg(feature = "i-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-client")))]
impl ClientInterface for RedisClient {}
#[cfg(feature = "i-cluster")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-cluster")))]
impl ClusterInterface for RedisClient {}
#[cfg(feature = "i-config")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-config")))]
impl ConfigInterface for RedisClient {}
#[cfg(feature = "i-geo")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-geo")))]
impl GeoInterface for RedisClient {}
#[cfg(feature = "i-hashes")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-hashes")))]
impl HashesInterface for RedisClient {}
#[cfg(feature = "i-hyperloglog")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-hyperloglog")))]
impl HyperloglogInterface for RedisClient {}
impl MetricsInterface for RedisClient {}
#[cfg(feature = "transactions")]
#[cfg_attr(docsrs, doc(cfg(feature = "transactions")))]
impl TransactionInterface for RedisClient {}
#[cfg(feature = "i-keys")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-keys")))]
impl KeysInterface for RedisClient {}
#[cfg(feature = "i-scripts")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-scripts")))]
impl LuaInterface for RedisClient {}
#[cfg(feature = "i-lists")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-lists")))]
impl ListInterface for RedisClient {}
#[cfg(feature = "i-memory")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-memory")))]
impl MemoryInterface for RedisClient {}
impl AuthInterface for RedisClient {}
#[cfg(feature = "i-server")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-server")))]
impl ServerInterface for RedisClient {}
#[cfg(feature = "i-slowlog")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-slowlog")))]
impl SlowlogInterface for RedisClient {}
#[cfg(feature = "i-sets")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-sets")))]
impl SetsInterface for RedisClient {}
#[cfg(feature = "i-sorted-sets")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-sorted-sets")))]
impl SortedSetsInterface for RedisClient {}
#[cfg(feature = "i-server")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-server")))]
impl HeartbeatInterface for RedisClient {}
#[cfg(feature = "i-streams")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-streams")))]
impl StreamsInterface for RedisClient {}
#[cfg(feature = "i-scripts")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-scripts")))]
impl FunctionInterface for RedisClient {}
#[cfg(feature = "i-tracking")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-tracking")))]
impl TrackingInterface for RedisClient {}
#[cfg(feature = "i-pubsub")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-pubsub")))]
impl PubsubInterface for RedisClient {}
#[cfg(feature = "i-redisearch")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-redisearch")))]
impl RediSearchInterface for RedisClient {}

impl RedisClient {
  /// Create a new client instance without connecting to the server.
  ///
  /// See the [builder](crate::types::Builder) interface for more information.
  pub fn new(
    config: RedisConfig,
    perf: Option<PerformanceConfig>,
    connection: Option<ConnectionConfig>,
    policy: Option<ReconnectPolicy>,
  ) -> RedisClient {
    RedisClient {
      inner: RedisClientInner::new(config, perf.unwrap_or_default(), connection.unwrap_or_default(), policy),
    }
  }

  /// Create a new `RedisClient` from the config provided to this client.
  ///
  /// The returned client will **not** be connected to the server.
  pub fn clone_new(&self) -> Self {
    let mut policy = self.inner.policy.read().clone();
    if let Some(policy) = policy.as_mut() {
      policy.reset_attempts();
    }

    RedisClient::new(
      self.inner.config.as_ref().clone(),
      Some(self.inner.performance_config()),
      Some(self.inner.connection_config()),
      policy,
    )
  }

  /// Split a clustered Redis client into a set of centralized clients - one for each primary node in the cluster.
  ///
  /// Alternatively, callers can use [with_cluster_node](crate::clients::RedisClient::with_cluster_node) to avoid
  /// creating new connections.
  ///
  /// The clients returned by this function will not be connected to their associated servers. The caller needs to
  /// call `connect` on each client before sending any commands.
  pub fn split_cluster(&self) -> Result<Vec<RedisClient>, RedisError> {
    if self.inner.config.server.is_clustered() {
      commands::server::split(&self.inner)
    } else {
      Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Client is not using a clustered deployment.",
      ))
    }
  }

  // --------------- SCANNING ---------------

  /// Incrementally iterate over a set of keys matching the `pattern` argument, returning `count` results per page, if
  /// specified.
  ///
  /// The scan operation can be canceled by dropping the returned stream.
  ///
  /// See [scan_buffered](Self::scan_buffered) or [scan_cluster_buffered](Self::scan_cluster_buffered) for
  /// alternatives that automatically continue scanning in the background.
  ///
  /// <https://redis.io/commands/scan>
  pub fn scan<P>(
    &self,
    pattern: P,
    count: Option<u32>,
    r#type: Option<ScanType>,
  ) -> impl Stream<Item = Result<ScanResult, RedisError>>
  where
    P: Into<Str>,
  {
    commands::scan::scan(&self.inner, pattern.into(), count, r#type)
  }

  /// Scan the keys in the keyspace, buffering all results in memory as quickly as the server returns them.
  ///
  /// This function should be used with care as it can result in the caller buffering the entire keyspace in memory if
  /// results are not processed quickly. Additionally, since results are paged in the background the cursor is not
  /// exposed to the caller with each page of results.
  ///
  /// See [scan](Self::scan) or [scan_cluster](Self::scan_cluster) for alternatives that allow callers to control the
  /// rate at which pages are scanned.
  ///
  /// <https://redis.io/commands/scan>
  pub fn scan_buffered<P>(
    &self,
    pattern: P,
    count: Option<u32>,
    r#type: Option<ScanType>,
  ) -> impl Stream<Item = Result<RedisKey, RedisError>>
  where
    P: Into<Str>,
  {
    commands::scan::scan_buffered(&self.inner, pattern.into(), count, r#type, None)
  }

  /// Run the `SCAN` command on each primary/main node in a cluster concurrently.
  ///
  /// In order for this function to work reliably the cluster state must not change while scanning. If nodes are added
  /// or removed, or hash slots are rebalanced, it may result in missing keys or duplicate keys in the result
  /// stream. See [split_cluster](Self::split_cluster) for use cases that require scanning to work while the cluster
  /// state changes.
  ///
  /// Unlike `SCAN`, `HSCAN`, etc, the returned stream may continue even if
  /// [has_more](crate::types::ScanResult::has_more) returns false on a given page of keys.
  ///
  /// See [scan_buffered](Self::scan_buffered) or [scan_cluster_buffered](Self::scan_cluster_buffered) for
  /// alternatives that automatically continue scanning in the background.
  pub fn scan_cluster<P>(
    &self,
    pattern: P,
    count: Option<u32>,
    r#type: Option<ScanType>,
  ) -> impl Stream<Item = Result<ScanResult, RedisError>>
  where
    P: Into<Str>,
  {
    commands::scan::scan_cluster(&self.inner, pattern.into(), count, r#type)
  }

  /// Scan the keys in the keyspace concurrently across all nodes in the cluster, buffering all results in memory as
  /// quickly as the server returns them.
  ///
  /// This function should be used with care as it can result in the caller buffering the entire keyspace in memory if
  /// results are not processed quickly. Additionally, since results are paged in the background the cursor is not
  /// exposed to the caller with each page of results.
  ///
  /// See [scan](Self::scan) or [scan_cluster](Self::scan_cluster) for alternatives that allow callers to control the
  /// rate at which pages are scanned.
  ///
  /// <https://redis.io/commands/scan>
  pub fn scan_cluster_buffered<P>(
    &self,
    pattern: P,
    count: Option<u32>,
    r#type: Option<ScanType>,
  ) -> impl Stream<Item = Result<RedisKey, RedisError>>
  where
    P: Into<Str>,
  {
    commands::scan::scan_cluster_buffered(&self.inner, pattern.into(), count, r#type)
  }

  /// Incrementally iterate over pages of the hash map stored at `key`, returning `count` results per page, if
  /// specified.
  ///
  /// <https://redis.io/commands/hscan>
  pub fn hscan<K, P>(
    &self,
    key: K,
    pattern: P,
    count: Option<u32>,
  ) -> impl Stream<Item = Result<HScanResult, RedisError>>
  where
    K: Into<RedisKey>,
    P: Into<Str>,
  {
    commands::scan::hscan(&self.inner, key.into(), pattern.into(), count)
  }

  /// Incrementally iterate over pages of the set stored at `key`, returning `count` results per page, if specified.
  ///
  /// <https://redis.io/commands/sscan>
  pub fn sscan<K, P>(
    &self,
    key: K,
    pattern: P,
    count: Option<u32>,
  ) -> impl Stream<Item = Result<SScanResult, RedisError>>
  where
    K: Into<RedisKey>,
    P: Into<Str>,
  {
    commands::scan::sscan(&self.inner, key.into(), pattern.into(), count)
  }

  /// Incrementally iterate over pages of the sorted set stored at `key`, returning `count` results per page, if
  /// specified.
  ///
  /// <https://redis.io/commands/zscan>
  pub fn zscan<K, P>(
    &self,
    key: K,
    pattern: P,
    count: Option<u32>,
  ) -> impl Stream<Item = Result<ZScanResult, RedisError>>
  where
    K: Into<RedisKey>,
    P: Into<Str>,
  {
    commands::scan::zscan(&self.inner, key.into(), pattern.into(), count)
  }

  /// Send a series of commands in a [pipeline](https://redis.io/docs/manual/pipelining/).
  pub fn pipeline(&self) -> Pipeline<RedisClient> {
    Pipeline::from(self.clone())
  }

  /// Shorthand to route subsequent commands to the provided server.
  ///
  /// See [with_options](crate::interfaces::ClientLike::with_options) for more information.
  ///
  /// ```rust
  /// # use fred::prelude::*;
  /// async fn example(client: &RedisClient) -> Result<(), RedisError> {
  ///   // discover servers via the `RedisConfig` or active connections
  ///   let connections = client.active_connections().await?;
  ///
  ///   // ping each node in the cluster individually
  ///   for server in connections.into_iter() {
  ///     let _: () = client.with_cluster_node(server).ping().await?;
  ///   }
  ///
  ///   // or use the cached cluster routing table to discover servers
  ///   let servers = client
  ///     .cached_cluster_state()
  ///     .expect("Failed to read cached cluster state")
  ///     .unique_primary_nodes();
  ///
  ///   for server in servers.into_iter() {
  ///     // verify the server address with `CLIENT INFO`
  ///     let server_addr = client
  ///       .with_cluster_node(&server)
  ///       .client_info::<String>()
  ///       .await?
  ///       .split(" ")
  ///       .find_map(|s| {
  ///         let parts: Vec<&str> = s.split("=").collect();
  ///         if parts[0] == "laddr" {
  ///           Some(parts[1].to_owned())
  ///         } else {
  ///           None
  ///         }
  ///       })
  ///       .expect("Failed to read or parse client info.");
  ///
  ///     assert_eq!(server_addr, server.to_string());
  ///   }
  ///
  ///   Ok(())
  /// }
  /// ```
  pub fn with_cluster_node<S>(&self, server: S) -> WithOptions<Self>
  where
    S: Into<Server>,
  {
    WithOptions {
      client:  self.clone(),
      options: Options {
        cluster_node: Some(server.into()),
        ..Default::default()
      },
    }
  }

  /// Create a client that interacts with replica nodes.
  #[cfg(feature = "replicas")]
  #[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
  pub fn replicas(&self) -> Replicas {
    Replicas::from(&self.inner)
  }
}

#[cfg(test)]
mod tests {
  #[cfg(feature = "sha-1")]
  use crate::util;

  #[test]
  #[cfg(feature = "sha-1")]
  fn should_correctly_sha1_hash() {
    assert_eq!(
      &util::sha1_hash("foobarbaz"),
      "5f5513f8822fdbe5145af33b64d8d970dcf95c6e"
    );
    assert_eq!(&util::sha1_hash("abc123"), "6367c48dd193d56ea7b0baad25b19455e529f5ee");
    assert_eq!(
      &util::sha1_hash("jakdjfkldajfklej8a4tjkaldsnvkl43kjakljdvk42"),
      "45c118f5de7c3fd3a4022135dc6acfb526f3c225"
    );
  }
}
