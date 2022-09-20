use crate::{
  commands,
  error::{RedisError, RedisErrorKind},
  interfaces::{
    AclInterface,
    AuthInterface,
    ClientInterface,
    ClusterInterface,
    ConfigInterface,
    FunctionInterface,
    GeoInterface,
    HashesInterface,
    HeartbeatInterface,
    HyperloglogInterface,
    KeysInterface,
    ListInterface,
    LuaInterface,
    MemoryInterface,
    MetricsInterface,
    PubsubInterface,
    ServerInterface,
    SetsInterface,
    SlowlogInterface,
    SortedSetsInterface,
    TransactionInterface,
  },
  modules::inner::RedisClientInner,
  prelude::{ClientLike, StreamsInterface},
  types::*,
  utils,
};
use bytes_utils::Str;
use futures::Stream;
use std::{fmt, sync::Arc};
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// The primary Redis client struct.
#[derive(Clone)]
pub struct RedisClient {
  pub(crate) inner: Arc<RedisClientInner>,
}

impl fmt::Display for RedisClient {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("RedisClient")
      .field("id", &self.inner.id)
      .field("state", &self.state())
      .finish()
  }
}

#[doc(hidden)]
impl<'a> From<&'a Arc<RedisClientInner>> for RedisClient {
  fn from(inner: &'a Arc<RedisClientInner>) -> RedisClient {
    RedisClient { inner: inner.clone() }
  }
}

impl ClientLike for RedisClient {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }
}

impl AclInterface for RedisClient {}
impl ClientInterface for RedisClient {}
impl ClusterInterface for RedisClient {}
impl PubsubInterface for RedisClient {}
impl ConfigInterface for RedisClient {}
impl GeoInterface for RedisClient {}
impl HashesInterface for RedisClient {}
impl HyperloglogInterface for RedisClient {}
impl MetricsInterface for RedisClient {}
impl TransactionInterface for RedisClient {}
impl KeysInterface for RedisClient {}
impl LuaInterface for RedisClient {}
impl FunctionInterface for RedisClient {}
impl ListInterface for RedisClient {}
impl MemoryInterface for RedisClient {}
impl AuthInterface for RedisClient {}
impl ServerInterface for RedisClient {}
impl SlowlogInterface for RedisClient {}
impl SetsInterface for RedisClient {}
impl SortedSetsInterface for RedisClient {}
impl HeartbeatInterface for RedisClient {}
impl StreamsInterface for RedisClient {}

impl RedisClient {
  /// Create a new client instance without connecting to the server.
  pub fn new(config: RedisConfig) -> RedisClient {
    RedisClient {
      inner: RedisClientInner::new(config),
    }
  }

  /// Create a new `RedisClient` from the config provided to this client.
  ///
  /// The returned client will not be connected to the server, and it will use new connections after connecting.
  pub fn clone_new(&self) -> Self {
    RedisClient::new(utils::read_locked(&self.inner.config))
  }

  /// Listen for reconnection notifications.
  ///
  /// This function can be used to receive notifications whenever the client successfully reconnects in order to
  /// select the right database again, re-subscribe to channels, etc.
  ///
  /// A reconnection event is also triggered upon first connecting to the server.
  pub fn on_reconnect(&self) -> impl Stream<Item = Self> {
    let (tx, rx) = unbounded_channel();
    self.inner().reconnect_tx.write().push_back(tx);

    UnboundedReceiverStream::new(rx)
  }

  /// Listen for notifications whenever the cluster state changes.
  ///
  /// This is usually triggered in response to a `MOVED` or `ASK` error, but can also happen when connections close
  /// unexpectedly.
  pub fn on_cluster_change(&self) -> impl Stream<Item = Vec<ClusterStateChange>> {
    let (tx, rx) = unbounded_channel();
    self.inner().cluster_change_tx.write().push_back(tx);

    UnboundedReceiverStream::new(rx)
  }

  /// Split a clustered Redis client into a list of centralized clients - one for each primary node in the cluster.
  ///
  /// Some Redis commands are not designed to work with hash slots against a clustered deployment. For example,
  /// `FLUSHDB`, `PING`, etc all work on one node in the cluster, but no interface exists for the client to
  /// select a specific node in the cluster against which to run the command. This function allows the caller to
  /// create a list of clients such that each connect to one of the primary nodes in the cluster and functions
  /// as if it were operating against a single centralized Redis server.
  ///
  /// **The clients returned by this function will not be connected to their associated servers. The caller needs to
  /// call `connect` on each client before sending any commands.**
  ///
  /// Note: For this to work reliably this function needs to be called each time nodes are added or removed from the
  /// cluster.
  pub async fn split_cluster(&self) -> Result<Vec<RedisClient>, RedisError> {
    if utils::is_clustered(&self.inner.config) {
      commands::server::split(&self.inner).await
    } else {
      Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Client is not using a clustered deployment.",
      ))
    }
  }

  // --------------- SCANNING ---------------
  // if/when `impl Trait` works inside traits we can move this to a trait

  /// Incrementally iterate over a set of keys matching the `pattern` argument, returning `count` results per page, if
  /// specified.
  ///
  /// The scan operation can be canceled by dropping the returned stream.
  ///
  /// Note: scanning data in a cluster can be tricky. To make this easier this function supports [hash tags](https://redis.io/topics/cluster-spec#keys-hash-tags) in the
  /// `pattern` so callers can direct scanning operations to specific nodes in the cluster. Callers can also use
  /// [split_cluster](Self::split_cluster) with this function if hash tags are not used in the keys that should be
  /// scanned.
  ///
  /// <https://redis.io/commands/scan>
  pub fn scan<P>(
    &self,
    pattern: P,
    count: Option<u32>,
    r#type: Option<ScanType>,
  ) -> impl Stream<Item = Result<ScanResult, RedisError>>
  where
    P: Into<Str>, {
    commands::scan::scan(&self.inner, pattern.into(), count, r#type)
  }

  /// Run the `SCAN` command on each primary/main node in a cluster concurrently.
  ///
  /// In order for this function to work reliably the cluster state must not change while scanning. If nodes are added
  /// or removed, or hash slots are rebalanced, it may result in missing keys or duplicate keys in the result
  /// stream. If callers need to support cluster scanning while the cluster state may change please see
  /// [split_cluster](Self::split_cluster).
  ///
  /// Unlike `SCAN`, `HSCAN`, etc, the returned stream may continue even if
  /// [has_more](crate::types::ScanResult::has_more) returns false on a given page of keys.
  pub fn scan_cluster<P>(
    &self,
    pattern: P,
    count: Option<u32>,
    r#type: Option<ScanType>,
  ) -> impl Stream<Item = Result<ScanResult, RedisError>>
  where
    P: Into<Str>, {
    commands::scan::scan_cluster(&self.inner, pattern.into(), count, r#type)
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
    P: Into<Str>, {
    commands::scan::hscan(&self.inner, key, pattern.into(), count)
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
    P: Into<Str>, {
    commands::scan::sscan(&self.inner, key, pattern.into(), count)
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
    P: Into<Str>, {
    commands::scan::zscan(&self.inner, key, pattern.into(), count)
  }
}

#[cfg(test)]
mod tests {
  use crate::util;

  #[test]
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
