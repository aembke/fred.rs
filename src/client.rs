use crate::commands;
use crate::error::{RedisError, RedisErrorKind};
use crate::interfaces::{
  AclInterface, AsyncResult, AuthInterface, ClientInterface, ClusterInterface, ConfigInterface, GeoInterface,
  HashesInterface, HeartbeatInterface, HyperloglogInterface, KeysInterface, ListInterface, LuaInterface,
  MemoryInterface, MetricsInterface, PubsubInterface, ServerInterface, SetsInterface, SlowlogInterface,
  SortedSetsInterface, TransactionInterface,
};
use crate::modules::inner::{MultiPolicy, RedisClientInner};
use crate::modules::response::FromRedis;
use crate::multiplexer::commands as multiplexer_commands;
use crate::multiplexer::utils as multiplexer_utils;
use crate::prelude::{AsyncStream, ClientLike};
use crate::types::*;
use crate::utils;
use crate::utils::check_and_set_bool;
use futures::Stream;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::interval as tokio_interval;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Utility functions used by the client that may also be useful to callers.
pub mod util {
  pub use crate::utils::f64_to_redis_string;
  pub use crate::utils::redis_string_to_f64;
  pub use redis_protocol::redis_keyslot;

  /// Calculate the SHA1 hash output as a hex string. This is provided for clients that use the Lua interface to manage their own script caches.
  pub fn sha1_hash(input: &str) -> String {
    use sha1::Digest;

    let mut hasher = sha1::Sha1::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
  }
}

/// A Redis client struct.
#[derive(Clone)]
pub struct RedisClient {
  pub(crate) inner: Arc<RedisClientInner>,
}

impl fmt::Display for RedisClient {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[RedisClient {}: {}]", self.inner.id, self.state())
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
impl ListInterface for RedisClient {}
impl MemoryInterface for RedisClient {}
impl AuthInterface for RedisClient {}
impl ServerInterface for RedisClient {}
impl SlowlogInterface for RedisClient {}
impl SetsInterface for RedisClient {}
impl SortedSetsInterface for RedisClient {}
impl HeartbeatInterface for RedisClient {}

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
  /// This function can be used to receive notifications whenever the client successfully reconnects in order to select the right database again, re-subscribe to channels, etc.
  ///
  /// A reconnection event is also triggered upon first connecting to the server.
  pub fn on_reconnect(&self) -> impl Stream<Item = Self> {
    let (tx, rx) = unbounded_channel();
    self.inner().reconnect_tx.write().push_back(tx);

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
  /// Note: For this to work reliably this function needs to be called each time nodes are added or removed from the cluster.
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

  /// Incrementally iterate over a set of keys matching the `pattern` argument, returning `count` results per page, if specified.
  ///
  /// The scan operation can be canceled by dropping the returned stream.
  ///
  /// Note: scanning data in a cluster can be tricky. To make this easier this function supports [hash tags](https://redis.io/topics/cluster-spec#keys-hash-tags) in the
  /// `pattern` so callers can direct scanning operations to specific nodes in the cluster. Callers can also use [split_cluster](Self::split_cluster) with this function if
  /// hash tags are not used in the keys that should be scanned.
  ///
  /// <https://redis.io/commands/scan>
  pub fn scan<P>(
    &self,
    pattern: P,
    count: Option<u32>,
    r#type: Option<ScanType>,
  ) -> impl Stream<Item = Result<ScanResult, RedisError>>
  where
    P: Into<String>,
  {
    commands::scan::scan(&self.inner, pattern, count, r#type)
  }

  /// Incrementally iterate over pages of the hash map stored at `key`, returning `count` results per page, if specified.
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
    P: Into<String>,
  {
    commands::scan::hscan(&self.inner, key, pattern, count)
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
    P: Into<String>,
  {
    commands::scan::sscan(&self.inner, key, pattern, count)
  }

  /// Incrementally iterate over pages of the sorted set stored at `key`, returning `count` results per page, if specified.
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
    P: Into<String>,
  {
    commands::scan::zscan(&self.inner, key, pattern, count)
  }
}

/// A wrapping struct for commands in a MULTI/EXEC transaction block.
pub struct TransactionClient {
  inner: Arc<RedisClientInner>,
  finished: Arc<RwLock<bool>>,
}

impl fmt::Display for TransactionClient {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "[TransactionClient {}: {}]",
      self.inner.id,
      self.inner().state.read()
    )
  }
}

impl Drop for TransactionClient {
  fn drop(&mut self) {
    if !*self.finished.read() {
      warn!(
        "{}: Dropping transaction client without finishing transaction!",
        self.inner.client_name()
      );
    }
  }
}

impl ClientLike for TransactionClient {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }
}

impl AclInterface for TransactionClient {}
impl ClientInterface for TransactionClient {}
impl PubsubInterface for TransactionClient {}
impl ConfigInterface for TransactionClient {}
impl GeoInterface for TransactionClient {}
impl HashesInterface for TransactionClient {}
impl HyperloglogInterface for TransactionClient {}
impl MetricsInterface for TransactionClient {}
impl TransactionInterface for TransactionClient {}
impl KeysInterface for TransactionClient {}
impl ListInterface for TransactionClient {}
impl MemoryInterface for TransactionClient {}
impl AuthInterface for TransactionClient {}
impl ServerInterface for TransactionClient {}
impl SetsInterface for TransactionClient {}
impl SortedSetsInterface for TransactionClient {}

impl TransactionClient {
  /// Executes all previously queued commands in a transaction and restores the connection state to normal.
  ///
  /// <https://redis.io/commands/exec>
  ///
  /// Note: Automatic request retry policies in the event of a connection closing can present problems for transactions.
  /// If the underlying connection closes while a transaction is in process the client will abort the transaction by
  /// returning a `Canceled` error to the caller of any pending intermediate command, as well as this one. It's up to
  /// the caller to retry transactions as needed.
  pub async fn exec<R>(self) -> Result<R, RedisError>
  where
    R: FromRedis,
  {
    if check_and_set_bool(&self.finished, true) {
      return Err(RedisError::new(
        RedisErrorKind::InvalidCommand,
        "Transaction already finished.",
      ));
    }
    commands::server::exec(&self.inner).await?.convert()
  }

  /// Flushes all previously queued commands in a transaction and restores the connection state to normal.
  ///
  /// <https://redis.io/commands/discard>
  pub async fn discard(self) -> Result<(), RedisError> {
    if check_and_set_bool(&self.finished, true) {
      return Err(RedisError::new(
        RedisErrorKind::InvalidCommand,
        "Transaction already finished.",
      ));
    }
    commands::server::discard(&self.inner).await
  }

  /// Read the hash slot against which this transaction will run, if known.  
  pub fn hash_slot(&self) -> Option<u16> {
    utils::read_locked(&self.inner.multi_block).and_then(|b| b.hash_slot)
  }
}

#[doc(hidden)]
impl<'a> From<&'a Arc<RedisClientInner>> for TransactionClient {
  fn from(inner: &'a Arc<RedisClientInner>) -> Self {
    TransactionClient {
      inner: inner.clone(),
      finished: Arc::new(RwLock::new(false)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::util;

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
