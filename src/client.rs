use crate::commands;
use crate::error::{RedisError, RedisErrorKind};
use crate::interfaces::{
  AclInterface, ClientInterface, ClusterInterface, ConfigInterface, GeoInterface, HashesInterface,
  HyperloglogInterface, KeysInterface, LuaInterface, MetricsInterface, PubsubInterface, TransactionInterface,
};
use crate::modules::inner::{MultiPolicy, RedisClientInner};
use crate::modules::response::RedisResponse;
use crate::multiplexer::commands as multiplexer_commands;
use crate::multiplexer::utils as multiplexer_utils;
use crate::prelude::ClientLike;
use crate::types::*;
use crate::utils;
use futures::Stream;
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

/// A wrapping struct for commands in a MULTI/EXEC transaction block.
pub struct TransactionClient {
  client: RedisClient,
  finished: bool,
}

impl fmt::Display for TransactionClient {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "[TransactionClient {}: {}]",
      self.client.inner.id,
      self.client.state()
    )
  }
}

impl Drop for TransactionClient {
  fn drop(&mut self) {
    if !self.finished {
      warn!(
        "{}: Dropping transaction client without finishing transaction!",
        self.inner.client_name()
      );
    }
  }
}

impl TransactionClient {
  /// Executes all previously queued commands in a transaction and restores the connection state to normal.
  ///
  /// <https://redis.io/commands/exec>
  ///
  /// Note: Automatic request retry policies in the event of a connection closing can present problems for transactions.
  /// If the underlying connection closes while a transaction is in process the client will abort the transaction by
  /// returning a `Canceled` error to the caller of any pending intermediate command, as well as this one. It's up to
  /// the caller to retry transactions as needed.
  pub async fn exec<R>(mut self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    self.finished = true;
    commands::server::exec(&self.client.inner).await?.convert()
  }

  /// Flushes all previously queued commands in a transaction and restores the connection state to normal.
  ///
  /// <https://redis.io/commands/discard>
  pub async fn discard(mut self) -> Result<(), RedisError> {
    self.finished = true;
    commands::server::discard(&self.client.inner).await
  }

  /// Read the hash slot against which this transaction will run, if known.  
  pub fn hash_slot(&self) -> Option<u16> {
    utils::read_locked(&self.inner.multi_block).and_then(|b| b.hash_slot)
  }
}

impl Deref for TransactionClient {
  type Target = RedisClient;

  fn deref(&self) -> &Self::Target {
    &self.client
  }
}

#[doc(hidden)]
impl<'a> From<&'a Arc<RedisClientInner>> for TransactionClient {
  fn from(inner: &'a Arc<RedisClientInner>) -> Self {
    TransactionClient {
      client: RedisClient::from(inner),
      finished: false,
    }
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

  // it'd be nice if `impl Trait` worked inside other traits...

  /// Listen for reconnection notifications.
  ///
  /// This function can be used to receive notifications whenever the client successfully reconnects in order to select the right database again, re-subscribe to channels, etc.
  ///
  /// A reconnection event is also triggered upon first connecting to the server.
  pub fn on_reconnect(&self) -> impl Stream<Item = Self> {
    let (tx, rx) = unbounded_channel();
    self.inner.reconnect_tx.write().push_back(tx);

    UnboundedReceiverStream::new(rx)
  }

  /// Listen for protocol and connection errors. This stream can be used to more intelligently handle errors that may
  /// not appear in the request-response cycle, and so cannot be handled by response futures.
  ///
  /// Similar to [on_message](Self::on_message) and [on_reconnect](Self::on_reconnect), this function does not need to be called again if the connection closes.
  pub fn on_error(&self) -> impl Stream<Item = RedisError> {
    let (tx, rx) = unbounded_channel();
    self.inner.error_tx.write().push_back(tx);

    UnboundedReceiverStream::new(rx)
  }

  /// Listen for `(channel, message)` tuples on the publish-subscribe interface. **Keyspace events are not sent on this interface.**
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again. Messages will start appearing on the original stream after [subscribe](Self::subscribe) is called again.
  pub fn on_message(&self) -> impl Stream<Item = (String, RedisValue)> {
    let (tx, rx) = unbounded_channel();
    self.inner.message_tx.write().push_back(tx);

    UnboundedReceiverStream::new(rx)
  }

  /// Listen for keyspace and keyevent notifications on the publish subscribe interface.
  ///
  /// Callers still need to configure the server and subscribe to the relevant channels, but this interface will format the messages automatically.
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again.
  ///
  /// <https://redis.io/topics/notifications>
  pub fn on_keyspace_event(&self) -> impl Stream<Item = KeyspaceEvent> {
    let (tx, rx) = unbounded_channel();
    self.inner.keyspace_tx.write().push_back(tx);

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

  /// Request for authentication in a password-protected Redis server. Returns ok if successful.
  ///
  /// The client will automatically authenticate with the default user if a password is provided in the associated `RedisConfig` when calling [connect](Self::connect).
  ///
  /// If running against clustered servers this function will authenticate all connections.
  ///
  /// <https://redis.io/commands/auth>
  pub async fn auth<S>(&self, username: Option<String>, password: S) -> Result<(), RedisError>
  where
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::server::auth(&self.inner, username, password).await
  }

  /// Instruct Redis to start an Append Only File rewrite process.
  ///
  /// <https://redis.io/commands/bgrewriteaof>
  pub async fn bgrewriteaof<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::server::bgrewriteaof(&self.inner).await?.convert()
  }

  /// Save the DB in background.
  ///
  /// <https://redis.io/commands/bgsave>
  pub async fn bgsave<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::server::bgsave(&self.inner).await?.convert()
  }

  /// Return the number of keys in the selected database.
  ///
  /// <https://redis.io/commands/dbsize>
  pub async fn dbsize<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::server::dbsize(&self.inner).await?.convert()
  }

  /// Delete the keys in all databases.
  ///
  /// <https://redis.io/commands/flushall>
  pub async fn flushall<R>(&self, r#async: bool) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::server::flushall(&self.inner, r#async).await?.convert()
  }

  /// Delete the keys on all nodes in the cluster. This is a special function that does not map directly to the Redis interface.
  ///
  /// Note: ASYNC flushing of the db behaves badly with the automatic pipelining features of this library. If async flushing of the entire cluster
  /// is a requirement then callers should use [split_cluster](Self::split_cluster) with [flushall](Self::flushall) on each client instead.
  pub async fn flushall_cluster(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::server::flushall_cluster(&self.inner).await
  }

  /// Select the database this client should use.
  ///
  /// <https://redis.io/commands/select>
  pub async fn select(&self, db: u8) -> Result<(), RedisError> {
    commands::server::select(&self.inner, db).await?.convert()
  }

  /// Read info about the Redis server.
  ///
  /// <https://redis.io/commands/info>
  pub async fn info<R>(&self, section: Option<InfoKind>) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::server::info(&self.inner, section).await?.convert()
  }

  /// This command will start a coordinated failover between the currently-connected-to master and one of its replicas.
  ///
  /// <https://redis.io/commands/failover>
  pub async fn failover(
    &self,
    to: Option<(String, u16)>,
    force: bool,
    abort: bool,
    timeout: Option<u32>,
  ) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::server::failover(&self.inner, to, force, abort, timeout).await
  }

  /// Return the UNIX TIME of the last DB save executed with success.
  ///
  /// <https://redis.io/commands/lastsave>
  pub async fn lastsave<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::server::lastsave(&self.inner).await?.convert()
  }

  // ------------- SLOWLOG ----------------

  /// This command is used in order to read the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#reading-the-slow-log>
  pub async fn slowlog_get(&self, count: Option<i64>) -> Result<Vec<SlowlogEntry>, RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::slowlog::slowlog_get(&self.inner, count).await
  }

  /// This command is used in order to read length of the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#obtaining-the-current-length-of-the-slow-log>
  pub async fn slowlog_length(&self) -> Result<u64, RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::slowlog::slowlog_length(&self.inner).await
  }

  /// This command is used to reset the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#resetting-the-slow-log>
  pub async fn slowlog_reset(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::slowlog::slowlog_reset(&self.inner).await
  }

  // ---------------- MEMORY --------------------

  /// The MEMORY DOCTOR command reports about different memory-related issues that the Redis server experiences, and advises about possible remedies.
  ///
  /// <https://redis.io/commands/memory-doctor>
  pub async fn memory_doctor(&self) -> Result<String, RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::memory::memory_doctor(&self.inner).await
  }

  /// The MEMORY MALLOC-STATS command provides an internal statistics report from the memory allocator.
  ///
  /// <https://redis.io/commands/memory-malloc-stats>
  pub async fn memory_malloc_stats(&self) -> Result<String, RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::memory::memory_malloc_stats(&self.inner).await
  }

  /// The MEMORY PURGE command attempts to purge dirty pages so these can be reclaimed by the allocator.
  ///
  /// <https://redis.io/commands/memory-purge>
  pub async fn memory_purge(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::memory::memory_purge(&self.inner).await
  }

  /// The MEMORY STATS command returns an Array reply about the memory usage of the server.
  ///
  /// <https://redis.io/commands/memory-stats>
  pub async fn memory_stats(&self) -> Result<MemoryStats, RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::memory::memory_stats(&self.inner).await
  }

  /// The MEMORY USAGE command reports the number of bytes that a key and its value require to be stored in RAM.
  ///
  /// <https://redis.io/commands/memory-usage>
  pub async fn memory_usage<K>(&self, key: K, samples: Option<u32>) -> Result<Option<u64>, RedisError>
  where
    K: Into<RedisKey>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::memory::memory_usage(&self.inner, key, samples).await
  }

  // ------------- SETS --------------------

  /// Add the specified members to the set stored at `key`.
  ///
  /// <https://redis.io/commands/sadd>
  pub async fn sadd<R, K, V>(&self, key: K, members: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::sets::sadd(&self.inner, key, to!(members)?).await?.convert()
  }

  /// Returns the set cardinality (number of elements) of the set stored at `key`.
  ///
  /// <https://redis.io/commands/scard>
  pub async fn scard<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::sets::scard(&self.inner, key).await?.convert()
  }

  /// Returns the members of the set resulting from the difference between the first set and all the successive sets.
  ///
  /// <https://redis.io/commands/sdiff>
  pub async fn sdiff<R, K>(&self, keys: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<MultipleKeys>,
  {
    commands::sets::sdiff(&self.inner, keys).await?.convert()
  }

  /// This command is equal to SDIFF, but instead of returning the resulting set, it is stored in `destination`.
  ///
  /// <https://redis.io/commands/sdiffstore>
  pub async fn sdiffstore<R, D, K>(&self, dest: D, keys: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
  {
    commands::sets::sdiffstore(&self.inner, dest, keys).await?.convert()
  }

  /// Returns the members of the set resulting from the intersection of all the given sets.
  ///
  /// <https://redis.io/commands/sinter>
  pub async fn sinter<R, K>(&self, keys: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<MultipleKeys>,
  {
    commands::sets::sinter(&self.inner, keys).await?.convert()
  }

  /// This command is equal to SINTER, but instead of returning the resulting set, it is stored in `destination`.
  ///
  /// <https://redis.io/commands/sinterstore>
  pub async fn sinterstore<R, D, K>(&self, dest: D, keys: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
  {
    commands::sets::sinterstore(&self.inner, dest, keys).await?.convert()
  }

  /// Returns if `member` is a member of the set stored at `key`.
  ///
  /// <https://redis.io/commands/sismember>
  pub async fn sismember<R, K, V>(&self, key: K, member: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::sets::sismember(&self.inner, key, to!(member)?)
      .await?
      .convert()
  }

  /// Returns whether each member is a member of the set stored at `key`.
  ///
  /// <https://redis.io/commands/smismember>
  pub async fn smismember<R, K, V>(&self, key: K, members: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::sets::smismember(&self.inner, key, to!(members)?)
      .await?
      .convert()
  }

  /// Returns all the members of the set value stored at `key`.
  ///
  /// <https://redis.io/commands/smembers>
  pub async fn smembers<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::sets::smembers(&self.inner, key).await?.convert()
  }

  /// Move `member` from the set at `source` to the set at `destination`.
  ///
  /// <https://redis.io/commands/smove>
  pub async fn smove<R, S, D, V>(&self, source: S, dest: D, member: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::sets::smove(&self.inner, source, dest, to!(member)?)
      .await?
      .convert()
  }

  /// Removes and returns one or more random members from the set value store at `key`.
  ///
  /// <https://redis.io/commands/spop>
  pub async fn spop<R, K>(&self, key: K, count: Option<usize>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::sets::spop(&self.inner, key, count).await?.convert()
  }

  /// When called with just the key argument, return a random element from the set value stored at `key`.
  ///
  /// If the provided `count` argument is positive, return an array of distinct elements. The array's length is either count or the set's cardinality (SCARD), whichever is lower.
  ///
  /// <https://redis.io/commands/srandmember>
  pub async fn srandmember<R, K>(&self, key: K, count: Option<usize>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::sets::srandmember(&self.inner, key, count).await?.convert()
  }

  /// Remove the specified members from the set stored at `key`.
  ///
  /// <https://redis.io/commands/srem>
  pub async fn srem<R, K, V>(&self, key: K, members: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::sets::srem(&self.inner, key, to!(members)?).await?.convert()
  }

  /// Returns the members of the set resulting from the union of all the given sets.
  ///
  /// <https://redis.io/commands/sunion>
  pub async fn sunion<R, K>(&self, keys: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<MultipleKeys>,
  {
    commands::sets::sunion(&self.inner, keys).await?.convert()
  }

  /// This command is equal to SUNION, but instead of returning the resulting set, it is stored in `destination`.
  ///
  /// <https://redis.io/commands/sunionstore>
  pub async fn sunionstore<R, D, K>(&self, dest: D, keys: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
  {
    commands::sets::sunionstore(&self.inner, dest, keys).await?.convert()
  }

  // ------------- SORTED SETS ---------------

  /// The blocking variant of the ZPOPMIN command.
  ///
  /// <https://redis.io/commands/bzpopmin>
  pub async fn bzpopmin<K>(&self, keys: K, timeout: f64) -> Result<Option<(RedisKey, RedisValue, f64)>, RedisError>
  where
    K: Into<MultipleKeys>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::sorted_sets::bzpopmin(&self.inner, keys, timeout).await
  }

  /// The blocking variant of the ZPOPMAX command.
  ///
  /// <https://redis.io/commands/bzpopmax>
  pub async fn bzpopmax<K>(&self, keys: K, timeout: f64) -> Result<Option<(RedisKey, RedisValue, f64)>, RedisError>
  where
    K: Into<MultipleKeys>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::sorted_sets::bzpopmax(&self.inner, keys, timeout).await
  }

  /// Adds all the specified members with the specified scores to the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zadd>
  pub async fn zadd<R, K, V>(
    &self,
    key: K,
    options: Option<SetOptions>,
    ordering: Option<Ordering>,
    changed: bool,
    incr: bool,
    values: V,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleZaddValues>,
    V::Error: Into<RedisError>,
  {
    commands::sorted_sets::zadd(&self.inner, key, options, ordering, changed, incr, to!(values)?)
      .await?
      .convert()
  }

  /// Returns the sorted set cardinality (number of elements) of the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zcard>
  pub async fn zcard<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::sorted_sets::zcard(&self.inner, key).await?.convert()
  }

  /// Returns the number of elements in the sorted set at `key` with a score between `min` and `max`.
  ///
  /// <https://redis.io/commands/zcount>
  pub async fn zcount<R, K>(&self, key: K, min: f64, max: f64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::sorted_sets::zcount(&self.inner, key, min, max)
      .await?
      .convert()
  }

  /// This command is similar to ZDIFFSTORE, but instead of storing the resulting sorted set, it is returned to the client.
  ///
  /// <https://redis.io/commands/zdiff>
  pub async fn zdiff<R, K>(&self, keys: K, withscores: bool) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<MultipleKeys>,
  {
    commands::sorted_sets::zdiff(&self.inner, keys, withscores)
      .await?
      .convert()
  }

  /// Computes the difference between the first and all successive input sorted sets and stores the result in `destination`.
  ///
  /// <https://redis.io/commands/zdiffstore>
  pub async fn zdiffstore<R, D, K>(&self, dest: D, keys: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
  {
    commands::sorted_sets::zdiffstore(&self.inner, dest, keys)
      .await?
      .convert()
  }

  /// Increments the score of `member` in the sorted set stored at `key` by `increment`.
  ///
  /// <https://redis.io/commands/zincrby>
  pub async fn zincrby<R, K, V>(&self, key: K, increment: f64, member: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::sorted_sets::zincrby(&self.inner, key, increment, to!(member)?)
      .await?
      .convert()
  }

  /// This command is similar to ZINTERSTORE, but instead of storing the resulting sorted set, it is returned to the client.
  ///
  /// <https://redis.io/commands/zinter>
  pub async fn zinter<R, K, W>(
    &self,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
    withscores: bool,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<MultipleKeys>,
    W: Into<MultipleWeights>,
  {
    commands::sorted_sets::zinter(&self.inner, keys, weights, aggregate, withscores)
      .await?
      .convert()
  }

  /// Computes the intersection of the sorted sets given by the specified keys, and stores the result in `destination`.
  ///
  /// <https://redis.io/commands/zinterstore>
  pub async fn zinterstore<R, D, K, W>(
    &self,
    dest: D,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
    W: Into<MultipleWeights>,
  {
    commands::sorted_sets::zinterstore(&self.inner, dest, keys, weights, aggregate)
      .await?
      .convert()
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering,
  /// this command returns the number of elements in the sorted set at key with a value between min and max.
  ///
  /// <https://redis.io/commands/zlexcount>
  pub async fn zlexcount<R, K, M, N>(&self, key: K, min: M, max: N) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    commands::sorted_sets::zlexcount(&self.inner, key, to!(min)?, to!(max)?)
      .await?
      .convert()
  }

  /// Removes and returns up to count members with the highest scores in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zpopmax>
  pub async fn zpopmax<R, K>(&self, key: K, count: Option<usize>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::sorted_sets::zpopmax(&self.inner, key, count).await?.convert()
  }

  /// Removes and returns up to count members with the lowest scores in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zpopmin>
  pub async fn zpopmin<R, K>(&self, key: K, count: Option<usize>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::sorted_sets::zpopmin(&self.inner, key, count).await?.convert()
  }

  /// When called with just the key argument, return a random element from the sorted set value stored at `key`.
  ///
  /// <https://redis.io/commands/zrandmember>
  pub async fn zrandmember<R, K>(&self, key: K, count: Option<(i64, bool)>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::sorted_sets::zrandmember(&self.inner, key, count)
      .await?
      .convert()
  }

  /// This command is like ZRANGE, but stores the result in the `destination` key.
  ///
  /// <https://redis.io/commands/zrangestore>
  pub async fn zrangestore<R, D, S, M, N>(
    &self,
    dest: D,
    source: S,
    min: M,
    max: N,
    sort: Option<ZSort>,
    rev: bool,
    limit: Option<Limit>,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    D: Into<RedisKey>,
    S: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    commands::sorted_sets::zrangestore(&self.inner, dest, source, to!(min)?, to!(max)?, sort, rev, limit)
      .await?
      .convert()
  }

  /// Returns the specified range of elements in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zrange>
  pub async fn zrange<K, M, N>(
    &self,
    key: K,
    min: M,
    max: N,
    sort: Option<ZSort>,
    rev: bool,
    limit: Option<Limit>,
    withscores: bool,
  ) -> Result<RedisValue, RedisError>
  where
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    commands::sorted_sets::zrange(&self.inner, key, to!(min)?, to!(max)?, sort, rev, limit, withscores).await
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
  /// ordering, this command returns all the elements in the sorted set at `key` with a value between `min` and `max`.
  ///
  /// <https://redis.io/commands/zrangebylex>
  pub async fn zrangebylex<K, M, N>(
    &self,
    key: K,
    min: M,
    max: N,
    limit: Option<Limit>,
  ) -> Result<RedisValue, RedisError>
  where
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    commands::sorted_sets::zrangebylex(&self.inner, key, to!(min)?, to!(max)?, limit).await
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
  /// ordering, this command returns all the elements in the sorted set at `key` with a value between `max` and `min`.
  ///
  /// <https://redis.io/commands/zrevrangebylex>
  pub async fn zrevrangebylex<K, M, N>(
    &self,
    key: K,
    max: M,
    min: N,
    limit: Option<Limit>,
  ) -> Result<RedisValue, RedisError>
  where
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    commands::sorted_sets::zrevrangebylex(&self.inner, key, to!(max)?, to!(min)?, limit).await
  }

  /// Returns all the elements in the sorted set at key with a score between `min` and `max` (including elements
  /// with score equal to `min` or `max`).
  ///
  /// <https://redis.io/commands/zrangebyscore>
  pub async fn zrangebyscore<K, M, N>(
    &self,
    key: K,
    min: M,
    max: N,
    withscores: bool,
    limit: Option<Limit>,
  ) -> Result<RedisValue, RedisError>
  where
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    commands::sorted_sets::zrangebyscore(&self.inner, key, to!(min)?, to!(max)?, withscores, limit).await
  }

  /// Returns all the elements in the sorted set at `key` with a score between `max` and `min` (including
  /// elements with score equal to `max` or `min`).
  ///
  /// <https://redis.io/commands/zrevrangebyscore>
  pub async fn zrevrangebyscore<K, M, N>(
    &self,
    key: K,
    max: M,
    min: N,
    withscores: bool,
    limit: Option<Limit>,
  ) -> Result<RedisValue, RedisError>
  where
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    commands::sorted_sets::zrevrangebyscore(&self.inner, key, to!(max)?, to!(min)?, withscores, limit).await
  }

  /// Returns the rank of member in the sorted set stored at `key`, with the scores ordered from low to high.
  ///
  /// <https://redis.io/commands/zrank>
  pub async fn zrank<R, K, V>(&self, key: K, member: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::sorted_sets::zrank(&self.inner, key, to!(member)?)
      .await?
      .convert()
  }

  /// Removes the specified members from the sorted set stored at `key`. Non existing members are ignored.
  ///
  /// <https://redis.io/commands/zrem>
  pub async fn zrem<R, K, V>(&self, key: K, members: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::sorted_sets::zrem(&self.inner, key, to!(members)?)
      .await?
      .convert()
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
  /// ordering, this command removes all elements in the sorted set stored at `key` between the lexicographical range
  /// specified by `min` and `max`.
  ///
  /// <https://redis.io/commands/zremrangebylex>
  pub async fn zremrangebylex<R, K, M, N>(&self, key: K, min: M, max: N) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    commands::sorted_sets::zremrangebylex(&self.inner, key, to!(min)?, to!(max)?)
      .await?
      .convert()
  }

  /// Removes all elements in the sorted set stored at `key` with rank between `start` and `stop`.
  ///
  /// <https://redis.io/commands/zremrangebyrank>
  pub async fn zremrangebyrank<R, K>(&self, key: K, start: i64, stop: i64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::sorted_sets::zremrangebyrank(&self.inner, key, start, stop)
      .await?
      .convert()
  }

  /// Removes all elements in the sorted set stored at `key` with a score between `min` and `max`.
  ///
  /// <https://redis.io/commands/zremrangebyscore>
  pub async fn zremrangebyscore<R, K, M, N>(&self, key: K, min: M, max: N) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    commands::sorted_sets::zremrangebyscore(&self.inner, key, to!(min)?, to!(max)?)
      .await?
      .convert()
  }

  /// Returns the specified range of elements in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zrevrange>
  pub async fn zrevrange<R, K>(&self, key: K, start: i64, stop: i64, withscores: bool) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::sorted_sets::zrevrange(&self.inner, key, start, stop, withscores)
      .await?
      .convert()
  }

  /// Returns the rank of `member` in the sorted set stored at `key`, with the scores ordered from high to low.
  ///
  /// <https://redis.io/commands/zrevrank>
  pub async fn zrevrank<R, K, V>(&self, key: K, member: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::sorted_sets::zrevrank(&self.inner, key, to!(member)?)
      .await?
      .convert()
  }

  /// Returns the score of `member` in the sorted set at `key`.
  ///
  /// <https://redis.io/commands/zscore>
  pub async fn zscore<R, K, V>(&self, key: K, member: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::sorted_sets::zscore(&self.inner, key, to!(member)?)
      .await?
      .convert()
  }

  /// This command is similar to ZUNIONSTORE, but instead of storing the resulting sorted set, it is returned to the client.
  ///
  /// <https://redis.io/commands/zunion>
  pub async fn zunion<K, W>(
    &self,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
    withscores: bool,
  ) -> Result<RedisValue, RedisError>
  where
    K: Into<MultipleKeys>,
    W: Into<MultipleWeights>,
  {
    commands::sorted_sets::zunion(&self.inner, keys, weights, aggregate, withscores).await
  }

  /// Computes the union of the sorted sets given by the specified keys, and stores the result in `destination`.
  ///
  /// <https://redis.io/commands/zunionstore>
  pub async fn zunionstore<R, D, K, W>(
    &self,
    dest: D,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
    W: Into<MultipleWeights>,
  {
    commands::sorted_sets::zunionstore(&self.inner, dest, keys, weights, aggregate)
      .await?
      .convert()
  }

  /// Returns the scores associated with the specified members in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zmscore>
  pub async fn zmscore<R, K, V>(&self, key: K, members: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::sorted_sets::zmscore(&self.inner, key, to!(members)?)
      .await?
      .convert()
  }

  // ------------- LISTS ------------------

  /// BLPOP is a blocking list pop primitive. It is the blocking version of LPOP because it blocks the connection when there are no elements to pop from
  /// any of the given lists. An element is popped from the head of the first list that is non-empty, with the given keys being checked in the order that they are given.
  ///
  /// <https://redis.io/commands/blpop>
  pub async fn blpop<R, K>(&self, keys: K, timeout: f64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<MultipleKeys>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::lists::blpop(&self.inner, keys, timeout).await?.convert()
  }

  /// BRPOP is a blocking list pop primitive. It is the blocking version of RPOP because it blocks the connection when there are no elements to pop from any of the
  /// given lists. An element is popped from the tail of the first list that is non-empty, with the given keys being checked in the order that they are given.
  ///
  /// <https://redis.io/commands/brpop>
  pub async fn brpop<R, K>(&self, keys: K, timeout: f64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<MultipleKeys>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::lists::brpop(&self.inner, keys, timeout).await?.convert()
  }

  /// The blocking equivalent of [Self::rpoplpush].
  ///
  /// <https://redis.io/commands/brpoplpush>
  pub async fn brpoplpush<R, S, D>(&self, source: S, destination: D, timeout: f64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::lists::brpoplpush(&self.inner, source, destination, timeout)
      .await?
      .convert()
  }

  /// The blocking equivalent of [Self::lmove].
  ///
  /// <https://redis.io/commands/blmove>
  pub async fn blmove<R, S, D>(
    &self,
    source: S,
    destination: D,
    source_direction: LMoveDirection,
    destination_direction: LMoveDirection,
    timeout: f64,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
  {
    utils::disallow_during_transaction(&self.inner)?;

    commands::lists::blmove(
      &self.inner,
      source,
      destination,
      source_direction,
      destination_direction,
      timeout,
    )
    .await?
    .convert()
  }

  /// Returns the element at index index in the list stored at key.
  ///
  /// <https://redis.io/commands/lindex>
  pub async fn lindex<R, K>(&self, key: K, index: i64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::lists::lindex(&self.inner, key, index).await?.convert()
  }

  /// Inserts element in the list stored at key either before or after the reference value `pivot`.
  ///
  /// <https://redis.io/commands/linsert>
  pub async fn linsert<R, K, P, V>(
    &self,
    key: K,
    location: ListLocation,
    pivot: P,
    element: V,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    P: TryInto<RedisValue>,
    P::Error: Into<RedisError>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::lists::linsert(&self.inner, key, location, to!(pivot)?, to!(element)?)
      .await?
      .convert()
  }

  /// Returns the length of the list stored at key.
  ///
  /// <https://redis.io/commands/llen>
  pub async fn llen<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::lists::llen(&self.inner, key).await?.convert()
  }

  /// Removes and returns the first elements of the list stored at key.
  ///
  /// <https://redis.io/commands/lpop>
  pub async fn lpop<R, K>(&self, key: K, count: Option<usize>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::lists::lpop(&self.inner, key, count).await?.convert()
  }

  /// The command returns the index of matching elements inside a Redis list.
  ///
  /// <https://redis.io/commands/lpos>
  pub async fn lpos<R, K, V>(
    &self,
    key: K,
    element: V,
    rank: Option<i64>,
    count: Option<i64>,
    maxlen: Option<i64>,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::lists::lpos(&self.inner, key, to!(element)?, rank, count, maxlen)
      .await?
      .convert()
  }

  /// Insert all the specified values at the head of the list stored at `key`.
  ///
  /// <https://redis.io/commands/lpush>
  pub async fn lpush<R, K, V>(&self, key: K, elements: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::lists::lpush(&self.inner, key, to!(elements)?)
      .await?
      .convert()
  }

  /// Inserts specified values at the head of the list stored at `key`, only if `key` already exists and holds a list.
  ///
  /// <https://redis.io/commands/lpushx>
  pub async fn lpushx<R, K, V>(&self, key: K, elements: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::lists::lpushx(&self.inner, key, to!(elements)?)
      .await?
      .convert()
  }

  /// Returns the specified elements of the list stored at `key`.
  ///
  /// <https://redis.io/commands/lrange>
  pub async fn lrange<R, K>(&self, key: K, start: i64, stop: i64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::lists::lrange(&self.inner, key, start, stop).await?.convert()
  }

  /// Removes the first `count` occurrences of elements equal to `element` from the list stored at `key`.
  ///
  /// <https://redis.io/commands/lrem>
  pub async fn lrem<R, K, V>(&self, key: K, count: i64, element: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::lists::lrem(&self.inner, key, count, to!(element)?)
      .await?
      .convert()
  }

  /// Sets the list element at `index` to `element`.
  ///
  /// <https://redis.io/commands/lset>
  pub async fn lset<R, K, V>(&self, key: K, index: i64, element: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::lists::lset(&self.inner, key, index, to!(element)?)
      .await?
      .convert()
  }

  /// Trim an existing list so that it will contain only the specified range of elements specified.
  ///
  /// <https://redis.io/commands/ltrim>
  pub async fn ltrim<R, K>(&self, key: K, start: i64, stop: i64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::lists::ltrim(&self.inner, key, start, stop).await?.convert()
  }

  /// Removes and returns the last elements of the list stored at `key`.
  ///
  /// <https://redis.io/commands/rpop>
  pub async fn rpop<R, K>(&self, key: K, count: Option<usize>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::lists::rpop(&self.inner, key, count).await?.convert()
  }

  /// Atomically returns and removes the last element (tail) of the list stored at `source`, and pushes the element at the first element (head) of the list stored at `destination`.
  ///
  /// <https://redis.io/commands/rpoplpush>
  pub async fn rpoplpush<R, S, D>(&self, source: S, dest: D) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
  {
    commands::lists::rpoplpush(&self.inner, source, dest).await?.convert()
  }

  /// Atomically returns and removes the first/last element (head/tail depending on the source direction argument) of the list stored at `source`, and pushes
  /// the element at the first/last element (head/tail depending on the destination direction argument) of the list stored at `destination`.
  ///
  /// <https://redis.io/commands/lmove>
  pub async fn lmove<R, S, D>(
    &self,
    source: S,
    dest: D,
    source_direction: LMoveDirection,
    dest_direction: LMoveDirection,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
  {
    commands::lists::lmove(&self.inner, source, dest, source_direction, dest_direction)
      .await?
      .convert()
  }

  /// Insert all the specified values at the tail of the list stored at `key`.
  ///
  /// <https://redis.io/commands/rpush>
  pub async fn rpush<R, K, V>(&self, key: K, elements: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::lists::rpush(&self.inner, key, to!(elements)?)
      .await?
      .convert()
  }

  /// Inserts specified values at the tail of the list stored at `key`, only if key already exists and holds a list.
  ///
  /// <https://redis.io/commands/rpushx>
  pub async fn rpushx<R, K, V>(&self, key: K, elements: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::lists::rpushx(&self.inner, key, to!(elements)?)
      .await?
      .convert()
  }

  // --------------- SCANNING ---------------

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

  // --------------- STREAMS ----------------
}

#[cfg(test)]
mod tests {

  #[cfg(feature = "sha1-support")]
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
