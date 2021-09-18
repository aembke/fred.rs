use crate::commands;
use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::{MultiPolicy, RedisClientInner};
use crate::modules::metrics::Stats;
use crate::modules::response::RedisResponse;
use crate::multiplexer::commands as multiplexer_commands;
use crate::multiplexer::utils as multiplexer_utils;
use crate::protocol::types::RedisCommand;
use crate::types::*;
use crate::utils;
use futures::Stream;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::time::interval as tokio_interval;
use tokio_stream::wrappers::UnboundedReceiverStream;

#[doc(hidden)]
pub type CommandSender = UnboundedSender<RedisCommand>;

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

impl RedisClient {
  /// Create a new client instance without connecting to the server.
  pub fn new(config: RedisConfig) -> RedisClient {
    RedisClient {
      inner: RedisClientInner::new(config),
    }
  }

  /// The unique ID identifying this client and underlying connections. All connections will use the ID of the client that created them.
  ///
  /// The client will use [CLIENT SETNAME](https://redis.io/commands/client-setname) upon initializing a connection so client logs can be associated with server logs.
  pub fn id(&self) -> &Arc<String> {
    &self.inner.id
  }

  /// Read the config used to initialize the client.
  pub fn client_config(&self) -> RedisConfig {
    utils::read_locked(&self.inner.config)
  }

  /// Read the reconnect policy used to initialize the client.
  pub fn client_reconnect_policy(&self) -> Option<ReconnectPolicy> {
    self.inner.policy.read().clone()
  }

  /// Whether or not the client has a reconnection policy.
  pub fn has_reconnect_policy(&self) -> bool {
    self.inner.policy.read().is_some()
  }

  /// Connect to the Redis server with an optional reconnection policy.
  ///
  /// This function returns a `JoinHandle` to a task that drives the connection. It will not resolve
  /// until the connection closes, and if a reconnection policy with unlimited attempts
  /// is provided then the `JoinHandle` will run forever.
  ///
  /// **Note:** See the [RedisConfig](crate::types::RedisConfig) documentation for more information on how the `policy` is applied to new connections.
  pub fn connect(&self, policy: Option<ReconnectPolicy>) -> ConnectHandle {
    let inner = self.inner.clone();

    tokio::spawn(async move {
      let result = multiplexer_commands::init(&inner, policy).await;
      if let Err(ref e) = result {
        multiplexer_utils::emit_connect_error(&inner, e);
      }
      utils::set_client_state(&inner.state, ClientState::Disconnected);
      result
    })
  }

  /// Wait for the client to connect to the server, or return an error if the initial connection cannot be established.
  /// If the client is already connected this future will resolve immediately.
  ///
  /// This can be used with `on_reconnect` to separate initialization logic that needs to occur only on the first connection attempt vs subsequent attempts.
  pub async fn wait_for_connect(&self) -> Result<(), RedisError> {
    utils::wait_for_connect(&self.inner).await
  }

  /// Create a new `RedisClient` from the config provided to this client.
  ///
  /// The returned client will not be connected to the server, and it will use new connections after connecting.
  pub fn clone_new(&self) -> Self {
    RedisClient::new(utils::read_locked(&self.inner.config))
  }

  /// Whether or not the client will pipeline commands.
  pub fn is_pipelined(&self) -> bool {
    self.inner.is_pipelined()
  }

  /// Return a future that will ping the server on an interval.
  ///
  /// If the underlying connection closes or `PING` returns an error this will break the interval and this function will need to be called again.
  pub async fn enable_heartbeat(&self, interval: Duration) -> Result<(), RedisError> {
    let mut interval = tokio_interval(interval);
    loop {
      interval.tick().await;

      if utils::is_locked_some(&self.inner.multi_block) {
        let inner = &self.inner;
        _debug!(inner, "Skip heartbeat while inside transaction.");
        continue;
      }

      if self.state() != ClientState::Connected {
        break;
      }
      let _ = self.ping().await?;
    }

    Ok(())
  }

  /// Read the number of request redeliveries.
  ///
  /// This is the number of times a request had to be sent again due to a connection closing while waiting on a response.
  pub fn read_redelivery_count(&self) -> usize {
    utils::read_atomic(&self.inner.redeliver_count)
  }

  /// Read and reset the number of request redeliveries.
  pub fn take_redelivery_count(&self) -> usize {
    utils::set_atomic(&self.inner.redeliver_count, 0)
  }

  /// Read the state of the underlying connection(s).
  ///
  /// If running against a cluster the underlying state will reflect the state of the least healthy connection, if any.
  pub fn state(&self) -> ClientState {
    self.inner.state.read().clone()
  }

  /// Whether or not the client has an active connection to the server(s).
  pub fn is_connected(&self) -> bool {
    *self.inner.state.read() == ClientState::Connected
  }

  /// Read the cached state of the cluster used for routing commands to the correct cluster nodes.
  pub fn cached_cluster_state(&self) -> Option<ClusterKeyCache> {
    self.inner.cluster_state.read().clone()
  }

  /// Read latency metrics across all commands.
  ///
  /// This metric reflects the total latency experienced by callers, including time spent waiting in memory to be written and network latency.
  /// Features such as automatic reconnect, `reconnect-on-auth-error`, and frame serialization time can all affect these values.
  #[cfg(feature = "metrics")]
  pub fn read_latency_metrics(&self) -> Stats {
    self.inner.latency_stats.read().read_metrics()
  }

  /// Read and consume latency metrics, resetting their values afterwards.
  #[cfg(feature = "metrics")]
  pub fn take_latency_metrics(&self) -> Stats {
    self.inner.latency_stats.write().take_metrics()
  }

  /// Read network latency metrics across all commands.
  ///
  /// This metric only reflects time spent waiting on a response. It will factor in reconnect time if a response doesn't arrive due to a connection
  /// closing, but it does not factor in the time a command spends waiting to be written, serialization time, backpressure, etc.  
  #[cfg(feature = "metrics")]
  pub fn read_network_latency_metrics(&self) -> Stats {
    self.inner.network_latency_stats.read().read_metrics()
  }

  /// Read and consume network latency metrics, resetting their values afterwards.
  #[cfg(feature = "metrics")]
  pub fn take_network_latency_metrics(&self) -> Stats {
    self.inner.network_latency_stats.write().take_metrics()
  }

  /// Read request payload size metrics across all commands.
  #[cfg(feature = "metrics")]
  pub fn read_req_size_metrics(&self) -> Stats {
    self.inner.req_size_stats.read().read_metrics()
  }

  /// Read and consume request payload size metrics, resetting their values afterwards.
  #[cfg(feature = "metrics")]
  pub fn take_req_size_metrics(&self) -> Stats {
    self.inner.req_size_stats.write().take_metrics()
  }

  /// Read response payload size metrics across all commands.
  #[cfg(feature = "metrics")]
  pub fn read_res_size_metrics(&self) -> Stats {
    self.inner.res_size_stats.read().read_metrics()
  }

  /// Read and consume response payload size metrics, resetting their values afterwards.
  #[cfg(feature = "metrics")]
  pub fn take_res_size_metrics(&self) -> Stats {
    self.inner.res_size_stats.write().take_metrics()
  }

  /// Read the number of buffered commands that have not yet been sent to the server.
  pub fn command_queue_len(&self) -> usize {
    utils::read_atomic(&self.inner.cmd_buffer_len)
  }

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

  /// Whether or not the client is using a clustered Redis deployment.
  pub fn is_clustered(&self) -> bool {
    utils::is_clustered(&self.inner.config)
  }

  /// Close the connection to the Redis server. The returned future resolves when the command has been written to the socket,
  /// not when the connection has been fully closed. Some time after this future resolves the future returned by [connect](Self::connect)
  /// will resolve which indicates that the connection has been fully closed.
  ///
  /// This function will also close all error, pubsub message, and reconnection event streams.
  pub async fn quit(&self) -> Result<(), RedisError> {
    commands::server::quit(&self.inner).await
  }

  /// Shut down the server and quit the client.
  ///
  /// <https://redis.io/commands/shutdown>
  pub async fn shutdown(&self, flags: Option<ShutdownFlags>) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::server::shutdown(&self.inner, flags).await
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

  /// Run a custom command that is not yet supported via another interface on this client. This is most useful when interacting with third party modules or extensions.
  ///
  /// This interface makes some assumptions about the nature of the provided command:
  /// * For commands comprised of multiple command strings they must be separated by a space.
  /// * The command string will be sent to the server exactly as written.
  /// * Arguments will be sent in the order provided.
  /// * When used against a cluster the caller must provide the correct hash slot to identify the cluster
  /// node that should receive the command. If one is not provided the command will be sent to a random node
  /// in the cluster.
  ///
  /// Callers should use the re-exported [redis_keyslot](crate::client::util::redis_keyslot) function to hash the command's key, if necessary.
  ///
  /// Callers that find themselves using this interface for commands that are not a part of a third party extension should file an issue
  /// to add the command to the list of supported commands. This interface should be used with caution as it may break the automatic pipeline
  /// features in the client if command flags are not properly configured.
  pub async fn custom<R, T>(&self, cmd: CustomCommand, args: Vec<T>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    T: TryInto<RedisValue>,
    T::Error: Into<RedisError>,
  {
    commands::server::custom(&self.inner, cmd, utils::try_into_vec(args)?)
      .await?
      .convert()
  }

  /// Subscribe to a channel on the PubSub interface, returning the number of channels to which the client is subscribed.
  ///
  /// Any messages received before [on_message](Self::on_message) is called will be discarded, so it's usually best to call [on_message](Self::on_message)
  /// before calling [subscribe](Self::subscribe) for the first time.
  ///
  /// <https://redis.io/commands/subscribe>
  pub async fn subscribe<S>(&self, channel: S) -> Result<usize, RedisError>
  where
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::pubsub::subscribe(&self.inner, channel).await
  }

  /// Unsubscribe from a channel on the PubSub interface, returning the number of channels to which hte client is subscribed.
  ///
  /// <https://redis.io/commands/unsubscribe>
  pub async fn unsubscribe<S>(&self, channel: S) -> Result<usize, RedisError>
  where
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::pubsub::unsubscribe(&self.inner, channel).await
  }

  /// Subscribes the client to the given patterns.
  ///
  /// <https://redis.io/commands/psubscribe>
  pub async fn psubscribe<S>(&self, patterns: S) -> Result<Vec<usize>, RedisError>
  where
    S: Into<MultipleStrings>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::pubsub::psubscribe(&self.inner, patterns).await
  }

  /// Unsubscribes the client from the given patterns, or from all of them if none is given.
  ///
  /// <https://redis.io/commands/punsubscribe>
  pub async fn punsubscribe<S>(&self, patterns: S) -> Result<Vec<usize>, RedisError>
  where
    S: Into<MultipleStrings>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::pubsub::punsubscribe(&self.inner, patterns).await
  }

  /// Publish a message on the PubSub interface, returning the number of clients that received the message.
  ///
  /// <https://redis.io/commands/publish>
  pub async fn publish<R, S, V>(&self, channel: S, message: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<String>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::pubsub::publish(&self.inner, channel, to!(message)?)
      .await?
      .convert()
  }

  /// Enter a MULTI block, executing subsequent commands as a transaction.
  ///
  /// <https://redis.io/commands/multi>
  ///
  /// The `abort_on_error` flag indicates whether the client should automatically abort the transaction when an error is received from a command within the transaction.
  ///
  /// See <https://redis.io/topics/transactions#errors-inside-a-transaction> for more information. If this flag is `false` then the caller will need to `exec` or `discard`
  /// the transaction before either retrying or moving on to new commands outside the transaction.
  pub async fn multi(&self, abort_on_error: bool) -> Result<TransactionClient, RedisError> {
    if utils::is_clustered(&self.inner.config) {
      let policy = MultiPolicy {
        hash_slot: None,
        abort_on_error,
        sent_multi: false,
      };

      if !utils::check_and_set_none(&self.inner.multi_block, policy) {
        return Err(RedisError::new(
          RedisErrorKind::InvalidCommand,
          "Client is already within a MULTI transaction.",
        ));
      }

      debug!("{}: Defer MULTI command until hash slot is specified.", self.inner.id);
      Ok(TransactionClient::from(&self.inner))
    } else {
      let policy = MultiPolicy {
        hash_slot: None,
        abort_on_error,
        sent_multi: true,
      };
      if !utils::check_and_set_none(&self.inner.multi_block, policy) {
        return Err(RedisError::new(
          RedisErrorKind::InvalidCommand,
          "Client is already within a MULTI transaction.",
        ));
      }

      commands::server::multi(&self.inner)
        .await
        .map(|_| TransactionClient::from(&self.inner))
    }
  }

  /// Whether or not the client is currently in the middle of a MULTI transaction.
  pub fn in_transaction(&self) -> bool {
    utils::is_locked_some(&self.inner.multi_block)
  }

  /// Force the client to abort any in-flight transactions.
  ///
  /// The `Drop` trait on the [TransactionClient] is not async and so callers that accidentally drop the transaction
  /// client associated with a MULTI block before calling EXEC or DISCARD can use this function to exit the transaction.
  /// A warning log line will be emitted if the transaction client is dropped before calling EXEC or DISCARD.
  pub async fn force_discard_transaction(&self) -> Result<(), RedisError> {
    commands::server::discard(&self.inner).await
  }

  /// Marks the given keys to be watched for conditional execution of a transaction.
  ///
  /// <https://redis.io/commands/watch>
  pub async fn watch<K>(&self, keys: K) -> Result<(), RedisError>
  where
    K: Into<MultipleKeys>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::keys::watch(&self.inner, keys).await
  }

  /// Flushes all the previously watched keys for a transaction.
  ///
  /// <https://redis.io/commands/unwatch>
  pub async fn unwatch(&self) -> Result<(), RedisError> {
    commands::keys::unwatch(&self.inner).await
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

  /// Ping the Redis server.
  ///
  /// <https://redis.io/commands/ping>
  pub async fn ping(&self) -> Result<(), RedisError> {
    commands::server::ping(&self.inner).await?.convert()
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

  // ------------- CLIENT ---------------

  /// Return the ID of the current connection.
  ///
  /// Note: Against a clustered deployment this will return the ID of a random connection. See [connection_ids](Self::connection_ids) for  more information.
  ///
  /// <https://redis.io/commands/client-id>
  pub async fn client_id<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::client::client_id(&self.inner).await?.convert()
  }

  /// Read the connection IDs for the active connections to each server.
  ///
  /// The returned map contains each server's `host:port` and the result of calling `CLIENT ID` on the connection.
  ///
  /// Note: despite being async this function will usually return cached information from the client if possible.
  pub async fn connection_ids(&self) -> Result<HashMap<Arc<String>, i64>, RedisError> {
    utils::read_connection_ids(&self.inner).await.ok_or(RedisError::new(
      RedisErrorKind::Unknown,
      "Failed to read connection IDs",
    ))
  }

  /// The command returns information and statistics about the current client connection in a mostly human readable format.
  ///
  /// <https://redis.io/commands/client-info>
  pub async fn client_info<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::client::client_info(&self.inner).await?.convert()
  }

  /// Close a given connection or set of connections.
  ///
  /// <https://redis.io/commands/client-kill>
  pub async fn client_kill<R>(&self, filters: Vec<ClientKillFilter>) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::client::client_kill(&self.inner, filters).await?.convert()
  }

  /// The CLIENT LIST command returns information and statistics about the client connections server in a mostly human readable format.
  ///
  /// <https://redis.io/commands/client-list>
  pub async fn client_list<R, I>(&self, r#type: Option<ClientKillType>, ids: Option<Vec<I>>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    I: Into<RedisKey>,
  {
    commands::client::client_list(&self.inner, r#type, ids).await?.convert()
  }

  /// The CLIENT GETNAME returns the name of the current connection as set by CLIENT SETNAME.
  ///
  /// <https://redis.io/commands/client-getname>
  pub async fn client_getname<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::client::client_getname(&self.inner).await?.convert()
  }

  /// Assign a name to the current connection.
  ///
  /// **Note: The client automatically generates a unique name for each client that is shared by all underlying connections.
  /// Use [Self::id] to read the automatically generated name.**
  ///
  /// <https://redis.io/commands/client-setname>
  pub async fn client_setname<S>(&self, name: S) -> Result<(), RedisError>
  where
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::client::client_setname(&self.inner, name).await
  }

  /// CLIENT PAUSE is a connections control command able to suspend all the Redis clients for the specified amount of time (in milliseconds).
  ///
  /// <https://redis.io/commands/client-pause>
  pub async fn client_pause(&self, timeout: i64, mode: Option<ClientPauseKind>) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::client::client_pause(&self.inner, timeout, mode).await
  }

  /// CLIENT UNPAUSE is used to resume command processing for all clients that were paused by CLIENT PAUSE.
  ///
  /// <https://redis.io/commands/client-unpause>
  pub async fn client_unpause(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::client::client_unpause(&self.inner).await
  }

  /// The CLIENT REPLY command controls whether the server will reply the client's commands. The following modes are available:
  ///
  /// <https://redis.io/commands/client-reply>
  pub async fn client_reply(&self, flag: ClientReplyFlag) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::client::client_reply(&self.inner, flag).await
  }

  /// This command can unblock, from a different connection, a client blocked in a blocking operation, such as for instance BRPOP or XREAD or WAIT.
  ///
  /// Note: this command is sent on a backchannel connection and will work even when the main connection is blocked.
  ///
  /// <https://redis.io/commands/client-unblock>
  pub async fn client_unblock<R, S>(&self, id: S, flag: Option<ClientUnblockFlag>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<RedisValue>,
  {
    commands::client::client_unblock(&self.inner, id, flag).await?.convert()
  }

  /// A convenience function to unblock any blocked connection on this client.
  pub async fn unblock_self(&self, flag: Option<ClientUnblockFlag>) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::client::unblock_self(&self.inner, flag).await
  }

  // ------------- CLUSTER -----------

  /// Advances the cluster config epoch.
  ///
  /// <https://redis.io/commands/cluster-bumpepoch>
  pub async fn cluster_bumpepoch<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::cluster::cluster_bumpepoch(&self.inner).await?.convert()
  }

  /// Deletes all slots from a node.
  ///
  /// <https://redis.io/commands/cluster-flushslots>
  pub async fn cluster_flushslots(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_flushslots(&self.inner).await
  }

  /// Returns the node's id.
  ///
  /// <https://redis.io/commands/cluster-myid>
  pub async fn cluster_myid<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::cluster::cluster_myid(&self.inner).await?.convert()
  }

  /// Read the current cluster node configuration.
  ///
  /// Note: The client keeps a cached, parsed version of the cluster state in memory available at [cached_cluster_state](Self::cached_cluster_state).
  ///
  /// <https://redis.io/commands/cluster-nodes>
  pub async fn cluster_nodes(&self) -> Result<String, RedisError> {
    commands::cluster::cluster_nodes(&self.inner).await?.convert()
  }

  /// Forces a node to save the nodes.conf configuration on disk.
  ///
  /// <https://redis.io/commands/cluster-saveconfig>
  pub async fn cluster_saveconfig(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_saveconfig(&self.inner).await
  }

  /// CLUSTER SLOTS returns details about which cluster slots map to which Redis instances.
  ///
  /// <https://redis.io/commands/cluster-slots>
  pub async fn cluster_slots(&self) -> Result<RedisValue, RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_slots(&self.inner).await
  }

  /// CLUSTER INFO provides INFO style information about Redis Cluster vital parameters.
  ///
  /// <https://redis.io/commands/cluster-info>
  pub async fn cluster_info(&self) -> Result<ClusterInfo, RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_info(&self.inner).await
  }

  /// This command is useful in order to modify a node's view of the cluster configuration. Specifically it assigns a set of hash slots to the node receiving the command.
  ///
  /// <https://redis.io/commands/cluster-addslots>
  pub async fn cluster_add_slots<S>(&self, slots: S) -> Result<(), RedisError>
  where
    S: Into<MultipleHashSlots>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_add_slots(&self.inner, slots).await
  }

  /// The command returns the number of failure reports for the specified node.
  ///
  /// <https://redis.io/commands/cluster-count-failure-reports>
  pub async fn cluster_count_failure_reports<R, S>(&self, node_id: S) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<String>,
  {
    commands::cluster::cluster_count_failure_reports(&self.inner, node_id)
      .await?
      .convert()
  }

  /// Returns the number of keys in the specified Redis Cluster hash slot.
  ///
  /// <https://redis.io/commands/cluster-countkeysinslot>
  pub async fn cluster_count_keys_in_slot<R>(&self, slot: u16) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::cluster::cluster_count_keys_in_slot(&self.inner, slot)
      .await?
      .convert()
  }

  /// The CLUSTER DELSLOTS command asks a particular Redis Cluster node to forget which master is serving the hash slots specified as arguments.
  ///
  /// <https://redis.io/commands/cluster-delslots>
  pub async fn cluster_del_slots<S>(&self, slots: S) -> Result<(), RedisError>
  where
    S: Into<MultipleHashSlots>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_del_slots(&self.inner, slots).await
  }

  /// This command, that can only be sent to a Redis Cluster replica node, forces the replica to start a manual failover of its master instance.
  ///
  /// <https://redis.io/commands/cluster-failover>
  pub async fn cluster_failover(&self, flag: Option<ClusterFailoverFlag>) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_failover(&self.inner, flag).await
  }

  /// The command is used in order to remove a node, specified via its node ID, from the set of known nodes of the Redis Cluster node receiving the command.
  /// In other words the specified node is removed from the nodes table of the node receiving the command.
  ///
  /// <https://redis.io/commands/cluster-forget>
  pub async fn cluster_forget<S>(&self, node_id: S) -> Result<(), RedisError>
  where
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_forget(&self.inner, node_id).await
  }

  /// The command returns an array of keys names stored in the contacted node and hashing to the specified hash slot.
  ///
  /// <https://redis.io/commands/cluster-getkeysinslot>
  pub async fn cluster_get_keys_in_slot<R>(&self, slot: u16, count: u64) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_get_keys_in_slot(&self.inner, slot, count)
      .await?
      .convert()
  }

  /// Returns an integer identifying the hash slot the specified key hashes to.
  ///
  /// <https://redis.io/commands/cluster-keyslot>
  pub async fn cluster_keyslot<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::cluster::cluster_keyslot(&self.inner, key).await?.convert()
  }

  /// CLUSTER MEET is used in order to connect different Redis nodes with cluster support enabled, into a working cluster.
  ///
  /// <https://redis.io/commands/cluster-meet>
  pub async fn cluster_meet<S>(&self, ip: S, port: u16) -> Result<(), RedisError>
  where
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_meet(&self.inner, ip, port).await
  }

  /// The command reconfigures a node as a replica of the specified master. If the node receiving the command is an empty master, as
  /// a side effect of the command, the node role is changed from master to replica.
  ///
  /// <https://redis.io/commands/cluster-replicate>
  pub async fn cluster_replicate<S>(&self, node_id: S) -> Result<(), RedisError>
  where
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_replicate(&self.inner, node_id).await
  }

  /// The command provides a list of replica nodes replicating from the specified master node.
  ///
  /// <https://redis.io/commands/cluster-replicas>
  pub async fn cluster_replicas<S>(&self, node_id: S) -> Result<String, RedisError>
  where
    S: Into<String>,
  {
    commands::cluster::cluster_replicas(&self.inner, node_id)
      .await?
      .convert()
  }

  /// Reset a Redis Cluster node, in a more or less drastic way depending on the reset type, that can be hard or soft. Note that
  /// this command does not work for masters if they hold one or more keys, in that case to completely reset a master node keys
  /// must be removed first, e.g. by using FLUSHALL first, and then CLUSTER RESET.
  ///
  /// <https://redis.io/commands/cluster-reset>
  pub async fn cluster_reset(&self, mode: Option<ClusterResetFlag>) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_reset(&self.inner, mode).await
  }

  /// This command sets a specific config epoch in a fresh node.
  ///
  /// <https://redis.io/commands/cluster-set-config-epoch>
  pub async fn cluster_set_config_epoch(&self, epoch: u64) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_set_config_epoch(&self.inner, epoch).await
  }

  /// CLUSTER SETSLOT is responsible of changing the state of a hash slot in the receiving node in different ways.
  ///
  /// <https://redis.io/commands/cluster-setslot>
  pub async fn cluster_setslot(&self, slot: u16, state: ClusterSetSlotState) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::cluster::cluster_setslot(&self.inner, slot, state).await
  }

  // -------------- CONFIG ---------------

  /// Resets the statistics reported by Redis using the INFO command.
  ///
  /// <https://redis.io/commands/config-resetstat>
  pub async fn config_resetstat(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::config::config_resetstat(&self.inner).await
  }

  /// The CONFIG REWRITE command rewrites the redis.conf file the server was started with, applying the minimal changes needed to make it
  /// reflect the configuration currently used by the server, which may be different compared to the original one because of the use of
  /// the CONFIG SET command.
  ///
  /// <https://redis.io/commands/config-rewrite>
  pub async fn config_rewrite(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::config::config_rewrite(&self.inner).await
  }

  /// The CONFIG GET command is used to read the configuration parameters of a running Redis server.
  ///
  /// <https://redis.io/commands/config-get>
  pub async fn config_get<R, S>(&self, parameter: S) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::config::config_get(&self.inner, parameter).await?.convert()
  }

  /// The CONFIG SET command is used in order to reconfigure the server at run time without the need to restart Redis.
  ///
  /// <https://redis.io/commands/config-set>
  pub async fn config_set<P, V>(&self, parameter: P, value: V) -> Result<(), RedisError>
  where
    P: Into<String>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::config::config_set(&self.inner, parameter, to!(value)?).await
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

  // ---------------- ACL ------------------------

  /// When Redis is configured to use an ACL file (with the aclfile configuration option), this command will reload the
  /// ACLs from the file, replacing all the current ACL rules with the ones defined in the file.
  ///
  /// <https://redis.io/commands/acl-load>
  pub async fn acl_load(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_load(&self.inner).await
  }

  /// When Redis is configured to use an ACL file (with the aclfile configuration option), this command will save the
  /// currently defined ACLs from the server memory to the ACL file.
  ///
  /// <https://redis.io/commands/acl-save>
  pub async fn acl_save(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_save(&self.inner).await
  }

  /// The command shows the currently active ACL rules in the Redis server.
  ///
  /// <https://redis.io/commands/acl-list>
  pub async fn acl_list<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_list(&self.inner).await?.convert()
  }

  /// The command shows a list of all the usernames of the currently configured users in the Redis ACL system.
  ///
  /// <https://redis.io/commands/acl-users>
  pub async fn acl_users<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_users(&self.inner).await?.convert()
  }

  /// The command returns all the rules defined for an existing ACL user.
  ///
  /// <https://redis.io/commands/acl-getuser>
  pub async fn acl_getuser<S>(&self, username: S) -> Result<Option<AclUser>, RedisError>
  where
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_getuser(&self.inner, username).await
  }

  /// Create an ACL user with the specified rules or modify the rules of an existing user.
  ///
  /// <https://redis.io/commands/acl-setuser>
  pub async fn acl_setuser<S>(&self, username: S, rules: Vec<AclRule>) -> Result<(), RedisError>
  where
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_setuser(&self.inner, username, rules).await
  }

  /// Delete all the specified ACL users and terminate all the connections that are authenticated with such users.
  ///
  /// <https://redis.io/commands/acl-deluser>
  pub async fn acl_deluser<R, S>(&self, usernames: S) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<MultipleStrings>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_deluser(&self.inner, usernames).await?.convert()
  }

  /// The command shows the available ACL categories if called without arguments. If a category name is given,
  /// the command shows all the Redis commands in the specified category.
  ///
  /// <https://redis.io/commands/acl-cat>
  pub async fn acl_cat<R, S>(&self, category: Option<S>) -> Result<Vec<String>, RedisError>
  where
    S: Into<String>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_cat(&self.inner, category).await?.convert()
  }

  /// Generate a password with length `bits`, returning the password.
  pub async fn acl_genpass(&self, bits: Option<u16>) -> Result<String, RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_genpass(&self.inner, bits).await?.convert()
  }

  /// Return the username the current connection is authenticated with. New connections are authenticated
  /// with the "default" user.
  ///
  /// <https://redis.io/commands/acl-whoami>
  pub async fn acl_whoami(&self) -> Result<String, RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_whoami(&self.inner).await?.convert()
  }

  /// Read `count` recent ACL security events.
  ///
  /// <https://redis.io/commands/acl-log>
  pub async fn acl_log_count(&self, count: Option<u32>) -> Result<RedisValue, RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_log_count(&self.inner, count).await
  }

  /// Clear the ACL security events logs.
  ///
  /// <https://redis.io/commands/acl-log>
  pub async fn acl_log_reset(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::acl::acl_log_reset(&self.inner).await
  }

  // ----------- KEYS ------------

  /// Return a random key from the currently selected database.
  ///
  /// <https://redis.io/commands/randomkey>
  pub async fn randomkey<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::keys::randomkey(&self.inner).await?.convert()
  }

  /// This command copies the value stored at the source key to the destination key.
  ///
  /// <https://redis.io/commands/copy>
  pub async fn copy<R, S, D>(&self, source: S, destination: D, db: Option<u8>, replace: bool) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
  {
    commands::keys::copy(&self.inner, source, destination, db, replace)
      .await?
      .convert()
  }

  /// Serialize the value stored at `key` in a Redis-specific format and return it as bulk string.
  ///
  /// <https://redis.io/commands/dump>
  pub async fn dump<K>(&self, key: K) -> Result<RedisValue, RedisError>
  where
    K: Into<RedisKey>,
  {
    commands::keys::dump(&self.inner, key).await
  }

  /// Create a key associated with a value that is obtained by deserializing the provided serialized value
  ///
  /// <https://redis.io/commands/restore>
  pub async fn restore<K>(
    &self,
    key: K,
    ttl: i64,
    serialized: RedisValue,
    replace: bool,
    absttl: bool,
    idletime: Option<i64>,
    frequency: Option<i64>,
  ) -> Result<RedisValue, RedisError>
  where
    K: Into<RedisKey>,
  {
    commands::keys::restore(&self.inner, key, ttl, serialized, replace, absttl, idletime, frequency).await
  }

  /// Set a value with optional NX|XX, EX|PX|EXAT|PXAT|KEEPTTL, and GET arguments.
  ///
  /// <https://redis.io/commands/set>
  pub async fn set<R, K, V>(
    &self,
    key: K,
    value: V,
    expire: Option<Expiration>,
    options: Option<SetOptions>,
    get: bool,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    let (key, value) = (key.into(), to!(value)?);
    commands::keys::set(&self.inner, key, value, expire, options, get)
      .await?
      .convert()
  }

  /// Read a value from the server.
  ///
  /// <https://redis.io/commands/get>
  pub async fn get<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::get(&self.inner, key).await?.convert()
  }

  /// Returns the substring of the string value stored at `key` with offsets `start` and `end` (both inclusive).
  ///
  /// Note: Command formerly called SUBSTR in Redis verison <=2.0.
  ///
  /// <https://redis.io/commands/getrange>
  pub async fn getrange<R, K>(&self, key: K, start: usize, end: usize) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::getrange(&self.inner, key, start, end).await?.convert()
  }

  /// Overwrites part of the string stored at `key`, starting at the specified `offset`, for the entire length of `value`.
  ///
  /// <https://redis.io/commands/setrange>
  pub async fn setrange<R, K, V>(&self, key: K, offset: u32, value: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::keys::setrange(&self.inner, key, offset, to!(value)?)
      .await?
      .convert()
  }

  /// Atomically sets `key` to `value` and returns the old value stored at `key`.
  ///
  /// Returns an error if `key` does not hold string value. Returns nil if `key` does not exist.
  ///
  /// <https://redis.io/commands/getset>
  pub async fn getset<R, K, V>(&self, key: K, value: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::keys::getset(&self.inner, key, to!(value)?).await?.convert()
  }

  /// Get the value of key and delete the key. This command is similar to GET, except for the fact that it also deletes the key on success (if and only if the key's value type is a string).
  ///
  /// <https://redis.io/commands/getdel>
  pub async fn getdel<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::getdel(&self.inner, key).await?.convert()
  }

  /// Returns the length of the string value stored at key. An error is returned when key holds a non-string value.
  ///
  /// <https://redis.io/commands/strlen>
  pub async fn strlen<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::strlen(&self.inner, key).await?.convert()
  }

  /// Removes the specified keys. A key is ignored if it does not exist.
  ///
  /// Returns the number of keys removed.
  ///
  /// <https://redis.io/commands/del>
  pub async fn del<R, K>(&self, keys: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<MultipleKeys>,
  {
    commands::keys::del(&self.inner, keys).await?.convert()
  }

  /// Returns the values of all specified keys. For every key that does not hold a string value or does not exist, the special value nil is returned.
  ///
  /// <https://redis.io/commands/mget>
  pub async fn mget<R, K>(&self, keys: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<MultipleKeys>,
  {
    commands::keys::mget(&self.inner, keys).await?.convert()
  }

  /// Sets the given keys to their respective values.
  ///
  /// <https://redis.io/commands/mset>
  pub async fn mset<V>(&self, values: V) -> Result<RedisValue, RedisError>
  where
    V: Into<RedisMap>,
  {
    commands::keys::mset(&self.inner, values).await
  }

  /// Sets the given keys to their respective values. MSETNX will not perform any operation at all even if just a single key already exists.
  ///
  /// <https://redis.io/commands/msetnx>
  pub async fn msetnx<R, V>(&self, values: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    V: Into<RedisMap>,
  {
    commands::keys::msetnx(&self.inner, values).await?.convert()
  }

  /// Increments the number stored at `key` by one. If the key does not exist, it is set to 0 before performing the operation.
  ///
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incr>
  pub async fn incr<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::incr(&self.inner, key).await?.convert()
  }

  /// Increments the number stored at `key` by `val`. If the key does not exist, it is set to 0 before performing the operation.
  ///
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incrby>
  pub async fn incr_by<R, K>(&self, key: K, val: i64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::incr_by(&self.inner, key, val).await?.convert()
  }

  /// Increment the string representing a floating point number stored at key by `val`. If the key does not exist, it is set to 0 before performing the operation.
  ///
  /// Returns an error if key value is the wrong type or if the current value cannot be parsed as a floating point value.
  ///
  /// <https://redis.io/commands/incrbyfloat>
  pub async fn incr_by_float<R, K>(&self, key: K, val: f64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::incr_by_float(&self.inner, key, val).await?.convert()
  }

  /// Decrements the number stored at `key` by one. If the key does not exist, it is set to 0 before performing the operation.
  ///
  /// Returns an error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decr>
  pub async fn decr<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::decr(&self.inner, key).await?.convert()
  }

  /// Decrements the number stored at `key` by `val`. If the key does not exist, it is set to 0 before performing the operation.
  ///
  /// Returns an error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decrby>
  pub async fn decr_by<R, K>(&self, key: K, val: i64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::decr_by(&self.inner, key, val).await?.convert()
  }

  /// Returns the remaining time to live of a key that has a timeout, in seconds.
  ///
  /// <https://redis.io/commands/ttl>
  pub async fn ttl<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::ttl(&self.inner, key).await?.convert()
  }

  /// Returns the remaining time to live of a key that has a timeout, in milliseconds.
  ///
  /// <https://redis.io/commands/pttl>
  pub async fn pttl<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::pttl(&self.inner, key).await?.convert()
  }

  /// Remove the existing timeout on a key, turning the key from volatile (a key with an expiration)
  /// to persistent (a key that will never expire as no timeout is associated).
  ///
  /// Returns a boolean value describing whether or not the timeout was removed.
  ///
  /// <https://redis.io/commands/persist>
  pub async fn persist<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::persist(&self.inner, key).await?.convert()
  }

  /// Set a timeout on key. After the timeout has expired, the key will be automatically deleted.
  ///
  /// Returns a boolean value describing whether or not the timeout was added.
  ///
  /// <https://redis.io/commands/expire>
  pub async fn expire<R, K>(&self, key: K, seconds: i64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::expire(&self.inner, key, seconds).await?.convert()
  }

  /// Set a timeout on a key based on a UNIX timestamp.
  ///
  /// Returns a boolean value describing whether or not the timeout was added.
  ///
  /// <https://redis.io/commands/expireat>
  pub async fn expire_at<R, K>(&self, key: K, timestamp: i64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::keys::expire_at(&self.inner, key, timestamp).await?.convert()
  }

  /// Returns number of keys that exist from the `keys` arguments.
  ///
  /// <https://redis.io/commands/exists>
  pub async fn exists<R, K>(&self, keys: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<MultipleKeys>,
  {
    commands::keys::exists(&self.inner, keys).await?.convert()
  }

  // ----------- HASHES ------------------

  /// Removes the specified fields from the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hdel>
  pub async fn hdel<R, K, F>(&self, key: K, fields: F) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    F: Into<MultipleKeys>,
  {
    commands::hashes::hdel(&self.inner, key, fields).await?.convert()
  }

  /// Returns if `field` is an existing field in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hexists>
  pub async fn hexists<R, K, F>(&self, key: K, field: F) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
  {
    commands::hashes::hexists(&self.inner, key, field).await?.convert()
  }

  /// Returns the value associated with `field` in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hget>
  pub async fn hget<R, K, F>(&self, key: K, field: F) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
  {
    commands::hashes::hget(&self.inner, key, field).await?.convert()
  }

  /// Returns all fields and values of the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hgetall>
  pub async fn hgetall<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::hashes::hgetall(&self.inner, key).await?.convert()
  }

  /// Increments the number stored at `field` in the hash stored at `key` by `increment`.
  ///
  /// <https://redis.io/commands/hincrby>
  pub async fn hincrby<R, K, F>(&self, key: K, field: F, increment: i64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
  {
    commands::hashes::hincrby(&self.inner, key, field, increment)
      .await?
      .convert()
  }

  /// Increment the specified `field` of a hash stored at `key`, and representing a floating point number, by the specified `increment`.
  ///
  /// <https://redis.io/commands/hincrbyfloat>
  pub async fn hincrbyfloat<R, K, F>(&self, key: K, field: F, increment: f64) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
  {
    commands::hashes::hincrbyfloat(&self.inner, key, field, increment)
      .await?
      .convert()
  }

  /// Returns all field names in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hkeys>
  pub async fn hkeys<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::hashes::hkeys(&self.inner, key).await?.convert()
  }

  /// Returns the number of fields contained in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hlen>
  pub async fn hlen<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::hashes::hlen(&self.inner, key).await?.convert()
  }

  /// Returns the values associated with the specified `fields` in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hmget>
  pub async fn hmget<R, K, F>(&self, key: K, fields: F) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    F: Into<MultipleKeys>,
  {
    commands::hashes::hmget(&self.inner, key, fields).await?.convert()
  }

  /// Sets the specified fields to their respective values in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hmset>
  pub async fn hmset<R, K, V>(&self, key: K, values: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: Into<RedisMap>,
  {
    commands::hashes::hmset(&self.inner, key, values).await?.convert()
  }

  /// Sets fields in the hash stored at `key` to their provided values.
  ///
  /// <https://redis.io/commands/hset>
  pub async fn hset<R, K, V>(&self, key: K, values: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: Into<RedisMap>,
  {
    commands::hashes::hset(&self.inner, key, values).await?.convert()
  }

  /// Sets `field` in the hash stored at `key` to `value`, only if `field` does not yet exist.
  ///
  /// <https://redis.io/commands/hsetnx>
  pub async fn hsetnx<R, K, F, V>(&self, key: K, field: F, value: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::hashes::hsetnx(&self.inner, key, field, to!(value)?)
      .await?
      .convert()
  }

  /// When called with just the `key` argument, return a random field from the hash value stored at `key`.
  ///
  /// If the provided `count` argument is positive, return an array of distinct fields.
  ///
  /// <https://redis.io/commands/hrandfield>
  pub async fn hrandfield<R, K>(&self, key: K, count: Option<(i64, bool)>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::hashes::hrandfield(&self.inner, key, count).await?.convert()
  }

  /// Returns the string length of the value associated with `field` in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hstrlen>
  pub async fn hstrlen<R, K, F>(&self, key: K, field: F) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
  {
    commands::hashes::hstrlen(&self.inner, key, field).await?.convert()
  }

  /// Returns all values in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hvals>
  pub async fn hvals<R, K>(&self, key: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
  {
    commands::hashes::hvals(&self.inner, key).await?.convert()
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

  // ------------- GEO --------------------

  /// Adds the specified geospatial items (longitude, latitude, name) to the specified key.
  ///
  /// <https://redis.io/commands/geoadd>
  pub async fn geoadd<R, K, V>(
    &self,
    key: K,
    options: Option<SetOptions>,
    changed: bool,
    values: V,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: Into<MultipleGeoValues>,
  {
    commands::geo::geoadd(&self.inner, key, options, changed, values)
      .await?
      .convert()
  }

  /// Return valid Geohash strings representing the position of one or more elements in a sorted set value representing a geospatial index (where elements were added using GEOADD).
  ///
  /// <https://redis.io/commands/geohash>
  pub async fn geohash<R, K, V>(&self, key: K, members: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::geo::geohash(&self.inner, key, to!(members)?).await?.convert()
  }

  /// Return the positions (longitude,latitude) of all the specified members of the geospatial index represented by the sorted set at key.
  ///
  /// Callers can use [as_geo_position](crate::types::RedisValue::as_geo_position) to lazily parse results as needed.
  ///
  /// <https://redis.io/commands/geopos>
  pub async fn geopos<K, V>(&self, key: K, members: V) -> Result<RedisValue, RedisError>
  where
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::geo::geopos(&self.inner, key, to!(members)?).await
  }

  /// Return the distance between two members in the geospatial index represented by the sorted set.
  ///
  /// <https://redis.io/commands/geodist>
  pub async fn geodist<R, K, S, D>(&self, key: K, src: S, dest: D, unit: Option<GeoUnit>) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    S: TryInto<RedisValue>,
    S::Error: Into<RedisError>,
    D: TryInto<RedisValue>,
    D::Error: Into<RedisError>,
  {
    commands::geo::geodist(&self.inner, key, to!(src)?, to!(dest)?, unit)
      .await?
      .convert()
  }

  /// Return the members of a sorted set populated with geospatial information using GEOADD, which are within the borders of the area specified with
  /// the center location and the maximum distance from the center (the radius).
  ///
  /// <https://redis.io/commands/georadius>
  pub async fn georadius<K, P>(
    &self,
    key: K,
    position: P,
    radius: f64,
    unit: GeoUnit,
    withcoord: bool,
    withdist: bool,
    withhash: bool,
    count: Option<(u64, Any)>,
    ord: Option<SortOrder>,
    store: Option<RedisKey>,
    storedist: Option<RedisKey>,
  ) -> Result<Vec<GeoRadiusInfo>, RedisError>
  where
    K: Into<RedisKey>,
    P: Into<GeoPosition>,
  {
    commands::geo::georadius(
      &self.inner,
      key,
      position,
      radius,
      unit,
      withcoord,
      withdist,
      withhash,
      count,
      ord,
      store,
      storedist,
    )
    .await
  }

  /// This command is exactly like GEORADIUS with the sole difference that instead of taking, as the center of the area to query, a longitude and
  /// latitude value, it takes the name of a member already existing inside the geospatial index represented by the sorted set.
  ///
  /// <https://redis.io/commands/georadiusbymember>
  pub async fn georadiusbymember<K, V>(
    &self,
    key: K,
    member: V,
    radius: f64,
    unit: GeoUnit,
    withcoord: bool,
    withdist: bool,
    withhash: bool,
    count: Option<(u64, Any)>,
    ord: Option<SortOrder>,
    store: Option<RedisKey>,
    storedist: Option<RedisKey>,
  ) -> Result<Vec<GeoRadiusInfo>, RedisError>
  where
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::geo::georadiusbymember(
      &self.inner,
      key,
      to!(member)?,
      radius,
      unit,
      withcoord,
      withdist,
      withhash,
      count,
      ord,
      store,
      storedist,
    )
    .await
  }

  /// Return the members of a sorted set populated with geospatial information using GEOADD, which are within the borders of the area specified by a given shape.
  ///
  /// <https://redis.io/commands/geosearch>
  pub async fn geosearch<K>(
    &self,
    key: K,
    from_member: Option<RedisValue>,
    from_lonlat: Option<GeoPosition>,
    by_radius: Option<(f64, GeoUnit)>,
    by_box: Option<(f64, f64, GeoUnit)>,
    ord: Option<SortOrder>,
    count: Option<(u64, Any)>,
    withcoord: bool,
    withdist: bool,
    withhash: bool,
  ) -> Result<Vec<GeoRadiusInfo>, RedisError>
  where
    K: Into<RedisKey>,
  {
    commands::geo::geosearch(
      &self.inner,
      key,
      from_member,
      from_lonlat,
      by_radius,
      by_box,
      ord,
      count,
      withcoord,
      withdist,
      withhash,
    )
    .await
  }

  /// This command is like GEOSEARCH, but stores the result in destination key. Returns the number of members added to the destination key.
  ///
  /// <https://redis.io/commands/geosearchstore>
  pub async fn geosearchstore<R, D, S>(
    &self,
    dest: D,
    source: S,
    from_member: Option<RedisValue>,
    from_lonlat: Option<GeoPosition>,
    by_radius: Option<(f64, GeoUnit)>,
    by_box: Option<(f64, f64, GeoUnit)>,
    ord: Option<SortOrder>,
    count: Option<(u64, Any)>,
    storedist: bool,
  ) -> Result<R, RedisError>
  where
    R: RedisResponse,
    D: Into<RedisKey>,
    S: Into<RedisKey>,
  {
    commands::geo::geosearchstore(
      &self.inner,
      dest,
      source,
      from_member,
      from_lonlat,
      by_radius,
      by_box,
      ord,
      count,
      storedist,
    )
    .await?
    .convert()
  }

  // ------------ HYPERLOGLOG --------------

  /// Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as first argument.
  ///
  /// <https://redis.io/commands/pfadd>
  pub async fn pfadd<R, K, V>(&self, key: K, elements: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    commands::hyperloglog::pfadd(&self.inner, key, to!(elements)?)
      .await?
      .convert()
  }

  /// When called with a single key, returns the approximated cardinality computed by the HyperLogLog data structure stored at
  /// the specified variable, which is 0 if the variable does not exist.
  ///
  /// When called with multiple keys, returns the approximated cardinality of the union of the HyperLogLogs passed, by
  /// internally merging the HyperLogLogs stored at the provided keys into a temporary HyperLogLog.
  ///
  /// <https://redis.io/commands/pfcount>
  pub async fn pfcount<R, K>(&self, keys: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<MultipleKeys>,
  {
    commands::hyperloglog::pfcount(&self.inner, keys).await?.convert()
  }

  /// Merge multiple HyperLogLog values into an unique value that will approximate the cardinality of the union of the observed
  /// sets of the source HyperLogLog structures.
  ///
  /// <https://redis.io/commands/pfmerge>
  pub async fn pfmerge<R, D, S>(&self, dest: D, sources: S) -> Result<R, RedisError>
  where
    R: RedisResponse,
    D: Into<RedisKey>,
    S: Into<MultipleKeys>,
  {
    commands::hyperloglog::pfmerge(&self.inner, dest, sources)
      .await?
      .convert()
  }

  // -------------- LUA ------------------

  /// Load a script into the scripts cache, without executing it. After the specified command is loaded into the script cache it will be callable using EVALSHA with the correct SHA1 digest of the script.
  ///
  /// <https://redis.io/commands/script-load>
  pub async fn script_load<S>(&self, script: S) -> Result<String, RedisError>
  where
    S: Into<String>,
  {
    commands::lua::script_load(&self.inner, script).await?.convert()
  }

  /// A clustered variant of [script_load](Self::script_load) that loads the script on all primary nodes in a cluster.
  pub async fn script_load_cluster<S>(&self, script: S) -> Result<String, RedisError>
  where
    S: Into<String>,
  {
    commands::lua::script_load_cluster(&self.inner, script).await?.convert()
  }

  /// Kills the currently executing Lua script, assuming no write operation was yet performed by the script.
  ///
  /// <https://redis.io/commands/script-kill>
  pub async fn script_kill(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::lua::script_kill(&self.inner).await
  }

  /// A clustered variant of the [script_kill](Self::script_kill) command that issues the command to all primary nodes in the cluster.
  pub async fn script_kill_cluster(&self) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::lua::script_kill_cluster(&self.inner).await
  }

  /// Flush the Lua scripts cache.
  ///
  /// <https://redis.io/commands/script-flush>
  pub async fn script_flush(&self, r#async: bool) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::lua::script_flush(&self.inner, r#async).await
  }

  /// A clustered variant of [script_flush](Self::script_flush) that flushes the script cache on all primary nodes in the cluster.
  pub async fn script_flush_cluster(&self, r#async: bool) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::lua::script_flush_cluster(&self.inner, r#async).await
  }

  /// Returns information about the existence of the scripts in the script cache.
  ///
  /// <https://redis.io/commands/script-exists>
  pub async fn script_exists<H>(&self, hashes: H) -> Result<Vec<bool>, RedisError>
  where
    H: Into<MultipleStrings>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::lua::script_exists(&self.inner, hashes).await
  }

  /// Set the debug mode for subsequent scripts executed with EVAL.
  ///
  /// <https://redis.io/commands/script-debug>
  pub async fn script_debug(&self, flag: ScriptDebugFlag) -> Result<(), RedisError> {
    utils::disallow_during_transaction(&self.inner)?;
    commands::lua::script_debug(&self.inner, flag).await
  }

  /// Evaluates a script cached on the server side by its SHA1 digest.
  ///
  /// <https://redis.io/commands/evalsha>
  pub async fn evalsha<R, S, K, V>(&self, hash: S, keys: K, args: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<String>,
    K: Into<MultipleKeys>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::lua::evalsha(&self.inner, hash, keys, to!(args)?)
      .await?
      .convert()
  }

  /// Evaluate a Lua script on the server.
  ///
  /// <https://redis.io/commands/eval>
  pub async fn eval<R, S, K, V>(&self, script: S, keys: K, args: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    S: Into<String>,
    K: Into<MultipleKeys>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    utils::disallow_during_transaction(&self.inner)?;
    commands::lua::eval(&self.inner, script, keys, to!(args)?)
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
