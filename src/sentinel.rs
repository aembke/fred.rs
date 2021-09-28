use crate::client::RedisClient;
use crate::commands;
use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::commands as multiplexer_commands;
use crate::multiplexer::utils as multiplexer_utils;
use crate::protocol::tls::TlsConfig;
use crate::types::{
  AclRule, AclUser, Blocking, ClientKillFilter, ClientKillType, ClientPauseKind, ClientState, ConnectHandle,
  InfoKind, MultipleStrings, ReconnectPolicy, RedisConfig, RedisKey, RedisMap, RedisResponse, RedisValue,
  SentinelFailureKind, ServerConfig, ShutdownFlags, Stats,
};
use crate::utils;
use futures::{Stream, StreamExt};
use std::convert::TryInto;
use std::fmt;
use std::net::IpAddr;
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

impl SentinelClient {
  /// Create a new client instance without connecting to the sentinel node.
  pub fn new(config: SentinelConfig) -> SentinelClient {
    SentinelClient {
      inner: RedisClientInner::new(config.into()),
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

  /// Read the state of the underlying connection.
  pub fn state(&self) -> ClientState {
    self.inner.state.read().clone()
  }

  /// Connect to the sentinel node with an optional reconnection policy.
  ///
  /// This function returns a `JoinHandle` to a task that drives the connection. It will not resolve until the connection closes, and if a
  /// reconnection policy with unlimited attempts is provided then the `JoinHandle` will run forever.
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

  /// Wait for the client to connect to the sentinel node, or return an error if the initial connection cannot be established.
  /// If the client is already connected this future will resolve immediately.
  ///
  /// This can be used with `on_reconnect` to separate initialization logic that needs to occur only on the first connection attempt vs subsequent attempts.
  pub async fn wait_for_connect(&self) -> Result<(), RedisError> {
    utils::wait_for_connect(&self.inner).await
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

  /// Listen for protocol and connection errors. This stream can be used to more intelligently handle errors that may not appear in the request-response
  /// cycle, and so cannot be handled by response futures.
  pub fn on_error(&self) -> impl Stream<Item = RedisError> {
    let (tx, rx) = unbounded_channel();
    self.inner.error_tx.write().push_back(tx);

    UnboundedReceiverStream::new(rx)
  }

  /// Listen for `(channel, message)` tuples on the publish-subscribe interface.
  ///
  /// If the connection to the Sentinel server closes for any reason this function does not need to be called again. Messages will start appearing on the original stream after [subscribe](Self::subscribe) is called again.
  ///
  /// <https://redis.io/topics/sentinel#pubsub-messages>
  pub fn on_message(&self) -> impl Stream<Item = (String, RedisValue)> {
    let (tx, rx) = unbounded_channel();
    self.inner.message_tx.write().push_back(tx);

    UnboundedReceiverStream::new(rx)
  }

  /// Whether or not the client has an active connection to the server(s).
  pub fn is_connected(&self) -> bool {
    *self.inner.state.read() == ClientState::Connected
  }

  /// Read the number of buffered commands that have not yet been sent to the server.
  pub fn command_queue_len(&self) -> usize {
    utils::read_atomic(&self.inner.cmd_buffer_len)
  }

  /// Read latency metrics across all commands.
  ///
  /// This metric reflects the total latency experienced by callers, including time spent waiting in memory to be written and network latency.
  /// Features such as automatic reconnect, `reconnect-on-auth-error`, and frame serialization time can all affect these values.
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  pub fn read_latency_metrics(&self) -> Stats {
    self.inner.latency_stats.read().read_metrics()
  }

  /// Read and consume latency metrics, resetting their values afterwards.
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  pub fn take_latency_metrics(&self) -> Stats {
    self.inner.latency_stats.write().take_metrics()
  }

  /// Read network latency metrics across all commands.
  ///
  /// This metric only reflects time spent waiting on a response. It will factor in reconnect time if a response doesn't arrive due to a connection
  /// closing, but it does not factor in the time a command spends waiting to be written, serialization time, backpressure, etc.  
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  pub fn read_network_latency_metrics(&self) -> Stats {
    self.inner.network_latency_stats.read().read_metrics()
  }

  /// Read and consume network latency metrics, resetting their values afterwards.
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  pub fn take_network_latency_metrics(&self) -> Stats {
    self.inner.network_latency_stats.write().take_metrics()
  }

  /// Read request payload size metrics across all commands.
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  pub fn read_req_size_metrics(&self) -> Stats {
    self.inner.req_size_stats.read().read_metrics()
  }

  /// Read and consume request payload size metrics, resetting their values afterwards.
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  pub fn take_req_size_metrics(&self) -> Stats {
    self.inner.req_size_stats.write().take_metrics()
  }

  /// Read response payload size metrics across all commands.
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  pub fn read_res_size_metrics(&self) -> Stats {
    self.inner.res_size_stats.read().read_metrics()
  }

  /// Read and consume response payload size metrics, resetting their values afterwards.
  #[cfg(feature = "metrics")]
  #[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
  pub fn take_res_size_metrics(&self) -> Stats {
    self.inner.res_size_stats.write().take_metrics()
  }

  /// Get the current value of a global Sentinel configuration parameter. The specified name may be a wildcard, similar to the Redis CONFIG GET command.
  pub async fn config_get<R, K>(&self, name: K) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<String>,
  {
    commands::sentinel::config_get(&self.inner, name).await?.convert()
  }

  /// Set the value of a global Sentinel configuration parameter.
  pub async fn config_set<R, K, V>(&self, name: K, value: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    K: Into<String>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    commands::sentinel::config_set(&self.inner, name, to!(value)?)
      .await?
      .convert()
  }

  /// Check if the current Sentinel configuration is able to reach the quorum needed to failover a master, and the majority needed to authorize the failover.
  pub async fn ckquorum<R, N>(&self, name: N) -> Result<R, RedisError>
  where
    R: RedisResponse,
    N: Into<String>,
  {
    commands::sentinel::ckquorum(&self.inner, name).await?.convert()
  }

  /// Force Sentinel to rewrite its configuration on disk, including the current Sentinel state.
  pub async fn flushconfig<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::sentinel::flushconfig(&self.inner).await?.convert()
  }

  /// Force a failover as if the master was not reachable, and without asking for agreement to other Sentinels.
  pub async fn failover<R, N>(&self, name: N) -> Result<R, RedisError>
  where
    R: RedisResponse,
    N: Into<String>,
  {
    commands::sentinel::failover(&self.inner, name).await?.convert()
  }

  /// Return the ip and port number of the master with that name.
  pub async fn get_master_addr_by_name<R, N>(&self, name: N) -> Result<R, RedisError>
  where
    R: RedisResponse,
    N: Into<String>,
  {
    commands::sentinel::get_master_addr_by_name(&self.inner, name)
      .await?
      .convert()
  }

  /// Return cached INFO output from masters and replicas.
  pub async fn info_cache<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::sentinel::info_cache(&self.inner).await?.convert()
  }

  /// Show the state and info of the specified master.
  pub async fn master<R, N>(&self, name: N) -> Result<R, RedisError>
  where
    R: RedisResponse,
    N: Into<String>,
  {
    commands::sentinel::master(&self.inner, name).await?.convert()
  }

  /// Show a list of monitored masters and their state.
  pub async fn masters<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::sentinel::masters(&self.inner).await?.convert()
  }

  /// Start Sentinel's monitoring.
  ///
  /// <https://redis.io/topics/sentinel#reconfiguring-sentinel-at-runtime>
  pub async fn monitor<R, N>(&self, name: N, ip: IpAddr, port: u16, quorum: u32) -> Result<R, RedisError>
  where
    R: RedisResponse,
    N: Into<String>,
  {
    commands::sentinel::monitor(&self.inner, name, ip, port, quorum)
      .await?
      .convert()
  }

  /// Return the ID of the Sentinel instance.
  pub async fn myid<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::sentinel::myid(&self.inner).await?.convert()
  }

  /// This command returns information about pending scripts.
  pub async fn pending_scripts<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::sentinel::pending_scripts(&self.inner).await?.convert()
  }

  /// Stop Sentinel's monitoring.
  ///
  /// <https://redis.io/topics/sentinel#reconfiguring-sentinel-at-runtime>
  pub async fn remove<R, N>(&self, name: N) -> Result<R, RedisError>
  where
    R: RedisResponse,
    N: Into<String>,
  {
    commands::sentinel::remove(&self.inner, name).await?.convert()
  }

  /// Show a list of replicas for this master, and their state.
  pub async fn replicas<R, N>(&self, name: N) -> Result<R, RedisError>
  where
    R: RedisResponse,
    N: Into<String>,
  {
    commands::sentinel::replicas(&self.inner, name).await?.convert()
  }

  /// Show a list of sentinel instances for this master, and their state.
  pub async fn sentinels<R, N>(&self, name: N) -> Result<R, RedisError>
  where
    R: RedisResponse,
    N: Into<String>,
  {
    commands::sentinel::sentinels(&self.inner, name).await?.convert()
  }

  /// Set Sentinel's monitoring configuration.
  ///
  /// <https://redis.io/topics/sentinel#reconfiguring-sentinel-at-runtime>
  pub async fn set<R, N, V>(&self, name: N, args: V) -> Result<R, RedisError>
  where
    R: RedisResponse,
    N: Into<String>,
    V: Into<RedisMap>,
  {
    commands::sentinel::set(&self.inner, name, args.into()).await?.convert()
  }

  /// This command simulates different Sentinel crash scenarios.
  pub async fn simulate_failure<R>(&self, kind: SentinelFailureKind) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::sentinel::simulate_failure(&self.inner, kind).await?.convert()
  }

  /// This command will reset all the masters with matching name.
  pub async fn reset<R, P>(&self, pattern: P) -> Result<R, RedisError>
  where
    R: RedisResponse,
    P: Into<String>,
  {
    commands::sentinel::reset(&self.inner, pattern).await?.convert()
  }

  /// Return the ID of the current connection.
  ///
  /// <https://redis.io/commands/client-id>
  pub async fn client_id<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::client::client_id(&self.inner).await?.convert()
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
    commands::client::client_setname(&self.inner, name).await
  }

  /// CLIENT PAUSE is a connections control command able to suspend all the Redis clients for the specified amount of time (in milliseconds).
  ///
  /// <https://redis.io/commands/client-pause>
  pub async fn client_pause(&self, timeout: i64, mode: Option<ClientPauseKind>) -> Result<(), RedisError> {
    commands::client::client_pause(&self.inner, timeout, mode).await
  }

  /// CLIENT UNPAUSE is used to resume command processing for all clients that were paused by CLIENT PAUSE.
  ///
  /// <https://redis.io/commands/client-unpause>
  pub async fn client_unpause(&self) -> Result<(), RedisError> {
    commands::client::client_unpause(&self.inner).await
  }

  /// When the sentinel is configured to use an ACL file (with the aclfile configuration option), this command will reload the
  /// ACLs from the file, replacing all the current ACL rules with the ones defined in the file.
  ///
  /// <https://redis.io/commands/acl-load>
  pub async fn acl_load(&self) -> Result<(), RedisError> {
    commands::acl::acl_load(&self.inner).await
  }

  /// When Redis is configured to use an ACL file (with the ACL file configuration option), this command will save the
  /// currently defined ACLs from the server memory to the ACL file.
  ///
  /// <https://redis.io/commands/acl-save>
  pub async fn acl_save(&self) -> Result<(), RedisError> {
    commands::acl::acl_save(&self.inner).await
  }

  /// The command shows the currently active ACL rules in the Redis server.
  ///
  /// <https://redis.io/commands/acl-list>
  pub async fn acl_list<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::acl::acl_list(&self.inner).await?.convert()
  }

  /// The command shows a list of all the usernames of the currently configured users in the Redis ACL system.
  ///
  /// <https://redis.io/commands/acl-users>
  pub async fn acl_users<R>(&self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::acl::acl_users(&self.inner).await?.convert()
  }

  /// The command returns all the rules defined for an existing ACL user.
  ///
  /// <https://redis.io/commands/acl-getuser>
  pub async fn acl_getuser<S>(&self, username: S) -> Result<Option<AclUser>, RedisError>
  where
    S: Into<String>,
  {
    commands::acl::acl_getuser(&self.inner, username).await
  }

  /// Create an ACL user with the specified rules or modify the rules of an existing user.
  ///
  /// <https://redis.io/commands/acl-setuser>
  pub async fn acl_setuser<S>(&self, username: S, rules: Vec<AclRule>) -> Result<(), RedisError>
  where
    S: Into<String>,
  {
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
    commands::acl::acl_cat(&self.inner, category).await?.convert()
  }

  /// Generate a password with length `bits`, returning the password.
  pub async fn acl_genpass(&self, bits: Option<u16>) -> Result<String, RedisError> {
    commands::acl::acl_genpass(&self.inner, bits).await?.convert()
  }

  /// Return the username the current connection is authenticated with. New connections are authenticated
  /// with the "default" user.
  ///
  /// <https://redis.io/commands/acl-whoami>
  pub async fn acl_whoami(&self) -> Result<String, RedisError> {
    commands::acl::acl_whoami(&self.inner).await?.convert()
  }

  /// Read `count` recent ACL security events.
  ///
  /// <https://redis.io/commands/acl-log>
  pub async fn acl_log_count(&self, count: Option<u32>) -> Result<RedisValue, RedisError> {
    commands::acl::acl_log_count(&self.inner, count).await
  }

  /// Clear the ACL security events logs.
  ///
  /// <https://redis.io/commands/acl-log>
  pub async fn acl_log_reset(&self) -> Result<(), RedisError> {
    commands::acl::acl_log_reset(&self.inner).await
  }

  /// Request for authentication in a password-protected Sentinel server. Returns ok if successful.
  ///
  /// The client will automatically authenticate with the default user if a password is provided in the associated `SentinelConfig`.
  ///
  /// <https://redis.io/commands/auth>
  pub async fn auth<S>(&self, username: Option<String>, password: S) -> Result<(), RedisError>
  where
    S: Into<String>,
  {
    commands::server::auth(&self.inner, username, password).await
  }

  /// Read info about the Sentinel server.
  ///
  /// <https://redis.io/commands/info>
  pub async fn info<R>(&self, section: Option<InfoKind>) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    commands::server::info(&self.inner, section).await?.convert()
  }

  /// Ping the Sentinel server.
  ///
  /// <https://redis.io/commands/ping>
  pub async fn ping(&self) -> Result<(), RedisError> {
    commands::server::ping(&self.inner).await?.convert()
  }

  /// Shut down the server and quit the client.
  ///
  /// <https://redis.io/commands/shutdown>
  pub async fn shutdown(&self, flags: Option<ShutdownFlags>) -> Result<(), RedisError> {
    commands::server::shutdown(&self.inner, flags).await
  }

  /// Close the connection to the Redis server. The returned future resolves when the command has been written to the socket,
  /// not when the connection has been fully closed. Some time after this future resolves the future returned by [connect](Self::connect)
  /// will resolve which indicates that the connection has been fully closed.
  ///
  /// This function will also close all error, pubsub message, and reconnection event streams.
  pub async fn quit(&self) -> Result<(), RedisError> {
    commands::server::quit(&self.inner).await
  }

  /// Subscribe to a channel on the PubSub interface, returning the number of channels to which the client is subscribed.
  ///
  /// <https://redis.io/commands/subscribe>
  pub async fn subscribe<S>(&self, channel: S) -> Result<usize, RedisError>
  where
    S: Into<String>,
  {
    commands::pubsub::subscribe(&self.inner, channel).await
  }

  /// Unsubscribe from a channel on the PubSub interface, returning the number of channels to which hte client is subscribed.
  ///
  /// <https://redis.io/commands/unsubscribe>
  pub async fn unsubscribe<S>(&self, channel: S) -> Result<usize, RedisError>
  where
    S: Into<String>,
  {
    commands::pubsub::unsubscribe(&self.inner, channel).await
  }

  /// Subscribes the client to the given patterns.
  ///
  /// <https://redis.io/commands/psubscribe>
  pub async fn psubscribe<S>(&self, patterns: S) -> Result<Vec<usize>, RedisError>
  where
    S: Into<MultipleStrings>,
  {
    commands::pubsub::psubscribe(&self.inner, patterns).await
  }

  /// Unsubscribes the client from the given patterns, or from all of them if none is given.
  ///
  /// <https://redis.io/commands/punsubscribe>
  pub async fn punsubscribe<S>(&self, patterns: S) -> Result<Vec<usize>, RedisError>
  where
    S: Into<MultipleStrings>,
  {
    commands::pubsub::punsubscribe(&self.inner, patterns).await
  }
}
