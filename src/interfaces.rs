use crate::{
  commands,
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  multiplexer::commands as multiplexer_commands,
  protocol::command::{MultiplexerCommand, RedisCommand},
  types::{
    ClientState,
    ClusterStateChange,
    ConnectHandle,
    CustomCommand,
    FromRedis,
    InfoKind,
    PerformanceConfig,
    ReconnectPolicy,
    RedisConfig,
    RedisValue,
    RespVersion,
    ShutdownFlags,
  },
  utils,
};
pub use redis_protocol::resp3::types::Frame as Resp3Frame;
use semver::Version;
use std::{convert::TryInto, sync::Arc};
use tokio::sync::broadcast::Receiver as BroadcastReceiver;

/// Type alias for `Result<T, RedisError>`.
pub type RedisResult<T> = Result<T, RedisError>;

#[cfg(feature = "dns")]
use crate::protocol::types::Resolve;

/// Send a single `RedisCommand` to the multiplexer.
pub(crate) fn default_send_command<C>(inner: &Arc<RedisClientInner>, command: C) -> Result<(), RedisError>
where
  C: Into<RedisCommand>,
{
  let command: RedisCommand = command.into();
  _trace!(
    inner,
    "Sending command {} ({}) to multiplexer.",
    command.kind.to_str_debug(),
    command.debug_id()
  );
  send_to_multiplexer(inner, command.into())
}

/// Send a `MultiplexerCommand` to the multiplexer.
pub(crate) fn send_to_multiplexer(
  inner: &Arc<RedisClientInner>,
  command: MultiplexerCommand,
) -> Result<(), RedisError> {
  inner.counters.incr_cmd_buffer_len();
  if let Err(e) = inner.command_tx.send(command) {
    _error!(inner, "Fatal error sending command to multiplexer.");
    inner.counters.decr_cmd_buffer_len();

    if let MultiplexerCommand::Command(mut command) = e.0 {
      // if a caller manages to trigger this it means that a connection task is not running
      command.respond_to_caller(Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Client is not initialized.",
      )));
    }

    Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Failed to send command to multiplexer.",
    ))
  } else {
    Ok(())
  }
}

/// Any Redis client that implements any part of the Redis interface.
#[async_trait]
pub trait ClientLike: Clone + Send + Sized {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner>;

  /// Helper function to intercept and modify a command without affecting how it is sent to the connection layer.
  #[doc(hidden)]
  fn change_command(&self, _: &mut RedisCommand) {}

  /// Helper function to intercept and customize how a command is sent to the connection layer.
  #[doc(hidden)]
  fn send_command<C>(&self, command: C) -> Result<(), RedisError>
  where
    C: Into<RedisCommand>,
  {
    let mut command: RedisCommand = command.into();
    self.change_command(&mut command);
    default_send_command(&self.inner(), command)
  }

  /// The unique ID identifying this client and underlying connections.
  ///
  /// All connections created by this client will use `CLIENT SETNAME` with this value unless the `no-client-setname`
  /// feature is enabled.
  fn id(&self) -> &str {
    self.inner().id.as_str()
  }

  /// Read the config used to initialize the client.
  fn client_config(&self) -> RedisConfig {
    self.inner().config.as_ref().clone()
  }

  /// Read the reconnect policy used to initialize the client.
  fn client_reconnect_policy(&self) -> Option<ReconnectPolicy> {
    self.inner().policy.read().clone()
  }

  /// Read the RESP version used by the client when communicating with the server.
  fn protocol_version(&self) -> RespVersion {
    if self.inner().is_resp3() {
      RespVersion::RESP3
    } else {
      RespVersion::RESP2
    }
  }

  /// Whether or not the client has a reconnection policy.
  fn has_reconnect_policy(&self) -> bool {
    self.inner().policy.read().is_some()
  }

  /// Whether or not the client will automatically pipeline commands.
  fn is_pipelined(&self) -> bool {
    self.inner().is_pipelined()
  }

  /// Whether or not the client is connected to a cluster.
  fn is_clustered(&self) -> bool {
    self.inner().config.server.is_clustered()
  }

  /// Whether or not the client uses the sentinel interface.
  fn uses_sentinels(&self) -> bool {
    self.inner().config.server.is_sentinel()
  }

  /// Update the internal [PerformanceConfig](crate::types::PerformanceConfig) in place with new values.
  fn update_perf_config(&self, config: PerformanceConfig) {
    self.inner().update_performance_config(config);
  }

  /// Read the [PerformanceConfig](crate::types::PerformanceConfig) associated with this client.
  fn perf_config(&self) -> PerformanceConfig {
    self.inner().performance_config()
  }

  /// Read the state of the underlying connection(s).
  ///
  /// If running against a cluster the underlying state will reflect the state of the least healthy connection.
  fn state(&self) -> ClientState {
    self.inner().state.read().clone()
  }

  /// Whether or not all underlying connections are healthy.
  fn is_connected(&self) -> bool {
    *self.inner().state.read() == ClientState::Connected
  }

  /// Read the set of active connections managed by the client.
  async fn active_connections(&self) -> Result<Vec<Server>, RedisError> {
    commands::client::active_connections(self).await
  }

  /// Read the server version, if known.
  fn server_version(&self) -> Option<Version> {
    self.inner().server_state.read().server_version()
  }

  /// Override the DNS resolution logic for the client.
  #[cfg(feature = "dns")]
  #[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
  async fn set_resolver(&self, resolver: Arc<dyn Resolve>) {
    self.inner().set_resolver(resolver).await;
  }

  /// Connect to the Redis server.
  ///
  /// This function returns a `JoinHandle` to a task that drives the connection. It will not resolve until the
  /// connection closes, and if a reconnection policy with unlimited attempts is provided then the `JoinHandle` will
  /// run forever, or until `QUIT` is called.
  fn connect(&self) -> ConnectHandle {
    let inner = self.inner().clone();

    tokio::spawn(async move {
      let result = multiplexer_commands::start(&inner).await;
      if let Err(ref e) = result {
        inner.notifications.broadcast_connect(Err(e.clone()));
      }
      utils::set_client_state(&inner.state, ClientState::Disconnected);
      result
    })
  }

  /// Force a reconnection to the server(s).
  ///
  /// When running against a cluster this function will also refresh the cached cluster routing table.
  async fn force_reconnection(&self) -> RedisResult<()> {
    commands::server::force_reconnection(self.inner()).await
  }

  /// Wait for the result of the next connection attempt.
  ///
  /// This can be used with `on_reconnect` to separate initialization logic that needs to occur only on the next
  /// connection attempt vs all subsequent attempts.
  async fn wait_for_connect(&self) -> RedisResult<()> {
    self.inner().notifications.connect.subscribe().recv().await?
  }

  /// Listen for reconnection notifications.
  ///
  /// This function can be used to receive notifications whenever the client successfully reconnects in order to
  /// re-subscribe to channels, etc.
  ///
  /// A reconnection event is also triggered upon first connecting to the server.
  fn on_reconnect(&self) -> BroadcastReceiver<()> {
    self.inner().notifications.reconnect.subscribe()
  }

  /// Listen for notifications whenever the cluster state changes.
  ///
  /// This is usually triggered in response to a `MOVED` error, but can also happen when connections close
  /// unexpectedly.
  fn on_cluster_change(&self) -> BroadcastReceiver<Vec<ClusterStateChange>> {
    self.inner().notifications.cluster_change.subscribe()
  }

  /// Listen for protocol and connection errors. This stream can be used to more intelligently handle errors that may
  /// not appear in the request-response cycle, and so cannot be handled by response futures.
  ///
  /// This function does not need to be called again if the connection closes.
  fn on_error(&self) -> BroadcastReceiver<RedisError> {
    self.inner().notifications.errors.subscribe()
  }

  /// Close the connection to the Redis server. The returned future resolves when the command has been written to the
  /// socket, not when the connection has been fully closed. Some time after this future resolves the future
  /// returned by [connect](Self::connect) will resolve which indicates that the connection has been fully closed.
  ///
  /// This function will also close all error, pubsub message, and reconnection event streams.
  async fn quit(&self) -> RedisResult<()> {
    commands::server::quit(self).await
  }

  /// Shut down the server and quit the client.
  ///
  /// <https://redis.io/commands/shutdown>
  async fn shutdown(&self, flags: Option<ShutdownFlags>) -> RedisResult<()> {
    commands::server::shutdown(self, flags).await
  }

  /// Ping the Redis server.
  ///
  /// <https://redis.io/commands/ping>
  async fn ping(&self) -> RedisResult<()> {
    commands::server::ping(self).await?.convert()
  }

  /// Read info about the server.
  ///
  /// <https://redis.io/commands/info>
  async fn info<R>(&self, section: Option<InfoKind>) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::server::info(self, section).await?.convert()
  }

  /// Run a custom command that is not yet supported via another interface on this client. This is most useful when
  /// interacting with third party modules or extensions.
  ///
  /// Callers should use the re-exported [redis_keyslot](crate::util::redis_keyslot) function to hash the command's
  /// key, if necessary.
  ///
  /// This interface should be used with caution as it may break the automatic pipeline features in the client if
  /// command flags are not properly configured.
  async fn custom<R, T>(&self, cmd: CustomCommand, args: Vec<T>) -> RedisResult<R>
  where
    R: FromRedis,
    T: TryInto<RedisValue> + Send,
    T::Error: Into<RedisError> + Send,
  {
    let args = utils::try_into_vec(args)?;
    commands::server::custom(self, cmd, args).await?.convert()
  }

  /// Run a custom command similar to [custom](Self::custom), but return the response frame directly without any
  /// parsing.
  ///
  /// Note: RESP2 frames from the server are automatically converted to the RESP3 format when parsed by the client.
  async fn custom_raw<T>(&self, cmd: CustomCommand, args: Vec<T>) -> RedisResult<Resp3Frame>
  where
    T: TryInto<RedisValue> + Send,
    T::Error: Into<RedisError> + Send,
  {
    let args = utils::try_into_vec(args)?;
    commands::server::custom_raw(self, cmd, args).await
  }
}

pub use crate::commands::interfaces::{
  acl::AclInterface,
  client::ClientInterface,
  cluster::ClusterInterface,
  config::ConfigInterface,
  geo::GeoInterface,
  hashes::HashesInterface,
  hyperloglog::HyperloglogInterface,
  keys::KeysInterface,
  lists::ListInterface,
  lua::{FunctionInterface, LuaInterface},
  memory::MemoryInterface,
  metrics::MetricsInterface,
  pubsub::PubsubInterface,
  server::{AuthInterface, HeartbeatInterface, ServerInterface},
  sets::SetsInterface,
  slowlog::SlowlogInterface,
  sorted_sets::SortedSetsInterface,
  streams::StreamsInterface,
  transactions::TransactionInterface,
};

#[cfg(feature = "sentinel-client")]
pub use crate::commands::interfaces::sentinel::SentinelInterface;
use crate::types::Server;
