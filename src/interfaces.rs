use crate::{
  clients::WithOptions,
  commands,
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::command::{RedisCommand, RouterCommand},
  router::commands as router_commands,
  types::{
    ClientState,
    ClusterStateChange,
    ConnectHandle,
    ConnectionConfig,
    CustomCommand,
    FromRedis,
    InfoKind,
    KeyspaceEvent,
    Message,
    Options,
    PerformanceConfig,
    ReconnectPolicy,
    RedisConfig,
    RedisValue,
    RespVersion,
    Server,
    ShutdownFlags,
  },
  utils,
};
use futures::Future;
pub use redis_protocol::resp3::types::Frame as Resp3Frame;
use semver::Version;
use std::{convert::TryInto, sync::Arc};
use tokio::{sync::broadcast::Receiver as BroadcastReceiver, task::JoinHandle};

/// Type alias for `Result<T, RedisError>`.
pub type RedisResult<T> = Result<T, RedisError>;

#[cfg(feature = "dns")]
use crate::protocol::types::Resolve;

/// Send a single `RedisCommand` to the router.
pub(crate) fn default_send_command<C>(inner: &Arc<RedisClientInner>, command: C) -> Result<(), RedisError>
where
  C: Into<RedisCommand>,
{
  let mut command: RedisCommand = command.into();
  _trace!(
    inner,
    "Sending command {} ({}) to router.",
    command.kind.to_str_debug(),
    command.debug_id()
  );
  command.inherit_options(inner);

  send_to_router(inner, command.into())
}

/// Send a `RouterCommand` to the router.
pub(crate) fn send_to_router(inner: &Arc<RedisClientInner>, command: RouterCommand) -> Result<(), RedisError> {
  #[allow(clippy::collapsible_if)]
  if command.should_check_fail_fast() {
    if utils::read_locked(&inner.state) != ClientState::Connected {
      _debug!(inner, "Responding early after fail fast check.");
      command.finish_with_error(RedisError::new(
        RedisErrorKind::Canceled,
        "Connection closed unexpectedly.",
      ));
      return Ok(());
    }
  }

  let new_len = inner.counters.incr_cmd_buffer_len();
  let should_apply_backpressure = inner.connection.max_command_buffer_len > 0
    && new_len > inner.connection.max_command_buffer_len
    && !command.should_skip_backpressure();

  if should_apply_backpressure {
    inner.counters.decr_cmd_buffer_len();
    command.finish_with_error(RedisError::new(
      RedisErrorKind::Backpressure,
      "Max command queue length exceeded.",
    ));
    return Ok(());
  }

  if let Err(e) = inner.command_tx.load().send(command) {
    // usually happens if the caller tries to send a command before calling `connect` or after calling `quit`
    inner.counters.decr_cmd_buffer_len();

    if let RouterCommand::Command(mut command) = e.0 {
      _warn!(
        inner,
        "Fatal error sending {} command to router. Client may be stopped or not yet initialized.",
        command.kind.to_str_debug()
      );

      command.respond_to_caller(Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Client is not initialized.",
      )));
    } else {
      _warn!(
        inner,
        "Fatal error sending command to router. Client may be stopped or not yet initialized."
      );
    }

    Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Failed to send command to router.",
    ))
  } else {
    Ok(())
  }
}

/// Any Redis client that implements any part of the Redis interface.
pub trait ClientLike: Clone + Send + Sync + Sized {
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
    default_send_command(self.inner(), command)
  }

  /// The unique ID identifying this client and underlying connections.
  fn id(&self) -> &str {
    &self.inner().id
  }

  /// Read the config used to initialize the client.
  fn client_config(&self) -> RedisConfig {
    self.inner().config.as_ref().clone()
  }

  /// Read the reconnect policy used to initialize the client.
  fn client_reconnect_policy(&self) -> Option<ReconnectPolicy> {
    self.inner().policy.read().clone()
  }

  /// Read the connection config used to initialize the client.
  fn connection_config(&self) -> &ConnectionConfig {
    self.inner().connection.as_ref()
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
  fn active_connections(&self) -> impl Future<Output = Result<Vec<Server>, RedisError>> + Send {
    async move { commands::client::active_connections(self).await }
  }

  /// Read the server version, if known.
  fn server_version(&self) -> Option<Version> {
    self.inner().server_state.read().kind.server_version()
  }

  /// Override the DNS resolution logic for the client.
  #[cfg(feature = "dns")]
  #[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
  fn set_resolver(&self, resolver: Arc<dyn Resolve>) -> impl Future + Send {
    async move { self.inner().set_resolver(resolver).await }
  }

  /// Connect to the Redis server.
  ///
  /// This function returns a `JoinHandle` to a task that drives the connection. It will not resolve until the
  /// connection closes, of if a reconnection policy with unlimited attempts is provided then it will
  /// run until `QUIT` is called.
  ///
  /// **Calling this function more than once will drop all state associated with the previous connection(s).** Any
  /// pending commands on the old connection(s) will either finish or timeout, but they will not be retried on the
  /// new connection(s).
  ///
  /// See [init](Self::init) for an alternative shorthand.
  fn connect(&self) -> ConnectHandle {
    let inner = self.inner().clone();
    utils::reset_router_task(&inner);

    tokio::spawn(async move {
      utils::clear_backchannel_state(&inner).await;
      let result = router_commands::start(&inner).await;
      // a canceled error means we intentionally closed the client
      _trace!(inner, "Ending connection task with {:?}", result);

      if let Err(ref error) = result {
        if !error.is_canceled() {
          inner.notifications.broadcast_connect(Err(error.clone()));
        }
      }

      utils::check_and_set_client_state(&inner.state, ClientState::Disconnecting, ClientState::Disconnected);
      result
    })
  }

  /// Force a reconnection to the server(s).
  ///
  /// When running against a cluster this function will also refresh the cached cluster routing table.
  fn force_reconnection(&self) -> impl Future<Output = RedisResult<()>> + Send {
    async move { commands::server::force_reconnection(self.inner()).await }
  }

  /// Wait for the result of the next connection attempt.
  ///
  /// This can be used with `on_reconnect` to separate initialization logic that needs to occur only on the next
  /// connection attempt vs all subsequent attempts.
  fn wait_for_connect(&self) -> impl Future<Output = RedisResult<()>> + Send {
    async move {
      if utils::read_locked(&self.inner().state) == ClientState::Connected {
        debug!("{}: Client is already connected.", self.inner().id);
        Ok(())
      } else {
        self.inner().notifications.connect.load().subscribe().recv().await?
      }
    }
  }

  /// Initialize a new routing and connection task and wait for it to connect successfully.
  ///
  /// The returned [ConnectHandle](crate::types::ConnectHandle) refers to the task that drives the routing and
  /// connection layer. It will not finish until the max reconnection count is reached.
  ///
  /// Callers can also use [connect](Self::connect) and [wait_for_connect](Self::wait_for_connect) separately if
  /// needed.
  ///
  /// ```rust
  /// use fred::prelude::*;
  ///
  /// #[tokio::main]
  /// async fn main() -> Result<(), RedisError> {
  ///   let client = RedisClient::default();
  ///   let connection_task = client.init().await?;
  ///
  ///   // ...
  ///
  ///   client.quit().await?;
  ///   connection_task.await?
  /// }
  /// ```
  fn init(&self) -> impl Future<Output = RedisResult<ConnectHandle>> + Send {
    async move {
      let mut rx = { self.inner().notifications.connect.load().subscribe() };
      let task = self.connect();
      let error = rx.recv().await.map_err(RedisError::from).and_then(|r| r).err();

      if let Some(error) = error {
        // the initial connection failed, so we should gracefully close the routing task
        utils::reset_router_task(self.inner());
        Err(error)
      } else {
        Ok(task)
      }
    }
  }

  /// Close the connection to the Redis server. The returned future resolves when the command has been written to the
  /// socket, not when the connection has been fully closed. Some time after this future resolves the future
  /// returned by [connect](Self::connect) will resolve which indicates that the connection has been fully closed.
  ///
  /// This function will also close all error, pubsub message, and reconnection event streams.
  fn quit(&self) -> impl Future<Output = RedisResult<()>> + Send {
    async move { commands::server::quit(self).await }
  }

  /// Shut down the server and quit the client.
  ///
  /// <https://redis.io/commands/shutdown>
  fn shutdown(&self, flags: Option<ShutdownFlags>) -> impl Future<Output = RedisResult<()>> + Send {
    async move { commands::server::shutdown(self, flags).await }
  }

  /// Ping the Redis server.
  ///
  /// <https://redis.io/commands/ping>
  fn ping<R>(&self) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
  {
    async move { commands::server::ping(self).await?.convert() }
  }

  /// Read info about the server.
  ///
  /// <https://redis.io/commands/info>
  fn info<R>(&self, section: Option<InfoKind>) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
  {
    async move { commands::server::info(self, section).await?.convert() }
  }

  /// Run a custom command that is not yet supported via another interface on this client. This is most useful when
  /// interacting with third party modules or extensions.
  ///
  /// Callers should use the re-exported [redis_keyslot](crate::util::redis_keyslot) function to hash the command's
  /// key, if necessary.
  ///
  /// This interface should be used with caution as it may break the automatic pipeline features in the client if
  /// command flags are not properly configured.
  fn custom<R, T>(&self, cmd: CustomCommand, args: Vec<T>) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    T: TryInto<RedisValue> + Send,
    T::Error: Into<RedisError> + Send,
  {
    async move {
      let args = utils::try_into_vec(args)?;
      commands::server::custom(self, cmd, args).await?.convert()
    }
  }

  /// Run a custom command similar to [custom](Self::custom), but return the response frame directly without any
  /// parsing.
  ///
  /// Note: RESP2 frames from the server are automatically converted to the RESP3 format when parsed by the client.
  fn custom_raw<T>(&self, cmd: CustomCommand, args: Vec<T>) -> impl Future<Output = RedisResult<Resp3Frame>> + Send
  where
    T: TryInto<RedisValue> + Send,
    T::Error: Into<RedisError> + Send,
  {
    async move {
      let args = utils::try_into_vec(args)?;
      commands::server::custom_raw(self, cmd, args).await
    }
  }

  /// Customize various configuration options on commands.
  fn with_options(&self, options: &Options) -> WithOptions<Self> {
    WithOptions {
      client:  self.clone(),
      options: options.clone(),
    }
  }
}

fn spawn_event_listener<T, F>(mut rx: BroadcastReceiver<T>, func: F) -> JoinHandle<RedisResult<()>>
where
  T: Clone + Send + 'static,
  F: Fn(T) -> RedisResult<()> + Send + 'static,
{
  tokio::spawn(async move {
    let mut result = Ok(());

    while let Ok(val) = rx.recv().await {
      if let Err(err) = func(val) {
        result = Err(err);
        break;
      }
    }

    result
  })
}

/// An interface that exposes various client and connection events.
///
/// Calling [quit](crate::interfaces::ClientLike::quit) will close all event streams.
pub trait EventInterface: ClientLike {
  /// Spawn a task that runs the provided function on each publish-subscribe message.
  ///
  /// See [message_rx](Self::message_rx) for more information.
  fn on_message<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(Message) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.message_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn a task that runs the provided function on each keyspace event.
  ///
  /// <https://redis.io/topics/notifications>
  fn on_keyspace_event<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(KeyspaceEvent) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.keyspace_event_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn a task that runs the provided function on each reconnection event.
  ///
  /// Errors returned by `func` will exit the task.
  fn on_reconnect<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(Server) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.reconnect_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn a task that runs the provided function on each cluster change event.
  ///
  /// Errors returned by `func` will exit the task.
  fn on_cluster_change<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(Vec<ClusterStateChange>) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.cluster_change_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn a task that runs the provided function on each connection error event.
  ///
  /// Errors returned by `func` will exit the task.
  fn on_error<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(RedisError) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.error_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn a task that runs the provided function whenever the client detects an unresponsive connection.
  fn on_unresponsive<F>(&self, func: F) -> JoinHandle<RedisResult<()>>
  where
    F: Fn(Server) -> RedisResult<()> + Send + 'static,
  {
    let rx = self.unresponsive_rx();
    spawn_event_listener(rx, func)
  }

  /// Spawn one task that listens for all connection management event types.
  ///
  /// Errors in any of the provided functions will exit the task.
  fn on_any<Fe, Fr, Fc>(&self, error_fn: Fe, reconnect_fn: Fr, cluster_change_fn: Fc) -> JoinHandle<RedisResult<()>>
  where
    Fe: Fn(RedisError) -> RedisResult<()> + Send + 'static,
    Fr: Fn(Server) -> RedisResult<()> + Send + 'static,
    Fc: Fn(Vec<ClusterStateChange>) -> RedisResult<()> + Send + 'static,
  {
    let mut error_rx = self.error_rx();
    let mut reconnect_rx = self.reconnect_rx();
    let mut cluster_rx = self.cluster_change_rx();

    tokio::spawn(async move {
      #[allow(unused_assignments)]
      let mut result = Ok(());

      loop {
        tokio::select! {
          Ok(error) = error_rx.recv() => {
            if let Err(err) = error_fn(error) {
              result = Err(err);
              break;
            }
          }
          Ok(server) = reconnect_rx.recv() => {
            if let Err(err) = reconnect_fn(server) {
              result = Err(err);
              break;
            }
          }
          Ok(changes) = cluster_rx.recv() => {
            if let Err(err) = cluster_change_fn(changes) {
              result = Err(err);
              break;
            }
          }
        }
      }

      result
    })
  }

  /// Listen for messages on the publish-subscribe interface.
  ///
  /// **Keyspace events are not sent on this interface.**
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again.
  /// Messages will start appearing on the original stream after
  /// [subscribe](crate::interfaces::PubsubInterface::subscribe) is called again.
  fn message_rx(&self) -> BroadcastReceiver<Message> {
    self.inner().notifications.pubsub.load().subscribe()
  }

  /// Listen for keyspace and keyevent notifications on the publish-subscribe interface.
  ///
  /// Callers still need to configure the server and subscribe to the relevant channels, but this interface will
  /// parse and format the messages automatically.
  ///
  /// <https://redis.io/topics/notifications>
  fn keyspace_event_rx(&self) -> BroadcastReceiver<KeyspaceEvent> {
    self.inner().notifications.keyspace.load().subscribe()
  }

  /// Listen for reconnection notifications.
  ///
  /// This function can be used to receive notifications whenever the client reconnects in order to
  /// re-subscribe to channels, etc.
  ///
  /// A reconnection event is also triggered upon first connecting to the server.
  fn reconnect_rx(&self) -> BroadcastReceiver<Server> {
    self.inner().notifications.reconnect.load().subscribe()
  }

  /// Listen for notifications whenever the cluster state changes.
  ///
  /// This is usually triggered in response to a `MOVED` error, but can also happen when connections close
  /// unexpectedly.
  fn cluster_change_rx(&self) -> BroadcastReceiver<Vec<ClusterStateChange>> {
    self.inner().notifications.cluster_change.load().subscribe()
  }

  /// Listen for protocol and connection errors. This stream can be used to more intelligently handle errors that may
  /// not appear in the request-response cycle, and so cannot be handled by response futures.
  fn error_rx(&self) -> BroadcastReceiver<RedisError> {
    self.inner().notifications.errors.load().subscribe()
  }

  /// Receive a message when the client initiates a reconnection after detecting an unresponsive connection.
  fn unresponsive_rx(&self) -> BroadcastReceiver<Server> {
    self.inner().notifications.unresponsive.load().subscribe()
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
};

#[cfg(feature = "redis-json")]
pub use crate::commands::interfaces::redis_json::RedisJsonInterface;
#[cfg(feature = "sentinel-client")]
pub use crate::commands::interfaces::sentinel::SentinelInterface;
#[cfg(feature = "time-series")]
pub use crate::commands::interfaces::timeseries::TimeSeriesInterface;
#[cfg(feature = "client-tracking")]
pub use crate::commands::interfaces::tracking::TrackingInterface;
#[cfg(feature = "transactions")]
pub use crate::commands::interfaces::transactions::TransactionInterface;
