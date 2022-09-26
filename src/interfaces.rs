use crate::{
  commands,
  error::RedisError,
  modules::inner::RedisClientInner,
  multiplexer::{commands as multiplexer_commands, utils as multiplexer_utils},
  protocol::command::{MultiplexerCommand, QueuedCommand, RedisCommand},
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
  utils::send_command,
};
use futures::Stream;
pub use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::{
  convert::TryInto,
  future::Future,
  ops::Mul,
  pin::Pin,
  sync::Arc,
  task::{Context, Poll},
};
use tokio::sync::{broadcast::Receiver as BroadcastReceiver, mpsc::unbounded_channel};
use tokio_stream::wrappers::UnboundedReceiverStream;

/// An enum used to represent the return value from a function that does some fallible synchronous work,
/// followed by some more fallible async logic inside a tokio task.
enum AsyncInner<T: Unpin + Send + 'static> {
  Result(Option<Result<T, RedisError>>),
  Task(Pin<Box<dyn Future<Output = Result<T, RedisError>> + Send + 'static>>),
}

/// A wrapper type for return values from async functions implemented in a trait.
pub struct AsyncResult<T: Unpin + Send + 'static> {
  inner: AsyncInner<T>,
}

#[doc(hidden)]
impl<T, E> From<Result<T, E>> for AsyncResult<T>
where
  T: Unpin + Send + 'static,
  E: Into<RedisError>,
{
  fn from(value: Result<T, E>) -> Self {
    AsyncResult {
      inner: AsyncInner::Result(Some(value.map_err(|e| e.into()))),
    }
  }
}

#[doc(hidden)]
impl<T> From<Pin<Box<dyn Future<Output = Result<T, RedisError>> + Send + 'static>>> for AsyncResult<T>
where
  T: Unpin + Send + 'static,
{
  fn from(f: Pin<Box<dyn Future<Output = Result<T, RedisError>> + Send + 'static>>) -> Self {
    AsyncResult {
      inner: AsyncInner::Task(f),
    }
  }
}

impl<T> Future for AsyncResult<T>
where
  T: Unpin + Send + 'static,
{
  type Output = Result<T, RedisError>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    match self.get_mut().inner {
      AsyncInner::Result(ref mut output) => {
        if let Some(value) = output.take() {
          Poll::Ready(value)
        } else {
          error!("Tried calling poll on an AsyncResult::Result more than once.");
          Poll::Ready(Err(RedisError::new_canceled()))
        }
      },
      AsyncInner::Task(ref mut fut) => Pin::new(fut).poll(cx),
    }
  }
}

/// Run a function in the context of an async block, returning an `AsyncResult` that wraps a trait object.
pub(crate) fn async_spawn<C, F, Fut, T>(client: &C, func: F) -> AsyncResult<T>
where
  C: ClientLike + Clone,
  Fut: Future<Output = Result<T, RedisError>> + Send + 'static,
  F: FnOnce(C) -> Fut,
  T: Unpin + Send + 'static, {
  let client = client.clone();
  AsyncResult {
    inner: AsyncInner::Task(Box::pin(func(client))),
  }
}

/// Run a function and wrap the result in an `AsyncResult` trait object.
#[cfg(feature = "subscriber-client")]
pub(crate) fn wrap_async<F, Fut, T>(func: F) -> AsyncResult<T>
where
  Fut: Future<Output = Result<T, RedisError>> + Send + 'static,
  F: FnOnce() -> Fut,
  T: Unpin + Send + 'static, {
  AsyncResult {
    inner: AsyncInner::Task(Box::pin(func())),
  }
}

/// Send a single `RedisCommand` to the multiplexer.
pub(crate) fn default_send_command<C>(inner: &Arc<RedisClientInner>, command: C) -> Result<(), RedisError>
where
  C: Into<RedisCommand>, {
  send_to_multiplexer(inner, command.into().into())
}

/// Send a `MultiplexerCommand` to the multiplexer.
pub(crate) fn send_to_multiplexer(
  inner: &Arc<RedisClientInner>,
  command: MultiplexerCommand,
) -> Result<(), RedisError> {
  inner.counters.incr_cmd_buffer_len();
  if let Err(mut e) = inner.command_tx.send(command) {
    _error!(inner, "Error sending command to multiplexer.");
    inner.counters.decr_cmd_buffer_len();
    let command = e.0;
    let error = RedisError::new(RedisErrorKind::Unknown, "Client is not initialized.");

    // TODO respond to caller differently for each command kind
    // use command.respond_with_error(error)
  }

  Ok(())
}

/// Any Redis client that implements any part of the Redis interface.
pub trait ClientLike: Clone + Unpin + Send + Sync + Sized {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner>;

  #[doc(hidden)]
  fn send_command<C>(&self, command: C) -> Result<(), RedisError>
  where
    C: Into<RedisCommand>, {
    default_send_command(inner, command)
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
    self.inner().resp_version.as_ref().load().as_ref().clone()
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
  fn force_reconnection(&self) -> AsyncResult<()> {
    async_spawn(self, |client| async move {
      commands::server::force_reconnection(client.inner()).await
    })
  }

  /// Wait for the result of the next connection attempt.
  ///
  /// This can be used with `on_reconnect` to separate initialization logic that needs to occur only on the next
  /// connection attempt vs all subsequent attempts.
  fn wait_for_connect(&self) -> AsyncResult<()> {
    let mut rx = self.inner().notifications.connect.subscribe();
    wrap_async(move || async move { rx.recv().await? })
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
  fn quit(&self) -> AsyncResult<()> {
    async_spawn(self, |client| async move { commands::server::quit(&client).await })
  }

  /// Shut down the server and quit the client.
  ///
  /// <https://redis.io/commands/shutdown>
  fn shutdown(&self, flags: Option<ShutdownFlags>) -> AsyncResult<()> {
    async_spawn(self, |client| async move {
      commands::server::shutdown(&client, flags).await
    })
  }

  /// Ping the Redis server.
  ///
  /// <https://redis.io/commands/ping>
  fn ping(&self) -> AsyncResult<()> {
    async_spawn(self, |client| async move {
      commands::server::ping(&client).await?.convert()
    })
  }

  /// Read info about the server.
  ///
  /// <https://redis.io/commands/info>
  fn info<R>(&self, section: Option<InfoKind>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send, {
    async_spawn(self, |client| async move {
      commands::server::info(&client, section).await?.convert()
    })
  }

  /// Run a custom command that is not yet supported via another interface on this client. This is most useful when
  /// interacting with third party modules or extensions.
  ///
  /// Callers should use the re-exported [redis_keyslot](crate::util::redis_keyslot) function to hash the command's
  /// key, if necessary.
  ///
  /// This interface should be used with caution as it may break the automatic pipeline features in the client if
  /// command flags are not properly configured.
  fn custom<R, T>(&self, cmd: CustomCommand, args: Vec<T>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    T: TryInto<RedisValue>,
    T::Error: Into<RedisError>, {
    let args = atry!(utils::try_into_vec(args));
    async_spawn(self, |client| async move {
      commands::server::custom(&client, cmd, args).await?.convert()
    })
  }

  /// Run a custom command similar to [custom](Self::custom), but return the response frame directly without any
  /// parsing.
  ///
  /// Note: RESP2 frames from the server are automatically converted to the RESP3 format when parsed by the client.
  fn custom_raw<T>(&self, cmd: CustomCommand, args: Vec<T>) -> AsyncResult<Resp3Frame>
  where
    T: TryInto<RedisValue>,
    T::Error: Into<RedisError>, {
    let args = atry!(utils::try_into_vec(args));
    async_spawn(self, |client| async move {
      commands::server::custom_raw(&client, cmd, args).await
    })
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
  lua::LuaInterface,
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
use crate::prelude::RedisErrorKind;
