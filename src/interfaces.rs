use crate::commands;
use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::{commands as multiplexer_commands, utils as multiplexer_utils};
use crate::types::{
  ClientState, ConnectHandle, CustomCommand, FromRedis, InfoKind, ReconnectPolicy, RedisConfig, RedisValue,
  ShutdownFlags,
};
use crate::types::{PerformanceConfig, RespVersion};
use crate::utils;
use futures::Stream;
pub use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::unbounded_channel;
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

/// A wrapper type for async stream return values from functions implemented in a trait.
///
/// This is used to work around the lack of `impl Trait` support in trait functions.
pub struct AsyncStream<T: Unpin + Send + 'static> {
  inner: UnboundedReceiverStream<T>,
}

impl<T> Stream for AsyncStream<T>
where
  T: Unpin + Send + 'static,
{
  type Item = T;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    Pin::new(&mut self.get_mut().inner).poll_next(cx)
  }
}

#[doc(hidden)]
impl<T> From<UnboundedReceiverStream<T>> for AsyncStream<T>
where
  T: Unpin + Send + 'static,
{
  fn from(rx: UnboundedReceiverStream<T>) -> Self {
    AsyncStream { inner: rx }
  }
}

/// Run a function in the context of an async block, returning an `AsyncResult` that wraps a trait object.
pub(crate) fn async_spawn<C, F, Fut, T>(client: &C, func: F) -> AsyncResult<T>
where
  C: ClientLike,
  Fut: Future<Output = Result<T, RedisError>> + Send + 'static,
  F: FnOnce(Arc<RedisClientInner>) -> Fut,
  T: Unpin + Send + 'static,
{
  // this is unfortunate but necessary without async functions in traits
  let inner = client.inner().clone();
  AsyncResult {
    inner: AsyncInner::Task(Box::pin(func(inner))),
  }
}

/// Run a function and wrap the result in an `AsyncResult` trait object.
#[cfg(feature = "subscriber-client")]
pub(crate) fn wrap_async<F, Fut, T>(func: F) -> AsyncResult<T>
where
  Fut: Future<Output = Result<T, RedisError>> + Send + 'static,
  F: FnOnce() -> Fut,
  T: Unpin + Send + 'static,
{
  AsyncResult {
    inner: AsyncInner::Task(Box::pin(func())),
  }
}

/// Any Redis client that implements any part of the Redis interface.
pub trait ClientLike: Unpin + Send + Sync + Sized {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner>;

  /// The unique ID identifying this client and underlying connections.
  ///
  /// All connections created by this client will use `CLIENT SETNAME` with this value.
  fn id(&self) -> &Arc<String> {
    &self.inner().id
  }

  /// Read the config used to initialize the client.
  fn client_config(&self) -> RedisConfig {
    utils::read_locked(&self.inner().config)
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

  /// Update the internal [PerformanceConfig](crate::types::PerformanceConfig) in place with new values.
  fn update_perf_config(&self, config: PerformanceConfig) {
    self.inner().perf_config.update(&config);

    let mut guard = self.inner().config.write();
    guard.performance = config;
  }

  /// Read the state of the underlying connection(s).
  ///
  /// If running against a cluster the underlying state will reflect the state of the least healthy connection, if any.
  fn state(&self) -> ClientState {
    self.inner().state.read().clone()
  }

  /// Whether or not the client has an active connection to the server(s).
  fn is_connected(&self) -> bool {
    *self.inner().state.read() == ClientState::Connected
  }

  /// Connect to the Redis server with an optional reconnection policy.
  ///
  /// This function returns a `JoinHandle` to a task that drives the connection. It will not resolve
  /// until the connection closes, and if a reconnection policy with unlimited attempts
  /// is provided then the `JoinHandle` will run forever, or until `QUIT` is called.
  ///
  /// **Note:** See the [RedisConfig](crate::types::RedisConfig) documentation for more information on how the `policy` is applied to new connections.
  fn connect(&self, policy: Option<ReconnectPolicy>) -> ConnectHandle {
    let inner = self.inner().clone();

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
  fn wait_for_connect(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move { utils::wait_for_connect(&inner).await })
  }

  /// Listen for protocol and connection errors. This stream can be used to more intelligently handle errors that may
  /// not appear in the request-response cycle, and so cannot be handled by response futures.
  ///
  /// This function does not need to be called again if the connection closes.
  fn on_error(&self) -> AsyncStream<RedisError> {
    let (tx, rx) = unbounded_channel();
    self.inner().error_tx.write().push_back(tx);

    UnboundedReceiverStream::new(rx).into()
  }

  /// Close the connection to the Redis server. The returned future resolves when the command has been written to the socket,
  /// not when the connection has been fully closed. Some time after this future resolves the future returned by [connect](Self::connect)
  /// will resolve which indicates that the connection has been fully closed.
  ///
  /// This function will also close all error, pubsub message, and reconnection event streams.
  fn quit(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move { commands::server::quit(&inner).await })
  }

  /// Shut down the server and quit the client.
  ///
  /// <https://redis.io/commands/shutdown>
  fn shutdown(&self, flags: Option<ShutdownFlags>) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::server::shutdown(&inner, flags).await
    })
  }

  /// Ping the Redis server.
  ///
  /// <https://redis.io/commands/ping>
  fn ping(&self) -> AsyncResult<()> {
    async_spawn(
      self,
      |inner| async move { commands::server::ping(&inner).await?.convert() },
    )
  }

  /// Read info about the server.
  ///
  /// <https://redis.io/commands/info>
  fn info<R>(&self, section: Option<InfoKind>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::server::info(&inner, section).await?.convert()
    })
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
  /// Callers should use the re-exported [redis_keyslot](crate::util::redis_keyslot) function to hash the command's key, if necessary.
  ///
  /// This interface should be used with caution as it may break the automatic pipeline features in the client if command flags are not properly configured.
  fn custom<R, T>(&self, cmd: CustomCommand, args: Vec<T>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    T: TryInto<RedisValue>,
    T::Error: Into<RedisError>,
  {
    let args = atry!(utils::try_into_vec(args));
    async_spawn(self, |inner| async move {
      commands::server::custom(&inner, cmd, args).await?.convert()
    })
  }

  /// Run a custom command similar to [custom](Self::custom), but return the response frame directly without any parsing.
  ///
  /// Note: RESP2 frames from the server are automatically converted to the RESP3 format when parsed by the client.
  fn custom_raw<T>(&self, cmd: CustomCommand, args: Vec<T>) -> AsyncResult<Resp3Frame>
  where
    T: TryInto<RedisValue>,
    T::Error: Into<RedisError>,
  {
    let args = atry!(utils::try_into_vec(args));
    async_spawn(self, |inner| async move {
      commands::server::custom_raw(&inner, cmd, args).await
    })
  }
}

pub use crate::commands::interfaces::{
  acl::AclInterface, client::ClientInterface, cluster::ClusterInterface, config::ConfigInterface, geo::GeoInterface,
  hashes::HashesInterface, hyperloglog::HyperloglogInterface, keys::KeysInterface, lists::ListInterface,
  lua::LuaInterface, memory::MemoryInterface, metrics::MetricsInterface, pubsub::PubsubInterface,
  server::AuthInterface, server::HeartbeatInterface, server::ServerInterface, sets::SetsInterface,
  slowlog::SlowlogInterface, sorted_sets::SortedSetsInterface, streams::StreamsInterface,
  transactions::TransactionInterface,
};

#[cfg(feature = "sentinel-client")]
pub use crate::commands::interfaces::sentinel::SentinelInterface;
