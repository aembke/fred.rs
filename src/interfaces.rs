use crate::commands;
use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::{commands as multiplexer_commands, utils as multiplexer_utils};
use crate::types::RespVersion;
use crate::types::{
  ClientState, ConnectHandle, CustomCommand, FromRedis, InfoKind, ReconnectPolicy, RedisConfig, RedisValue,
  ShutdownFlags,
};
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

// The motivation for these abstractions comes from the fact that the Redis interface is huge, and managing it all
// in one `RedisClient` struck is becoming unwieldy and unsustainable. Additionally, there are now use cases for
// supporting different client structs that implement subsets of the Redis interface where some of the underlying
// command implementations should be shared across multiple different client structs.
//
// For example, `SentinelClient` supports ACL commands and pubsub commands like a `RedisClient`, but also implements
// its own unique interface for sentinel nodes. However, it does *not* support zset commands, cluster commands, etc.
// In the future I would like the flexibility to add a `PubsubClient` for example, where it can share the same pubsub
// command interface as a `RedisClient`, but with some added features to manage subscriptions across reconnections
// automatically for callers.
//
// In an ideal world we could implement this with a trait for each section of the Redis interface. For example we
// could have an `AclInterface`, `ZsetInterface`, `HashInterface`, etc, where each interface implemented the command
// functions for that portion of the Redis interface. Since everything in this module revolves around a
// `RedisClientInner` this would be easy to do with a super trait that implemented a function to return this inner
// struct (`fn inner(&self) -> &Arc<RedisClientInner>`).
//
// However, async functions are not supported in traits, which makes this much more difficult, and if we don't find
// some way to use traits to reuse portions of the Redis interface then we'll end up duplicating a lot of code, or
// relying on `Deref`, which has downsides in that it doesn't let you remove certain sections of the underlying interface.
//
// The abstractions implemented here are necessary because of this lack of async functions in traits. The
// `async-trait` crate exists, but it reverts to using trait objects (a la futures 0.1 pre `impl Trait`) and I'm
// not a fan of how it obfuscates your function signatures and uses boxes as often as it does.
//
// That being said, originally when I implemented this file I tried using a new tokio task for each call to `async_spawn`. This
// worked and provided a relatively clean implementation for `AsyncResult` and `AsyncInner` that didn't require relying on trait
// objects, but it dramatically reduced performance. Prior to the introduction of the new tokio task for each command the pipeline
// test benchmark could do ~2MM req/sec, but after adding the tokio task it could only do around 540k req/sec.
//
// After noticing this I went back and re-implemented this with trait objects to fix the performance issue. This ended up making the
// implementation very similar to `async-trait`, largely contradicting the paragraph above about boxes. However, the `AsyncResult`
// abstraction does have the benefit of being quite a bit more readable than the `Pin<Box<...>>` return type from `async-trait`, so
// I'm going to keep the `AsyncResult` abstraction in place for the time being for that reason alone.
//
// After switching from a new `tokio::spawn` call to trait objects the performance went back to about 1.85MM req/sec, which seems to be
// about as good as we can hope for without support for async functions in traits. While it would be nice to remove the overhead of the
// new trait object to get performance back to 2MM req/sec I think there's lower hanging fruit elsewhere in the code to tackle first that
// would have an even greater impact on performance.
//
// The `Send` requirement exists because the underlying future must be marked as `Send` for commands to work inside a `tokio::spawn` call.
// Some of the tests and examples do this for various reasons, and I don't want to prevent callers from implementing similar patterns. The
// only use case I can think of where this might be problematic is one where callers are using a custom, probably-too-clever hashing
// implementation with a `HashMap`, since all other `FromRedis` implementations are already for `Send` types. Aside from that I don't think
// the `Send` requirement should be an issue (especially since a lot of the tokio interface already requires it anyways).
//
// That being said, if anybody has issues with the `Send` requirement I'd be very interested to hear more about the use case.

/// An enum used to represent the return value from a function that does some fallible synchronous work,
/// followed by some more fallible async logic inside a new tokio task.
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
      }
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
  slowlog::SlowlogInterface, sorted_sets::SortedSetsInterface, transactions::TransactionInterface,
};

#[cfg(feature = "sentinel-client")]
pub use crate::commands::interfaces::sentinel::SentinelInterface;
