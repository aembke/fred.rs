use crate::client::RedisClient;
use crate::commands;
use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::{commands as multiplexer_commands, utils as multiplexer_utils};
use crate::types::{
  ClientState, ClusterKeyCache, ConnectHandle, CustomCommand, FromRedis, InfoKind, ReconnectPolicy, RedisConfig,
  RedisValue, ShutdownFlags,
};
use crate::utils;
use futures::{Stream, TryFutureExt};
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc::unbounded_channel;
use tokio::task::JoinHandle;
use tokio::time::interval as tokio_interval;
use tokio_stream::wrappers::UnboundedReceiverStream;

// This is a strange file.
//
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
// not a fan of how it messes with your function signatures and uses boxes all over the place.
//
// Additionally, this module already spawns a tokio task for commands, so we can get around the lack of async
// functions in traits simply by moving the current `tokio::spawn` call up the stack a bit, which can turn an async
// function into a "sync" function that returns a Future, which is really the same thing. This is pretty much
// what `async-trait` does under the hood (minus the spawn), but instead of using a trait object we just return
// a struct that we define here which implements the `Future` trait.
//
// We'll call this file "some cost abstractions" until async functions in traits are available since it does require
// one more `Arc` clone than it otherwise would with async functions in traits. Once that feature is stable I'll come
// back and remove this to avoid the added `Arc` clone used here (which is required to `move` the inner struct into the
// new tokio task).

/// An enum used to represent the return value from a function that does some fallible synchronous work,
/// followed by some more fallible async logic inside a new tokio task.
enum AsyncInner<T: 'static> {
  Result(Option<Result<T, RedisError>>),
  Task(Pin<Box<dyn Future<Output = Result<T, RedisError>>>>),
}

// TODO the additional tokio task just kills the perf in benchmarks. maybe switch AsyncResult to hold a trait object
// and get rid of the tokio spawn

/// A wrapper type for return values from async functions implemented in a trait.
pub struct AsyncResult<T: 'static> {
  inner: AsyncInner<T>,
}

#[doc(hidden)]
impl<T, E> From<Result<T, E>> for AsyncResult<T>
where
  T: 'static,
  E: Into<RedisError>,
{
  fn from(value: Result<T, E>) -> Self {
    AsyncResult {
      inner: AsyncInner::Result(Some(value.map_err(|e| e.into()))),
    }
  }
}

#[doc(hidden)]
impl<T> From<Pin<Box<dyn Future<Output = Result<T, RedisError>>>>> for AsyncResult<T>
where
  T: 'static,
{
  fn from(f: Pin<Box<dyn Future<Output = Result<T, RedisError>>>>) -> Self {
    AsyncResult {
      inner: AsyncInner::Task(f),
    }
  }
}

impl<T> Future for AsyncResult<T>
where
  T: 'static,
{
  type Output = Result<T, RedisError>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    match self.inner {
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

/// Run a function in the context of a new tokio task, returning an `AsyncResult`.
pub(crate) fn async_spawn<C, F, Fut, T>(client: &C, func: F) -> AsyncResult<T>
where
  C: ClientLike,
  Fut: Future<Output = Result<T, RedisError>> + 'static,
  F: FnOnce(Arc<RedisClientInner>) -> Fut,
  T: 'static,
{
  // this is unfortunate but necessary without async functions in traits
  let inner = client.inner().clone();
  // TODO get rid of this ^?
  Box::pin(func(inner)).into()
}

/// Any Redis client that implements any part of the Redis interface.
pub trait ClientLike: Unpin + Send + Sync + Sized {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner>;

  /// The unique ID identifying this client and underlying connections.
  ///
  /// All connections will use the ID of the client that created them for `CLIENT SETNAME`.
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

  /// Whether or not the client has a reconnection policy.
  fn has_reconnect_policy(&self) -> bool {
    self.inner().policy.read().is_some()
  }

  /// Whether or not the client will pipeline commands.
  fn is_pipelined(&self) -> bool {
    self.inner().is_pipelined()
  }

  /// Read the number of request redeliveries.
  ///
  /// This is the number of times a request had to be sent again due to a connection closing while waiting on a response.
  fn read_redelivery_count(&self) -> usize {
    utils::read_atomic(&self.inner().redeliver_count)
  }

  /// Read and reset the number of request redeliveries.
  fn take_redelivery_count(&self) -> usize {
    utils::set_atomic(&self.inner().redeliver_count, 0)
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
  /// Callers should use the re-exported [redis_keyslot](crate::client::util::redis_keyslot) function to hash the command's key, if necessary.
  ///
  /// Callers that find themselves using this interface for commands that are not a part of a third party extension should file an issue
  /// to add the command to the list of supported commands. This interface should be used with caution as it may break the automatic pipeline
  /// features in the client if command flags are not properly configured.
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
