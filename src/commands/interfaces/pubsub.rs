use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, AsyncStream, ClientLike};
use crate::types::{KeyspaceEvent, MultipleStrings, FromRedis, RedisValue};
use crate::utils;
use futures::Stream;
use std::convert::TryInto;
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Functions that implement the [publish-subscribe](https://redis.io/commands#pubsub) interface.
pub trait PubsubInterface: ClientLike + Sized {
  /// Listen for `(channel, message)` tuples on the publish-subscribe interface. **Keyspace events are not sent on this interface.**
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again. Messages will start appearing on the original stream after [subscribe](Self::subscribe) is called again.
  fn on_message(&self) -> AsyncStream<(String, RedisValue)> {
    let (tx, rx) = unbounded_channel();
    self.inner().message_tx.write().push_back(tx);

    UnboundedReceiverStream::new(rx).into()
  }

  /// Listen for keyspace and keyevent notifications on the publish subscribe interface.
  ///
  /// Callers still need to configure the server and subscribe to the relevant channels, but this interface will format the messages automatically.
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again.
  ///
  /// <https://redis.io/topics/notifications>
  fn on_keyspace_event(&self) -> AsyncStream<KeyspaceEvent> {
    let (tx, rx) = unbounded_channel();
    self.inner().keyspace_tx.write().push_back(tx);

    UnboundedReceiverStream::new(rx).into()
  }

  /// Subscribe to a channel on the PubSub interface, returning the number of channels to which the client is subscribed.
  ///
  /// Any messages received before `on_message` is called will be discarded, so it's usually best to call `on_message`
  /// before calling `subscribe` for the first time.
  ///
  /// <https://redis.io/commands/subscribe>
  fn subscribe<S>(&self, channel: S) -> AsyncResult<usize>
  where
    S: Into<String>,
  {
    into!(channel);
    async_spawn(self, |inner| async move {
      commands::pubsub::subscribe(&inner, channel).await
    })
  }

  /// Unsubscribe from a channel on the PubSub interface, returning the number of channels to which hte client is subscribed.
  ///
  /// <https://redis.io/commands/unsubscribe>
  fn unsubscribe<S>(&self, channel: S) -> AsyncResult<usize>
  where
    S: Into<String>,
  {
    into!(channel);
    async_spawn(self, |inner| async move {
      commands::pubsub::unsubscribe(&inner, channel).await
    })
  }

  /// Subscribes the client to the given patterns.
  ///
  /// <https://redis.io/commands/psubscribe>
  fn psubscribe<S>(&self, patterns: S) -> AsyncResult<Vec<usize>>
  where
    S: Into<MultipleStrings>,
  {
    into!(patterns);
    async_spawn(self, |inner| async move {
      commands::pubsub::psubscribe(&inner, patterns).await
    })
  }

  /// Unsubscribes the client from the given patterns, or from all of them if none is given.
  ///
  /// <https://redis.io/commands/punsubscribe>
  fn punsubscribe<S>(&self, patterns: S) -> AsyncResult<Vec<usize>>
  where
    S: Into<MultipleStrings>,
  {
    into!(patterns);
    async_spawn(self, |inner| async move {
      commands::pubsub::punsubscribe(&inner, patterns).await
    })
  }

  /// Publish a message on the PubSub interface, returning the number of clients that received the message.
  ///
  /// <https://redis.io/commands/publish>
  fn publish<R, S, V>(&self, channel: S, message: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<String>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(channel);
    try_into!(message);
    async_spawn(self, |inner| async move {
      commands::pubsub::publish(&inner, channel, message).await?.convert()
    })
  }
}
