use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{FromRedis, KeyspaceEvent, MultipleStrings, RedisValue};
use bytes_utils::Str;
use std::convert::TryInto;
use tokio::sync::broadcast::Receiver as BroadcastReceiver;

/// Functions that implement the [publish-subscribe](https://redis.io/commands#pubsub) interface.
pub trait PubsubInterface: ClientLike + Sized {
  /// Listen for `(channel, message)` tuples on the publish-subscribe interface. **Keyspace events are not sent on this interface.**
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again. Messages will start appearing on the original stream after [subscribe](Self::subscribe) is called again.
  fn on_message(&self) -> BroadcastReceiver<(String, RedisValue)> {
    self.inner().notifications.pubsub.subscribe()
  }

  /// Listen for keyspace and keyevent notifications on the publish subscribe interface.
  ///
  /// Callers still need to configure the server and subscribe to the relevant channels, but this interface will format the messages automatically.
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again.
  ///
  /// <https://redis.io/topics/notifications>
  fn on_keyspace_event(&self) -> BroadcastReceiver<KeyspaceEvent> {
    self.inner().notifications.keyspace.subscribe()
  }

  /// Subscribe to a channel on the publish-subscribe interface, returning the number of channels to which the client is subscribed.
  ///
  /// <https://redis.io/commands/subscribe>
  fn subscribe<S>(&self, channel: S) -> AsyncResult<usize>
  where
    S: Into<Str>,
  {
    into!(channel);
    async_spawn(self, |_self| async move {
      commands::pubsub::subscribe(_self, channel).await
    })
  }

  /// Unsubscribe from a channel on the PubSub interface, returning the number of channels to which hte client is subscribed.
  ///
  /// <https://redis.io/commands/unsubscribe>
  fn unsubscribe<S>(&self, channel: S) -> AsyncResult<usize>
  where
    S: Into<Str>,
  {
    into!(channel);
    async_spawn(self, |_self| async move {
      commands::pubsub::unsubscribe(_self, channel).await
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
    async_spawn(self, |_self| async move {
      commands::pubsub::psubscribe(_self, patterns).await
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
    async_spawn(self, |_self| async move {
      commands::pubsub::punsubscribe(_self, patterns).await
    })
  }

  /// Publish a message on the PubSub interface, returning the number of clients that received the message.
  ///
  /// <https://redis.io/commands/publish>
  fn publish<R, S, V>(&self, channel: S, message: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<Str>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(channel);
    try_into!(message);
    async_spawn(self, |_self| async move {
      commands::pubsub::publish(_self, channel, message).await?.convert()
    })
  }
}
