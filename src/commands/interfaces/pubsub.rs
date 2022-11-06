use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{FromRedis, KeyspaceEvent, MultipleStrings, RedisValue},
};
use bytes_utils::Str;
use std::convert::TryInto;
use tokio::sync::broadcast::Receiver as BroadcastReceiver;

/// Functions that implement the [publish-subscribe](https://redis.io/commands#pubsub) interface.
#[async_trait]
pub trait PubsubInterface: ClientLike + Sized {
  /// Listen for `(channel, message)` tuples on the publish-subscribe interface. **Keyspace events are not sent on
  /// this interface.**
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again.
  /// Messages will start appearing on the original stream after [subscribe](Self::subscribe) is called again.
  fn on_message(&self) -> BroadcastReceiver<(String, RedisValue)> {
    self.inner().notifications.pubsub.subscribe()
  }

  /// Listen for keyspace and keyevent notifications on the publish subscribe interface.
  ///
  /// Callers still need to configure the server and subscribe to the relevant channels, but this interface will
  /// format the messages automatically.
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again.
  ///
  /// <https://redis.io/topics/notifications>
  fn on_keyspace_event(&self) -> BroadcastReceiver<KeyspaceEvent> {
    self.inner().notifications.keyspace.subscribe()
  }

  /// Subscribe to a channel on the publish-subscribe interface, returning the number of channels to which the client
  /// is subscribed.
  ///
  /// <https://redis.io/commands/subscribe>
  async fn subscribe<S>(&self, channel: S) -> RedisResult<usize>
  where
    S: Into<Str> + Send,
  {
    into!(channel);
    commands::pubsub::subscribe(self, channel).await?.convert()
  }

  /// Unsubscribe from a channel on the PubSub interface, returning the number of channels to which hte client is
  /// subscribed.
  ///
  /// <https://redis.io/commands/unsubscribe>
  async fn unsubscribe<S>(&self, channel: S) -> RedisResult<usize>
  where
    S: Into<Str> + Send,
  {
    into!(channel);
    commands::pubsub::unsubscribe(self, channel).await?.convert()
  }

  /// Subscribes the client to the given patterns.
  ///
  /// <https://redis.io/commands/psubscribe>
  async fn psubscribe<S>(&self, patterns: S) -> RedisResult<Vec<usize>>
  where
    S: Into<MultipleStrings> + Send,
  {
    into!(patterns);
    commands::pubsub::psubscribe(self, patterns).await?.convert()
  }

  /// Unsubscribes the client from the given patterns, or from all of them if none is given.
  ///
  /// <https://redis.io/commands/punsubscribe>
  async fn punsubscribe<S>(&self, patterns: S) -> RedisResult<Vec<usize>>
  where
    S: Into<MultipleStrings> + Send,
  {
    into!(patterns);
    commands::pubsub::punsubscribe(self, patterns).await?.convert()
  }

  /// Publish a message on the PubSub interface, returning the number of clients that received the message.
  ///
  /// <https://redis.io/commands/publish>
  async fn publish<R, S, V>(&self, channel: S, message: V) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(channel);
    try_into!(message);
    commands::pubsub::publish(self, channel, message).await?.convert()
  }
}
