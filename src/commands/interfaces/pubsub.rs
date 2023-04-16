use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{FromRedis, KeyspaceEvent, Message, MultipleStrings, RedisValue},
};
use bytes_utils::Str;
use std::convert::TryInto;
use tokio::sync::broadcast::Receiver as BroadcastReceiver;

/// Functions that implement the [pubsub](https://redis.io/commands#pubsub) interface.
#[async_trait]
pub trait PubsubInterface: ClientLike + Sized {
  /// Listen for messages on the publish-subscribe interface.
  ///
  /// **Keyspace events are not sent on this interface.**
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again.
  /// Messages will start appearing on the original stream after [subscribe](Self::subscribe) is called again.
  fn on_message(&self) -> BroadcastReceiver<Message> {
    self.inner().notifications.pubsub.load().subscribe()
  }

  /// Listen for keyspace and keyevent notifications on the publish-subscribe interface.
  ///
  /// Callers still need to configure the server and subscribe to the relevant channels, but this interface will
  /// parse and format the messages automatically.
  ///
  /// If the connection to the Redis server closes for any reason this function does not need to be called again.
  ///
  /// <https://redis.io/topics/notifications>
  fn on_keyspace_event(&self) -> BroadcastReceiver<KeyspaceEvent> {
    self.inner().notifications.keyspace.load().subscribe()
  }

  /// Subscribe to a channel on the publish-subscribe interface.
  ///
  /// <https://redis.io/commands/subscribe>
  async fn subscribe<R, S>(&self, channels: S) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<MultipleStrings> + Send,
  {
    into!(channels);
    commands::pubsub::subscribe(self, channels).await?.convert()
  }

  /// Unsubscribe from a channel on the PubSub interface.
  ///
  /// <https://redis.io/commands/unsubscribe>
  async fn unsubscribe<S>(&self, channels: S) -> RedisResult<()>
  where
    S: Into<MultipleStrings> + Send,
  {
    into!(channels);
    commands::pubsub::unsubscribe(self, channels).await?.convert()
  }

  /// Subscribes the client to the given patterns.
  ///
  /// <https://redis.io/commands/psubscribe>
  async fn psubscribe<R, S>(&self, patterns: S) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<MultipleStrings> + Send,
  {
    into!(patterns);
    commands::pubsub::psubscribe(self, patterns).await?.convert()
  }

  /// Unsubscribes the client from the given patterns, or from all of them if none is given.
  ///
  /// If no channels are provided this command returns an empty array.
  ///
  /// <https://redis.io/commands/punsubscribe>
  async fn punsubscribe<S>(&self, patterns: S) -> RedisResult<()>
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

  /// Subscribes the client to the specified shard channels.
  ///
  /// <https://redis.io/commands/ssubscribe/>
  async fn ssubscribe<R, C>(&self, channels: C) -> RedisResult<R>
  where
    R: FromRedis,
    C: Into<MultipleStrings> + Send,
  {
    into!(channels);
    commands::pubsub::ssubscribe(self, channels).await?.convert()
  }

  /// Unsubscribes the client from the given shard channels, or from all of them if none is given.
  ///
  /// If no channels are provided this command returns an empty array.
  ///
  /// <https://redis.io/commands/sunsubscribe/>
  async fn sunsubscribe<C>(&self, channels: C) -> RedisResult<()>
  where
    C: Into<MultipleStrings> + Send,
  {
    into!(channels);
    commands::pubsub::sunsubscribe(self, channels).await?.convert()
  }

  /// Posts a message to the given shard channel.
  ///
  /// <https://redis.io/commands/spublish/>
  async fn spublish<R, S, V>(&self, channel: S, message: V) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(channel);
    try_into!(message);
    commands::pubsub::spublish(self, channel, message).await?.convert()
  }

  // TODO pubsub channels, pubsub numpat, pubsub numsub, pubsub shardchannels, pubsub shardnumsub
}
