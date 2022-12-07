use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{FromRedis, MultipleKeys, MultipleValues, RedisKey, RedisValue},
};
use std::convert::TryInto;

/// Functions that implement the [Sets](https://redis.io/commands#set) interface.
#[async_trait]
pub trait SetsInterface: ClientLike + Sized {
  /// Add the specified members to the set stored at `key`.
  ///
  /// <https://redis.io/commands/sadd>
  async fn sadd<R, K, V>(&self, key: K, members: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(members);
    commands::sets::sadd(self, key, members).await?.convert()
  }

  /// Returns the set cardinality (number of elements) of the set stored at `key`.
  ///
  /// <https://redis.io/commands/scard>
  async fn scard<R, K>(&self, key: K) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::sets::scard(self, key).await?.convert()
  }

  /// Returns the members of the set resulting from the difference between the first set and all the successive sets.
  ///
  /// <https://redis.io/commands/sdiff>
  async fn sdiff<R, K>(&self, keys: K) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
  {
    into!(keys);
    commands::sets::sdiff(self, keys).await?.convert()
  }

  /// This command is equal to SDIFF, but instead of returning the resulting set, it is stored in `destination`.
  ///
  /// <https://redis.io/commands/sdiffstore>
  async fn sdiffstore<R, D, K>(&self, dest: D, keys: K) -> RedisResult<R>
  where
    R: FromRedis,
    D: Into<RedisKey> + Send,
    K: Into<MultipleKeys> + Send,
  {
    into!(dest, keys);
    commands::sets::sdiffstore(self, dest, keys).await?.convert()
  }

  /// Returns the members of the set resulting from the intersection of all the given sets.
  ///
  /// <https://redis.io/commands/sinter>
  async fn sinter<R, K>(&self, keys: K) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
  {
    into!(keys);
    commands::sets::sinter(self, keys).await?.convert()
  }

  /// This command is equal to SINTER, but instead of returning the resulting set, it is stored in `destination`.
  ///
  /// <https://redis.io/commands/sinterstore>
  async fn sinterstore<R, D, K>(&self, dest: D, keys: K) -> RedisResult<R>
  where
    R: FromRedis,
    D: Into<RedisKey> + Send,
    K: Into<MultipleKeys> + Send,
  {
    into!(dest, keys);
    commands::sets::sinterstore(self, dest, keys).await?.convert()
  }

  /// Returns if `member` is a member of the set stored at `key`.
  ///
  /// <https://redis.io/commands/sismember>
  async fn sismember<R, K, V>(&self, key: K, member: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(member);
    commands::sets::sismember(self, key, member).await?.convert()
  }

  /// Returns whether each member is a member of the set stored at `key`.
  ///
  /// <https://redis.io/commands/smismember>
  async fn smismember<R, K, V>(&self, key: K, members: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(members);
    commands::sets::smismember(self, key, members).await?.convert()
  }

  /// Returns all the members of the set value stored at `key`.
  ///
  /// <https://redis.io/commands/smembers>
  async fn smembers<R, K>(&self, key: K) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::sets::smembers(self, key).await?.convert()
  }

  /// Move `member` from the set at `source` to the set at `destination`.
  ///
  /// <https://redis.io/commands/smove>
  async fn smove<R, S, D, V>(&self, source: S, dest: D, member: V) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<RedisKey> + Send,
    D: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(source, dest);
    try_into!(member);
    commands::sets::smove(self, source, dest, member).await?.convert()
  }

  /// Removes and returns one or more random members from the set value store at `key`.
  ///
  /// <https://redis.io/commands/spop>
  async fn spop<R, K>(&self, key: K, count: Option<usize>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::sets::spop(self, key, count).await?.convert()
  }

  /// When called with just the key argument, return a random element from the set value stored at `key`.
  ///
  /// If the provided `count` argument is positive, return an array of distinct elements. The array's length is either
  /// count or the set's cardinality (SCARD), whichever is lower.
  ///
  /// <https://redis.io/commands/srandmember>
  async fn srandmember<R, K>(&self, key: K, count: Option<usize>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::sets::srandmember(self, key, count).await?.convert()
  }

  /// Remove the specified members from the set stored at `key`.
  ///
  /// <https://redis.io/commands/srem>
  async fn srem<R, K, V>(&self, key: K, members: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(members);
    commands::sets::srem(self, key, members).await?.convert()
  }

  /// Returns the members of the set resulting from the union of all the given sets.
  ///
  /// <https://redis.io/commands/sunion>
  async fn sunion<R, K>(&self, keys: K) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
  {
    into!(keys);
    commands::sets::sunion(self, keys).await?.convert()
  }

  /// This command is equal to SUNION, but instead of returning the resulting set, it is stored in `destination`.
  ///
  /// <https://redis.io/commands/sunionstore>
  async fn sunionstore<R, D, K>(&self, dest: D, keys: K) -> RedisResult<R>
  where
    R: FromRedis,
    D: Into<RedisKey> + Send,
    K: Into<MultipleKeys> + Send,
  {
    into!(dest, keys);
    commands::sets::sunionstore(self, dest, keys).await?.convert()
  }
}
