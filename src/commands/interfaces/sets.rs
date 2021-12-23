use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{MultipleKeys, MultipleValues, RedisKey, FromRedis, RedisValue};
use crate::{commands, utils};
use std::convert::TryInto;

/// Functions that implement the [Sets](https://redis.io/commands#set) interface.
pub trait SetsInterface: ClientLike + Sized {
  /// Add the specified members to the set stored at `key`.
  ///
  /// <https://redis.io/commands/sadd>
  fn sadd<R, K, V>(&self, key: K, members: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(members);
    async_spawn(self, |inner| async move {
      commands::sets::sadd(&inner, key, members).await?.convert()
    })
  }

  /// Returns the set cardinality (number of elements) of the set stored at `key`.
  ///
  /// <https://redis.io/commands/scard>
  fn scard<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::sets::scard(&inner, key).await?.convert()
    })
  }

  /// Returns the members of the set resulting from the difference between the first set and all the successive sets.
  ///
  /// <https://redis.io/commands/sdiff>
  fn sdiff<R, K>(&self, keys: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |inner| async move {
      commands::sets::sdiff(&inner, keys).await?.convert()
    })
  }

  /// This command is equal to SDIFF, but instead of returning the resulting set, it is stored in `destination`.
  ///
  /// <https://redis.io/commands/sdiffstore>
  fn sdiffstore<R, D, K>(&self, dest: D, keys: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
  {
    into!(dest, keys);
    async_spawn(self, |inner| async move {
      commands::sets::sdiffstore(&inner, dest, keys).await?.convert()
    })
  }

  /// Returns the members of the set resulting from the intersection of all the given sets.
  ///
  /// <https://redis.io/commands/sinter>
  fn sinter<R, K>(&self, keys: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |inner| async move {
      commands::sets::sinter(&inner, keys).await?.convert()
    })
  }

  /// This command is equal to SINTER, but instead of returning the resulting set, it is stored in `destination`.
  ///
  /// <https://redis.io/commands/sinterstore>
  fn sinterstore<R, D, K>(&self, dest: D, keys: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
  {
    into!(dest, keys);
    async_spawn(self, |inner| async move {
      commands::sets::sinterstore(&inner, dest, keys).await?.convert()
    })
  }

  /// Returns if `member` is a member of the set stored at `key`.
  ///
  /// <https://redis.io/commands/sismember>
  fn sismember<R, K, V>(&self, key: K, member: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(member);
    async_spawn(self, |inner| async move {
      commands::sets::sismember(&inner, key, member).await?.convert()
    })
  }

  /// Returns whether each member is a member of the set stored at `key`.
  ///
  /// <https://redis.io/commands/smismember>
  fn smismember<R, K, V>(&self, key: K, members: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(members);
    async_spawn(self, |inner| async move {
      commands::sets::smismember(&inner, key, members).await?.convert()
    })
  }

  /// Returns all the members of the set value stored at `key`.
  ///
  /// <https://redis.io/commands/smembers>
  fn smembers<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::sets::smembers(&inner, key).await?.convert()
    })
  }

  /// Move `member` from the set at `source` to the set at `destination`.
  ///
  /// <https://redis.io/commands/smove>
  fn smove<R, S, D, V>(&self, source: S, dest: D, member: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(source, dest);
    try_into!(member);
    async_spawn(self, |inner| async move {
      commands::sets::smove(&inner, source, dest, member).await?.convert()
    })
  }

  /// Removes and returns one or more random members from the set value store at `key`.
  ///
  /// <https://redis.io/commands/spop>
  fn spop<R, K>(&self, key: K, count: Option<usize>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::sets::spop(&inner, key, count).await?.convert()
    })
  }

  /// When called with just the key argument, return a random element from the set value stored at `key`.
  ///
  /// If the provided `count` argument is positive, return an array of distinct elements. The array's length is either count or the set's cardinality (SCARD), whichever is lower.
  ///
  /// <https://redis.io/commands/srandmember>
  fn srandmember<R, K>(&self, key: K, count: Option<usize>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::sets::srandmember(&inner, key, count).await?.convert()
    })
  }

  /// Remove the specified members from the set stored at `key`.
  ///
  /// <https://redis.io/commands/srem>
  fn srem<R, K, V>(&self, key: K, members: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(members);
    async_spawn(self, |inner| async move {
      commands::sets::srem(&inner, key, members).await?.convert()
    })
  }

  /// Returns the members of the set resulting from the union of all the given sets.
  ///
  /// <https://redis.io/commands/sunion>
  fn sunion<R, K>(&self, keys: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |inner| async move {
      commands::sets::sunion(&inner, keys).await?.convert()
    })
  }

  /// This command is equal to SUNION, but instead of returning the resulting set, it is stored in `destination`.
  ///
  /// <https://redis.io/commands/sunionstore>
  fn sunionstore<R, D, K>(&self, dest: D, keys: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
  {
    into!(dest, keys);
    async_spawn(self, |inner| async move {
      commands::sets::sunionstore(&inner, dest, keys).await?.convert()
    })
  }
}
