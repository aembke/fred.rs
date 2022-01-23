use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{FromRedis, MultipleKeys, RedisKey, RedisMap, RedisValue};
use std::convert::TryInto;

/// Functions that implement the [Hashes](https://redis.io/commands#hashes) interface.
pub trait HashesInterface: ClientLike + Sized {
  /// Returns all fields and values of the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hgetall>
  fn hgetall<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::hashes::hgetall(&inner, key).await?.convert()
    })
  }

  /// Removes the specified fields from the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hdel>
  fn hdel<R, K, F>(&self, key: K, fields: F) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    F: Into<MultipleKeys>,
  {
    into!(key, fields);
    async_spawn(self, |inner| async move {
      commands::hashes::hdel(&inner, key, fields).await?.convert()
    })
  }

  /// Returns if `field` is an existing field in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hexists>
  fn hexists<R, K, F>(&self, key: K, field: F) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
  {
    into!(key, field);
    async_spawn(self, |inner| async move {
      commands::hashes::hexists(&inner, key, field).await?.convert()
    })
  }

  /// Returns the value associated with `field` in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hget>
  fn hget<R, K, F>(&self, key: K, field: F) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
  {
    into!(key, field);
    async_spawn(self, |inner| async move {
      commands::hashes::hget(&inner, key, field).await?.convert()
    })
  }

  /// Increments the number stored at `field` in the hash stored at `key` by `increment`.
  ///
  /// <https://redis.io/commands/hincrby>
  fn hincrby<R, K, F>(&self, key: K, field: F, increment: i64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
  {
    into!(key, field);
    async_spawn(self, |inner| async move {
      commands::hashes::hincrby(&inner, key, field, increment)
        .await?
        .convert()
    })
  }

  /// Increment the specified `field` of a hash stored at `key`, and representing a floating point number, by the specified `increment`.
  ///
  /// <https://redis.io/commands/hincrbyfloat>
  fn hincrbyfloat<R, K, F>(&self, key: K, field: F, increment: f64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
  {
    into!(key, field);
    async_spawn(self, |inner| async move {
      commands::hashes::hincrbyfloat(&inner, key, field, increment)
        .await?
        .convert()
    })
  }

  /// Returns all field names in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hkeys>
  fn hkeys<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::hashes::hkeys(&inner, key).await?.convert()
    })
  }

  /// Returns the number of fields contained in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hlen>
  fn hlen<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::hashes::hlen(&inner, key).await?.convert()
    })
  }

  /// Returns the values associated with the specified `fields` in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hmget>
  fn hmget<R, K, F>(&self, key: K, fields: F) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    F: Into<MultipleKeys>,
  {
    into!(key, fields);
    async_spawn(self, |inner| async move {
      commands::hashes::hmget(&inner, key, fields).await?.convert()
    })
  }

  /// Sets the specified fields to their respective values in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hmset>
  fn hmset<R, K, V>(&self, key: K, values: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisMap>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(values);
    async_spawn(self, |inner| async move {
      commands::hashes::hmset(&inner, key, values).await?.convert()
    })
  }

  /// Sets fields in the hash stored at `key` to their provided values.
  ///
  /// <https://redis.io/commands/hset>
  fn hset<R, K, V>(&self, key: K, values: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisMap>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(values);
    async_spawn(self, |inner| async move {
      commands::hashes::hset(&inner, key, values).await?.convert()
    })
  }

  /// Sets `field` in the hash stored at `key` to `value`, only if `field` does not yet exist.
  ///
  /// <https://redis.io/commands/hsetnx>
  fn hsetnx<R, K, F, V>(&self, key: K, field: F, value: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key, field);
    try_into!(value);
    async_spawn(self, |inner| async move {
      commands::hashes::hsetnx(&inner, key, field, value).await?.convert()
    })
  }

  /// When called with just the `key` argument, return a random field from the hash value stored at `key`.
  ///
  /// If the provided `count` argument is positive, return an array of distinct fields.
  ///
  /// <https://redis.io/commands/hrandfield>
  fn hrandfield<R, K>(&self, key: K, count: Option<(i64, bool)>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::hashes::hrandfield(&inner, key, count).await?.convert()
    })
  }

  /// Returns the string length of the value associated with `field` in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hstrlen>
  fn hstrlen<R, K, F>(&self, key: K, field: F) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    F: Into<RedisKey>,
  {
    into!(key, field);
    async_spawn(self, |inner| async move {
      commands::hashes::hstrlen(&inner, key, field).await?.convert()
    })
  }

  /// Returns all values in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hvals>
  fn hvals<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::hashes::hvals(&inner, key).await?.convert()
    })
  }
}
