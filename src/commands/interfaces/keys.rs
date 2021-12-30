use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{Expiration, FromRedis, MultipleKeys, RedisKey, RedisMap, RedisValue, SetOptions};
use std::convert::TryInto;

/// Functions that implement the generic [keys](https://redis.io/commands#generic) interface.
pub trait KeysInterface: ClientLike + Sized {
  /// Return a random key from the currently selected database.
  ///
  /// <https://redis.io/commands/randomkey>
  fn randomkey<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::keys::randomkey(&inner).await?.convert()
    })
  }

  /// This command copies the value stored at the source key to the destination key.
  ///
  /// <https://redis.io/commands/copy>
  fn copy<R, S, D>(&self, source: S, destination: D, db: Option<u8>, replace: bool) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
  {
    into!(source, destination);
    async_spawn(self, |inner| async move {
      commands::keys::copy(&inner, source, destination, db, replace)
        .await?
        .convert()
    })
  }

  /// Serialize the value stored at `key` in a Redis-specific format and return it as bulk string.
  ///
  /// <https://redis.io/commands/dump>
  fn dump<K>(&self, key: K) -> AsyncResult<RedisValue>
  where
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move { commands::keys::dump(&inner, key).await })
  }

  /// Create a key associated with a value that is obtained by deserializing the provided serialized value
  ///
  /// <https://redis.io/commands/restore>
  fn restore<K>(
    &self,
    key: K,
    ttl: i64,
    serialized: RedisValue,
    replace: bool,
    absttl: bool,
    idletime: Option<i64>,
    frequency: Option<i64>,
  ) -> AsyncResult<RedisValue>
  where
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::restore(&inner, key, ttl, serialized, replace, absttl, idletime, frequency).await
    })
  }

  /// Set a value with optional NX|XX, EX|PX|EXAT|PXAT|KEEPTTL, and GET arguments.
  ///
  /// <https://redis.io/commands/set>
  fn set<R, K, V>(
    &self,
    key: K,
    value: V,
    expire: Option<Expiration>,
    options: Option<SetOptions>,
    get: bool,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(value);
    async_spawn(self, |inner| async move {
      commands::keys::set(&inner, key, value, expire, options, get)
        .await?
        .convert()
    })
  }

  /// Read a value from the server.
  ///
  /// <https://redis.io/commands/get>
  fn get<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::get(&inner, key).await?.convert()
    })
  }

  /// Returns the substring of the string value stored at `key` with offsets `start` and `end` (both inclusive).
  ///
  /// Note: Command formerly called SUBSTR in Redis verison <=2.0.
  ///
  /// <https://redis.io/commands/getrange>
  fn getrange<R, K>(&self, key: K, start: usize, end: usize) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::getrange(&inner, key, start, end).await?.convert()
    })
  }

  /// Overwrites part of the string stored at `key`, starting at the specified `offset`, for the entire length of `value`.
  ///
  /// <https://redis.io/commands/setrange>
  fn setrange<R, K, V>(&self, key: K, offset: u32, value: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(value);
    async_spawn(self, |inner| async move {
      commands::keys::setrange(&inner, key, offset, value).await?.convert()
    })
  }

  /// Atomically sets `key` to `value` and returns the old value stored at `key`.
  ///
  /// Returns an error if `key` does not hold string value. Returns nil if `key` does not exist.
  ///
  /// <https://redis.io/commands/getset>
  fn getset<R, K, V>(&self, key: K, value: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(value);
    async_spawn(self, |inner| async move {
      commands::keys::getset(&inner, key, value).await?.convert()
    })
  }

  /// Get the value of key and delete the key. This command is similar to GET, except for the fact that it also deletes the key on success (if and only if the key's value type is a string).
  ///
  /// <https://redis.io/commands/getdel>
  fn getdel<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::getdel(&inner, key).await?.convert()
    })
  }

  /// Returns the length of the string value stored at key. An error is returned when key holds a non-string value.
  ///
  /// <https://redis.io/commands/strlen>
  fn strlen<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::strlen(&inner, key).await?.convert()
    })
  }

  /// Removes the specified keys. A key is ignored if it does not exist.
  ///
  /// Returns the number of keys removed.
  ///
  /// <https://redis.io/commands/del>
  fn del<R, K>(&self, keys: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |inner| async move {
      commands::keys::del(&inner, keys).await?.convert()
    })
  }

  /// Returns the values of all specified keys. For every key that does not hold a string value or does not exist, the special value nil is returned.
  ///
  /// <https://redis.io/commands/mget>
  fn mget<R, K>(&self, keys: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |inner| async move {
      commands::keys::mget(&inner, keys).await?.convert()
    })
  }

  /// Sets the given keys to their respective values.
  ///
  /// <https://redis.io/commands/mset>
  fn mset<V>(&self, values: V) -> AsyncResult<()>
  where
    V: Into<RedisMap>,
  {
    into!(values);
    async_spawn(self, |inner| async move {
      commands::keys::mset(&inner, values).await?.convert()
    })
  }

  /// Sets the given keys to their respective values. MSETNX will not perform any operation at all even if just a single key already exists.
  ///
  /// <https://redis.io/commands/msetnx>
  fn msetnx<R, V>(&self, values: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    V: Into<RedisMap>,
  {
    into!(values);
    async_spawn(self, |inner| async move {
      commands::keys::msetnx(&inner, values).await?.convert()
    })
  }

  /// Increments the number stored at `key` by one. If the key does not exist, it is set to 0 before performing the operation.
  ///
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incr>
  fn incr<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::incr(&inner, key).await?.convert()
    })
  }

  /// Increments the number stored at `key` by `val`. If the key does not exist, it is set to 0 before performing the operation.
  ///
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incrby>
  fn incr_by<R, K>(&self, key: K, val: i64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::incr_by(&inner, key, val).await?.convert()
    })
  }

  /// Increment the string representing a floating point number stored at key by `val`. If the key does not exist, it is set to 0 before performing the operation.
  ///
  /// Returns an error if key value is the wrong type or if the current value cannot be parsed as a floating point value.
  ///
  /// <https://redis.io/commands/incrbyfloat>
  fn incr_by_float<R, K>(&self, key: K, val: f64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::incr_by_float(&inner, key, val).await?.convert()
    })
  }

  /// Decrements the number stored at `key` by one. If the key does not exist, it is set to 0 before performing the operation.
  ///
  /// Returns an error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decr>
  fn decr<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::decr(&inner, key).await?.convert()
    })
  }

  /// Decrements the number stored at `key` by `val`. If the key does not exist, it is set to 0 before performing the operation.
  ///
  /// Returns an error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decrby>
  fn decr_by<R, K>(&self, key: K, val: i64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::decr_by(&inner, key, val).await?.convert()
    })
  }

  /// Returns the remaining time to live of a key that has a timeout, in seconds.
  ///
  /// <https://redis.io/commands/ttl>
  fn ttl<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::ttl(&inner, key).await?.convert()
    })
  }

  /// Returns the remaining time to live of a key that has a timeout, in milliseconds.
  ///
  /// <https://redis.io/commands/pttl>
  fn pttl<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::pttl(&inner, key).await?.convert()
    })
  }

  /// Remove the existing timeout on a key, turning the key from volatile (a key with an expiration)
  /// to persistent (a key that will never expire as no timeout is associated).
  ///
  /// Returns a boolean value describing whether or not the timeout was removed.
  ///
  /// <https://redis.io/commands/persist>
  fn persist<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::persist(&inner, key).await?.convert()
    })
  }

  /// Set a timeout on key. After the timeout has expired, the key will be automatically deleted.
  ///
  /// Returns a boolean value describing whether or not the timeout was added.
  ///
  /// <https://redis.io/commands/expire>
  fn expire<R, K>(&self, key: K, seconds: i64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::expire(&inner, key, seconds).await?.convert()
    })
  }

  /// Set a timeout on a key based on a UNIX timestamp.
  ///
  /// Returns a boolean value describing whether or not the timeout was added.
  ///
  /// <https://redis.io/commands/expireat>
  fn expire_at<R, K>(&self, key: K, timestamp: i64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::keys::expire_at(&inner, key, timestamp).await?.convert()
    })
  }

  /// Returns number of keys that exist from the `keys` arguments.
  ///
  /// <https://redis.io/commands/exists>
  fn exists<R, K>(&self, keys: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |inner| async move {
      commands::keys::exists(&inner, keys).await?.convert()
    })
  }
}
