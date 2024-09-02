use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{Expiration, ExpireOptions, FromRedis, MultipleKeys, RedisKey, RedisMap, RedisValue, SetOptions},
};
use futures::Future;
use rm_send_macros::rm_send_if;
use std::convert::TryInto;

/// Functions that implement the generic [keys](https://redis.io/commands#generic) interface.
#[rm_send_if(feature = "glommio")]
pub trait KeysInterface: ClientLike + Sized {
  /// Marks the given keys to be watched for conditional execution of a transaction.
  ///
  /// <https://redis.io/commands/watch>
  fn watch<K>(&self, keys: K) -> impl Future<Output = RedisResult<()>> + Send
  where
    K: Into<MultipleKeys> + Send,
  {
    async move {
      into!(keys);
      commands::keys::watch(self, keys).await
    }
  }

  /// Flushes all the previously watched keys for a transaction.
  ///
  /// <https://redis.io/commands/unwatch>
  fn unwatch(&self) -> impl Future<Output = RedisResult<()>> + Send {
    async move { commands::keys::unwatch(self).await }
  }

  /// Return a random key from the currently selected database.
  ///
  /// <https://redis.io/commands/randomkey>
  fn randomkey<R>(&self) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
  {
    async move { commands::keys::randomkey(self).await?.convert() }
  }

  /// This command copies the value stored at the source key to the destination key.
  ///
  /// <https://redis.io/commands/copy>
  fn copy<R, S, D>(
    &self,
    source: S,
    destination: D,
    db: Option<u8>,
    replace: bool,
  ) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    S: Into<RedisKey> + Send,
    D: Into<RedisKey> + Send,
  {
    async move {
      into!(source, destination);
      commands::keys::copy(self, source, destination, db, replace)
        .await?
        .convert()
    }
  }

  /// Serialize the value stored at `key` in a Redis-specific format and return it as bulk string.
  ///
  /// <https://redis.io/commands/dump>
  fn dump<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::dump(self, key).await?.convert()
    }
  }

  /// Create a key associated with a value that is obtained by deserializing the provided serialized value
  ///
  /// <https://redis.io/commands/restore>
  fn restore<R, K>(
    &self,
    key: K,
    ttl: i64,
    serialized: RedisValue,
    replace: bool,
    absttl: bool,
    idletime: Option<i64>,
    frequency: Option<i64>,
  ) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::restore(self, key, ttl, serialized, replace, absttl, idletime, frequency)
        .await?
        .convert()
    }
  }

  /// Set a value with optional NX|XX, EX|PX|EXAT|PXAT|KEEPTTL, and GET arguments.
  ///
  /// Note: the `get` flag was added in 6.2.0. Setting it as `false` works with Redis versions <=6.2.0.
  ///
  /// <https://redis.io/commands/set>
  fn set<R, K, V>(
    &self,
    key: K,
    value: V,
    expire: Option<Expiration>,
    options: Option<SetOptions>,
    get: bool,
  ) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    async move {
      into!(key);
      try_into!(value);
      commands::keys::set(self, key, value, expire, options, get)
        .await?
        .convert()
    }
  }

  /// Read a value from the server.
  ///
  /// <https://redis.io/commands/get>
  fn get<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::get(self, key).await?.convert()
    }
  }

  /// Returns the substring of the string value stored at `key` with offsets `start` and `end` (both inclusive).
  ///
  /// Note: Command formerly called SUBSTR in Redis verison <=2.0.
  ///
  /// <https://redis.io/commands/getrange>
  fn getrange<R, K>(&self, key: K, start: usize, end: usize) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::getrange(self, key, start, end).await?.convert()
    }
  }

  /// Overwrites part of the string stored at `key`, starting at the specified `offset`, for the entire length of
  /// `value`.
  ///
  /// <https://redis.io/commands/setrange>
  fn setrange<R, K, V>(&self, key: K, offset: u32, value: V) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    async move {
      into!(key);
      try_into!(value);
      commands::keys::setrange(self, key, offset, value).await?.convert()
    }
  }

  /// Atomically sets `key` to `value` and returns the old value stored at `key`.
  ///
  /// Returns an error if `key` does not hold string value. Returns nil if `key` does not exist.
  ///
  /// <https://redis.io/commands/getset>
  fn getset<R, K, V>(&self, key: K, value: V) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    async move {
      into!(key);
      try_into!(value);
      commands::keys::getset(self, key, value).await?.convert()
    }
  }

  /// Get the value of key and delete the key. This command is similar to GET, except for the fact that it also
  /// deletes the key on success (if and only if the key's value type is a string).
  ///
  /// <https://redis.io/commands/getdel>
  fn getdel<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::getdel(self, key).await?.convert()
    }
  }

  /// Returns the length of the string value stored at key. An error is returned when key holds a non-string value.
  ///
  /// <https://redis.io/commands/strlen>
  fn strlen<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::strlen(self, key).await?.convert()
    }
  }

  /// Removes the specified keys. A key is ignored if it does not exist.
  ///
  /// Returns the number of keys removed.
  ///
  /// <https://redis.io/commands/del>
  fn del<R, K>(&self, keys: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
  {
    async move {
      into!(keys);
      commands::keys::del(self, keys).await?.convert()
    }
  }

  /// Unlinks the specified keys. A key is ignored if it does not exist
  ///
  /// Returns the number of keys removed.
  ///
  /// <https://redis.io/commands/del>
  fn unlink<R, K>(&self, keys: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
  {
    async move {
      into!(keys);
      commands::keys::unlink(self, keys).await?.convert()
    }
  }

  /// Renames `source` key to `destination`.
  ///
  /// Returns an error when `source` does not exist. If `destination` exists, it gets overwritten.
  ///
  /// <https://redis.io/commands/rename>
  fn rename<R, S, D>(&self, source: S, destination: D) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    S: Into<RedisKey> + Send,
    D: Into<RedisKey> + Send,
  {
    async move {
      into!(source);
      into!(destination);
      commands::keys::rename(self, source, destination).await?.convert()
    }
  }

  /// Renames `source` key to `destination` if `destination` does not yet exist.
  ///
  /// Returns an error when `source` does not exist.
  ///
  /// <https://redis.io/commands/renamenx>
  fn renamenx<R, S, D>(&self, source: S, destination: D) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    S: Into<RedisKey> + Send,
    D: Into<RedisKey> + Send,
  {
    async move {
      into!(source);
      into!(destination);
      commands::keys::renamenx(self, source, destination).await?.convert()
    }
  }

  /// Append `value` to `key` if it's a string.
  ///
  /// <https://redis.io/commands/append/>
  fn append<R, K, V>(&self, key: K, value: V) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    async move {
      into!(key);
      try_into!(value);
      commands::keys::append(self, key, value).await?.convert()
    }
  }

  /// Returns the values of all specified keys. For every key that does not hold a string value or does not exist, the
  /// special value nil is returned.
  ///
  /// <https://redis.io/commands/mget>
  fn mget<R, K>(&self, keys: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
  {
    async move {
      into!(keys);
      commands::keys::mget(self, keys).await?.convert()
    }
  }

  /// Sets the given keys to their respective values.
  ///
  /// <https://redis.io/commands/mset>
  fn mset<V>(&self, values: V) -> impl Future<Output = RedisResult<()>> + Send
  where
    V: TryInto<RedisMap> + Send,
    V::Error: Into<RedisError> + Send,
  {
    async move {
      try_into!(values);
      commands::keys::mset(self, values).await?.convert()
    }
  }

  /// Sets the given keys to their respective values. MSETNX will not perform any operation at all even if just a
  /// single key already exists.
  ///
  /// <https://redis.io/commands/msetnx>
  fn msetnx<R, V>(&self, values: V) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    V: TryInto<RedisMap> + Send,
    V::Error: Into<RedisError> + Send,
  {
    async move {
      try_into!(values);
      commands::keys::msetnx(self, values).await?.convert()
    }
  }

  /// Increments the number stored at `key` by one. If the key does not exist, it is set to 0 before performing the
  /// operation.
  ///
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incr>
  fn incr<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::incr(self, key).await?.convert()
    }
  }

  /// Increments the number stored at `key` by `val`. If the key does not exist, it is set to 0 before performing the
  /// operation.
  ///
  /// Returns an error if the value at key is of the wrong type.
  ///
  /// <https://redis.io/commands/incrby>
  fn incr_by<R, K>(&self, key: K, val: i64) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::incr_by(self, key, val).await?.convert()
    }
  }

  /// Increment the string representing a floating point number stored at key by `val`. If the key does not exist, it
  /// is set to 0 before performing the operation.
  ///
  /// Returns an error if key value is the wrong type or if the current value cannot be parsed as a floating point
  /// value.
  ///
  /// <https://redis.io/commands/incrbyfloat>
  fn incr_by_float<R, K>(&self, key: K, val: f64) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::incr_by_float(self, key, val).await?.convert()
    }
  }

  /// Decrements the number stored at `key` by one. If the key does not exist, it is set to 0 before performing the
  /// operation.
  ///
  /// Returns an error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decr>
  fn decr<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::decr(self, key).await?.convert()
    }
  }

  /// Decrements the number stored at `key` by `val`. If the key does not exist, it is set to 0 before performing the
  /// operation.
  ///
  /// Returns an error if the key contains a value of the wrong type.
  ///
  /// <https://redis.io/commands/decrby>
  fn decr_by<R, K>(&self, key: K, val: i64) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::decr_by(self, key, val).await?.convert()
    }
  }

  /// Returns the remaining time to live of a key that has a timeout, in seconds.
  ///
  /// <https://redis.io/commands/ttl>
  fn ttl<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::ttl(self, key).await?.convert()
    }
  }

  /// Returns the remaining time to live of a key that has a timeout, in milliseconds.
  ///
  /// <https://redis.io/commands/pttl>
  fn pttl<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::pttl(self, key).await?.convert()
    }
  }

  /// Remove the existing timeout on a key, turning the key from volatile (a key with an expiration)
  /// to persistent (a key that will never expire as no timeout is associated).
  ///
  /// Returns a boolean value describing whether the timeout was removed.
  ///
  /// <https://redis.io/commands/persist>
  fn persist<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::persist(self, key).await?.convert()
    }
  }

  /// Set a timeout on key. After the timeout has expired, the key will be automatically deleted.
  ///
  /// <https://redis.io/commands/expire>
  fn expire<R, K>(&self, key: K, seconds: i64) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::expire(self, key, seconds).await?.convert()
    }
  }

  /// Set a timeout on a key based on a UNIX timestamp.
  ///
  /// <https://redis.io/commands/expireat>
  fn expire_at<R, K>(&self, key: K, timestamp: i64) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::expire_at(self, key, timestamp).await?.convert()
    }
  }

  /// This command works exactly like EXPIRE but the time to live of the key is specified in milliseconds instead of
  /// seconds.
  ///
  /// <https://redis.io/docs/latest/commands/pexpire/>
  fn pexpire<R, K>(
    &self,
    key: K,
    milliseconds: i64,
    options: Option<ExpireOptions>,
  ) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::pexpire(self, key, milliseconds, options)
        .await?
        .convert()
    }
  }

  /// PEXPIREAT has the same effect and semantic as EXPIREAT, but the Unix time at which the key will expire is
  /// specified in milliseconds instead of seconds.
  ///
  /// <https://redis.io/docs/latest/commands/pexpireat/>
  fn pexpire_at<R, K>(
    &self,
    key: K,
    timestamp: i64,
    options: Option<ExpireOptions>,
  ) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::keys::pexpire_at(self, key, timestamp, options)
        .await?
        .convert()
    }
  }

  /// Returns number of keys that exist from the `keys` arguments.
  ///
  /// <https://redis.io/commands/exists>
  fn exists<R, K>(&self, keys: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
  {
    async move {
      into!(keys);
      commands::keys::exists(self, keys).await?.convert()
    }
  }

  /// Runs the longest common subsequence algorithm on two keys.
  ///
  /// <https://redis.io/commands/lcs/>
  fn lcs<R, K1, K2>(
    &self,
    key1: K1,
    key2: K2,
    len: bool,
    idx: bool,
    minmatchlen: Option<i64>,
    withmatchlen: bool,
  ) -> impl Future<Output = Result<R, RedisError>> + Send
  where
    R: FromRedis,
    K1: Into<RedisKey> + Send,
    K2: Into<RedisKey> + Send,
  {
    async move {
      into!(key1, key2);
      commands::keys::lcs(self, key1, key2, len, idx, minmatchlen, withmatchlen)
        .await?
        .convert()
    }
  }
}
