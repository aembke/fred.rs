use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{ExpireOptions, FromRedis, MultipleKeys, RedisKey, RedisMap, RedisValue},
};
use fred_macros::rm_send_if;
use futures::Future;
use std::convert::TryInto;

/// Functions that implement the [hashes](https://redis.io/commands#hashes) interface.
#[rm_send_if(feature = "glommio")]
pub trait HashesInterface: ClientLike + Sized {
  /// Returns all fields and values of the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hgetall>
  fn hgetall<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::hashes::hgetall(self, key).await?.convert()
    }
  }

  /// Removes the specified fields from the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hdel>
  fn hdel<R, K, F>(&self, key: K, fields: F) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<MultipleKeys> + Send,
  {
    async move {
      into!(key, fields);
      commands::hashes::hdel(self, key, fields).await?.convert()
    }
  }

  /// Returns if `field` is an existing field in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hexists>
  fn hexists<R, K, F>(&self, key: K, field: F) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<RedisKey> + Send,
  {
    async move {
      into!(key, field);
      commands::hashes::hexists(self, key, field).await?.convert()
    }
  }

  /// Returns the value associated with `field` in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hget>
  fn hget<R, K, F>(&self, key: K, field: F) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<RedisKey> + Send,
  {
    async move {
      into!(key, field);
      commands::hashes::hget(self, key, field).await?.convert()
    }
  }

  /// Increments the number stored at `field` in the hash stored at `key` by `increment`.
  ///
  /// <https://redis.io/commands/hincrby>
  fn hincrby<R, K, F>(&self, key: K, field: F, increment: i64) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<RedisKey> + Send,
  {
    async move {
      into!(key, field);
      commands::hashes::hincrby(self, key, field, increment).await?.convert()
    }
  }

  /// Increment the specified `field` of a hash stored at `key`, and representing a floating point number, by the
  /// specified `increment`.
  ///
  /// <https://redis.io/commands/hincrbyfloat>
  fn hincrbyfloat<R, K, F>(&self, key: K, field: F, increment: f64) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<RedisKey> + Send,
  {
    async move {
      into!(key, field);
      commands::hashes::hincrbyfloat(self, key, field, increment)
        .await?
        .convert()
    }
  }

  /// Returns all field names in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hkeys>
  fn hkeys<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::hashes::hkeys(self, key).await?.convert()
    }
  }

  /// Returns the number of fields contained in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hlen>
  fn hlen<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::hashes::hlen(self, key).await?.convert()
    }
  }

  /// Returns the values associated with the specified `fields` in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hmget>
  fn hmget<R, K, F>(&self, key: K, fields: F) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<MultipleKeys> + Send,
  {
    async move {
      into!(key, fields);
      commands::hashes::hmget(self, key, fields).await?.convert()
    }
  }

  /// Sets the specified fields to their respective values in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hmset>
  fn hmset<R, K, V>(&self, key: K, values: V) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<RedisMap> + Send,
    V::Error: Into<RedisError> + Send,
  {
    async move {
      into!(key);
      try_into!(values);
      commands::hashes::hmset(self, key, values).await?.convert()
    }
  }

  /// Sets fields in the hash stored at `key` to their provided values.
  ///
  /// <https://redis.io/commands/hset>
  fn hset<R, K, V>(&self, key: K, values: V) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<RedisMap> + Send,
    V::Error: Into<RedisError> + Send,
  {
    async move {
      into!(key);
      try_into!(values);
      commands::hashes::hset(self, key, values).await?.convert()
    }
  }

  /// Sets `field` in the hash stored at `key` to `value`, only if `field` does not yet exist.
  ///
  /// <https://redis.io/commands/hsetnx>
  fn hsetnx<R, K, F, V>(&self, key: K, field: F, value: V) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    async move {
      into!(key, field);
      try_into!(value);
      commands::hashes::hsetnx(self, key, field, value).await?.convert()
    }
  }

  /// When called with just the `key` argument, return a random field from the hash value stored at `key`.
  ///
  /// If the provided `count` argument is positive, return an array of distinct fields.
  ///
  /// <https://redis.io/commands/hrandfield>
  fn hrandfield<R, K>(&self, key: K, count: Option<(i64, bool)>) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::hashes::hrandfield(self, key, count).await?.convert()
    }
  }

  /// Returns the string length of the value associated with `field` in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hstrlen>
  fn hstrlen<R, K, F>(&self, key: K, field: F) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<RedisKey> + Send,
  {
    async move {
      into!(key, field);
      commands::hashes::hstrlen(self, key, field).await?.convert()
    }
  }

  /// Returns all values in the hash stored at `key`.
  ///
  /// <https://redis.io/commands/hvals>
  fn hvals<R, K>(&self, key: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    async move {
      into!(key);
      commands::hashes::hvals(self, key).await?.convert()
    }
  }

  /// Returns the remaining TTL (time to live) of a hash key's field(s) that have a set expiration.
  ///
  /// <https://redis.io/docs/latest/commands/httl/>
  fn httl<R, K, F>(&self, key: K, fields: F) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<MultipleKeys> + Send,
  {
    async move {
      into!(key, fields);
      commands::hashes::httl(self, key, fields).await?.convert()
    }
  }

  /// Set an expiration (TTL or time to live) on one or more fields of a given hash key.
  ///
  /// <https://redis.io/docs/latest/commands/hexpire/>
  fn hexpire<R, K, F>(
    &self,
    key: K,
    seconds: i64,
    options: Option<ExpireOptions>,
    fields: F,
  ) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<MultipleKeys> + Send,
  {
    async move {
      into!(key, fields);
      commands::hashes::hexpire(self, key, seconds, options, fields)
        .await?
        .convert()
    }
  }

  /// HEXPIREAT has the same effect and semantics as HEXPIRE, but instead of specifying the number of seconds for the
  /// TTL (time to live), it takes an absolute Unix timestamp in seconds since Unix epoch.
  ///
  /// <https://redis.io/docs/latest/commands/hexpireat/>
  fn hexpire_at<R, K, F>(
    &self,
    key: K,
    time: i64,
    options: Option<ExpireOptions>,
    fields: F,
  ) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<MultipleKeys> + Send,
  {
    async move {
      into!(key, fields);
      commands::hashes::hexpire_at(self, key, time, options, fields)
        .await?
        .convert()
    }
  }

  /// Returns the absolute Unix timestamp in seconds since Unix epoch at which the given key's field(s) will expire.
  ///
  /// <https://redis.io/docs/latest/commands/hexpiretime/>
  fn hexpire_time<R, K, F>(&self, key: K, fields: F) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<MultipleKeys> + Send,
  {
    async move {
      into!(key, fields);
      commands::hashes::hexpire_time(self, key, fields).await?.convert()
    }
  }

  /// Like HTTL, this command returns the remaining TTL (time to live) of a field that has an expiration set, but in
  /// milliseconds instead of seconds.
  ///
  /// <https://redis.io/docs/latest/commands/hpttl/>
  fn hpttl<R, K, F>(&self, key: K, fields: F) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<MultipleKeys> + Send,
  {
    async move {
      into!(key, fields);
      commands::hashes::hpttl(self, key, fields).await?.convert()
    }
  }

  /// This command works like HEXPIRE, but the expiration of a field is specified in milliseconds instead of seconds.
  ///
  /// <https://redis.io/docs/latest/commands/hpexpire/>
  fn hpexpire<R, K, F>(
    &self,
    key: K,
    milliseconds: i64,
    options: Option<ExpireOptions>,
    fields: F,
  ) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<MultipleKeys> + Send,
  {
    async move {
      into!(key, fields);
      commands::hashes::hpexpire(self, key, milliseconds, options, fields)
        .await?
        .convert()
    }
  }

  /// HPEXPIREAT has the same effect and semantics as HEXPIREAT, but the Unix time at which the field will expire is
  /// specified in milliseconds since Unix epoch instead of seconds.
  ///
  /// <https://redis.io/docs/latest/commands/hpexpireat/>
  fn hpexpire_at<R, K, F>(
    &self,
    key: K,
    time: i64,
    options: Option<ExpireOptions>,
    fields: F,
  ) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<MultipleKeys> + Send,
  {
    async move {
      into!(key, fields);
      commands::hashes::hpexpire_at(self, key, time, options, fields)
        .await?
        .convert()
    }
  }

  /// HPEXPIRETIME has the same semantics as HEXPIRETIME, but returns the absolute Unix expiration timestamp in
  /// milliseconds since Unix epoch instead of seconds.
  ///
  /// <https://redis.io/docs/latest/commands/hpexpiretime/>
  fn hpexpire_time<R, K, F>(&self, key: K, fields: F) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<MultipleKeys> + Send,
  {
    async move {
      into!(key, fields);
      commands::hashes::hpexpire_time(self, key, fields).await?.convert()
    }
  }

  /// Remove the existing expiration on a hash key's field(s), turning the field(s) from volatile (a field with
  /// expiration set) to persistent (a field that will never expire as no TTL (time to live) is associated).
  ///
  /// <https://redis.io/docs/latest/commands/hpersist/>
  fn hpersist<R, K, F>(&self, key: K, fields: F) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: Into<MultipleKeys> + Send,
  {
    async move {
      into!(key, fields);
      commands::hashes::hpersist(self, key, fields).await?.convert()
    }
  }
}
