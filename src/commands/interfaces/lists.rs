use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{FromRedis, LMoveDirection, ListLocation, MultipleKeys, MultipleValues, RedisKey, RedisValue};
use std::convert::TryInto;

/// Functions that implement the [Lists](https://redis.io/commands#lists) interface.
pub trait ListInterface: ClientLike + Sized {
  /// BLPOP is a blocking list pop primitive. It is the blocking version of LPOP because it blocks the connection when there are no elements to pop from
  /// any of the given lists. An element is popped from the head of the first list that is non-empty, with the given keys being checked in the order that they are given.
  ///
  /// <https://redis.io/commands/blpop>
  fn blpop<R, K>(&self, keys: K, timeout: f64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |_self| async move {
      commands::lists::blpop(_self, keys, timeout).await?.convert()
    })
  }

  /// BRPOP is a blocking list pop primitive. It is the blocking version of RPOP because it blocks the connection when there are no elements to pop from any of the
  /// given lists. An element is popped from the tail of the first list that is non-empty, with the given keys being checked in the order that they are given.
  ///
  /// <https://redis.io/commands/brpop>
  fn brpop<R, K>(&self, keys: K, timeout: f64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |_self| async move {
      commands::lists::brpop(_self, keys, timeout).await?.convert()
    })
  }

  /// The blocking equivalent of [Self::rpoplpush].
  ///
  /// <https://redis.io/commands/brpoplpush>
  fn brpoplpush<R, S, D>(&self, source: S, destination: D, timeout: f64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
  {
    into!(source, destination);
    async_spawn(self, |_self| async move {
      commands::lists::brpoplpush(_self, source, destination, timeout)
        .await?
        .convert()
    })
  }

  /// The blocking equivalent of [Self::lmove].
  ///
  /// <https://redis.io/commands/blmove>
  fn blmove<R, S, D>(
    &self,
    source: S,
    destination: D,
    source_direction: LMoveDirection,
    destination_direction: LMoveDirection,
    timeout: f64,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
  {
    into!(source, destination);
    async_spawn(self, |_self| async move {
      commands::lists::blmove(
        _self,
        source,
        destination,
        source_direction,
        destination_direction,
        timeout,
      )
      .await?
      .convert()
    })
  }

  /// Returns the element at index index in the list stored at key.
  ///
  /// <https://redis.io/commands/lindex>
  fn lindex<R, K>(&self, key: K, index: i64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |_self| async move {
      commands::lists::lindex(_self, key, index).await?.convert()
    })
  }

  /// Inserts element in the list stored at key either before or after the reference value `pivot`.
  ///
  /// <https://redis.io/commands/linsert>
  fn linsert<R, K, P, V>(&self, key: K, location: ListLocation, pivot: P, element: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    P: TryInto<RedisValue>,
    P::Error: Into<RedisError>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(pivot, element);
    async_spawn(self, |_self| async move {
      commands::lists::linsert(_self, key, location, pivot, element)
        .await?
        .convert()
    })
  }

  /// Returns the length of the list stored at key.
  ///
  /// <https://redis.io/commands/llen>
  fn llen<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |_self| async move {
      commands::lists::llen(_self, key).await?.convert()
    })
  }

  /// Removes and returns the first elements of the list stored at key.
  ///
  /// <https://redis.io/commands/lpop>
  fn lpop<R, K>(&self, key: K, count: Option<usize>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |_self| async move {
      commands::lists::lpop(_self, key, count).await?.convert()
    })
  }

  /// The command returns the index of matching elements inside a Redis list.
  ///
  /// <https://redis.io/commands/lpos>
  fn lpos<R, K, V>(
    &self,
    key: K,
    element: V,
    rank: Option<i64>,
    count: Option<i64>,
    maxlen: Option<i64>,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(element);
    async_spawn(self, |_self| async move {
      commands::lists::lpos(_self, key, element, rank, count, maxlen)
        .await?
        .convert()
    })
  }

  /// Insert all the specified values at the head of the list stored at `key`.
  ///
  /// <https://redis.io/commands/lpush>
  fn lpush<R, K, V>(&self, key: K, elements: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(elements);
    async_spawn(self, |_self| async move {
      commands::lists::lpush(_self, key, elements).await?.convert()
    })
  }

  /// Inserts specified values at the head of the list stored at `key`, only if `key` already exists and holds a list.
  ///
  /// <https://redis.io/commands/lpushx>
  fn lpushx<R, K, V>(&self, key: K, elements: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(elements);
    async_spawn(self, |_self| async move {
      commands::lists::lpushx(_self, key, elements).await?.convert()
    })
  }

  /// Returns the specified elements of the list stored at `key`.
  ///
  /// <https://redis.io/commands/lrange>
  fn lrange<R, K>(&self, key: K, start: i64, stop: i64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |_self| async move {
      commands::lists::lrange(_self, key, start, stop).await?.convert()
    })
  }

  /// Removes the first `count` occurrences of elements equal to `element` from the list stored at `key`.
  ///
  /// <https://redis.io/commands/lrem>
  fn lrem<R, K, V>(&self, key: K, count: i64, element: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(element);
    async_spawn(self, |_self| async move {
      commands::lists::lrem(_self, key, count, element).await?.convert()
    })
  }

  /// Sets the list element at `index` to `element`.
  ///
  /// <https://redis.io/commands/lset>
  fn lset<R, K, V>(&self, key: K, index: i64, element: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(element);
    async_spawn(self, |_self| async move {
      commands::lists::lset(_self, key, index, element).await?.convert()
    })
  }

  /// Trim an existing list so that it will contain only the specified range of elements specified.
  ///
  /// <https://redis.io/commands/ltrim>
  fn ltrim<R, K>(&self, key: K, start: i64, stop: i64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |_self| async move {
      commands::lists::ltrim(_self, key, start, stop).await?.convert()
    })
  }

  /// Removes and returns the last elements of the list stored at `key`.
  ///
  /// <https://redis.io/commands/rpop>
  fn rpop<R, K>(&self, key: K, count: Option<usize>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |_self| async move {
      commands::lists::rpop(_self, key, count).await?.convert()
    })
  }

  /// Atomically returns and removes the last element (tail) of the list stored at `source`, and pushes the element at the first element (head) of the list stored at `destination`.
  ///
  /// <https://redis.io/commands/rpoplpush>
  fn rpoplpush<R, S, D>(&self, source: S, dest: D) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
  {
    into!(source, dest);
    async_spawn(self, |_self| async move {
      commands::lists::rpoplpush(_self, source, dest).await?.convert()
    })
  }

  /// Atomically returns and removes the first/last element (head/tail depending on the source direction argument) of the list stored at `source`, and pushes
  /// the element at the first/last element (head/tail depending on the destination direction argument) of the list stored at `destination`.
  ///
  /// <https://redis.io/commands/lmove>
  fn lmove<R, S, D>(
    &self,
    source: S,
    dest: D,
    source_direction: LMoveDirection,
    dest_direction: LMoveDirection,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<RedisKey>,
    D: Into<RedisKey>,
  {
    into!(source, dest);
    async_spawn(self, |_self| async move {
      commands::lists::lmove(_self, source, dest, source_direction, dest_direction)
        .await?
        .convert()
    })
  }

  /// Insert all the specified values at the tail of the list stored at `key`.
  ///
  /// <https://redis.io/commands/rpush>
  fn rpush<R, K, V>(&self, key: K, elements: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(elements);
    async_spawn(self, |_self| async move {
      commands::lists::rpush(_self, key, elements).await?.convert()
    })
  }

  /// Inserts specified values at the tail of the list stored at `key`, only if key already exists and holds a list.
  ///
  /// <https://redis.io/commands/rpushx>
  fn rpushx<R, K, V>(&self, key: K, elements: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(elements);
    async_spawn(self, |_self| async move {
      commands::lists::rpushx(_self, key, elements).await?.convert()
    })
  }
}
