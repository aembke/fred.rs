use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{
  AggregateOptions, FromRedis, Limit, MultipleKeys, MultipleValues, MultipleWeights, MultipleZaddValues, Ordering,
  RedisKey, RedisValue, SetOptions, ZRange, ZSort,
};
use std::convert::TryInto;

/// Functions that implement the [Sorted Sets](https://redis.io/commands#sorted_set) interface.
pub trait SortedSetsInterface: ClientLike + Sized {
  /// The blocking variant of the ZPOPMIN command.
  ///
  /// <https://redis.io/commands/bzpopmin>
  fn bzpopmin<K>(&self, keys: K, timeout: f64) -> AsyncResult<Option<(RedisKey, RedisValue, f64)>>
  where
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::bzpopmin(&inner, keys, timeout).await
    })
  }

  /// The blocking variant of the ZPOPMAX command.
  ///
  /// <https://redis.io/commands/bzpopmax>
  fn bzpopmax<K>(&self, keys: K, timeout: f64) -> AsyncResult<Option<(RedisKey, RedisValue, f64)>>
  where
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::bzpopmax(&inner, keys, timeout).await
    })
  }

  /// Adds all the specified members with the specified scores to the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zadd>
  fn zadd<R, K, V>(
    &self,
    key: K,
    options: Option<SetOptions>,
    ordering: Option<Ordering>,
    changed: bool,
    incr: bool,
    values: V,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleZaddValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(values);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zadd(&inner, key, options, ordering, changed, incr, values)
        .await?
        .convert()
    })
  }

  /// Returns the sorted set cardinality (number of elements) of the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zcard>
  fn zcard<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zcard(&inner, key).await?.convert()
    })
  }

  /// Returns the number of elements in the sorted set at `key` with a score between `min` and `max`.
  ///
  /// <https://redis.io/commands/zcount>
  fn zcount<R, K>(&self, key: K, min: f64, max: f64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zcount(&inner, key, min, max).await?.convert()
    })
  }

  /// This command is similar to ZDIFFSTORE, but instead of storing the resulting sorted set, it is returned to the client.
  ///
  /// <https://redis.io/commands/zdiff>
  fn zdiff<R, K>(&self, keys: K, withscores: bool) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zdiff(&inner, keys, withscores).await?.convert()
    })
  }

  /// Computes the difference between the first and all successive input sorted sets and stores the result in `destination`.
  ///
  /// <https://redis.io/commands/zdiffstore>
  fn zdiffstore<R, D, K>(&self, dest: D, keys: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
  {
    into!(dest, keys);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zdiffstore(&inner, dest, keys).await?.convert()
    })
  }

  /// Increments the score of `member` in the sorted set stored at `key` by `increment`.
  ///
  /// <https://redis.io/commands/zincrby>
  fn zincrby<R, K, V>(&self, key: K, increment: f64, member: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(member);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zincrby(&inner, key, increment, member)
        .await?
        .convert()
    })
  }

  /// This command is similar to ZINTERSTORE, but instead of storing the resulting sorted set, it is returned to the client.
  ///
  /// <https://redis.io/commands/zinter>
  fn zinter<R, K, W>(
    &self,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
    withscores: bool,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
    W: Into<MultipleWeights>,
  {
    into!(keys, weights);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zinter(&inner, keys, weights, aggregate, withscores)
        .await?
        .convert()
    })
  }

  /// Computes the intersection of the sorted sets given by the specified keys, and stores the result in `destination`.
  ///
  /// <https://redis.io/commands/zinterstore>
  fn zinterstore<R, D, K, W>(
    &self,
    dest: D,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
    W: Into<MultipleWeights>,
  {
    into!(dest, keys, weights);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zinterstore(&inner, dest, keys, weights, aggregate)
        .await?
        .convert()
    })
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical ordering,
  /// this command returns the number of elements in the sorted set at key with a value between min and max.
  ///
  /// <https://redis.io/commands/zlexcount>
  fn zlexcount<R, K, M, N>(&self, key: K, min: M, max: N) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(min, max);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zlexcount(&inner, key, min, max).await?.convert()
    })
  }

  /// Removes and returns up to count members with the highest scores in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zpopmax>
  fn zpopmax<R, K>(&self, key: K, count: Option<usize>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zpopmax(&inner, key, count).await?.convert()
    })
  }

  /// Removes and returns up to count members with the lowest scores in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zpopmin>
  fn zpopmin<R, K>(&self, key: K, count: Option<usize>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zpopmin(&inner, key, count).await?.convert()
    })
  }

  /// When called with just the key argument, return a random element from the sorted set value stored at `key`.
  ///
  /// <https://redis.io/commands/zrandmember>
  fn zrandmember<R, K>(&self, key: K, count: Option<(i64, bool)>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zrandmember(&inner, key, count).await?.convert()
    })
  }

  /// This command is like ZRANGE, but stores the result in the `destination` key.
  ///
  /// <https://redis.io/commands/zrangestore>
  fn zrangestore<R, D, S, M, N>(
    &self,
    dest: D,
    source: S,
    min: M,
    max: N,
    sort: Option<ZSort>,
    rev: bool,
    limit: Option<Limit>,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    D: Into<RedisKey>,
    S: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    into!(dest, source);
    try_into!(min, max);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zrangestore(&inner, dest, source, min, max, sort, rev, limit)
        .await?
        .convert()
    })
  }

  /// Returns the specified range of elements in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zrange>
  fn zrange<R, K, M, N>(
    &self,
    key: K,
    min: M,
    max: N,
    sort: Option<ZSort>,
    rev: bool,
    limit: Option<Limit>,
    withscores: bool,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(min, max);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zrange(&inner, key, min, max, sort, rev, limit, withscores)
        .await?
        .convert()
    })
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
  /// ordering, this command returns all the elements in the sorted set at `key` with a value between `min` and `max`.
  ///
  /// <https://redis.io/commands/zrangebylex>
  fn zrangebylex<R, K, M, N>(&self, key: K, min: M, max: N, limit: Option<Limit>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(min, max);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zrangebylex(&inner, key, min, max, limit)
        .await?
        .convert()
    })
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
  /// ordering, this command returns all the elements in the sorted set at `key` with a value between `max` and `min`.
  ///
  /// <https://redis.io/commands/zrevrangebylex>
  fn zrevrangebylex<R, K, M, N>(&self, key: K, max: M, min: N, limit: Option<Limit>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(max, min);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zrevrangebylex(&inner, key, max, min, limit)
        .await?
        .convert()
    })
  }

  /// Returns all the elements in the sorted set at key with a score between `min` and `max` (including elements
  /// with score equal to `min` or `max`).
  ///
  /// <https://redis.io/commands/zrangebyscore>
  fn zrangebyscore<R, K, M, N>(
    &self,
    key: K,
    min: M,
    max: N,
    withscores: bool,
    limit: Option<Limit>,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(min, max);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zrangebyscore(&inner, key, min, max, withscores, limit)
        .await?
        .convert()
    })
  }

  /// Returns all the elements in the sorted set at `key` with a score between `max` and `min` (including
  /// elements with score equal to `max` or `min`).
  ///
  /// <https://redis.io/commands/zrevrangebyscore>
  fn zrevrangebyscore<R, K, M, N>(
    &self,
    key: K,
    max: M,
    min: N,
    withscores: bool,
    limit: Option<Limit>,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(max, min);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zrevrangebyscore(&inner, key, max, min, withscores, limit)
        .await?
        .convert()
    })
  }

  /// Returns the rank of member in the sorted set stored at `key`, with the scores ordered from low to high.
  ///
  /// <https://redis.io/commands/zrank>
  fn zrank<R, K, V>(&self, key: K, member: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(member);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zrank(&inner, key, member).await?.convert()
    })
  }

  /// Removes the specified members from the sorted set stored at `key`. Non existing members are ignored.
  ///
  /// <https://redis.io/commands/zrem>
  fn zrem<R, K, V>(&self, key: K, members: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(members);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zrem(&inner, key, members).await?.convert()
    })
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
  /// ordering, this command removes all elements in the sorted set stored at `key` between the lexicographical range
  /// specified by `min` and `max`.
  ///
  /// <https://redis.io/commands/zremrangebylex>
  fn zremrangebylex<R, K, M, N>(&self, key: K, min: M, max: N) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(min, max);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zremrangebylex(&inner, key, min, max)
        .await?
        .convert()
    })
  }

  /// Removes all elements in the sorted set stored at `key` with rank between `start` and `stop`.
  ///
  /// <https://redis.io/commands/zremrangebyrank>
  fn zremrangebyrank<R, K>(&self, key: K, start: i64, stop: i64) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zremrangebyrank(&inner, key, start, stop)
        .await?
        .convert()
    })
  }

  /// Removes all elements in the sorted set stored at `key` with a score between `min` and `max`.
  ///
  /// <https://redis.io/commands/zremrangebyscore>
  fn zremrangebyscore<R, K, M, N>(&self, key: K, min: M, max: N) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    M: TryInto<ZRange>,
    M::Error: Into<RedisError>,
    N: TryInto<ZRange>,
    N::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(min, max);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zremrangebyscore(&inner, key, min, max)
        .await?
        .convert()
    })
  }

  /// Returns the specified range of elements in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zrevrange>
  fn zrevrange<R, K>(&self, key: K, start: i64, stop: i64, withscores: bool) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zrevrange(&inner, key, start, stop, withscores)
        .await?
        .convert()
    })
  }

  /// Returns the rank of `member` in the sorted set stored at `key`, with the scores ordered from high to low.
  ///
  /// <https://redis.io/commands/zrevrank>
  fn zrevrank<R, K, V>(&self, key: K, member: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(member);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zrevrank(&inner, key, member).await?.convert()
    })
  }

  /// Returns the score of `member` in the sorted set at `key`.
  ///
  /// <https://redis.io/commands/zscore>
  fn zscore<R, K, V>(&self, key: K, member: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(member);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zscore(&inner, key, member).await?.convert()
    })
  }

  /// This command is similar to ZUNIONSTORE, but instead of storing the resulting sorted set, it is returned to the client.
  ///
  /// <https://redis.io/commands/zunion>
  fn zunion<K, W>(
    &self,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
    withscores: bool,
  ) -> AsyncResult<RedisValue>
  where
    K: Into<MultipleKeys>,
    W: Into<MultipleWeights>,
  {
    into!(keys, weights);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zunion(&inner, keys, weights, aggregate, withscores).await
    })
  }

  /// Computes the union of the sorted sets given by the specified keys, and stores the result in `destination`.
  ///
  /// <https://redis.io/commands/zunionstore>
  fn zunionstore<R, D, K, W>(
    &self,
    dest: D,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    D: Into<RedisKey>,
    K: Into<MultipleKeys>,
    W: Into<MultipleWeights>,
  {
    into!(dest, keys, weights);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zunionstore(&inner, dest, keys, weights, aggregate)
        .await?
        .convert()
    })
  }

  /// Returns the scores associated with the specified members in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zmscore>
  fn zmscore<R, K, V>(&self, key: K, members: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(members);
    async_spawn(self, |inner| async move {
      commands::sorted_sets::zmscore(&inner, key, members).await?.convert()
    })
  }
}
