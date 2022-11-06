use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{
    AggregateOptions,
    FromRedis,
    Limit,
    MultipleKeys,
    MultipleValues,
    MultipleWeights,
    MultipleZaddValues,
    Ordering,
    RedisKey,
    RedisValue,
    SetOptions,
    ZRange,
    ZSort,
  },
};
use std::convert::TryInto;

/// Functions that implement the [Sorted Sets](https://redis.io/commands#sorted_set) interface.
#[async_trait]
pub trait SortedSetsInterface: ClientLike + Sized {
  /// The blocking variant of the ZPOPMIN command.
  ///
  /// <https://redis.io/commands/bzpopmin>
  async fn bzpopmin<R, K>(&self, keys: K, timeout: f64) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
  {
    into!(keys);
    commands::sorted_sets::bzpopmin(self, keys, timeout).await?.convert()
  }

  /// The blocking variant of the ZPOPMAX command.
  ///
  /// <https://redis.io/commands/bzpopmax>
  async fn bzpopmax<R, K>(&self, keys: K, timeout: f64) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
  {
    into!(keys);
    commands::sorted_sets::bzpopmax(self, keys, timeout).await?.convert()
  }

  /// Adds all the specified members with the specified scores to the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zadd>
  async fn zadd<R, K, V>(
    &self,
    key: K,
    options: Option<SetOptions>,
    ordering: Option<Ordering>,
    changed: bool,
    incr: bool,
    values: V,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<MultipleZaddValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(values);
    commands::sorted_sets::zadd(self, key, options, ordering, changed, incr, values)
      .await?
      .convert()
  }

  /// Returns the sorted set cardinality (number of elements) of the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zcard>
  async fn zcard<R, K>(&self, key: K) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::sorted_sets::zcard(self, key).await?.convert()
  }

  /// Returns the number of elements in the sorted set at `key` with a score between `min` and `max`.
  ///
  /// <https://redis.io/commands/zcount>
  async fn zcount<R, K>(&self, key: K, min: f64, max: f64) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::sorted_sets::zcount(self, key, min, max).await?.convert()
  }

  /// This command is similar to ZDIFFSTORE, but instead of storing the resulting sorted set, it is returned to the
  /// client.
  ///
  /// <https://redis.io/commands/zdiff>
  async fn zdiff<R, K>(&self, keys: K, withscores: bool) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
  {
    into!(keys);
    commands::sorted_sets::zdiff(self, keys, withscores).await?.convert()
  }

  /// Computes the difference between the first and all successive input sorted sets and stores the result in
  /// `destination`.
  ///
  /// <https://redis.io/commands/zdiffstore>
  async fn zdiffstore<R, D, K>(&self, dest: D, keys: K) -> RedisResult<R>
  where
    R: FromRedis,
    D: Into<RedisKey> + Send,
    K: Into<MultipleKeys> + Send,
  {
    into!(dest, keys);
    commands::sorted_sets::zdiffstore(self, dest, keys).await?.convert()
  }

  /// Increments the score of `member` in the sorted set stored at `key` by `increment`.
  ///
  /// <https://redis.io/commands/zincrby>
  async fn zincrby<R, K, V>(&self, key: K, increment: f64, member: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(member);
    commands::sorted_sets::zincrby(self, key, increment, member)
      .await?
      .convert()
  }

  /// This command is similar to ZINTERSTORE, but instead of storing the resulting sorted set, it is returned to the
  /// client.
  ///
  /// <https://redis.io/commands/zinter>
  async fn zinter<R, K, W>(
    &self,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
    withscores: bool,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
    W: Into<MultipleWeights> + Send,
  {
    into!(keys, weights);
    commands::sorted_sets::zinter(self, keys, weights, aggregate, withscores)
      .await?
      .convert()
  }

  /// Computes the intersection of the sorted sets given by the specified keys, and stores the result in
  /// `destination`.
  ///
  /// <https://redis.io/commands/zinterstore>
  async fn zinterstore<R, D, K, W>(
    &self,
    dest: D,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    D: Into<RedisKey> + Send,
    K: Into<MultipleKeys> + Send,
    W: Into<MultipleWeights> + Send,
  {
    into!(dest, keys, weights);
    commands::sorted_sets::zinterstore(self, dest, keys, weights, aggregate)
      .await?
      .convert()
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
  /// ordering, this command returns the number of elements in the sorted set at key with a value between min and
  /// max.
  ///
  /// <https://redis.io/commands/zlexcount>
  async fn zlexcount<R, K, M, N>(&self, key: K, min: M, max: N) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    M: TryInto<ZRange> + Send,
    M::Error: Into<RedisError> + Send,
    N: TryInto<ZRange> + Send,
    N::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(min, max);
    commands::sorted_sets::zlexcount(self, key, min, max).await?.convert()
  }

  /// Removes and returns up to count members with the highest scores in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zpopmax>
  async fn zpopmax<R, K>(&self, key: K, count: Option<usize>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::sorted_sets::zpopmax(self, key, count).await?.convert()
  }

  /// Removes and returns up to count members with the lowest scores in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zpopmin>
  async fn zpopmin<R, K>(&self, key: K, count: Option<usize>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::sorted_sets::zpopmin(self, key, count).await?.convert()
  }

  /// When called with just the key argument, return a random element from the sorted set value stored at `key`.
  ///
  /// <https://redis.io/commands/zrandmember>
  async fn zrandmember<R, K>(&self, key: K, count: Option<(i64, bool)>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::sorted_sets::zrandmember(self, key, count).await?.convert()
  }

  /// This command is like ZRANGE, but stores the result in the `destination` key.
  ///
  /// <https://redis.io/commands/zrangestore>
  async fn zrangestore<R, D, S, M, N>(
    &self,
    dest: D,
    source: S,
    min: M,
    max: N,
    sort: Option<ZSort>,
    rev: bool,
    limit: Option<Limit>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    D: Into<RedisKey> + Send,
    S: Into<RedisKey> + Send,
    M: TryInto<ZRange> + Send,
    M::Error: Into<RedisError> + Send,
    N: TryInto<ZRange> + Send,
    N::Error: Into<RedisError> + Send,
  {
    into!(dest, source);
    try_into!(min, max);
    commands::sorted_sets::zrangestore(self, dest, source, min, max, sort, rev, limit)
      .await?
      .convert()
  }

  /// Returns the specified range of elements in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zrange>
  async fn zrange<R, K, M, N>(
    &self,
    key: K,
    min: M,
    max: N,
    sort: Option<ZSort>,
    rev: bool,
    limit: Option<Limit>,
    withscores: bool,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    M: TryInto<ZRange> + Send,
    M::Error: Into<RedisError> + Send,
    N: TryInto<ZRange> + Send,
    N::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(min, max);
    commands::sorted_sets::zrange(self, key, min, max, sort, rev, limit, withscores)
      .await?
      .convert()
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
  /// ordering, this command returns all the elements in the sorted set at `key` with a value between `min` and `max`.
  ///
  /// <https://redis.io/commands/zrangebylex>
  async fn zrangebylex<R, K, M, N>(&self, key: K, min: M, max: N, limit: Option<Limit>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    M: TryInto<ZRange> + Send,
    M::Error: Into<RedisError> + Send,
    N: TryInto<ZRange> + Send,
    N::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(min, max);
    commands::sorted_sets::zrangebylex(self, key, min, max, limit)
      .await?
      .convert()
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
  /// ordering, this command returns all the elements in the sorted set at `key` with a value between `max` and `min`.
  ///
  /// <https://redis.io/commands/zrevrangebylex>
  async fn zrevrangebylex<R, K, M, N>(&self, key: K, max: M, min: N, limit: Option<Limit>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    M: TryInto<ZRange> + Send,
    M::Error: Into<RedisError> + Send,
    N: TryInto<ZRange> + Send,
    N::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(max, min);
    commands::sorted_sets::zrevrangebylex(self, key, max, min, limit)
      .await?
      .convert()
  }

  /// Returns all the elements in the sorted set at key with a score between `min` and `max` (including elements
  /// with score equal to `min` or `max`).
  ///
  /// <https://redis.io/commands/zrangebyscore>
  async fn zrangebyscore<R, K, M, N>(
    &self,
    key: K,
    min: M,
    max: N,
    withscores: bool,
    limit: Option<Limit>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    M: TryInto<ZRange> + Send,
    M::Error: Into<RedisError> + Send,
    N: TryInto<ZRange> + Send,
    N::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(min, max);
    commands::sorted_sets::zrangebyscore(self, key, min, max, withscores, limit)
      .await?
      .convert()
  }

  /// Returns all the elements in the sorted set at `key` with a score between `max` and `min` (including
  /// elements with score equal to `max` or `min`).
  ///
  /// <https://redis.io/commands/zrevrangebyscore>
  async fn zrevrangebyscore<R, K, M, N>(
    &self,
    key: K,
    max: M,
    min: N,
    withscores: bool,
    limit: Option<Limit>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    M: TryInto<ZRange> + Send,
    M::Error: Into<RedisError> + Send,
    N: TryInto<ZRange> + Send,
    N::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(max, min);
    commands::sorted_sets::zrevrangebyscore(self, key, max, min, withscores, limit)
      .await?
      .convert()
  }

  /// Returns the rank of member in the sorted set stored at `key`, with the scores ordered from low to high.
  ///
  /// <https://redis.io/commands/zrank>
  async fn zrank<R, K, V>(&self, key: K, member: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(member);
    commands::sorted_sets::zrank(self, key, member).await?.convert()
  }

  /// Removes the specified members from the sorted set stored at `key`. Non existing members are ignored.
  ///
  /// <https://redis.io/commands/zrem>
  async fn zrem<R, K, V>(&self, key: K, members: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(members);
    commands::sorted_sets::zrem(self, key, members).await?.convert()
  }

  /// When all the elements in a sorted set are inserted with the same score, in order to force lexicographical
  /// ordering, this command removes all elements in the sorted set stored at `key` between the lexicographical range
  /// specified by `min` and `max`.
  ///
  /// <https://redis.io/commands/zremrangebylex>
  async fn zremrangebylex<R, K, M, N>(&self, key: K, min: M, max: N) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    M: TryInto<ZRange> + Send,
    M::Error: Into<RedisError> + Send,
    N: TryInto<ZRange> + Send,
    N::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(min, max);
    commands::sorted_sets::zremrangebylex(self, key, min, max)
      .await?
      .convert()
  }

  /// Removes all elements in the sorted set stored at `key` with rank between `start` and `stop`.
  ///
  /// <https://redis.io/commands/zremrangebyrank>
  async fn zremrangebyrank<R, K>(&self, key: K, start: i64, stop: i64) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::sorted_sets::zremrangebyrank(self, key, start, stop)
      .await?
      .convert()
  }

  /// Removes all elements in the sorted set stored at `key` with a score between `min` and `max`.
  ///
  /// <https://redis.io/commands/zremrangebyscore>
  async fn zremrangebyscore<R, K, M, N>(&self, key: K, min: M, max: N) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    M: TryInto<ZRange> + Send,
    M::Error: Into<RedisError> + Send,
    N: TryInto<ZRange> + Send,
    N::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(min, max);
    commands::sorted_sets::zremrangebyscore(self, key, min, max)
      .await?
      .convert()
  }

  /// Returns the specified range of elements in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zrevrange>
  async fn zrevrange<R, K>(&self, key: K, start: i64, stop: i64, withscores: bool) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::sorted_sets::zrevrange(self, key, start, stop, withscores)
      .await?
      .convert()
  }

  /// Returns the rank of `member` in the sorted set stored at `key`, with the scores ordered from high to low.
  ///
  /// <https://redis.io/commands/zrevrank>
  async fn zrevrank<R, K, V>(&self, key: K, member: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(member);
    commands::sorted_sets::zrevrank(self, key, member).await?.convert()
  }

  /// Returns the score of `member` in the sorted set at `key`.
  ///
  /// <https://redis.io/commands/zscore>
  async fn zscore<R, K, V>(&self, key: K, member: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(member);
    commands::sorted_sets::zscore(self, key, member).await?.convert()
  }

  /// This command is similar to ZUNIONSTORE, but instead of storing the resulting sorted set, it is returned to the
  /// client.
  ///
  /// <https://redis.io/commands/zunion>
  async fn zunion<K, W>(
    &self,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
    withscores: bool,
  ) -> RedisResult<RedisValue>
  where
    K: Into<MultipleKeys> + Send,
    W: Into<MultipleWeights> + Send,
  {
    into!(keys, weights);
    commands::sorted_sets::zunion(self, keys, weights, aggregate, withscores).await
  }

  /// Computes the union of the sorted sets given by the specified keys, and stores the result in `destination`.
  ///
  /// <https://redis.io/commands/zunionstore>
  async fn zunionstore<R, D, K, W>(
    &self,
    dest: D,
    keys: K,
    weights: W,
    aggregate: Option<AggregateOptions>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    D: Into<RedisKey> + Send,
    K: Into<MultipleKeys> + Send,
    W: Into<MultipleWeights> + Send,
  {
    into!(dest, keys, weights);
    commands::sorted_sets::zunionstore(self, dest, keys, weights, aggregate)
      .await?
      .convert()
  }

  /// Returns the scores associated with the specified members in the sorted set stored at `key`.
  ///
  /// <https://redis.io/commands/zmscore>
  async fn zmscore<R, K, V>(&self, key: K, members: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(members);
    commands::sorted_sets::zmscore(self, key, members).await?.convert()
  }
}
