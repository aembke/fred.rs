use crate::{
  commands,
  interfaces::ClientLike,
  prelude::{RedisError, RedisKey, RedisResult},
  types::{
    Aggregator,
    DuplicatePolicy,
    Encoding,
    FromRedis,
    GetLabels,
    GetTimestamp,
    GroupBy,
    RangeAggregation,
    RedisMap,
    Timestamp,
  },
};
use bytes_utils::Str;

#[async_trait]
#[cfg_attr(docsrs, doc(cfg(feature = "time-series")))]
pub trait TimeSeriesInterface: ClientLike {
  /// Append a sample to a time series.
  ///
  /// <https://redis.io/commands/ts.add/>
  async fn ts_add<R, K, T, L>(
    &self,
    key: K,
    timestamp: T,
    value: f64,
    retention: Option<u64>,
    encoding: Option<Encoding>,
    chunk_size: Option<u64>,
    on_duplicate: Option<DuplicatePolicy>,
    labels: L,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    T: TryInto<Timestamp> + Send,
    T::Error: Into<RedisError> + Send,
    L: TryInto<RedisMap> + Send,
    L::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(timestamp, labels);
    commands::timeseries::ts_add(
      self,
      key,
      timestamp,
      value,
      retention,
      encoding,
      chunk_size,
      on_duplicate,
      labels,
    )
    .await?
    .convert()
  }

  /// Update the retention, chunk size, duplicate policy, and labels of an existing time series.
  ///
  /// <https://redis.io/commands/ts.alter/>
  async fn ts_alter<R, K, L>(
    &self,
    key: K,
    retention: Option<u64>,
    chunk_size: Option<u64>,
    duplicate_policy: Option<DuplicatePolicy>,
    labels: L,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    L: TryInto<RedisMap> + Send,
    L::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(labels);
    commands::timeseries::ts_alter(self, key, retention, chunk_size, duplicate_policy, labels)
      .await?
      .convert()
  }

  /// Create a new time series.
  ///
  /// <https://redis.io/commands/ts.create/>
  async fn ts_create<R, K, L>(
    &self,
    key: K,
    retention: Option<u64>,
    encoding: Option<Encoding>,
    chunk_size: Option<u64>,
    duplicate_policy: Option<DuplicatePolicy>,
    labels: L,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    L: TryInto<RedisMap> + Send,
    L::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(labels);
    commands::timeseries::ts_create(self, key, retention, encoding, chunk_size, duplicate_policy, labels)
      .await?
      .convert()
  }

  /// Create a compaction rule.
  ///
  /// <https://redis.io/commands/ts.createrule/>
  async fn ts_createrule<R, S, D>(
    &self,
    src: S,
    dest: D,
    aggregation: (Aggregator, u64),
    align_timestamp: Option<u64>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<RedisKey> + Send,
    D: Into<RedisKey> + Send,
  {
    into!(src, dest);
    commands::timeseries::ts_createrule(self, src, dest, aggregation, align_timestamp)
      .await?
      .convert()
  }

  /// Decrease the value of the sample with the maximum existing timestamp, or create a new sample with a value equal
  /// to the value of the sample with the maximum existing timestamp with a given decrement.
  ///
  /// <https://redis.io/commands/ts.decrby/>
  async fn ts_decrby<R, K, L>(
    &self,
    key: K,
    subtrahend: f64,
    timestamp: Option<Timestamp>,
    retention: Option<u64>,
    uncompressed: bool,
    chunk_size: Option<u64>,
    labels: L,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    L: TryInto<RedisMap> + Send,
    L::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(labels);
    commands::timeseries::ts_decrby(
      self,
      key,
      subtrahend,
      timestamp,
      retention,
      uncompressed,
      chunk_size,
      labels,
    )
    .await?
    .convert()
  }

  /// Delete all samples between two timestamps for a given time series.
  ///
  /// <https://redis.io/commands/ts.del/>
  async fn ts_del<R, K>(&self, key: K, from: i64, to: i64) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::timeseries::ts_del(self, key, from, to).await?.convert()
  }

  /// Delete a compaction rule.
  ///
  /// <https://redis.io/commands/ts.deleterule/>
  async fn ts_deleterule<R, S, D>(&self, src: S, dest: D) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<RedisKey> + Send,
    D: Into<RedisKey> + Send,
  {
    into!(src, dest);
    commands::timeseries::ts_deleterule(self, src, dest).await?.convert()
  }

  /// Get the sample with the highest timestamp from a given time series.
  ///
  /// <https://redis.io/commands/ts.get/>
  async fn ts_get<R, K>(&self, key: K, latest: bool) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::timeseries::ts_get(self, key, latest).await?.convert()
  }

  /// Increase the value of the sample with the maximum existing timestamp, or create a new sample with a value equal
  /// to the value of the sample with the maximum existing timestamp with a given increment.
  ///
  /// <https://redis.io/commands/ts.incrby/>
  async fn ts_incrby<R, K, L>(
    &self,
    key: K,
    addend: f64,
    timestamp: Option<Timestamp>,
    retention: Option<u64>,
    uncompressed: bool,
    chunk_size: Option<u64>,
    labels: L,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    L: TryInto<RedisMap> + Send,
    L::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(labels);
    commands::timeseries::ts_incrby(
      self,
      key,
      addend,
      timestamp,
      retention,
      uncompressed,
      chunk_size,
      labels,
    )
    .await?
    .convert()
  }

  /// Return information and statistics for a time series.
  ///
  /// <https://redis.io/commands/ts.info/>
  async fn ts_info<R, K>(&self, key: K, debug: bool) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::timeseries::ts_info(self, key, debug).await?.convert()
  }

  /// Append new samples to one or more time series.
  ///
  /// <https://redis.io/commands/ts.madd/>
  async fn ts_madd<R, K, I>(&self, samples: I) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    I: IntoIterator<Item = (K, Timestamp, f64)> + Send,
  {
    let samples: Vec<_> = samples
      .into_iter()
      .map(|(key, ts, val)| (key.into(), ts, val))
      .collect();

    commands::timeseries::ts_madd(self, samples).await?.convert()
  }

  /// Get the sample with the highest timestamp from each time series matching a specific filter.
  ///
  /// See [Resp2TimeSeriesValues](crate::types::Resp2TimeSeriesValues) and
  /// [Resp3TimeSeriesValues](crate::types::Resp3TimeSeriesValues) for more information.
  ///
  /// <https://redis.io/commands/ts.mget/>
  async fn ts_mget<R, L, S, I>(&self, latest: bool, labels: Option<L>, filters: I) -> RedisResult<R>
  where
    R: FromRedis,
    L: Into<GetLabels> + Send,
    S: Into<Str> + Send,
    I: IntoIterator<Item = S> + Send,
  {
    let labels = labels.map(|l| l.into());
    let filters = filters.into_iter().map(|s| s.into()).collect();

    commands::timeseries::ts_mget(self, latest, labels, filters)
      .await?
      .convert()
  }

  /// Query a range across multiple time series by filters in the forward direction.
  ///
  /// See [Resp2TimeSeriesValues](crate::types::Resp2TimeSeriesValues) and
  /// [Resp3TimeSeriesValues](crate::types::Resp3TimeSeriesValues) for more information.
  ///
  /// <https://redis.io/commands/ts.mrange/>
  async fn ts_mrange<R, F, T, I, S, J>(
    &self,
    from: F,
    to: T,
    latest: bool,
    filter_by_ts: I,
    filter_by_value: Option<(i64, i64)>,
    labels: Option<GetLabels>,
    count: Option<u64>,
    aggregation: Option<RangeAggregation>,
    filters: J,
    group_by: Option<GroupBy>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    F: TryInto<GetTimestamp> + Send,
    F::Error: Into<RedisError> + Send,
    T: TryInto<GetTimestamp> + Send,
    T::Error: Into<RedisError> + Send,
    S: Into<Str> + Send,
    I: IntoIterator<Item = i64> + Send,
    J: IntoIterator<Item = S> + Send,
  {
    try_into!(from, to);
    let filters = filters.into_iter().map(|s| s.into()).collect();
    let filter_by_ts = filter_by_ts.into_iter().collect();

    commands::timeseries::ts_mrange(
      self,
      from,
      to,
      latest,
      filter_by_ts,
      filter_by_value,
      labels,
      count,
      aggregation,
      filters,
      group_by,
    )
    .await?
    .convert()
  }

  /// Query a range across multiple time series by filters in the reverse direction.
  ///
  /// See [Resp2TimeSeriesValues](crate::types::Resp2TimeSeriesValues) and
  /// [Resp3TimeSeriesValues](crate::types::Resp3TimeSeriesValues) for more information.
  ///
  /// <https://redis.io/commands/ts.mrevrange/>
  async fn ts_mrevrange<R, F, T, I, S, J>(
    &self,
    from: F,
    to: T,
    latest: bool,
    filter_by_ts: I,
    filter_by_value: Option<(i64, i64)>,
    labels: Option<GetLabels>,
    count: Option<u64>,
    aggregation: Option<RangeAggregation>,
    filters: J,
    group_by: Option<GroupBy>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    F: TryInto<GetTimestamp> + Send,
    F::Error: Into<RedisError> + Send,
    T: TryInto<GetTimestamp> + Send,
    T::Error: Into<RedisError> + Send,
    S: Into<Str> + Send,
    I: IntoIterator<Item = i64> + Send,
    J: IntoIterator<Item = S> + Send,
  {
    try_into!(from, to);
    let filters = filters.into_iter().map(|s| s.into()).collect();
    let filter_by_ts = filter_by_ts.into_iter().collect();

    commands::timeseries::ts_mrevrange(
      self,
      from,
      to,
      latest,
      filter_by_ts,
      filter_by_value,
      labels,
      count,
      aggregation,
      filters,
      group_by,
    )
    .await?
    .convert()
  }

  /// Get all time series keys matching a filter list.
  ///
  /// <https://redis.io/commands/ts.queryindex/>
  async fn ts_queryindex<R, S, I>(&self, filters: I) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
    I: IntoIterator<Item = S> + Send,
  {
    let filters = filters.into_iter().map(|s| s.into()).collect();
    commands::timeseries::ts_queryindex(self, filters).await?.convert()
  }

  /// Query a range in forward direction.
  ///
  /// <https://redis.io/commands/ts.range/>
  async fn ts_range<R, K, F, T, I>(
    &self,
    key: K,
    from: F,
    to: T,
    latest: bool,
    filter_by_ts: I,
    filter_by_value: Option<(i64, i64)>,
    count: Option<u64>,
    aggregation: Option<RangeAggregation>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: TryInto<GetTimestamp> + Send,
    F::Error: Into<RedisError> + Send,
    T: TryInto<GetTimestamp> + Send,
    T::Error: Into<RedisError> + Send,
    I: IntoIterator<Item = i64> + Send,
  {
    into!(key);
    try_into!(from, to);
    let filter_by_ts = filter_by_ts.into_iter().collect();

    commands::timeseries::ts_range(
      self,
      key,
      from,
      to,
      latest,
      filter_by_ts,
      filter_by_value,
      count,
      aggregation,
    )
    .await?
    .convert()
  }

  /// Query a range in reverse direction.
  ///
  /// <https://redis.io/commands/ts.revrange/>
  async fn ts_revrange<R, K, F, T, I>(
    &self,
    key: K,
    from: F,
    to: T,
    latest: bool,
    filter_by_ts: I,
    filter_by_value: Option<(i64, i64)>,
    count: Option<u64>,
    aggregation: Option<RangeAggregation>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    F: TryInto<GetTimestamp> + Send,
    F::Error: Into<RedisError> + Send,
    T: TryInto<GetTimestamp> + Send,
    T::Error: Into<RedisError> + Send,
    I: IntoIterator<Item = i64> + Send,
  {
    into!(key);
    try_into!(from, to);
    let filter_by_ts = filter_by_ts.into_iter().collect();

    commands::timeseries::ts_revrange(
      self,
      key,
      from,
      to,
      latest,
      filter_by_ts,
      filter_by_value,
      count,
      aggregation,
    )
    .await?
    .convert()
  }
}
