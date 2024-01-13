use crate::{
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
  async fn ts_add<R, K, T, V, L>(
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
    unimplemented!()
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
    unimplemented!()
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
    unimplemented!()
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
    unimplemented!()
  }

  /// Decrease the value of the sample with the maximum existing timestamp, or create a new sample with a value equal
  /// to the value of the sample with the maximum existing timestamp with a given decrement.
  ///
  /// <https://redis.io/commands/ts.decrby/>
  async fn ts_decrby<R, K, T, L>(
    &self,
    key: K,
    subtrahend: f64,
    timestamp: Option<T>,
    retention: Option<u64>,
    uncompressed: bool,
    chunk_size: Option<u64>,
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
    unimplemented!()
  }

  /// Delete all samples between two timestamps for a given time series.
  ///
  /// <https://redis.io/commands/ts.del/>
  async fn ts_del<R, K>(&self, key: K, from: u64, to: u64) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    unimplemented!()
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
    unimplemented!()
  }

  /// Get the sample with the highest timestamp from a given time series.
  ///
  /// <https://redis.io/commands/ts.get/>
  async fn ts_get<R, K>(&self, key: K, latest: bool) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    unimplemented!()
  }

  /// Increase the value of the sample with the maximum existing timestamp, or create a new sample with a value equal
  /// to the value of the sample with the maximum existing timestamp with a given increment.
  ///
  /// <https://redis.io/commands/ts.incrby/>
  async fn ts_incrby<R, K, T, L>(
    &self,
    key: K,
    addend: f64,
    timestamp: Option<T>,
    retention: Option<u64>,
    uncompressed: bool,
    chunk_size: Option<u64>,
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
    unimplemented!()
  }

  /// Return information and statistics for a time series.
  ///
  /// <https://redis.io/commands/ts.info/>
  async fn ts_info<R, K>(&self, key: K, debug: bool) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
  {
    unimplemented!()
  }

  /// Append new samples to one or more time series.
  ///
  /// <https://redis.io/commands/ts.madd/>
  async fn ts_madd<R, K, T>(&self, samples: Vec<(K, T, f64)>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    T: TryInto<Timestamp> + Send,
    T::Error: Into<RedisError> + Send,
  {
    unimplemented!()
  }

  /// Get the sample with the highest timestamp from each time series matching a specific filter.
  ///
  /// <https://redis.io/commands/ts.mget/>
  async fn ts_mget<R, L, S>(&self, latest: bool, labels: Option<L>, filters: Vec<S>) -> RedisResult<R>
  where
    R: FromRedis,
    L: Into<GetLabels> + Send,
    S: Into<Str> + Send,
  {
    unimplemented!()
  }

  /// Query a range across multiple time series by filters in the forward direction.
  ///
  /// <https://redis.io/commands/ts.mrange/>
  async fn ts_mrange<R, F, T, L, S, G>(
    &self,
    from: F,
    to: T,
    latest: bool,
    filter_by_ts: Vec<u64>,
    filter_by_value: Option<(u64, u64)>,
    labels: Option<L>,
    count: Option<u64>,
    aggregation: Option<RangeAggregation>,
    filters: Vec<S>,
    group_by: Option<G>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    F: TryInto<GetTimestamp> + Send,
    F::Error: Into<RedisError> + Send,
    T: TryInto<GetTimestamp> + Send,
    T::Error: Into<RedisError> + Send,
    L: Into<GetLabels> + Send,
    S: Into<Str> + Send,
    G: Into<GroupBy> + Send,
  {
    unimplemented!()
  }

  /// Query a range across multiple time series by filters in the reverse direction.
  ///
  /// <https://redis.io/commands/ts.mrevrange/>
  async fn ts_mrevrange<R, F, T, L, S, G>(
    &self,
    from: F,
    to: T,
    latest: bool,
    filter_by_ts: Vec<u64>,
    filter_by_value: Option<(u64, u64)>,
    labels: Option<L>,
    count: Option<u64>,
    aggregation: Option<RangeAggregation>,
    filters: Vec<S>,
    group_by: Option<G>,
  ) -> RedisResult<R>
  where
    R: FromRedis,
    F: TryInto<GetTimestamp> + Send,
    F::Error: Into<RedisError> + Send,
    T: TryInto<GetTimestamp> + Send,
    T::Error: Into<RedisError> + Send,
    L: Into<GetLabels> + Send,
    S: Into<Str> + Send,
    G: Into<GroupBy> + Send,
  {
    unimplemented!()
  }

  /// Get all time series keys matching a filter list.
  ///
  /// <https://redis.io/commands/ts.queryindex/>
  async fn ts_queryindex<R, S>(&self, filters: Vec<S>) -> RedisResult<R> {
    unimplemented!()
  }

  /// Query a range in forward direction.
  ///
  /// <https://redis.io/commands/ts.range/>
  async fn ts_range<R, K, F, T>(
    &self,
    key: K,
    from: F,
    to: T,
    latest: bool,
    filter_by_ts: Vec<u64>,
    filter_by_value: Option<(u64, u64)>,
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
  {
    unimplemented!()
  }

  /// Query a range in reverse direction.
  ///
  /// <https://redis.io/commands/ts.revrange/>
  async fn ts_revrange<R, K, F, T>(
    &self,
    key: K,
    from: F,
    to: T,
    latest: bool,
    filter_by_ts: Vec<u64>,
    filter_by_value: Option<(u64, u64)>,
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
  {
    unimplemented!()
  }
}
