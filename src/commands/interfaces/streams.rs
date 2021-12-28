use crate::commands;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::prelude::RedisError;
use crate::types::{
  FromRedis, MultipleIDs, MultipleKeys, MultipleOrderedPairs, MultipleStrings, RedisKey, RedisValue, XCap, XID,
};
use std::convert::TryInto;

/// A trait that implements the [streams](https://redis.io/commands#stream) interface.
pub trait StreamsInterface: ClientLike + Sized {
  /// This command returns the list of consumers that belong to the `groupname` consumer group of the stream stored at `key`.
  ///
  /// <https://redis.io/commands/xinfo-consumers>
  fn xinfo_consumers<R, K, S>(&self, key: K, groupname: S) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: Into<String>,
  {
    into!(key, groupname);
    async_spawn(self, |inner| async move {
      commands::streams::xinfo_consumers(&inner, key, groupname)
        .await?
        .convert()
    })
  }

  /// This command returns the list of all consumers groups of the stream stored at `key`.
  ///
  /// <https://redis.io/commands/xinfo-groups>
  fn xinfo_groups<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::streams::xinfo_groups(&inner, key).await?.convert()
    })
  }

  /// This command returns information about the stream stored at `key`.
  ///
  /// <https://redis.io/commands/xinfo-stream>
  fn xinfo_stream<R, K>(&self, key: K, full: bool, count: Option<u64>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::streams::xinfo_stream(&inner, key, full, count)
        .await?
        .convert()
    })
  }

  /// Appends the specified stream entry to the stream at the specified key. If the key does not exist, as a side effect of
  /// running this command the key is created with a stream value. The creation of stream's key can be disabled with the
  /// NOMKSTREAM option.
  ///
  /// <https://redis.io/commands/xadd>
  fn xadd<R, K, I, F>(&self, key: K, nomkstream: bool, cap: Option<XCap>, id: I, fields: F) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    I: Into<XID>,
    F: TryInto<MultipleOrderedPairs>,
    F::Error: Into<RedisError>,
  {
    into!(key, id);
    try_into!(fields);
    async_spawn(self, |inner| async move {
      commands::streams::xadd(&inner, key, nomkstream, cap, id, fields)
        .await?
        .convert()
    })
  }

  /// Trims the stream by evicting older entries (entries with lower IDs) if needed.
  ///
  /// <https://redis.io/commands/xtrim>
  // TODO make XCAP more generic for argument type conversions
  fn xtrim<R, K>(&self, key: K, cap: XCap) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::streams::xtrim(&inner, key, cap).await?.convert()
    })
  }

  /// Removes the specified entries from a stream, and returns the number of entries deleted.
  ///
  /// <https://redis.io/commands/xdel>
  fn xdel<R, K, S>(&self, key: K, ids: S) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: Into<MultipleStrings>,
  {
    into!(key, ids);
    async_spawn(self, |inner| async move {
      commands::streams::xdel(&inner, key, ids).await?.convert()
    })
  }

  /// The command returns the stream entries matching a given range of IDs. The range is specified by a minimum
  /// and maximum ID. All the entries having an ID between the two specified or exactly one of the two IDs specified
  /// (closed interval) are returned.
  ///
  /// <https://redis.io/commands/xrange>
  fn xrange<R, K, S, E>(&self, key: K, start: S, end: E, count: Option<u64>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: TryInto<RedisValue>,
    S::Error: Into<RedisError>,
    E: TryInto<RedisValue>,
    E::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(start, end);
    async_spawn(self, |inner| async move {
      commands::streams::xrange(&inner, key, start, end, count)
        .await?
        .convert()
    })
  }

  /// Similar to `XRANGE`, but with the results returned in reverse order.
  ///
  /// <https://redis.io/commands/xrevrange>
  fn xrevrange<R, K, S, E>(&self, key: K, end: E, start: S, count: Option<u64>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: TryInto<RedisValue>,
    S::Error: Into<RedisError>,
    E: TryInto<RedisValue>,
    E::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(start, end);
    async_spawn(self, |inner| async move {
      commands::streams::xrevrange(&inner, key, end, start, count)
        .await?
        .convert()
    })
  }

  /// Returns the number of entries inside a stream.
  ///
  /// <https://redis.io/commands/xlen>
  fn xlen<R, K>(&self, key: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::streams::xlen(&inner, key).await?.convert()
    })
  }

  /// Read data from one or multiple streams, only returning entries with an ID greater than the last received
  /// ID reported by the caller.
  ///
  /// <https://redis.io/commands/xread>
  fn xread<R, K, I>(&self, count: Option<u64>, block: Option<u64>, keys: K, ids: I) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
    I: Into<MultipleIDs>,
  {
    into!(keys, ids);
    async_spawn(self, |inner| async move {
      commands::streams::xread(&inner, count, block, keys, ids)
        .await?
        .convert()
    })
  }

  /// This command creates a new consumer group uniquely identified by `groupname` for the stream stored at `key`.
  ///
  /// <https://redis.io/commands/xgroup-create>
  fn xgroup_create<R, K, S, I>(&self, key: K, groupname: S, id: I, mkstream: bool) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: Into<String>,
    I: Into<XID>,
  {
    into!(key, groupname, id);
    async_spawn(self, |inner| async move {
      commands::streams::xgroup_create(&inner, key, groupname, id, mkstream)
        .await?
        .convert()
    })
  }

  /// Create a consumer named `consumername` in the consumer group `groupname` of the stream that's stored at `key`.
  ///
  /// <https://redis.io/commands/xgroup-createconsumer>
  fn xgroup_createconsumer<R, K, G, C>(&self, key: K, groupname: G, consumername: C) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    G: Into<String>,
    C: Into<String>,
  {
    into!(key, groupname, consumername);
    async_spawn(self, |inner| async move {
      commands::streams::xgroup_createconsumer(&inner, key, groupname, consumername)
        .await?
        .convert()
    })
  }

  /// Delete a consumer named `consumername` in the consumer group `groupname` of the stream that's stored at `key`.
  ///
  /// <https://redis.io/commands/xgroup-delconsumer>
  fn xgroup_delconsumer<R, K, G, C>(&self, key: K, groupname: G, consumername: C) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    G: Into<String>,
    C: Into<String>,
  {
    into!(key, groupname, consumername);
    async_spawn(self, |inner| async move {
      commands::streams::xgroup_delconsumer(&inner, key, groupname, consumername)
        .await?
        .convert()
    })
  }

  /// Completely destroy a consumer group.
  ///
  /// <https://redis.io/commands/xgroup-destroy>
  fn xgroup_destroy<R, K, S>(&self, key: K, groupname: S) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: Into<String>,
  {
    into!(key, groupname);
    async_spawn(self, |inner| async move {
      commands::streams::xgroup_destroy(&inner, key, groupname)
        .await?
        .convert()
    })
  }

  /// Set the last delivered ID for a consumer group.
  ///
  /// <https://redis.io/commands/xgroup-setid>
  fn xgroup_setid<R, K, S, I>(&self, key: K, groupname: S, id: I) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: Into<String>,
    I: Into<XID>,
  {
    into!(key, groupname, id);
    async_spawn(self, |inner| async move {
      commands::streams::xgroup_setid(&inner, key, groupname, id)
        .await?
        .convert()
    })
  }

  /// A special version of the `XREAD` command with support for consumer groups.
  ///
  /// <https://redis.io/commands/xreadgroup>
  fn xreadgroup<R, G, C, K, I>(
    &self,
    group: G,
    consumer: C,
    count: Option<u64>,
    block: Option<u64>,
    noack: bool,
    keys: K,
    ids: I,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    G: Into<String>,
    C: Into<String>,
    K: Into<MultipleKeys>,
    I: Into<MultipleIDs>,
  {
    into!(group, consumer, keys, ids);
    async_spawn(self, |inner| async move {
      commands::streams::xreadgroup(&inner, group, consumer, count, block, noack, keys, ids)
        .await?
        .convert()
    })
  }

  /// Remove one or more messages from the Pending Entries List (PEL) of a stream consumer group.
  ///
  /// <https://redis.io/commands/xack>
  fn xack<R, K, G, I>(&self, key: K, group: G, ids: I) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    G: Into<String>,
    I: Into<MultipleIDs>,
  {
    into!(key, group, ids);
    async_spawn(self, |inner| async move {
      commands::streams::xack(&inner, key, group, ids).await?.convert()
    })
  }

  /// n the context of a stream consumer group, this command changes the ownership of a pending message,
  /// so that the new owner is the consumer specified as the command argument.
  ///
  /// <https://redis.io/commands/xclaim>
  fn xclaim<R, K, G, C, I>(
    &self,
    key: K,
    group: G,
    consumer: C,
    min_idle_time: u64,
    ids: I,
    idle: Option<u64>,
    time: Option<u64>,
    retry_count: Option<u64>,
    force: bool,
    justid: bool,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    G: Into<String>,
    C: Into<String>,
    I: Into<MultipleIDs>,
  {
    into!(key, group, consumer, ids);
    async_spawn(self, |inner| async move {
      commands::streams::xclaim(
        &inner,
        key,
        group,
        consumer,
        min_idle_time,
        ids,
        idle,
        time,
        retry_count,
        force,
        justid,
      )
      .await?
      .convert()
    })
  }

  /// This command transfers ownership of pending stream entries that match the specified criteria.
  ///
  /// <https://redis.io/commands/xautoclaim>
  fn xautoclaim<R, K, G, C, I>(
    &self,
    key: K,
    group: G,
    consumer: C,
    min_idle_time: u64,
    start: I,
    count: Option<u64>,
    justid: bool,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    G: Into<String>,
    C: Into<String>,
    I: Into<XID>,
  {
    into!(key, group, consumer, start);
    async_spawn(self, |inner| async move {
      commands::streams::xautoclaim(&inner, key, group, consumer, min_idle_time, start, count, justid)
        .await?
        .convert()
    })
  }

  /// Inspect the list of pending messages in a consumer group.
  ///
  /// The `args` argument has the form `[[IDLE min-idle-time] start end count [consumer]]`.
  ///
  /// <https://redis.io/commands/xpending>
  fn xpending<R, K, G>(
    &self,
    key: K,
    group: G,
    args: Option<(Option<u64>, XID, XID, u64, Option<String>)>,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    G: Into<String>,
  {
    into!(key, group);
    async_spawn(self, |inner| async move {
      commands::streams::xpending(&inner, key, group, args).await?.convert()
    })
  }
}
