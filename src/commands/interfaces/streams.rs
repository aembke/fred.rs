use crate::{
  commands,
  interfaces::{async_spawn, AsyncResult, ClientLike},
  prelude::RedisError,
  types::{
    FromRedis,
    FromRedisKey,
    MultipleIDs,
    MultipleKeys,
    MultipleOrderedPairs,
    MultipleStrings,
    RedisKey,
    RedisValue,
    XCap,
    XPendingArgs,
    XReadResponse,
    XReadValue,
    XID,
  },
};
use bytes_utils::Str;
use std::{convert::TryInto, hash::Hash};

/// A trait that implements the [streams](https://redis.io/commands#stream) interface.
///
/// **Note:** Several of the stream commands can return types with verbose type declarations. Additionally, certain
/// commands can be parsed differently in RESP2 and RESP3 modes. As a result this interface provides some utility
/// functions that can make this easier. Functions such as [xread_map](Self::xread_map),
/// [xreadgroup_map](Self::xreadgroup_map), [xrange_values](Self::xrange_values), etc exist to make this easier on
/// callers. These functions apply an additional layer of parsing logic that can make declaring response types easier,
/// as well as automatically handling the differences between RESP2 and RESP3 return value types.
pub trait StreamsInterface: ClientLike + Sized {
  /// This command returns the list of consumers that belong to the `groupname` consumer group of the stream stored at
  /// `key`.
  ///
  /// <https://redis.io/commands/xinfo-consumers>
  fn xinfo_consumers<R, K, S>(&self, key: K, groupname: S) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: Into<Str>, {
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
    K: Into<RedisKey>, {
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
    K: Into<RedisKey>, {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::streams::xinfo_stream(&inner, key, full, count)
        .await?
        .convert()
    })
  }

  /// Appends the specified stream entry to the stream at the specified key. If the key does not exist, as a side
  /// effect of running this command the key is created with a stream value. The creation of stream's key can be
  /// disabled with the NOMKSTREAM option.
  ///
  /// <https://redis.io/commands/xadd>
  fn xadd<R, K, C, I, F>(&self, key: K, nomkstream: bool, cap: C, id: I, fields: F) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    I: Into<XID>,
    F: TryInto<MultipleOrderedPairs>,
    F::Error: Into<RedisError>,
    C: TryInto<XCap>,
    C::Error: Into<RedisError>, {
    into!(key, id);
    try_into!(fields, cap);
    async_spawn(self, |inner| async move {
      commands::streams::xadd(&inner, key, nomkstream, cap, id, fields)
        .await?
        .convert()
    })
  }

  /// Trims the stream by evicting older entries (entries with lower IDs) if needed.
  ///
  /// <https://redis.io/commands/xtrim>
  fn xtrim<R, K, C>(&self, key: K, cap: C) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    C: TryInto<XCap>,
    C::Error: Into<RedisError>, {
    into!(key);
    try_into!(cap);
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
    S: Into<MultipleStrings>, {
    into!(key, ids);
    async_spawn(self, |inner| async move {
      commands::streams::xdel(&inner, key, ids).await?.convert()
    })
  }

  /// Return the stream entries matching the provided range of IDs, automatically converting to a less verbose type
  /// definition.
  ///
  /// <https://redis.io/commands/xrange>
  fn xrange_values<Ri, Rk, Rv, K, S, E>(
    &self,
    key: K,
    start: S,
    end: E,
    count: Option<u64>,
  ) -> AsyncResult<Vec<XReadValue<Ri, Rk, Rv>>>
  where
    Ri: FromRedis + Unpin + Send,
    Rk: FromRedisKey + Hash + Eq + Unpin + Send,
    Rv: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: TryInto<RedisValue>,
    S::Error: Into<RedisError>,
    E: TryInto<RedisValue>,
    E::Error: Into<RedisError>, {
    into!(key);
    try_into!(start, end);
    async_spawn(self, |inner| async move {
      commands::streams::xrange(&inner, key, start, end, count)
        .await?
        .into_xread_value()
    })
  }

  /// The command returns the stream entries matching a given range of IDs. The range is specified by a minimum
  /// and maximum ID. All the entries having an ID between the two specified or exactly one of the two IDs specified
  /// (closed interval) are returned.
  ///
  /// <https://redis.io/commands/xrange>
  ///
  /// **See [xrange_values](Self::xrange_values) for a variation of this function that may be more useful.**
  fn xrange<R, K, S, E>(&self, key: K, start: S, end: E, count: Option<u64>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: TryInto<RedisValue>,
    S::Error: Into<RedisError>,
    E: TryInto<RedisValue>,
    E::Error: Into<RedisError>, {
    into!(key);
    try_into!(start, end);
    async_spawn(self, |inner| async move {
      commands::streams::xrange(&inner, key, start, end, count)
        .await?
        .convert()
    })
  }

  /// Similar to `XRANGE`, but with the results returned in reverse order. The results will be automatically converted
  /// to a less verbose type definition.
  ///
  /// <https://redis.io/commands/xrevrange>
  fn xrevrange_values<Ri, Rk, Rv, K, E, S>(
    &self,
    key: K,
    end: E,
    start: S,
    count: Option<u64>,
  ) -> AsyncResult<Vec<XReadValue<Ri, Rk, Rv>>>
  where
    Ri: FromRedis + Unpin + Send,
    Rk: FromRedisKey + Hash + Eq + Unpin + Send,
    Rv: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: TryInto<RedisValue>,
    S::Error: Into<RedisError>,
    E: TryInto<RedisValue>,
    E::Error: Into<RedisError>, {
    into!(key);
    try_into!(start, end);
    async_spawn(self, |inner| async move {
      commands::streams::xrevrange(&inner, key, end, start, count)
        .await?
        .into_xread_value()
    })
  }

  /// Similar to `XRANGE`, but with the results returned in reverse order.
  ///
  /// <https://redis.io/commands/xrevrange>
  ///
  /// **See the [xrevrange_values](Self::xrevrange_values) for a variation of this function that may be more useful.**
  fn xrevrange<R, K, S, E>(&self, key: K, end: E, start: S, count: Option<u64>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: TryInto<RedisValue>,
    S::Error: Into<RedisError>,
    E: TryInto<RedisValue>,
    E::Error: Into<RedisError>, {
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
    K: Into<RedisKey>, {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::streams::xlen(&inner, key).await?.convert()
    })
  }

  /// Read data from one or multiple streams, only returning entries with an ID greater than the last received ID
  /// reported by the caller.
  ///
  /// <https://redis.io/commands/xread>
  ///
  /// The `XREAD` and `XREADGROUP` commands return values that can be interpreted differently in RESP2 and RESP3 mode.
  /// In many cases it is also easier to operate on the return values of these functions as a `HashMap`, but
  /// manually declaring this type can be very verbose. This function will automatically convert the response to the
  /// [most common](crate::types::XReadResponse) map representation while also handling the encoding differences
  /// between RESP2 and RESP3.
  ///
  /// ```rust no_run
  /// # use fred::types::XReadResponse;
  /// // borrowed from the tests. XREAD and XREADGROUP are very similar.
  /// let result: XReadResponse<String, String, String, usize> = client  
  ///   .xreadgroup_map("group1", "consumer1", None, None, false, "foo", ">")
  ///   .await?;
  /// println!("Result: {:?}", result);
  /// // Result: {"foo": [("1646240801081-0", {"count": 0}), ("1646240801082-0", {"count": 1}), ("1646240801082-1", {"count": 2})]}
  ///
  /// assert_eq!(result.len(), 1);
  /// for (idx, (id, record)) in result.get("foo").unwrap().into_iter().enumerate() {
  ///   let value = record.get("count").expect("Failed to read count");
  ///   assert_eq!(idx, *value);
  /// }
  /// ```
  // The underlying issue here isn't so much a semantic difference between RESP2 and RESP3, but rather an assumption
  // that went into the logic behind the `FromRedis` trait.
  //
  // In all other Redis commands that return "maps" in RESP2 (or responses that should be interpreted as maps) a map
  // is encoded as an array with an even number of elements representing `(key, value)` pairs.
  //
  // As a result the `FromRedis` implementation for `HashMap`, `BTreeMap`, etc, took a dependency on this behavior. For example: https://redis.io/commands/hgetall#return-value
  //
  // ```
  // 127.0.0.1:6379> hset foo bar 0
  // (integer) 1
  // 127.0.0.1:6379> hset foo baz 1
  // (integer) 1
  // 127.0.0.1:6379> hgetall foo
  // 1) "bar"
  // 2) "0"
  // 3) "baz"
  // 4) "1"
  // // now switch to RESP3 which has a specific type for maps on the wire
  // 127.0.0.1:6379> hello 3
  // ...
  // 127.0.0.1:6379> hgetall foo
  // 1# "bar" => "0"
  // 2# "baz" => "1"
  // ```
  //
  // However, with XREAD/XREADGROUP there's an extra array wrapper in RESP2 around both the "outer" map and "inner"
  // map(s):
  //
  // ```
  // // RESP3
  // 127.0.0.1:6379> xread count 2 streams foo bar 1643479648480-0 1643479834990-0
  // 1# "foo" => 1) 1) "1643479650336-0"
  //       2) 1) "count"
  //          2) "3"
  // 2# "bar" => 1) 1) "1643479837746-0"
  //       2) 1) "count"
  //          2) "5"
  //    2) 1) "1643479925582-0"
  //       2) 1) "count"
  //          2) "6"
  //
  // // RESP2
  // 127.0.0.1:6379> xread count 2 streams foo bar 1643479648480-0 1643479834990-0
  // 1) 1) "foo"
  //    2) 1) 1) "1643479650336-0"
  //          2) 1) "count"
  //             2) "3"
  // 2) 1) "bar"
  //    2) 1) 1) "1643479837746-0"
  //          2) 1) "count"
  //             2) "5"
  //       2) 1) "1643479925582-0"
  //          2) 1) "count"
  //             2) "6"
  // ```
  //
  // In pseudo-Rust types: we expect `Vec<K1, V1, K2, V2, ...>` but instead get `Vec<Vec<K1, V1>, Vec<K2, V2>, ...>`.
  //
  // This left two choices: either make this specific use case (XREAD/XREADGROUP) easier with some utility functions
  // and/or types, or try to add custom type conversion logic in `FromRedis` for this type of map encoding.
  //
  // There is a downside with the second approach outside of this use case though. It is possible for callers to write
  // lua scripts that return pretty much anything. If we were to build in generic logic that modified response
  // values in all cases when they matched this format then we could risk unexpected behavior for callers that just
  // happen to write a lua script that returns this format. This is not likely to happen, but is still probably
  // worth considering.
  //
  // Actually implementing that logic could also be pretty complicated and brittle. It's certainly possible, but seems
  // like more trouble than it's worth when the issue only shows up with 2 commands out of hundreds. Additionally,
  // we don't want to take away the ability for callers to manually declare the RESP2 structure as-is.
  //
  // This function (and `xreadgroup_map`) provide an easier but optional way to handle the encoding differences with
  // the streams interface.
  //
  // The underlying functions that do the RESP2 vs RESP3 conversion are public for callers as well, so one could use a
  // `BTreeMap` instead of a `HashMap` like so:
  //
  // ```
  // let value: BTreeMap<String, Vec<(String, BTreeMap<String, usize>)>> = client
  //   .xread::<RedisValue, _, _>(None, None, "foo", "0")
  //   .await?
  //   .flatten_array_values(2)
  //   .convert()?;
  // ```
  //
  // Thanks for attending my TED talk.
  fn xread_map<Rk1, Rk2, Rk3, Rv, K, I>(
    &self,
    count: Option<u64>,
    block: Option<u64>,
    keys: K,
    ids: I,
  ) -> AsyncResult<XReadResponse<Rk1, Rk2, Rk3, Rv>>
  where
    Rk1: FromRedisKey + Hash + Eq + Unpin + Send,
    Rk2: FromRedis + Unpin + Send,
    Rk3: FromRedisKey + Hash + Eq + Unpin + Send,
    Rv: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
    I: Into<MultipleIDs>, {
    into!(keys, ids);
    async_spawn(self, |inner| async move {
      commands::streams::xread(&inner, count, block, keys, ids)
        .await?
        .into_xread_response()
    })
  }

  /// Read data from one or multiple streams, only returning entries with an ID greater than the last received ID
  /// reported by the caller.
  ///
  /// <https://redis.io/commands/xread>
  ///
  /// **See [xread_map](Self::xread_map) for more information on a variation of this function that might be more
  /// useful.**
  fn xread<R, K, I>(&self, count: Option<u64>, block: Option<u64>, keys: K, ids: I) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
    I: Into<MultipleIDs>, {
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
    S: Into<Str>,
    I: Into<XID>, {
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
    G: Into<Str>,
    C: Into<Str>, {
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
    G: Into<Str>,
    C: Into<Str>, {
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
    S: Into<Str>, {
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
    S: Into<Str>,
    I: Into<XID>, {
    into!(key, groupname, id);
    async_spawn(self, |inner| async move {
      commands::streams::xgroup_setid(&inner, key, groupname, id)
        .await?
        .convert()
    })
  }

  /// A special version of the `XREAD` command with support for consumer groups.
  ///
  /// Declaring proper type declarations for this command can be complicated due to the complex nature of the response
  /// values and the differences between RESP2 and RESP3. See the [xread](Self::xread) documentation for more
  /// information.
  ///
  /// <https://redis.io/commands/xreadgroup>
  ///
  /// The `XREAD` and `XREADGROUP` commands return values that can be interpreted differently in RESP2 and RESP3 mode.
  /// In many cases it is also easier to operate on the return values of these functions as a `HashMap`, but
  /// manually declaring this type can be very verbose. This function will automatically convert the response to the
  /// [most common](crate::types::XReadResponse) map representation while also handling the encoding differences
  /// between RESP2 and RESP3.
  ///
  /// See the [xread_map](Self::xread_map) documentation for more information.
  // See the `xread_map` source docs for more information.
  fn xreadgroup_map<Rk1, Rk2, Rk3, Rv, G, C, K, I>(
    &self,
    group: G,
    consumer: C,
    count: Option<u64>,
    block: Option<u64>,
    noack: bool,
    keys: K,
    ids: I,
  ) -> AsyncResult<XReadResponse<Rk1, Rk2, Rk3, Rv>>
  where
    Rk1: FromRedisKey + Hash + Eq + Unpin + Send,
    Rk2: FromRedis + Unpin + Send,
    Rk3: FromRedisKey + Hash + Eq + Unpin + Send,
    Rv: FromRedis + Unpin + Send,
    G: Into<Str>,
    C: Into<Str>,
    K: Into<MultipleKeys>,
    I: Into<MultipleIDs>, {
    into!(group, consumer, keys, ids);
    async_spawn(self, |inner| async move {
      commands::streams::xreadgroup(&inner, group, consumer, count, block, noack, keys, ids)
        .await?
        .into_xread_response()
    })
  }

  /// A special version of the `XREAD` command with support for consumer groups.
  ///
  /// Declaring proper type declarations for this command can be complicated due to the complex nature of the response
  /// values and the differences between RESP2 and RESP3. See the [xread](Self::xread) documentation for more
  /// information.
  ///
  /// <https://redis.io/commands/xreadgroup>
  ///
  /// **See [xreadgroup_map](Self::xreadgroup_map) for a variation of this function that might be more useful.**
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
    G: Into<Str>,
    C: Into<Str>,
    K: Into<MultipleKeys>,
    I: Into<MultipleIDs>, {
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
    G: Into<Str>,
    I: Into<MultipleIDs>, {
    into!(key, group, ids);
    async_spawn(self, |inner| async move {
      commands::streams::xack(&inner, key, group, ids).await?.convert()
    })
  }

  /// A variation of [xclaim](Self::xclaim) with a less verbose return type.
  fn xclaim_values<Ri, Rk, Rv, K, G, C, I>(
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
  ) -> AsyncResult<Vec<XReadValue<Ri, Rk, Rv>>>
  where
    Ri: FromRedis + Unpin + Send,
    Rk: FromRedisKey + Hash + Eq + Unpin + Send,
    Rv: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    G: Into<Str>,
    C: Into<Str>,
    I: Into<MultipleIDs>, {
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
      .into_xread_value()
    })
  }

  /// In the context of a stream consumer group, this command changes the ownership of a pending message,
  /// so that the new owner is the consumer specified as the command argument.
  ///
  /// <https://redis.io/commands/xclaim>
  ///
  /// **See [xclaim_values](Self::xclaim_values) for a variation of this function that might be more useful.**
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
    G: Into<Str>,
    C: Into<Str>,
    I: Into<MultipleIDs>, {
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

  /// This command transfers ownership of pending stream entries that match the specified criteria. It also converts
  /// the response type to a less verbose type declaration and handles potential differences between RESP2 and RESP3.
  ///
  /// <https://redis.io/commands/xautoclaim>
  // FIXME: this type declaration wont work for Redis v7. Probably need a new FF for this...
  fn xautoclaim_values<Ri, Rk, Rv, K, G, C, I>(
    &self,
    key: K,
    group: G,
    consumer: C,
    min_idle_time: u64,
    start: I,
    count: Option<u64>,
    justid: bool,
  ) -> AsyncResult<(String, Vec<XReadValue<Ri, Rk, Rv>>)>
  where
    Ri: FromRedis + Unpin + Send,
    Rk: FromRedisKey + Hash + Eq + Unpin + Send,
    Rv: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    G: Into<Str>,
    C: Into<Str>,
    I: Into<XID>, {
    into!(key, group, consumer, start);
    async_spawn(self, |inner| async move {
      commands::streams::xautoclaim(&inner, key, group, consumer, min_idle_time, start, count, justid)
        .await?
        .into_xautoclaim_values()
    })
  }

  /// This command transfers ownership of pending stream entries that match the specified criteria.
  ///
  /// <https://redis.io/commands/xautoclaim>
  ///
  /// **Note: See [xautoclaim_values](Self::xautoclaim_values) for a variation of this function that may be more
  /// useful.**
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
    G: Into<Str>,
    C: Into<Str>,
    I: Into<XID>, {
    into!(key, group, consumer, start);
    async_spawn(self, |inner| async move {
      commands::streams::xautoclaim(&inner, key, group, consumer, min_idle_time, start, count, justid)
        .await?
        .convert()
    })
  }

  /// Inspect the list of pending messages in a consumer group.
  ///
  /// <https://redis.io/commands/xpending>
  fn xpending<R, K, G, A>(&self, key: K, group: G, args: A) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    G: Into<Str>,
    A: Into<XPendingArgs>, {
    into!(key, group, args);
    async_spawn(self, |inner| async move {
      commands::streams::xpending(&inner, key, group, args).await?.convert()
    })
  }
}
