use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{FromRedis, MultipleKeys, MultipleValues, RedisKey};
use std::convert::TryInto;

/// Functions that implement the [HyperLogLog](https://redis.io/commands#hyperloglog) interface.
pub trait HyperloglogInterface: ClientLike + Sized {
  /// Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as first argument.
  ///
  /// <https://redis.io/commands/pfadd>
  fn pfadd<R, K, V>(&self, key: K, elements: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(elements);
    async_spawn(self, |_self| async move {
      commands::hyperloglog::pfadd(_self, key, elements).await?.convert()
    })
  }

  /// When called with a single key, returns the approximated cardinality computed by the HyperLogLog data structure stored at
  /// the specified variable, which is 0 if the variable does not exist.
  ///
  /// When called with multiple keys, returns the approximated cardinality of the union of the HyperLogLogs passed, by
  /// internally merging the HyperLogLogs stored at the provided keys into a temporary HyperLogLog.
  ///
  /// <https://redis.io/commands/pfcount>
  fn pfcount<R, K>(&self, keys: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |_self| async move {
      commands::hyperloglog::pfcount(_self, keys).await?.convert()
    })
  }

  /// Merge multiple HyperLogLog values into an unique value that will approximate the cardinality of the union of the observed
  /// sets of the source HyperLogLog structures.
  ///
  /// <https://redis.io/commands/pfmerge>
  fn pfmerge<R, D, S>(&self, dest: D, sources: S) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    D: Into<RedisKey>,
    S: Into<MultipleKeys>,
  {
    into!(dest, sources);
    async_spawn(self, |_self| async move {
      commands::hyperloglog::pfmerge(_self, dest, sources).await?.convert()
    })
  }
}
