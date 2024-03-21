use futures::Future;

use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{FromRedis, MultipleKeys, MultipleValues, RedisKey},
};
use std::convert::TryInto;

/// Functions that implement the [HyperLogLog](https://redis.io/commands#hyperloglog) interface.
pub trait HyperloglogInterface: ClientLike + Sized {
  /// Adds all the element arguments to the HyperLogLog data structure stored at the variable name specified as first
  /// argument.
  ///
  /// <https://redis.io/commands/pfadd>
  fn pfadd<R, K, V>(&self, key: K, elements: V) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    async move {
      into!(key);
      try_into!(elements);
      commands::hyperloglog::pfadd(self, key, elements).await?.convert()
    }
  }

  /// When called with a single key, returns the approximated cardinality computed by the HyperLogLog data structure
  /// stored at the specified variable, which is 0 if the variable does not exist.
  ///
  /// When called with multiple keys, returns the approximated cardinality of the union of the HyperLogLogs passed, by
  /// internally merging the HyperLogLogs stored at the provided keys into a temporary HyperLogLog.
  ///
  /// <https://redis.io/commands/pfcount>
  fn pfcount<R, K>(&self, keys: K) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    K: Into<MultipleKeys> + Send,
  {
    async move {
      into!(keys);
      commands::hyperloglog::pfcount(self, keys).await?.convert()
    }
  }

  /// Merge multiple HyperLogLog values into an unique value that will approximate the cardinality of the union of the
  /// observed sets of the source HyperLogLog structures.
  ///
  /// <https://redis.io/commands/pfmerge>
  fn pfmerge<R, D, S>(&self, dest: D, sources: S) -> impl Future<Output = RedisResult<R>> + Send
  where
    R: FromRedis,
    D: Into<RedisKey> + Send,
    S: Into<MultipleKeys> + Send,
  {
    async move {
      into!(dest, sources);
      commands::hyperloglog::pfmerge(self, dest, sources).await?.convert()
    }
  }
}
