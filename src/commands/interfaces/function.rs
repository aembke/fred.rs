use crate::{
  commands,
  interfaces::{async_spawn, AsyncResult, ClientLike},
  prelude::RedisError,
  types::{FromRedis, MultipleKeys, MultipleValues},
  utils,
};
use bytes_utils::Str;

/// Functions that implement the [Functions](https://redis.io/commands/?name=function) interface.
pub trait FunctionInterface: ClientLike + Sized {
  /// Load a library to Redis
  ///
  /// <https://redis.io/commands/function-load/>
  fn function_load<R, S>(&self, replace: bool, script: S) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<Str>, {
    into!(script);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::function::function_load(&inner, replace, script)
        .await?
        .convert()
    })
  }

  /// Invoke a function
  ///
  /// <https://redis.io/commands/fcall/>
  fn fcall<R, S, K, V>(&self, function: S, keys: K, args: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<Str>,
    K: Into<MultipleKeys>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>, {
    into!(function, keys);
    try_into!(args);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::function::fcall(&inner, function, keys, args).await?.convert()
    })
  }

  /// This is a read-only variant of the FCALL
  ///
  /// <https://redis.io/commands/fcall_ro/>
  fn fcall_ro<R, S, K, V>(&self, function: S, keys: K, args: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<Str>,
    K: Into<MultipleKeys>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>, {
    into!(function, keys);
    try_into!(args);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::function::fcall_ro(&inner, function, keys, args)
        .await?
        .convert()
    })
  }
}
