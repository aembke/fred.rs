use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{FromRedis, RedisValue};
use crate::utils;
use bytes_utils::Str;
use std::convert::TryInto;

/// Functions that implement the [CONFIG](https://redis.io/commands#server) interface.
pub trait ConfigInterface: ClientLike + Sized {
  /// Resets the statistics reported by Redis using the INFO command.
  ///
  /// <https://redis.io/commands/config-resetstat>
  fn config_resetstat(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::config::config_resetstat(&inner).await
    })
  }

  /// The CONFIG REWRITE command rewrites the redis.conf file the server was started with, applying the minimal changes needed to make it
  /// reflect the configuration currently used by the server, which may be different compared to the original one because of the use of
  /// the CONFIG SET command.
  ///
  /// <https://redis.io/commands/config-rewrite>
  fn config_rewrite(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::config::config_rewrite(&inner).await
    })
  }

  /// The CONFIG GET command is used to read the configuration parameters of a running Redis server.
  ///
  /// <https://redis.io/commands/config-get>
  fn config_get<R, S>(&self, parameter: S) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    S: Into<Str>,
  {
    into!(parameter);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::config::config_get(&inner, parameter).await?.convert()
    })
  }

  /// The CONFIG SET command is used in order to reconfigure the server at run time without the need to restart Redis.
  ///
  /// <https://redis.io/commands/config-set>
  fn config_set<P, V>(&self, parameter: P, value: V) -> AsyncResult<()>
  where
    P: Into<Str>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(parameter);
    try_into!(value);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::config::config_set(&inner, parameter, value).await
    })
  }
}
