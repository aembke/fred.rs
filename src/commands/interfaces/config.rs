use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{FromRedis, RedisValue},
};
use bytes_utils::Str;
use std::convert::TryInto;

/// Functions that implement the [CONFIG](https://redis.io/commands#server) interface.
#[async_trait]
pub trait ConfigInterface: ClientLike + Sized {
  /// Resets the statistics reported by Redis using the INFO command.
  ///
  /// <https://redis.io/commands/config-resetstat>
  async fn config_resetstat(&self) -> RedisResult<()> {
    commands::config::config_resetstat(self).await
  }

  /// The CONFIG REWRITE command rewrites the redis.conf file the server was started with, applying the minimal
  /// changes needed to make it reflect the configuration currently used by the server, which may be different
  /// compared to the original one because of the use of the CONFIG SET command.
  ///
  /// <https://redis.io/commands/config-rewrite>
  async fn config_rewrite(&self) -> RedisResult<()> {
    commands::config::config_rewrite(self).await
  }

  /// The CONFIG GET command is used to read the configuration parameters of a running Redis server.
  ///
  /// <https://redis.io/commands/config-get>
  async fn config_get<R, S>(&self, parameter: S) -> RedisResult<R>
  where
    R: FromRedis,
    S: Into<Str> + Send,
  {
    into!(parameter);
    commands::config::config_get(self, parameter).await?.convert()
  }

  /// The CONFIG SET command is used in order to reconfigure the server at run time without the need to restart Redis.
  ///
  /// <https://redis.io/commands/config-set>
  async fn config_set<P, V>(&self, parameter: P, value: V) -> RedisResult<()>
  where
    P: Into<Str> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(parameter);
    try_into!(value);
    commands::config::config_set(self, parameter, value).await
  }
}
