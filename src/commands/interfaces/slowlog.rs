use crate::{
  commands,
  interfaces::{ClientLike, RedisResult},
  types::FromRedis,
};

/// Functions that implement the [slowlog](https://redis.io/commands#server) interface.
#[async_trait]
pub trait SlowlogInterface: ClientLike + Sized {
  /// This command is used to read the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#reading-the-slow-log>
  async fn slowlog_get<R>(&self, count: Option<i64>) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::slowlog::slowlog_get(self, count).await?.convert()
  }

  /// This command is used to read length of the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#obtaining-the-current-length-of-the-slow-log>
  async fn slowlog_length<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::slowlog::slowlog_length(self).await?.convert()
  }

  /// This command is used to reset the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#resetting-the-slow-log>
  async fn slowlog_reset(&self) -> RedisResult<()> {
    commands::slowlog::slowlog_reset(self).await
  }
}
