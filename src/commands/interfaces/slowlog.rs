use crate::{
  commands,
  interfaces::{ClientLike, RedisResult},
  types::SlowlogEntry,
  utils,
};

/// Functions that implement the [slowlog](https://redis.io/commands#server) interface.
#[async_trait]
pub trait SlowlogInterface: ClientLike + Sized {
  /// This command is used to read the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#reading-the-slow-log>
  async fn slowlog_get(&self, count: Option<i64>) -> RedisResult<Vec<SlowlogEntry>> {
    commands::slowlog::slowlog_get(self, count).await
  }

  /// This command is used to read length of the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#obtaining-the-current-length-of-the-slow-log>
  async fn slowlog_length(&self) -> RedisResult<u64> {
    commands::slowlog::slowlog_length(self).await
  }

  /// This command is used to reset the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#resetting-the-slow-log>
  async fn slowlog_reset(&self) -> RedisResult<()> {
    commands::slowlog::slowlog_reset(self).await
  }
}
