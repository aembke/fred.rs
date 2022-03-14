use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::SlowlogEntry;
use crate::{commands, utils};

/// Functions that implement the [slowlog](https://redis.io/commands#server) interface.
pub trait SlowlogInterface: ClientLike + Sized {
  /// This command is used to read the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#reading-the-slow-log>
  fn slowlog_get(&self, count: Option<i64>) -> AsyncResult<Vec<SlowlogEntry>> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::slowlog::slowlog_get(&inner, count).await
    })
  }

  /// This command is used to read length of the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#obtaining-the-current-length-of-the-slow-log>
  fn slowlog_length(&self) -> AsyncResult<u64> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::slowlog::slowlog_length(&inner).await
    })
  }

  /// This command is used to reset the slow queries log.
  ///
  /// <https://redis.io/commands/slowlog#resetting-the-slow-log>
  fn slowlog_reset(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::slowlog::slowlog_reset(&inner).await
    })
  }
}
