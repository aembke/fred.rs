use crate::clients::Transaction;
use crate::commands;
use crate::error::{RedisError, RedisErrorKind};
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::modules::inner::MultiPolicy;
use crate::types::MultipleKeys;
use crate::utils;

/// Functions that implement the [transactions](https://redis.io/commands#transactions) interface.
///
/// See the [Transaction](crate::clients::Transaction) client for more information;
pub trait TransactionInterface: ClientLike + Sized {
  /// Enter a MULTI block, executing subsequent commands as a transaction.
  ///
  /// <https://redis.io/commands/multi>
  fn multi(&self) -> Transaction {
    self.inner().into()
  }

  /// Marks the given keys to be watched for conditional execution of a transaction.
  ///
  /// <https://redis.io/commands/watch>
  fn watch<K>(&self, keys: K) -> AsyncResult<()>
  where
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |_self| async move { commands::keys::watch(_self, keys).await })
  }

  /// Flushes all the previously watched keys for a transaction.
  ///
  /// <https://redis.io/commands/unwatch>
  fn unwatch(&self) -> AsyncResult<()> {
    async_spawn(self, |_self| async move { commands::keys::unwatch(_self).await })
  }
}
