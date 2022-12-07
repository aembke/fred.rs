use crate::{
  clients::Transaction,
  commands,
  interfaces::{ClientLike, RedisResult},
  types::MultipleKeys,
};

/// Functions that implement the [transactions](https://redis.io/commands#transactions) interface.
///
/// See the [Transaction](crate::clients::Transaction) client for more information;
#[async_trait]
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
  async fn watch<K>(&self, keys: K) -> RedisResult<()>
  where
    K: Into<MultipleKeys> + Send,
  {
    into!(keys);
    commands::keys::watch(self, keys).await
  }

  /// Flushes all the previously watched keys for a transaction.
  ///
  /// <https://redis.io/commands/unwatch>
  async fn unwatch(&self) -> RedisResult<()> {
    commands::keys::unwatch(self).await
  }
}
