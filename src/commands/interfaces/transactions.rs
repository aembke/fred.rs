use crate::{clients::Transaction, interfaces::ClientLike};

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
}
