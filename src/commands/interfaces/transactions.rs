use crate::clients::TransactionClient;
use crate::commands;
use crate::error::{RedisError, RedisErrorKind};
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::modules::inner::MultiPolicy;
use crate::types::MultipleKeys;
use crate::utils;

/// Functions that implement the [transactions](https://redis.io/commands#transactions) interface.
///
/// See the [TransactionClient](crate::clients::TransactionClient) for more information;
pub trait TransactionInterface: ClientLike + Sized {
  /// Enter a MULTI block, executing subsequent commands as a transaction.
  ///
  /// <https://redis.io/commands/multi>
  ///
  /// The `abort_on_error` flag indicates whether the client should automatically abort the transaction when an error is received from a command within
  /// the transaction (i.e. the server responds with an error before `EXEC` is called).
  ///
  /// See <https://redis.io/topics/transactions#errors-inside-a-transaction> for more information. If this flag is `false` then the caller will need to
  /// `exec` or `discard` the transaction before either retrying or moving on to new commands outside the transaction.
  ///
  /// When used against a cluster the client will wait to send the `MULTI` command until the hash slot is known from a subsequent command. If no hash slot
  /// is provided the transaction will run against a random cluster node.
  // TODO make sure this works with multiple commands that don't have a hash slot
  fn multi(&self, abort_on_error: bool) -> AsyncResult<TransactionClient> {
    async_spawn(self, |inner| async move {
      if utils::is_clustered(&inner.config) {
        let policy = MultiPolicy {
          hash_slot: None,
          abort_on_error,
          sent_multi: false,
        };

        if !utils::check_and_set_none(&inner.multi_block, policy) {
          return Err(RedisError::new(
            RedisErrorKind::InvalidCommand,
            "Client is already within a MULTI transaction.",
          ));
        }

        debug!("{}: Defer MULTI command until hash slot is specified.", inner.id);
        Ok(TransactionClient::from(&inner))
      } else {
        let policy = MultiPolicy {
          hash_slot: None,
          abort_on_error,
          sent_multi: true,
        };
        if !utils::check_and_set_none(&inner.multi_block, policy) {
          return Err(RedisError::new(
            RedisErrorKind::InvalidCommand,
            "Client is already within a MULTI transaction.",
          ));
        }

        commands::server::multi(&inner)
          .await
          .map(|_| TransactionClient::from(&inner))
      }
    })
  }

  /// Whether or not the client is currently in the middle of a MULTI transaction.
  fn in_transaction(&self) -> bool {
    utils::is_locked_some(&self.inner().multi_block)
  }

  /// Force the client to abort any in-flight transactions.
  ///
  /// The `Drop` trait on the [TransactionClient](crate::clients::TransactionClient) is not async and so callers that accidentally drop the transaction
  /// client associated with a MULTI block before calling EXEC or DISCARD can use this function to exit the transaction.
  /// A warning log line will be emitted if the transaction client is dropped before calling EXEC or DISCARD.
  fn force_discard_transaction(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move { commands::server::discard(&inner).await })
  }

  /// Marks the given keys to be watched for conditional execution of a transaction.
  ///
  /// <https://redis.io/commands/watch>
  fn watch<K>(&self, keys: K) -> AsyncResult<()>
  where
    K: Into<MultipleKeys>,
  {
    into!(keys);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::keys::watch(&inner, keys).await
    })
  }

  /// Flushes all the previously watched keys for a transaction.
  ///
  /// <https://redis.io/commands/unwatch>
  fn unwatch(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move { commands::keys::unwatch(&inner).await })
  }
}
