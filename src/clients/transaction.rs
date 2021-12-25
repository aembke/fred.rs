use crate::error::{RedisError, RedisErrorKind};
use crate::interfaces::*;
use crate::modules::inner::RedisClientInner;
use crate::types::FromRedis;
use crate::utils::check_and_set_bool;
use crate::{commands, utils};
use parking_lot::RwLock;
use std::fmt;
use std::sync::Arc;

/// A client struct for commands in a MULTI/EXEC transaction block.
///
/// This struct will use the same connection(s) as the client from which it was created.
pub struct TransactionClient {
  inner: Arc<RedisClientInner>,
  finished: Arc<RwLock<bool>>,
}

impl fmt::Display for TransactionClient {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "[TransactionClient {}: {}]",
      self.inner.id,
      self.inner().state.read()
    )
  }
}

impl Drop for TransactionClient {
  fn drop(&mut self) {
    if !*self.finished.read() {
      warn!(
        "{}: Dropping transaction client without finishing transaction!",
        self.inner.client_name()
      );
    }
  }
}

impl ClientLike for TransactionClient {
  #[doc(hidden)]
  fn inner(&self) -> &Arc<RedisClientInner> {
    &self.inner
  }
}

impl AclInterface for TransactionClient {}
impl ClientInterface for TransactionClient {}
impl PubsubInterface for TransactionClient {}
impl ConfigInterface for TransactionClient {}
impl GeoInterface for TransactionClient {}
impl HashesInterface for TransactionClient {}
impl HyperloglogInterface for TransactionClient {}
impl MetricsInterface for TransactionClient {}
impl TransactionInterface for TransactionClient {}
impl KeysInterface for TransactionClient {}
impl ListInterface for TransactionClient {}
impl MemoryInterface for TransactionClient {}
impl AuthInterface for TransactionClient {}
impl ServerInterface for TransactionClient {}
impl SetsInterface for TransactionClient {}
impl SortedSetsInterface for TransactionClient {}

impl TransactionClient {
  /// Executes all previously queued commands in a transaction and restores the connection state to normal.
  ///
  /// <https://redis.io/commands/exec>
  ///
  /// Note: Automatic request retry policies in the event of a connection closing can present problems for transactions.
  /// If the underlying connection closes while a transaction is in process the client will abort the transaction by
  /// returning a `Canceled` error to the caller of any pending intermediate command, as well as this one. It's up to
  /// the caller to retry transactions as needed.
  pub async fn exec<R>(self) -> Result<R, RedisError>
  where
    R: FromRedis,
  {
    if check_and_set_bool(&self.finished, true) {
      return Err(RedisError::new(
        RedisErrorKind::InvalidCommand,
        "Transaction already finished.",
      ));
    }
    commands::server::exec(&self.inner).await?.convert()
  }

  /// Flushes all previously queued commands in a transaction and restores the connection state to normal.
  ///
  /// <https://redis.io/commands/discard>
  pub async fn discard(self) -> Result<(), RedisError> {
    if check_and_set_bool(&self.finished, true) {
      return Err(RedisError::new(
        RedisErrorKind::InvalidCommand,
        "Transaction already finished.",
      ));
    }
    commands::server::discard(&self.inner).await
  }

  /// Read the hash slot against which this transaction will run, if known.  
  pub fn hash_slot(&self) -> Option<u16> {
    utils::read_locked(&self.inner.multi_block).and_then(|b| b.hash_slot)
  }
}

#[doc(hidden)]
impl<'a> From<&'a Arc<RedisClientInner>> for TransactionClient {
  fn from(inner: &'a Arc<RedisClientInner>) -> Self {
    TransactionClient {
      inner: inner.clone(),
      finished: Arc::new(RwLock::new(false)),
    }
  }
}
