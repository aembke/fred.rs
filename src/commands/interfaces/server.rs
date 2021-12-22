use crate::commands;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::RedisResponse;
use crate::utils;

/// Functions for authenticating clients.
pub trait AuthInterface: ClientLike + Sized {
  /// Request for authentication in a password-protected Redis server. Returns ok if successful.
  ///
  /// The client will automatically authenticate with the default user if a password is provided in the associated `RedisConfig` when calling [connect](Self::connect).
  ///
  /// If running against clustered servers this function will authenticate all connections.
  ///
  /// <https://redis.io/commands/auth>
  fn auth<S>(&self, username: Option<String>, password: S) -> AsyncResult<()>
  where
    S: Into<String>,
  {
    into!(password);
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::server::auth(&inner, username, password).await
    })
  }

  // TODO add HELLO here
}

/// Functions that implement the [Server](https://redis.io/commands#server) interface.
pub trait ServerInterface: ClientLike + Sized {
  /// Instruct Redis to start an Append Only File rewrite process.
  ///
  /// <https://redis.io/commands/bgrewriteaof>
  fn bgrewriteaof<R>(&self) -> AsyncResult<R>
  where
    R: RedisResponse + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::server::bgrewriteaof(&inner).await?.convert()
    })
  }

  /// Save the DB in background.
  ///
  /// <https://redis.io/commands/bgsave>
  fn bgsave<R>(&self) -> AsyncResult<R>
  where
    R: RedisResponse + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::server::bgsave(&inner).await?.convert()
    })
  }

  /// Return the number of keys in the selected database.
  ///
  /// <https://redis.io/commands/dbsize>
  fn dbsize<R>(&self) -> AsyncResult<R>
  where
    R: RedisResponse + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::server::dbsize(&inner).await?.convert()
    })
  }

  /// Delete the keys in all databases.
  ///
  /// <https://redis.io/commands/flushall>
  fn flushall<R>(&self, r#async: bool) -> AsyncResult<R>
  where
    R: RedisResponse + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::server::flushall(&inner, r#async).await?.convert()
    })
  }

  /// Delete the keys on all nodes in the cluster. This is a special function that does not map directly to the Redis interface.
  fn flushall_cluster(&self) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::server::flushall_cluster(&inner).await
    })
  }

  /// Select the database this client should use.
  ///
  /// <https://redis.io/commands/select>
  fn select(&self, db: u8) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      commands::server::select(&inner, db).await?.convert()
    })
  }

  /// This command will start a coordinated failover between the currently-connected-to master and one of its replicas.
  ///
  /// <https://redis.io/commands/failover>
  fn failover(&self, to: Option<(String, u16)>, force: bool, abort: bool, timeout: Option<u32>) -> AsyncResult<()> {
    async_spawn(self, |inner| async move {
      utils::disallow_during_transaction(&inner)?;
      commands::server::failover(&inner, to, force, abort, timeout).await
    })
  }

  /// Return the UNIX TIME of the last DB save executed with success.
  ///
  /// <https://redis.io/commands/lastsave>
  fn lastsave<R>(&self) -> AsyncResult<R>
  where
    R: RedisResponse + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::server::lastsave(&inner).await?.convert()
    })
  }
}
