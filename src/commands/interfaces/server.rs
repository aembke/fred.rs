use crate::commands;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::FromRedis;
use crate::types::RespVersion;
use crate::utils;
use std::time::Duration;
use tokio::time::interval as tokio_interval;

/// Functions for authenticating clients.
pub trait AuthInterface: ClientLike + Sized {
  /// Request for authentication in a password-protected Redis server. Returns ok if successful.
  ///
  /// The client will automatically authenticate with the default user if a password is provided in the associated `RedisConfig` when calling [connect](crate::interfaces::ClientLike::connect).
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
  fn hello<S>(&self, version: RespVersion, username: Option<String>, password: S) -> AsyncResult<()> {
    unimplemented!()
  }
}

/// Functions that provide a connection heartbeat interface.
pub trait HeartbeatInterface: ClientLike + Sized + Clone + 'static {
  /// Return a future that will ping the server on an interval.
  ///
  /// When running against a cluster this will ping a random node on each interval.
  #[allow(unreachable_code)]
  fn enable_heartbeat(&self, interval: Duration, break_on_error: bool) -> AsyncResult<()> {
    let _self = self.clone();

    async_spawn(self, |inner| async move {
      let mut interval = tokio_interval(interval);

      loop {
        interval.tick().await;

        if utils::is_locked_some(&inner.multi_block) {
          _debug!(inner, "Skip heartbeat while inside transaction.");
          continue;
        }

        if break_on_error {
          let _ = _self.ping().await?;
        } else {
          if let Err(e) = _self.ping().await {
            _warn!(inner, "Heartbeat ping failed with error: {:?}", e);
          }
        }
      }

      Ok(())
    })
  }
}

/// Functions that implement the [Server](https://redis.io/commands#server) interface.
pub trait ServerInterface: ClientLike + Sized {
  /// Instruct Redis to start an Append Only File rewrite process.
  ///
  /// <https://redis.io/commands/bgrewriteaof>
  fn bgrewriteaof<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
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
    R: FromRedis + Unpin + Send,
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
    R: FromRedis + Unpin + Send,
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
    R: FromRedis + Unpin + Send,
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
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::server::lastsave(&inner).await?.convert()
    })
  }
}
