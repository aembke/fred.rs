use crate::{
  commands,
  interfaces::{async_spawn, AsyncResult, ClientLike},
  types::{FromRedis, RespVersion},
  utils,
};
use arcstr::ArcStr;
use bytes_utils::Str;
use std::time::Duration;
use tokio::time::interval as tokio_interval;

/// Functions for authenticating clients.
pub trait AuthInterface: ClientLike + Sized {
  /// Request for authentication in a password-protected Redis server. Returns ok if successful.
  ///
  /// The client will automatically authenticate with the default user if a password is provided in the associated
  /// `RedisConfig` when calling [connect](crate::interfaces::ClientLike::connect).
  ///
  /// If running against clustered servers this function will authenticate all connections.
  ///
  /// <https://redis.io/commands/auth>
  fn auth<S>(&self, username: Option<String>, password: S) -> AsyncResult<()>
  where
    S: Into<Str>,
  {
    into!(password);
    async_spawn(self, |_self| async move {
      commands::server::auth(_self, username, password).await
    })
  }

  /// Switch to a different protocol, optionally authenticating in the process.
  ///
  /// If running against clustered servers this function will issue the HELLO command to each server concurrently.
  ///
  /// <https://redis.io/commands/hello>
  fn hello(&self, version: RespVersion, auth: Option<(String, String)>) -> AsyncResult<()> {
    async_spawn(self, |_self| async move {
      commands::server::hello(_self, version, auth).await
    })
  }
}

/// Functions that provide a connection heartbeat interface.
pub trait HeartbeatInterface: ClientLike + Sized + 'static {
  /// Return a future that will ping the server on an interval.
  ///
  /// When running against a cluster this will ping a random node on each interval.
  #[allow(unreachable_code)]
  fn enable_heartbeat(&self, interval: Duration, break_on_error: bool) -> AsyncResult<()> {
    let _self = self.clone();

    async_spawn(self, |_self| async move {
      let mut interval = tokio_interval(interval);

      loop {
        interval.tick().await;

        if break_on_error {
          let _ = _self.ping().await?;
        } else {
          if let Err(e) = _self.ping().await {
            warn!("{}: Heartbeat ping failed with error: {:?}", _self.inner().id, e);
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
    async_spawn(self, |_self| async move {
      commands::server::bgrewriteaof(_self).await?.convert()
    })
  }

  /// Save the DB in background.
  ///
  /// <https://redis.io/commands/bgsave>
  fn bgsave<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(
      self,
      |_self| async move { commands::server::bgsave(_self).await?.convert() },
    )
  }

  /// Return the number of keys in the selected database.
  ///
  /// <https://redis.io/commands/dbsize>
  fn dbsize<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(
      self,
      |_self| async move { commands::server::dbsize(_self).await?.convert() },
    )
  }

  /// Delete the keys in all databases.
  ///
  /// <https://redis.io/commands/flushall>
  fn flushall<R>(&self, r#async: bool) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |_self| async move {
      commands::server::flushall(_self, r#async).await?.convert()
    })
  }

  /// Delete the keys on all nodes in the cluster. This is a special function that does not map directly to the Redis
  /// interface.
  fn flushall_cluster(&self) -> AsyncResult<()> {
    async_spawn(
      self,
      |_self| async move { commands::server::flushall_cluster(_self).await },
    )
  }

  /// Select the database this client should use.
  ///
  /// <https://redis.io/commands/select>
  fn select(&self, db: u8) -> AsyncResult<()> {
    async_spawn(self, |_self| async move {
      commands::server::select(_self, db).await?.convert()
    })
  }

  /// This command will start a coordinated failover between the currently-connected-to master and one of its
  /// replicas.
  ///
  /// <https://redis.io/commands/failover>
  fn failover(&self, to: Option<(String, u16)>, force: bool, abort: bool, timeout: Option<u32>) -> AsyncResult<()> {
    async_spawn(self, |_self| async move {
      commands::server::failover(_self, to, force, abort, timeout).await
    })
  }

  /// Return the UNIX TIME of the last DB save executed with success.
  ///
  /// <https://redis.io/commands/lastsave>
  fn lastsave<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |_self| async move {
      commands::server::lastsave(_self).await?.convert()
    })
  }

  /// Read the primary Redis server identifier returned from the sentinel nodes.
  fn sentinel_primary(&self) -> Option<ArcStr> {
    self.inner().sentinel_primary()
  }

  /// Read the set of known sentinel nodes.
  fn sentinel_nodes(&self) -> Option<Vec<(String, u16)>> {
    self.inner().read_sentinel_nodes()
  }
}
