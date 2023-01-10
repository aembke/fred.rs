use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{FromRedis, RespVersion, Server},
};
use bytes_utils::Str;
use std::time::Duration;
use tokio::time::interval as tokio_interval;

/// Functions for authenticating clients.
#[async_trait]
pub trait AuthInterface: ClientLike {
  /// Request for authentication in a password-protected Redis server. Returns ok if successful.
  ///
  /// The client will automatically authenticate with the default user if a password is provided in the associated
  /// `RedisConfig` when calling [connect](crate::interfaces::ClientLike::connect).
  ///
  /// If running against clustered servers this function will authenticate all connections.
  ///
  /// <https://redis.io/commands/auth>
  async fn auth<S>(&self, username: Option<String>, password: S) -> RedisResult<()>
  where
    S: Into<Str> + Send,
  {
    into!(password);
    commands::server::auth(self, username, password).await
  }

  /// Switch to a different protocol, optionally authenticating in the process.
  ///
  /// If running against clustered servers this function will issue the HELLO command to each server concurrently.
  ///
  /// <https://redis.io/commands/hello>
  async fn hello(&self, version: RespVersion, auth: Option<(String, String)>) -> RedisResult<()> {
    commands::server::hello(self, version, auth).await
  }
}

/// Functions that provide a connection heartbeat interface.
#[async_trait]
pub trait HeartbeatInterface: ClientLike {
  /// Return a future that will ping the server on an interval.
  ///
  /// When running against a cluster this will ping a random node on each interval.
  #[allow(unreachable_code)]
  async fn enable_heartbeat(&self, interval: Duration, break_on_error: bool) -> RedisResult<()> {
    let _self = self.clone();
    let mut interval = tokio_interval(interval);

    loop {
      interval.tick().await;

      if break_on_error {
        let _: () = _self.ping().await?;
      } else {
        if let Err(e) = _self.ping::<()>().await {
          warn!("{}: Heartbeat ping failed with error: {:?}", _self.inner().id, e);
        }
      }
    }

    Ok(())
  }
}

/// Functions that implement the [Server](https://redis.io/commands#server) interface.
#[async_trait]
pub trait ServerInterface: ClientLike {
  /// Instruct Redis to start an Append Only File rewrite process.
  ///
  /// <https://redis.io/commands/bgrewriteaof>
  async fn bgrewriteaof<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::server::bgrewriteaof(self).await?.convert()
  }

  /// Save the DB in background.
  ///
  /// <https://redis.io/commands/bgsave>
  async fn bgsave<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::server::bgsave(self).await?.convert()
  }

  /// Return the number of keys in the selected database.
  ///
  /// <https://redis.io/commands/dbsize>
  async fn dbsize<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::server::dbsize(self).await?.convert()
  }

  /// Delete the keys in all databases.
  ///
  /// <https://redis.io/commands/flushall>
  async fn flushall<R>(&self, r#async: bool) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::server::flushall(self, r#async).await?.convert()
  }

  /// Delete the keys on all nodes in the cluster. This is a special function that does not map directly to the Redis
  /// interface.
  async fn flushall_cluster(&self) -> RedisResult<()> {
    commands::server::flushall_cluster(self).await
  }

  /// Select the database this client should use.
  ///
  /// <https://redis.io/commands/select>
  async fn select(&self, db: u8) -> RedisResult<()> {
    commands::server::select(self, db).await?.convert()
  }

  /// This command will start a coordinated failover between the currently-connected-to master and one of its
  /// replicas.
  ///
  /// <https://redis.io/commands/failover>
  async fn failover(
    &self,
    to: Option<(String, u16)>,
    force: bool,
    abort: bool,
    timeout: Option<u32>,
  ) -> RedisResult<()> {
    commands::server::failover(self, to, force, abort, timeout).await
  }

  /// Return the UNIX TIME of the last DB save executed with success.
  ///
  /// <https://redis.io/commands/lastsave>
  async fn lastsave<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::server::lastsave(self).await?.convert()
  }

  /// This command blocks the current client until all the previous write commands are successfully transferred and
  /// acknowledged by at least the specified number of replicas. If the timeout, specified in milliseconds, is
  /// reached, the command returns even if the specified number of replicas were not yet reached.
  ///
  /// <https://redis.io/commands/wait/>
  async fn wait<R>(&self, numreplicas: i64, timeout: i64) -> Result<R, RedisError>
  where
    R: FromRedis,
  {
    commands::server::wait(self, numreplicas, timeout).await?.convert()
  }

  /// Read the primary Redis server identifier returned from the sentinel nodes.
  fn sentinel_primary(&self) -> Option<Server> {
    self.inner().server_state.read().kind.sentinel_primary()
  }

  /// Read the set of known sentinel nodes.
  fn sentinel_nodes(&self) -> Option<Vec<Server>> {
    let inner = self.inner();
    inner.server_state.read().kind.read_sentinel_nodes(&inner.config.server)
  }
}
