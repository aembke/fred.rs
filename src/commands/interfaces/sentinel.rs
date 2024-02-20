use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{FromRedis, RedisMap, RedisValue, SentinelFailureKind},
};
use bytes_utils::Str;
use std::{convert::TryInto, net::IpAddr};

/// Functions that implement the [sentinel](https://redis.io/topics/sentinel#sentinel-commands) interface.
#[async_trait]
pub trait SentinelInterface: ClientLike + Sized {
  /// Check if the current Sentinel configuration is able to reach the quorum needed to failover a master, and the
  /// majority needed to authorize the failover.
  async fn ckquorum<R, N>(&self, name: N) -> RedisResult<R>
  where
    R: FromRedis,
    N: Into<Str> + Send,
  {
    into!(name);
    commands::sentinel::ckquorum(self, name).await?.convert()
  }

  /// Force Sentinel to rewrite its configuration on disk, including the current Sentinel state.
  async fn flushconfig<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::sentinel::flushconfig(self).await?.convert()
  }

  /// Force a failover as if the master was not reachable, and without asking for agreement to other Sentinels.
  async fn failover<R, N>(&self, name: N) -> RedisResult<R>
  where
    R: FromRedis,
    N: Into<Str> + Send,
  {
    into!(name);
    commands::sentinel::failover(self, name).await?.convert()
  }

  /// Return the ip and port number of the master with that name.
  async fn get_master_addr_by_name<R, N>(&self, name: N) -> RedisResult<R>
  where
    R: FromRedis,
    N: Into<Str> + Send,
  {
    into!(name);
    commands::sentinel::get_master_addr_by_name(self, name).await?.convert()
  }

  /// Return cached INFO output from masters and replicas.
  async fn info_cache<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::sentinel::info_cache(self).await?.convert()
  }

  /// Show the state and info of the specified master.
  async fn master<R, N>(&self, name: N) -> RedisResult<R>
  where
    R: FromRedis,
    N: Into<Str> + Send,
  {
    into!(name);
    commands::sentinel::master(self, name).await?.convert()
  }

  /// Show a list of monitored masters and their state.
  async fn masters<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::sentinel::masters(self).await?.convert()
  }

  /// Start Sentinel's monitoring.
  ///
  /// <https://redis.io/topics/sentinel#reconfiguring-sentinel-at-runtime>
  async fn monitor<R, N>(&self, name: N, ip: IpAddr, port: u16, quorum: u32) -> RedisResult<R>
  where
    R: FromRedis,
    N: Into<Str> + Send,
  {
    into!(name);
    commands::sentinel::monitor(self, name, ip, port, quorum)
      .await?
      .convert()
  }

  /// Return the ID of the Sentinel instance.
  async fn myid<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::sentinel::myid(self).await?.convert()
  }

  /// This command returns information about pending scripts.
  async fn pending_scripts<R>(&self) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::sentinel::pending_scripts(self).await?.convert()
  }

  /// Stop Sentinel's monitoring.
  ///
  /// <https://redis.io/topics/sentinel#reconfiguring-sentinel-at-runtime>
  async fn remove<R, N>(&self, name: N) -> RedisResult<R>
  where
    R: FromRedis,
    N: Into<Str> + Send,
  {
    into!(name);
    commands::sentinel::remove(self, name).await?.convert()
  }

  /// Show a list of replicas for this master, and their state.
  async fn replicas<R, N>(&self, name: N) -> RedisResult<R>
  where
    R: FromRedis,
    N: Into<Str> + Send,
  {
    into!(name);
    commands::sentinel::replicas(self, name).await?.convert()
  }

  /// Show a list of sentinel instances for this master, and their state.
  async fn sentinels<R, N>(&self, name: N) -> RedisResult<R>
  where
    R: FromRedis,
    N: Into<Str> + Send,
  {
    into!(name);
    commands::sentinel::sentinels(self, name).await?.convert()
  }

  /// Set Sentinel's monitoring configuration.
  ///
  /// <https://redis.io/topics/sentinel#reconfiguring-sentinel-at-runtime>
  async fn set<R, N, V>(&self, name: N, args: V) -> RedisResult<R>
  where
    R: FromRedis,
    N: Into<Str> + Send,
    V: TryInto<RedisMap> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(name);
    try_into!(args);
    commands::sentinel::set(self, name, args).await?.convert()
  }

  /// This command simulates different Sentinel crash scenarios.
  async fn simulate_failure<R>(&self, kind: SentinelFailureKind) -> RedisResult<R>
  where
    R: FromRedis,
  {
    commands::sentinel::simulate_failure(self, kind).await?.convert()
  }

  /// This command will reset all the masters with matching name.
  async fn reset<R, P>(&self, pattern: P) -> RedisResult<R>
  where
    R: FromRedis,
    P: Into<Str> + Send,
  {
    into!(pattern);
    commands::sentinel::reset(self, pattern).await?.convert()
  }

  /// Get the current value of a global Sentinel configuration parameter. The specified name may be a wildcard,
  /// similar to the Redis CONFIG GET command.
  async fn config_get<R, K>(&self, name: K) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<Str> + Send,
  {
    into!(name);
    commands::sentinel::config_get(self, name).await?.convert()
  }

  /// Set the value of a global Sentinel configuration parameter.
  async fn config_set<R, K, V>(&self, name: K, value: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<Str> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(name);
    try_into!(value);
    commands::sentinel::config_set(self, name, value).await?.convert()
  }
}
