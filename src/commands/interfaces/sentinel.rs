use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{FromRedis, RedisMap, RedisValue, SentinelFailureKind};
use bytes_utils::Str;
use std::convert::TryInto;
use std::net::IpAddr;

/// Functions that implement the [Sentinel](https://redis.io/topics/sentinel#sentinel-commands) interface.
pub trait SentinelInterface: ClientLike + Sized {
  /// Check if the current Sentinel configuration is able to reach the quorum needed to failover a master, and the majority needed to authorize the failover.
  fn ckquorum<R, N>(&self, name: N) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    N: Into<Str>,
  {
    into!(name);
    async_spawn(self, |inner| async move {
      commands::sentinel::ckquorum(&inner, name).await?.convert()
    })
  }

  /// Force Sentinel to rewrite its configuration on disk, including the current Sentinel state.
  fn flushconfig<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::sentinel::flushconfig(&inner).await?.convert()
    })
  }

  /// Force a failover as if the master was not reachable, and without asking for agreement to other Sentinels.
  fn failover<R, N>(&self, name: N) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    N: Into<Str>,
  {
    into!(name);
    async_spawn(self, |inner| async move {
      commands::sentinel::failover(&inner, name).await?.convert()
    })
  }

  /// Return the ip and port number of the master with that name.
  fn get_master_addr_by_name<R, N>(&self, name: N) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    N: Into<Str>,
  {
    into!(name);
    async_spawn(self, |inner| async move {
      commands::sentinel::get_master_addr_by_name(&inner, name)
        .await?
        .convert()
    })
  }

  /// Return cached INFO output from masters and replicas.
  fn info_cache<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::sentinel::info_cache(&inner).await?.convert()
    })
  }

  /// Show the state and info of the specified master.
  fn master<R, N>(&self, name: N) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    N: Into<Str>,
  {
    into!(name);
    async_spawn(self, |inner| async move {
      commands::sentinel::master(&inner, name).await?.convert()
    })
  }

  /// Show a list of monitored masters and their state.
  fn masters<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::sentinel::masters(&inner).await?.convert()
    })
  }

  /// Start Sentinel's monitoring.
  ///
  /// <https://redis.io/topics/sentinel#reconfiguring-sentinel-at-runtime>
  fn monitor<R, N>(&self, name: N, ip: IpAddr, port: u16, quorum: u32) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    N: Into<Str>,
  {
    into!(name);
    async_spawn(self, |inner| async move {
      commands::sentinel::monitor(&inner, name, ip, port, quorum)
        .await?
        .convert()
    })
  }

  /// Return the ID of the Sentinel instance.
  fn myid<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::sentinel::myid(&inner).await?.convert()
    })
  }

  /// This command returns information about pending scripts.
  fn pending_scripts<R>(&self) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::sentinel::pending_scripts(&inner).await?.convert()
    })
  }

  /// Stop Sentinel's monitoring.
  ///
  /// <https://redis.io/topics/sentinel#reconfiguring-sentinel-at-runtime>
  fn remove<R, N>(&self, name: N) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    N: Into<Str>,
  {
    into!(name);
    async_spawn(self, |inner| async move {
      commands::sentinel::remove(&inner, name).await?.convert()
    })
  }

  /// Show a list of replicas for this master, and their state.
  fn replicas<R, N>(&self, name: N) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    N: Into<Str>,
  {
    into!(name);
    async_spawn(self, |inner| async move {
      commands::sentinel::replicas(&inner, name).await?.convert()
    })
  }

  /// Show a list of sentinel instances for this master, and their state.
  fn sentinels<R, N>(&self, name: N) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    N: Into<Str>,
  {
    into!(name);
    async_spawn(self, |inner| async move {
      commands::sentinel::sentinels(&inner, name).await?.convert()
    })
  }

  /// Set Sentinel's monitoring configuration.
  ///
  /// <https://redis.io/topics/sentinel#reconfiguring-sentinel-at-runtime>
  fn set<R, N, V>(&self, name: N, args: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    N: Into<Str>,
    V: TryInto<RedisMap>,
    V::Error: Into<RedisError>,
  {
    into!(name);
    try_into!(args);
    async_spawn(self, |inner| async move {
      commands::sentinel::set(&inner, name, args.into()).await?.convert()
    })
  }

  /// This command simulates different Sentinel crash scenarios.
  fn simulate_failure<R>(&self, kind: SentinelFailureKind) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
  {
    async_spawn(self, |inner| async move {
      commands::sentinel::simulate_failure(&inner, kind).await?.convert()
    })
  }

  /// This command will reset all the masters with matching name.
  fn reset<R, P>(&self, pattern: P) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    P: Into<Str>,
  {
    into!(pattern);
    async_spawn(self, |inner| async move {
      commands::sentinel::reset(&inner, pattern).await?.convert()
    })
  }

  /// Get the current value of a global Sentinel configuration parameter. The specified name may be a wildcard, similar to the Redis CONFIG GET command.
  fn config_get<R, K>(&self, name: K) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<Str>,
  {
    into!(name);
    async_spawn(self, |inner| async move {
      commands::sentinel::config_get(&inner, name).await?.convert()
    })
  }

  /// Set the value of a global Sentinel configuration parameter.
  fn config_set<R, K, V>(&self, name: K, value: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<Str>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(name);
    try_into!(value);
    async_spawn(self, |inner| async move {
      commands::sentinel::config_set(&inner, name, value).await?.convert()
    })
  }
}
