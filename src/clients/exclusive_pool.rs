use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces::ClientLike,
  prelude::{RedisClient, RedisResult},
  types::{ConnectHandle, ConnectionConfig, PerformanceConfig, ReconnectPolicy, RedisConfig},
  utils,
};
use futures::future::{join_all, try_join_all};
use std::{
  fmt,
  fmt::Formatter,
  sync::{atomic::AtomicUsize, Arc},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

#[cfg(feature = "dns")]
use crate::types::Resolve;

struct PoolInner {
  clients: Vec<Arc<AsyncMutex<RedisClient>>>,
  counter: AtomicUsize,
}

/// A cheaply cloneable round-robin client pool that provides exclusive ownership over the inner clients.
///
/// This interface can be significantly slower than [RedisPool](crate::clients::RedisPool) when shared among many
/// Tokio tasks, but can be used when callers require exclusive ownership over the connection. For example,
///
/// ```no_run no_compile
/// WATCH foo
/// foo = GET foo
/// if foo > 1:
///   MULTI
///     INCR foo
///     INCR bar
///     INCR baz
///   EXEC
/// ```
///
/// Unlike [RedisPool](crate::clients::RedisPool), this pooling interface does not directly implement
/// [ClientLike](crate::interfaces::ClientLike). Callers must `await` and manage the returned
/// [MutexGuard](OwnedMutexGuard) scopes directly.
///
/// ```rust
/// use fred::{
///   clients::{ExclusivePool, RedisPool},
///   prelude::*,
/// };
///
/// async fn example() -> Result<(), RedisError> {
///   let builder = Builder::default_centralized();
///   let shared_pool = builder.build_pool(5)?;
///   let exclusive_pool = builder.build_exclusive_pool(5)?;
///   shared_pool.init().await?;
///   exclusive_pool.init().await?;
///
///   // since `RedisPool` implements `ClientLike` we can use most command interfaces directly
///   let foo: Option<String> = shared_pool.set("foo", 1, None, None, false).await?;
///
///   // with an `ExclusivePool` callers acquire and release clients with an async lock guard
///   let results: Option<(i64, i64, i64)> = {
///     let client = exclusive_pool.next().await;
///
///     client.watch("foo").await?;
///     if let Some(1) = client.get::<Option<i64>, _>("foo").await? {
///       let trx = client.multi();
///       trx.incr("foo").await?;
///       trx.incr("bar").await?;
///       trx.incr("baz").await?;
///       Some(trx.exec(true).await?)
///     } else {
///       None
///     }
///   };
///   assert_eq!(results, Some((2, 1, 1)));
///
///   Ok(())
/// }
/// ```
///
/// Callers should avoid cloning the inner clients, if possible.
#[derive(Clone)]
pub struct ExclusivePool {
  inner: Arc<PoolInner>,
}

impl fmt::Debug for ExclusivePool {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("ExclusivePool")
      .field("size", &self.inner.clients.len())
      .finish()
  }
}

impl ExclusivePool {
  /// Create a new pool without connecting to the server.
  ///
  /// See the [builder](crate::types::Builder) interface for more information.
  pub fn new(
    config: RedisConfig,
    perf: Option<PerformanceConfig>,
    connection: Option<ConnectionConfig>,
    policy: Option<ReconnectPolicy>,
    size: usize,
  ) -> Result<Self, RedisError> {
    if size == 0 {
      Err(RedisError::new(RedisErrorKind::Config, "Pool cannot be empty."))
    } else {
      let mut clients = Vec::with_capacity(size);
      for _ in 0 .. size {
        clients.push(Arc::new(AsyncMutex::new(RedisClient::new(
          config.clone(),
          perf.clone(),
          connection.clone(),
          policy.clone(),
        ))));
      }

      Ok(ExclusivePool {
        inner: Arc::new(PoolInner {
          clients,
          counter: AtomicUsize::new(0),
        }),
      })
    }
  }

  /// Read the clients in the pool.
  pub fn clients(&self) -> &[Arc<AsyncMutex<RedisClient>>] {
    &self.inner.clients
  }

  /// Connect each client to the server, returning the task driving each connection.
  ///
  /// Use the base [connect](Self::connect) function to return one handle that drives all connections via [join](https://docs.rs/futures/latest/futures/macro.join.html).
  pub async fn connect_pool(&self) -> Vec<ConnectHandle> {
    let mut connect_tasks = Vec::with_capacity(self.inner.clients.len());
    for locked_client in self.inner.clients.iter() {
      connect_tasks.push(locked_client.lock().await.connect());
    }
    connect_tasks
  }

  /// Connect each client to the server.
  ///
  /// This function returns a `JoinHandle` to a task that drives **all** connections via [join](https://docs.rs/futures/latest/futures/macro.join.html).
  ///
  /// See [connect_pool](crate::clients::RedisPool::connect_pool) for a variation of this function that separates the
  /// connection tasks.
  ///
  /// See [init](Self::init) for an alternative shorthand.
  pub async fn connect(&self) -> ConnectHandle {
    let tasks = self.connect_pool().await;
    tokio::spawn(async move {
      for result in join_all(tasks).await.into_iter() {
        result??;
      }

      Ok(())
    })
  }

  /// Force a reconnection to the server(s) for each client.
  ///
  /// When running against a cluster this function will also refresh the cached cluster routing table.
  pub async fn force_reconnection(&self) -> RedisResult<()> {
    let mut fts = Vec::with_capacity(self.inner.clients.len());
    for locked_client in self.inner.clients.iter() {
      let client = locked_client.clone();
      fts.push(async move { client.lock_owned().await.force_reconnection().await });
    }

    try_join_all(fts).await?;
    Ok(())
  }

  /// Wait for all the clients to connect to the server.
  pub async fn wait_for_connect(&self) -> RedisResult<()> {
    let mut fts = Vec::with_capacity(self.inner.clients.len());
    for locked_client in self.inner.clients.iter() {
      let client = locked_client.clone();
      fts.push(async move { client.lock().await.wait_for_connect().await });
    }

    try_join_all(fts).await?;
    Ok(())
  }

  /// Initialize a new routing and connection task for each client and wait for them to connect successfully.
  ///
  /// The returned [ConnectHandle](crate::types::ConnectHandle) refers to the task that drives the routing and
  /// connection layer for each client. It will not finish until the max reconnection count is reached.
  ///
  /// Callers can also use [connect](Self::connect) and [wait_for_connect](Self::wait_for_connect) separately if
  /// needed.
  ///
  /// ```rust
  /// use fred::prelude::*;
  ///
  /// #[tokio::main]
  /// async fn main() -> Result<(), RedisError> {
  ///   let pool = Builder::default_centralized().build_exclusive_pool(5)?;
  ///   let connection_task = pool.init().await?;
  ///
  ///   // ...
  ///
  ///   pool.quit().await?;
  ///   connection_task.await?
  /// }
  /// ```
  pub async fn init(&self) -> RedisResult<ConnectHandle> {
    let mut rxs = Vec::with_capacity(self.inner.clients.len());
    for locked_client in self.inner.clients.iter() {
      let mut rx = {
        locked_client
          .lock()
          .await
          .inner
          .notifications
          .connect
          .load()
          .subscribe()
      };

      rxs.push(async move { rx.recv().await });
    }

    let connect_task = self.connect().await;
    let init_err = join_all(rxs).await.into_iter().find_map(|r| match r {
      Ok(Err(e)) => Some(e),
      Err(e) => Some(e.into()),
      _ => None,
    });

    if let Some(err) = init_err {
      for client in self.inner.clients.iter() {
        utils::reset_router_task(client.lock().await.inner());
      }

      Err(err)
    } else {
      Ok(connect_task)
    }
  }

  /// Read the size of the pool.
  pub fn size(&self) -> usize {
    self.inner.clients.len()
  }

  /// Read the client that should run the next command.
  pub async fn next(&self) -> OwnedMutexGuard<RedisClient> {
    let mut idx = utils::incr_atomic(&self.inner.counter) % self.inner.clients.len();

    for _ in 0 .. self.inner.clients.len() {
      if let Ok(client) = self.inner.clients[idx].clone().try_lock_owned() {
        return client;
      }

      idx = (idx + 1) % self.inner.clients.len();
    }

    self.inner.clients[idx].clone().lock_owned().await
  }

  /// Update the internal [PerformanceConfig](crate::types::PerformanceConfig) on each client in place with new
  /// values.
  pub async fn update_perf_config(&self, config: PerformanceConfig) {
    for client in self.inner.clients.iter() {
      client.lock().await.update_perf_config(config.clone());
    }
  }

  /// Override the DNS resolution logic for all clients in the pool.
  #[cfg(feature = "dns")]
  #[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
  #[allow(refining_impl_trait)]
  pub async fn set_resolver(&self, resolver: Arc<dyn Resolve>) {
    for client in self.inner.clients.iter() {
      client.lock().await.set_resolver(resolver.clone()).await;
    }
  }

  /// Close the connection to the Redis server for each client. The returned future resolves when the command has been
  /// written to the socket, not when the connection has been fully closed. Some time after this future resolves the
  /// future returned by [connect](Self::connect) will resolve which indicates that the connection has been fully
  /// closed.
  ///
  /// This function will also close all error, pubsub message, and reconnection event streams on all clients in the
  /// pool.
  pub async fn quit(&self) -> RedisResult<()> {
    let mut fts = Vec::with_capacity(self.inner.clients.len());
    for locked_client in self.inner.clients.iter() {
      let client = locked_client.clone();
      fts.push(async move { client.lock().await.quit().await });
    }

    join_all(fts).await;
    Ok(())
  }
}
