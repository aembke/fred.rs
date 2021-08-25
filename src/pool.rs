use crate::client::RedisClient;
use crate::error::{RedisError, RedisErrorKind};
use crate::types::{ConnectHandle, ReconnectPolicy, RedisConfig};
use crate::utils;
use parking_lot::RwLock;
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

/// The inner state used by a `DynamicRedisPool`.
pub(crate) struct DynamicPoolInner {
  clients: RwLock<Vec<RedisClient>>,
  last: Arc<AtomicUsize>,
  config: RedisConfig,
  policy: Option<ReconnectPolicy>,
  connect_guard: AsyncMutex<()>,
}

/// A struct to pool multiple Redis clients together into one interface that will round-robin requests among clients,
/// preferring clients with an active connection if specified.
///
/// This module supports scaling operations at runtime but cannot use the `Deref` trait to dereference the pool to a client.
#[derive(Clone)]
pub struct DynamicRedisPool {
  inner: Arc<DynamicPoolInner>,
}

impl fmt::Display for DynamicRedisPool {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[Dynamic Redis Pool]")
  }
}

impl<'a> From<&'a DynamicRedisPool> for RedisClient {
  fn from(p: &'a DynamicRedisPool) -> RedisClient {
    p.next()
  }
}

impl DynamicRedisPool {
  /// Create a new pool without connecting to the server.
  pub fn new(config: RedisConfig, policy: Option<ReconnectPolicy>, size: usize, max_size: usize) -> DynamicRedisPool {
    let mut clients = Vec::with_capacity(max_size);
    for _ in 0..size {
      clients.push(RedisClient::new(config.clone()));
    }

    DynamicRedisPool {
      inner: Arc::new(DynamicPoolInner {
        clients: RwLock::new(clients),
        last: Arc::new(AtomicUsize::new(0)),
        connect_guard: AsyncMutex::new(()),
        config,
        policy,
      }),
    }
  }

  /// Connect each client in the pool to the server, returning the task driving the connection for each client.
  ///
  /// The caller is responsible for calling `wait_for_connect` or any `on_*` functions on each client.
  pub async fn connect(&self) -> Vec<ConnectHandle> {
    let _guard = self.inner.connect_guard.lock().await;

    self
      .inner
      .clients
      .read()
      .iter()
      .map(|c| c.connect(self.inner.policy.clone()))
      .collect()
  }

  /// Wait for all the clients to connect to the server.
  pub async fn wait_for_connect(&self) -> Result<(), RedisError> {
    for client in self.clients() {
      let _ = client.wait_for_connect().await?;
    }

    Ok(())
  }

  /// Read the client that should run the next command.
  #[cfg(feature = "pool-prefer-active")]
  pub fn next(&self) -> RedisClient {
    let clients_guard = self.inner.clients.read();
    let clients_ref = &*clients_guard;

    if clients_ref.is_empty() {
      warn!("Attempted to read a client from an empty redis pool.");
      return RedisClient::new(self.inner.config.clone());
    }

    let num_clients = clients_ref.len();
    let mut last_idx = utils::incr_atomic(&self.inner.last) % num_clients;

    for _ in 0..num_clients {
      let client = &clients_ref[last_idx];
      if client.is_connected() {
        return client.clone();
      }
      last_idx = (last_idx + 1) % num_clients;
    }

    clients_ref[last_idx].clone()
  }

  /// Read the client that should run the next command.
  #[cfg(not(feature = "pool-prefer-active"))]
  pub fn next(&self) -> RedisClient {
    let clients_guard = self.inner.clients.read();
    let clients_ref = &*clients_guard;

    if clients_ref.is_empty() {
      warn!("Attempted to read a client from an empty redis pool.");
      RedisClient::new(self.inner.config.clone())
    } else {
      clients_ref[utils::incr_atomic(&self.inner.last) % clients_ref.len()].clone()
    }
  }

  /// Read the client that should run the next command, creating a new client first if the pool is empty.
  pub async fn next_connect(&self, wait_for_connect: bool) -> RedisClient {
    if has_clients(&self.inner.clients) {
      self.next()
    } else {
      let (client, _) = self.scale_up().await;
      if wait_for_connect {
        let _ = client.wait_for_connect().await;
      }
      client
    }
  }

  /// Read the client that ran the last command.
  pub fn last(&self) -> RedisClient {
    let clients_guard = self.inner.clients.read();
    let clients_ref = &*clients_guard;

    if clients_ref.is_empty() {
      warn!("Attempted to read a client from an empty redis pool.");
      RedisClient::new(self.inner.config.clone())
    } else {
      clients_ref[utils::read_atomic(&self.inner.last) % clients_ref.len()].clone()
    }
  }

  /// Return a list of clients that have an active, healthy connection to the server.
  pub fn connected_clients(&self) -> Vec<RedisClient> {
    self
      .inner
      .clients
      .read()
      .iter()
      .filter_map(|client| {
        if client.is_connected() {
          Some(client.clone())
        } else {
          None
        }
      })
      .collect()
  }

  /// Return a list of clients that are not in a healthy, connected state.
  pub fn disconnected_clients(&self) -> Vec<RedisClient> {
    self
      .inner
      .clients
      .read()
      .iter()
      .filter_map(|client| {
        if client.is_connected() {
          None
        } else {
          Some(client.clone())
        }
      })
      .collect()
  }

  /// An iterator over the inner client array.
  pub fn clients(&self) -> Vec<RedisClient> {
    self.inner.clients.read().iter().map(|c| c.clone()).collect()
  }

  /// Read the number of clients in the pool.
  pub fn size(&self) -> usize {
    self.inner.clients.read().len()
  }

  /// Call `QUIT` on each client in the pool.
  pub async fn quit_pool(&self) {
    for client in self.inner.clients.read().iter() {
      let _ = client.quit().await;
    }
  }

  /// Add a client to the pool, using the same config and reconnection policy from the initial connections.
  pub async fn scale_up(&self) -> (RedisClient, ConnectHandle) {
    let _guard = self.inner.connect_guard.lock().await;

    let client = RedisClient::new(self.inner.config.clone());
    let connection = client.connect(self.inner.policy.clone());
    self.inner.clients.write().push(client.clone());

    (client, connection)
  }

  /// Remove a client from the pool, optionally using `QUIT` after removing it from the pool.
  ///
  /// A new, uninitialized client will be used if the caller tries to read a client from an empty pool.
  pub async fn scale_down(&self, quit: bool) -> Option<RedisClient> {
    let _guard = self.inner.connect_guard.lock().await;

    if let Some(client) = self.inner.clients.write().pop() {
      if quit {
        let _ = client.quit().await;
      }
      Some(client)
    } else {
      None
    }
  }
}

fn has_clients(clients: &RwLock<Vec<RedisClient>>) -> bool {
  clients.read().len() > 0
}

/// The inner state used by a `StaticRedisPool`.
#[derive(Clone)]
pub(crate) struct StaticRedisPoolInner {
  clients: Vec<RedisClient>,
  last: Arc<AtomicUsize>,
}

/// A struct to pool multiple Redis clients together into one interface that will round-robin requests among clients,
/// preferring clients with an active connection if specified.
///
/// This module does not support modifications to the pool after initialization, but does automatically dereference
/// to the next client that should run a command.
#[derive(Clone)]
pub struct StaticRedisPool {
  inner: Arc<StaticRedisPoolInner>,
}

impl fmt::Display for StaticRedisPool {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[Static Redis Pool]")
  }
}

impl Deref for StaticRedisPool {
  type Target = RedisClient;

  fn deref(&self) -> &Self::Target {
    self.next()
  }
}

impl<'a> From<&'a StaticRedisPool> for &'a RedisClient {
  fn from(p: &'a StaticRedisPool) -> &'a RedisClient {
    p.next()
  }
}

impl<'a> From<&'a StaticRedisPool> for RedisClient {
  fn from(p: &'a StaticRedisPool) -> RedisClient {
    p.next().clone()
  }
}

impl StaticRedisPool {
  /// Create a new pool without connecting to the server.
  pub fn new(config: RedisConfig, size: usize) -> Result<StaticRedisPool, RedisError> {
    if size > 0 {
      let mut clients = Vec::with_capacity(size);
      for _ in 0..size {
        clients.push(RedisClient::new(config.clone()));
      }
      let last = Arc::new(AtomicUsize::new(0));

      Ok(StaticRedisPool {
        inner: Arc::new(StaticRedisPoolInner { clients, last }),
      })
    } else {
      Err(RedisError::new(RedisErrorKind::Config, "Pool cannot be empty."))
    }
  }

  /// Read the individual clients in the pool.
  pub fn clients(&self) -> &[RedisClient] {
    &self.inner.clients
  }

  /// Connect each client to the server, returning the task driving each connection.
  ///
  /// The caller is responsible for calling `wait_for_connect` or any `on_*` functions on each client.
  pub fn connect(&self, policy: Option<ReconnectPolicy>) -> Vec<ConnectHandle> {
    self.inner.clients.iter().map(|c| c.connect(policy.clone())).collect()
  }

  /// Wait for all the clients to connect to the server.
  pub async fn wait_for_connect(&self) -> Result<(), RedisError> {
    for client in self.inner.clients.iter() {
      let _ = client.wait_for_connect().await?;
    }

    Ok(())
  }

  /// Read the size of the pool.
  pub fn size(&self) -> usize {
    self.inner.clients.len()
  }

  /// Read the client that should run the next command.
  #[cfg(feature = "pool-prefer-active")]
  pub fn next(&self) -> &RedisClient {
    let mut idx = utils::incr_atomic(&self.inner.last) % self.inner.clients.len();

    for _ in 0..self.inner.clients.len() {
      let client = &self.inner.clients[idx];
      if client.is_connected() {
        return client;
      }
      idx = (idx + 1) % self.inner.clients.len();
    }

    &self.inner.clients[idx]
  }

  /// Read the client that should run the next command.
  #[cfg(not(feature = "pool-prefer-active"))]
  pub fn next(&self) -> &RedisClient {
    &self.inner.clients[utils::incr_atomic(&self.inner.last) % self.inner.clients.len()]
  }

  /// Read the client that ran the last command.
  pub fn last(&self) -> &RedisClient {
    &self.inner.clients[utils::read_atomic(&self.inner.last) % self.inner.clients.len()]
  }

  /// Call `QUIT` on each client in the pool.
  pub async fn quit_pool(&self) {
    for client in self.inner.clients.iter() {
      let _ = client.quit().await;
    }
  }
}
