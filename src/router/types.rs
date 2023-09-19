use crate::protocol::types::Server;

#[cfg(feature = "replicas")]
use crate::router::replicas::Replicas;
#[cfg(feature = "check-unresponsive")]
use crate::{
  globals::globals,
  modules::inner::RedisClientInner,
  protocol::connection::SharedBuffer,
  router::Connections,
};
#[cfg(feature = "check-unresponsive")]
use parking_lot::RwLock;
#[cfg(feature = "check-unresponsive")]
use std::{
  collections::{HashMap, VecDeque},
  sync::Arc,
  time::{Duration, Instant},
};
#[cfg(feature = "check-unresponsive")]
use tokio::{
  sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
  task::JoinHandle,
  time::sleep,
};

/// Options describing how to change connections in a cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterChange {
  pub add:    Vec<Server>,
  pub remove: Vec<Server>,
}

impl Default for ClusterChange {
  fn default() -> Self {
    ClusterChange {
      add:    Vec::new(),
      remove: Vec::new(),
    }
  }
}

/// Server command state shared between the Router, reader tasks, and network timeout task.
#[cfg(feature = "check-unresponsive")]
#[derive(Clone)]
pub struct ConnectionState {
  commands:   Arc<RwLock<HashMap<Server, SharedBuffer>>>,
  interrupts: Arc<RwLock<HashMap<Server, UnboundedSender<()>>>>,
}

#[cfg(feature = "check-unresponsive")]
impl ConnectionState {
  pub fn new() -> Self {
    ConnectionState {
      commands:   Arc::new(RwLock::new(HashMap::new())),
      interrupts: Arc::new(RwLock::new(HashMap::new())),
    }
  }

  pub fn interrupt(&self, inner: &Arc<RedisClientInner>, servers: VecDeque<Server>) {
    let guard = self.interrupts.read();

    for server in servers.into_iter() {
      inner.notifications.broadcast_unresponsive(server.clone());

      if let Some(tx) = guard.get(&server) {
        _debug!(inner, "Interrupting reader task for {}", server);
        let _ = tx.send(());
      } else {
        _debug!(inner, "Could not interrupt reader for {}", server);
      }
    }
  }

  pub fn subscribe(&self, inner: &Arc<RedisClientInner>, server: &Server) -> UnboundedReceiver<()> {
    _debug!(inner, "Subscribe to interrupts for {}", server);
    let (tx, rx) = unbounded_channel();
    self.interrupts.write().insert(server.clone(), tx);
    rx
  }

  pub fn unsubscribe(&self, inner: &Arc<RedisClientInner>, server: &Server) {
    _debug!(inner, "Unsubscribe from interrupts for {}", server);
    self.interrupts.write().remove(server);
  }

  pub fn sync(&self, inner: &Arc<RedisClientInner>, connections: &Connections) {
    _debug!(inner, "Syncing connection state with unresponsive network task.");
    let mut guard = self.commands.write();
    guard.clear();

    match connections {
      Connections::Centralized { writer } | Connections::Sentinel { writer, .. } => {
        if let Some(writer) = writer.as_ref() {
          guard.insert(writer.server.clone(), writer.buffer.clone());
        }
      },
      Connections::Clustered { writers, .. } => {
        for (server, writer) in writers.iter() {
          guard.insert(server.clone(), writer.buffer.clone());
        }
      },
    };
  }

  /// Add the replica connections to the internal connection map.
  #[cfg(feature = "replicas")]
  pub fn sync_replicas(&self, inner: &Arc<RedisClientInner>, replicas: &Replicas) {
    _debug!(
      inner,
      "Syncing replica connection state with unresponsive network task."
    );
    let mut guard = self.commands.write();
    for (server, writer) in replicas.writers.iter() {
      guard.insert(server.clone(), writer.buffer.clone());
    }
  }

  pub fn unresponsive_connections(&self, inner: &Arc<RedisClientInner>) -> VecDeque<Server> {
    _debug!(inner, "Checking unresponsive connections...");

    let now = Instant::now();
    _trace!(
      inner,
      "Using network timeout: {:?}",
      inner.connection.unresponsive_timeout
    );

    let mut unresponsive = VecDeque::new();
    for (server, commands) in self.commands.read().iter() {
      let last_command_sent = {
        let sent = commands.lock().front().and_then(|command| {
          if command.blocks_connection() {
            // blocking commands don't count. maybe make this configurable?
            None
          } else {
            command.network_start.clone()
          }
        });

        if let Some(sent) = sent {
          sent
        } else {
          continue;
        }
      };

      // manually check timestamps to avoid panics in older rust versions
      if last_command_sent >= now {
        continue;
      }
      let command_duration = now.duration_since(last_command_sent);
      if command_duration > inner.connection.unresponsive_timeout {
        _warn!(
          inner,
          "Server {} unresponsive after {} ms",
          server,
          command_duration.as_millis()
        );
        unresponsive.push_back(server.clone());
      }
    }

    unresponsive
  }
}

/// State associated with tracking unresponsive connections.
#[cfg(feature = "check-unresponsive")]
pub struct NetworkTimeout {
  handle: Arc<RwLock<Option<JoinHandle<()>>>>,
  state:  ConnectionState,
}

#[cfg(feature = "check-unresponsive")]
impl NetworkTimeout {
  pub fn new() -> Self {
    NetworkTimeout {
      state:  ConnectionState::new(),
      handle: Arc::new(RwLock::new(None)),
    }
  }

  pub fn state(&self) -> &ConnectionState {
    &self.state
  }

  pub fn take_handle(&self) -> Option<JoinHandle<()>> {
    self.handle.write().take()
  }

  pub fn task_is_finished(&self) -> bool {
    self.handle.read().as_ref().map(|t| t.is_finished()).unwrap_or(true)
  }

  pub fn spawn_task(&self, inner: &Arc<RedisClientInner>) {
    let inner = inner.clone();
    let state = self.state.clone();
    let interval = Duration::from_millis(globals().unresponsive_interval_ms());

    *self.handle.write() = Some(tokio::spawn(async move {
      loop {
        let unresponsive = state.unresponsive_connections(&inner);
        if unresponsive.len() > 0 {
          state.interrupt(&inner, unresponsive);
        }

        sleep(interval).await;
      }
    }));
  }
}
