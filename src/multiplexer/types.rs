use crate::{
  modules::inner::RedisClientInner,
  multiplexer::Connections,
  protocol::{
    connection::{CommandBuffer, SharedBuffer},
    types::Server,
  },
};
use parking_lot::{Mutex, RwLock};
use std::{
  collections::HashMap,
  ops::Deref,
  sync::Arc,
  time::{Duration, Instant},
};
use tokio::{
  sync::{
    broadcast::{channel as broadcast_channel, Receiver as BroadcastReceiver, Sender as BroadcastSender},
    mpsc::UnboundedSender,
  },
  task::JoinHandle,
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

/// Server command state shared between the Multiplexer and network timeout task.
#[cfg(feature = "check-unresponsive")]
#[derive(Clone)]
pub struct ConnectionState {
  duration:  Duration,
  commands:  Arc<RwLock<HashMap<Server, SharedBuffer>>>,
  interrupt: BroadcastSender<()>,
}

#[cfg(feature = "check-unresponsive")]
impl ConnectionState {
  pub fn new(duration: u64) -> Self {
    let (tx, _) = broadcast_channel(4);

    ConnectionState {
      commands:  Arc::new(RwLock::new(HashMap::new())),
      duration:  Duration::from_millis(duration),
      interrupt: tx,
    }
  }

  pub fn interrupt(&self, inner: &Arc<RedisClientInner>) {
    _debug!(inner, "Interrupting reader tasks.");
    let _ = self.interrupt.send(());
  }

  pub fn subscribe(&self) -> BroadcastReceiver<()> {
    self.interrupt.subscribe()
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

  pub fn has_unresponsive_connection(&self, inner: &Arc<RedisClientInner>) -> bool {
    _debug!(inner, "Checking unresponsive connections...");

    for (server, commands) in self.commands.read().iter() {
      let last_command_sent = {
        if let Some(sent) = commands.lock().front().and_then(|cmd| cmd.network_start.as_ref()) {
          sent.clone()
        } else {
          continue;
        }
      };

      let command_duration = last_command_sent.duration_since(Instant::now());
      if command_duration > self.duration {
        _warn!(
          inner,
          "Server {} unresponsive after {} ms",
          server,
          command_duration.as_millis()
        );
        return true;
      }
    }

    false
  }
}

#[cfg(feature = "check-unresponsive")]
pub struct NetworkTimeout {
  handle: Option<JoinHandle<()>>,
  state:  ConnectionState,
}

#[cfg(feature = "check-unresponsive")]
impl NetworkTimeout {
  pub fn new(duration: u64) -> Self {
    NetworkTimeout {
      state:  ConnectionState::new(duration),
      handle: None,
    }
  }

  pub fn state(&self) -> &ConnectionState {
    &self.state
  }

  pub fn take_handle(&mut self) -> Option<JoinHandle<()>> {
    self.handle.take()
  }

  pub fn task_is_finished(&self) -> bool {
    self.handle.as_ref().map(|t| t.is_finished()).unwrap_or(true)
  }
}
