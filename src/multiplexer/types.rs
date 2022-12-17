use crate::protocol::{
  connection::{CommandBuffer, SharedBuffer},
  types::Server,
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
  commands: Arc<RwLock<HashMap<Server, SharedBuffer>>>,
}

#[cfg(feature = "check-unresponsive")]
impl ConnectionState {
  pub fn new() -> Self {
    ConnectionState {
      commands: Arc::new(RwLock::new(HashMap::new())),
    }
  }
}

#[cfg(feature = "check-unresponsive")]
pub struct NetworkTimeout {
  duration:  Duration,
  handle:    Option<JoinHandle<()>>,
  state:     ConnectionState,
  interrupt: BroadcastSender<()>,
}

#[cfg(feature = "check-unresponsive")]
impl NetworkTimeout {
  pub fn new(duration: u64) -> Self {
    let (tx, _) = broadcast_channel(16);
    NetworkTimeout {
      duration:  Duration::from_millis(duration),
      state:     ConnectionState::new(),
      handle:    None,
      interrupt: tx,
    }
  }

  pub fn subscribe(&self) -> BroadcastReceiver<()> {
    self.interrupt.subscribe()
  }
}
