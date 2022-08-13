use crate::clients::RedisClient;
use crate::error::*;
use crate::modules::backchannel::Backchannel;
use crate::multiplexer::SentCommand;
use crate::protocol::types::DefaultResolver;
use crate::protocol::types::RedisCommand;
use crate::types::*;
use crate::utils;
use arc_swap::access::Access;
use arc_swap::{ArcSwap, ArcSwapOption};
use arcstr::ArcStr;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::broadcast::{self, Sender as BroadcastSender};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender as OneshotSender;
use tokio::sync::RwLock as AsyncRwLock;
use tokio::task::JoinHandle;

const DEFAULT_NOTIFICATION_CAPACITY: usize = 32;

#[cfg(feature = "metrics")]
use crate::modules::metrics::MovingStats;
use crate::protocol::command::QueuedCommand;

pub type CommandSender = UnboundedSender<QueuedCommand>;
pub type CommandReceiver = UnboundedReceiver<QueuedCommand>;

#[derive(Clone)]
pub struct Notifications {
  pub id: ArcStr,
  pub errors: BroadcastSender<RedisError>,
  pub pubsub: BroadcastSender<(String, RedisValue)>,
  pub keyspace: BroadcastSender<KeyspaceEvent>,
  pub reconnect: BroadcastSender<()>,
  pub cluster_change: BroadcastSender<Vec<ClusterStateChange>>,
  pub connect: BroadcastSender<Result<(), RedisError>>,
  /// A channel for events that should close all client tasks with `Canceled` errors.
  ///
  /// Emitted when QUIT, SHUTDOWN, etc are called.
  pub close: BroadcastSender<()>,
}

impl Notifications {
  pub fn new(id: &ArcStr) -> Self {
    let (errors, _) = broadcast::channel(DEFAULT_NOTIFICATION_CAPACITY);
    let (pubsub, _) = broadcast::channel(DEFAULT_NOTIFICATION_CAPACITY);
    let (keyspace, _) = broadcast::channel(DEFAULT_NOTIFICATION_CAPACITY);
    let (reconnect, _) = broadcast::channel(DEFAULT_NOTIFICATION_CAPACITY);
    let (cluster_change, _) = broadcast::channel(DEFAULT_NOTIFICATION_CAPACITY);
    let (connect, _) = broadcast::channel(DEFAULT_NOTIFICATION_CAPACITY);
    let (close, _) = broadcast::channel(DEFAULT_NOTIFICATION_CAPACITY);

    Notifications {
      id: id.clone(),
      errors,
      pubsub,
      keyspace,
      reconnect,
      cluster_change,
      connect,
      close,
    }
  }

  pub fn broadcast_error(&self, error: RedisError) {
    if let Err(e) = self.errors.send(error) {
      warn!("{}: Error notifying `on_error` listeners: {:?}", self.id, e);
    }
  }

  pub fn broadcast_pubsub(&self, channel: String, message: RedisValue) {
    if let Err(_) = self.pubsub.send((channel, message)) {
      warn!("{}: Error notifying `on_message` listeners.", self.id);
    }
  }

  pub fn broadcast_keyspace(&self, event: KeyspaceEvent) {
    if let Err(_) = self.keyspace.send(event) {
      warn!("{}: Error notifying `on_keyspace_event` listeners.", self.id);
    }
  }

  pub fn broadcast_reconnect(&self) {
    if let Err(_) = self.reconnect.send(()) {
      warn!("{}: Error notifying `on_reconnect` listeners.", self.id);
    }
  }

  pub fn broadcast_cluster_change(&self, changes: Vec<ClusterStateChange>) {
    if let Err(_) = self.cluster_change.send(changes) {
      warn!("{}: Error notifying `on_cluster_change` listeners.", self.id);
    }
  }

  pub fn broadcast_connect(&self, result: Result<(), RedisError>) {
    if let Err(_) = self.connect.send(result) {
      warn!("{}: Error notifying `on_connect` listeners.", self.id);
    }
  }

  pub fn broadcast_close(&self) {
    if let Err(_) = self.close.send(()) {
      warn!("{}: Error notifying `close` listeners.", self.id);
    }
  }
}

#[derive(Clone)]
pub struct ClientCounters {
  cmd_buffer_len: Arc<AtomicUsize>,
  redelivery_count: Arc<AtomicUsize>,
}

impl Default for ClientCounters {
  fn default() -> Self {
    ClientCounters {
      cmd_buffer_len: Arc::new(AtomicUsize::new(0)),
      redelivery_count: Arc::new(AtomicUsize::new(0)),
    }
  }
}

impl ClientCounters {
  pub fn incr_cmd_buffer_len(&self) -> usize {
    utils::incr_atomic(&self.cmd_buffer_len)
  }

  pub fn decr_cmd_buffer_len(&self) -> usize {
    utils::decr_atomic(&self.cmd_buffer_len)
  }

  pub fn incr_redelivery_count(&self) -> usize {
    utils::incr_atomic(&self.redelivery_count)
  }

  pub fn cmd_buffer_len(&self) -> usize {
    utils::read_atomic(&self.cmd_buffer_len)
  }

  pub fn redelivery_count(&self) -> usize {
    utils::read_atomic(&self.redelivery_count)
  }

  pub fn reset(&self) {
    utils::set_atomic(&self.cmd_buffer_len, 0);
    utils::set_atomic(&self.redelivery_count, 0);
  }
}

pub struct RedisClientInner {
  /// The client ID used for logging and the default `CLIENT SETNAME` value.
  pub id: ArcStr,
  /// The RESP version used by the underlying connections.
  pub resp_version: Arc<ArcSwap<RespVersion>>,
  /// The state of the underlying connection.
  pub state: RwLock<ClientState>,
  /// Client configuration options.
  pub config: Arc<RedisConfig>,
  /// Performance config options for the client.
  pub performance: Arc<ArcSwap<PerformanceConfig>>,
  /// An optional reconnect policy.
  pub policy: RwLock<Option<ReconnectPolicy>>,
  ///
  pub notifications: Notifications,
  /// An mpsc sender for commands to the multiplexer.
  pub command_tx: CommandSender,
  /// Temporary storage for the receiver half of the multiplexer command channel.
  pub command_rx: RwLock<Option<CommandReceiver>>,
  ///
  pub counters: ClientCounters,
  /// The cached view of the cluster state, if running against a clustered deployment.
  pub cluster_state: Arc<ArcSwapOption<ClusterKeyCache>>,
  /// The DNS resolver to use when establishing new connections.
  // TODO make this generic via the Resolve trait
  pub resolver: DefaultResolver,
  /// A backchannel that can be used to control the multiplexer connections even while the connections are blocked.
  pub backchannel: Arc<AsyncRwLock<Backchannel>>,
  /// The server host/port resolved from the sentinel nodes, if known.
  pub sentinel_primary: RwLock<Option<ArcStr>>,

  /// Command latency metrics.
  #[cfg(feature = "metrics")]
  pub latency_stats: RwLock<MovingStats>,
  /// Network latency metrics.
  #[cfg(feature = "metrics")]
  pub network_latency_stats: RwLock<MovingStats>,
  /// Payload size metrics tracking for requests.
  #[cfg(feature = "metrics")]
  pub req_size_stats: Arc<RwLock<MovingStats>>,
  /// Payload size metrics tracking for responses
  #[cfg(feature = "metrics")]
  pub res_size_stats: Arc<RwLock<MovingStats>>,
}

// TODO reconnect logic needs to select() on a second ft from quit(), shutdown(), etc

impl RedisClientInner {
  pub fn new(config: RedisConfig, perf: PerformanceConfig, policy: Option<ReconnectPolicy>) -> Arc<RedisClientInner> {
    let id = ArcStr::from(format!("fred-{}", utils::random_string(10)));
    let resolver = DefaultResolver::new(&id);
    let (command_tx, command_rx) = unbounded_channel();
    let notifications = Notifications::new(&id);
    let (config, policy) = (Arc::new(config), RwLock::new(policy));
    let performance = Arc::new(ArcSwap::new(Arc::new(perf)));
    let resp_version = Arc::new(ArcSwap::new(Arc::new(config.version.clone())));
    let (counters, state) = (ClientCounters::default(), RwLock::new(ClientState::Disconnected));
    let (command_rx, policy) = (RwLock::new(Some(command_rx)), RwLock::new(policy));
    let (cluster_state, sentinel_primary) = (Arc::new(ArcSwapOption::empty()), RwLock::new(None));
    let backchannel = Arc::new(AsyncRwLock::new(Backchannel::default()));

    Arc::new(RedisClientInner {
      #[cfg(feature = "metrics")]
      latency_stats: RwLock::new(MovingStats::default()),
      #[cfg(feature = "metrics")]
      network_latency_stats: RwLock::new(MovingStats::default()),
      #[cfg(feature = "metrics")]
      req_size_stats: Arc::new(RwLock::new(MovingStats::default())),
      #[cfg(feature = "metrics")]
      res_size_stats: Arc::new(RwLock::new(MovingStats::default())),

      backchannel,
      command_rx,
      cluster_state,
      sentinel_primary,
      command_tx,
      state,
      counters,
      config,
      performance,
      policy,
      resp_version,
      notifications,
      resolver,
      id,
    })
  }

  pub fn is_pipelined(&self) -> bool {
    self.performance.as_ref().load().pipeline
  }

  pub fn log_client_name_fn<F>(&self, level: log::Level, func: F)
  where
    F: FnOnce(&str),
  {
    if log_enabled!(level) {
      func(self.id.as_str())
    }
  }

  pub fn client_name(&self) -> &str {
    self.id.as_str()
  }

  pub fn client_name_ref(&self) -> &ArcStr {
    &self.id
  }

  pub fn update_cluster_state(&self, state: Option<ClusterKeyCache>) {
    self.cluster_state.as_ref().store(state.map(|s| Arc::new(s)));
  }

  pub fn update_sentinel_primary(&self, server: &ArcStr) {
    let mut guard = self.sentinel_primary.write();
    *guard = Some(server.clone());
  }

  #[cfg(feature = "partial-tracing")]
  pub fn should_trace(&self) -> bool {
    self.config.tracing
  }

  #[cfg(not(feature = "partial-tracing"))]
  pub fn should_trace(&self) -> bool {
    false
  }

  pub fn take_command_rx(&self) -> Option<CommandReceiver> {
    self.command_rx.write().take()
  }

  pub fn store_command_rx(&self, rx: CommandReceiver) {
    let mut guard = self.command_rx.write();
    *guard = Some(rx);
  }

  pub fn is_resp3(&self) -> bool {
    *self.resp_version.as_ref().load().as_ref() == RespVersion::RESP3
  }

  pub fn switch_protocol_versions(&self, version: RespVersion) {
    self.resp_version.as_ref().store(Arc::new(version))
  }

  pub fn update_performance_config(&self, config: PerformanceConfig) {
    self.performance.as_ref().store(Arc::new(config));
  }

  pub fn performance_config(&self) -> PerformanceConfig {
    self.performance.as_ref().load().as_ref().clone()
  }

  pub fn reset_protocol_version(&self) {
    let version = self.config.version.clone();
    self.resp_version.as_ref().store(Arc::new(version));
  }
}
