use crate::{
  clients::RedisClient,
  error::*,
  modules::backchannel::Backchannel,
  protocol::{
    command::{MultiplexerCommand, ResponseSender},
    connection::RedisTransport,
    types::{ClusterRouting, DefaultResolver},
  },
  types::*,
  utils,
};
use arc_swap::{ArcSwap, ArcSwapOption};
use arcstr::ArcStr;
use futures::future::{select, Either};
use parking_lot::RwLock;
use std::{
  collections::VecDeque,
  ops::DerefMut,
  sync::{atomic::AtomicUsize, Arc},
  time::Duration,
};
use tokio::{
  sync::{
    broadcast::{self, Sender as BroadcastSender},
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot::Sender as OneshotSender,
    RwLock as AsyncRwLock,
  },
  task::JoinHandle,
  time::sleep,
};

const DEFAULT_NOTIFICATION_CAPACITY: usize = 32;

#[cfg(feature = "metrics")]
use crate::modules::metrics::MovingStats;

pub type CommandSender = UnboundedSender<MultiplexerCommand>;
pub type CommandReceiver = UnboundedReceiver<MultiplexerCommand>;

#[derive(Clone)]
pub struct Notifications {
  /// The client ID.
  pub id:             ArcStr,
  /// A broadcast channel for the `on_error` interface.
  pub errors:         BroadcastSender<RedisError>,
  /// A broadcast channel for the `on_message` interface.
  pub pubsub:         BroadcastSender<(String, RedisValue)>,
  /// A broadcast channel for the `on_keyspace_event` interface.
  pub keyspace:       BroadcastSender<KeyspaceEvent>,
  /// A broadcast channel for the `on_reconnect` interface.
  pub reconnect:      BroadcastSender<()>,
  /// A broadcast channel for the `on_cluster_change` interface.
  pub cluster_change: BroadcastSender<Vec<ClusterStateChange>>,
  /// A broadcast channel for the `on_connect` interface.
  pub connect:        BroadcastSender<Result<(), RedisError>>,
  /// A channel for events that should close all client tasks with `Canceled` errors.
  ///
  /// Emitted when QUIT, SHUTDOWN, etc are called.
  pub close:          BroadcastSender<()>,
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
      debug!("{}: Error notifying `on_error` listeners: {:?}", self.id, e);
    }
  }

  pub fn broadcast_pubsub(&self, channel: String, message: RedisValue) {
    if let Err(_) = self.pubsub.send((channel, message)) {
      debug!("{}: Error notifying `on_message` listeners.", self.id);
    }
  }

  pub fn broadcast_keyspace(&self, event: KeyspaceEvent) {
    if let Err(_) = self.keyspace.send(event) {
      debug!("{}: Error notifying `on_keyspace_event` listeners.", self.id);
    }
  }

  pub fn broadcast_reconnect(&self) {
    if let Err(_) = self.reconnect.send(()) {
      debug!("{}: Error notifying `on_reconnect` listeners.", self.id);
    }
  }

  pub fn broadcast_cluster_change(&self, changes: Vec<ClusterStateChange>) {
    if let Err(_) = self.cluster_change.send(changes) {
      debug!("{}: Error notifying `on_cluster_change` listeners.", self.id);
    }
  }

  pub fn broadcast_connect(&self, result: Result<(), RedisError>) {
    if let Err(_) = self.connect.send(result) {
      debug!("{}: Error notifying `on_connect` listeners.", self.id);
    }
  }

  pub fn broadcast_close(&self) {
    if let Err(_) = self.close.send(()) {
      debug!("{}: Error notifying `close` listeners.", self.id);
    }
  }
}

#[derive(Clone)]
pub struct ClientCounters {
  pub cmd_buffer_len:   Arc<AtomicUsize>,
  pub redelivery_count: Arc<AtomicUsize>,
}

impl Default for ClientCounters {
  fn default() -> Self {
    ClientCounters {
      cmd_buffer_len:   Arc::new(AtomicUsize::new(0)),
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

  pub fn read_cmd_buffer_len(&self) -> usize {
    utils::read_atomic(&self.cmd_buffer_len)
  }

  pub fn read_redelivery_count(&self) -> usize {
    utils::read_atomic(&self.redelivery_count)
  }

  pub fn take_cmd_buffer_len(&self) -> usize {
    utils::set_atomic(&self.cmd_buffer_len, 0)
  }

  pub fn take_redelivery_count(&self) -> usize {
    utils::set_atomic(&self.redelivery_count, 0)
  }

  pub fn reset(&self) {
    utils::set_atomic(&self.cmd_buffer_len, 0);
    utils::set_atomic(&self.redelivery_count, 0);
  }
}

/// Added state associated with different server deployment types.
pub enum ServerState {
  Sentinel {
    /// An updated set of known sentinel nodes.
    sentinels: Vec<(String, u16)>,
    /// The server host/port resolved from the sentinel nodes, if known.
    primary:   Option<ArcStr>,
  },
  Cluster {
    /// The cached cluster routing table.
    cache: Option<ClusterRouting>,
  },
  Centralized,
}

impl ServerState {
  /// Create a new, empty server state cache.
  pub fn new(config: &RedisConfig) -> Self {
    match config.server {
      ServerConfig::Clustered { .. } => ServerState::Cluster { cache: None },
      ServerConfig::Sentinel { ref hosts, .. } => ServerState::Sentinel {
        sentinels: hosts.clone(),
        primary:   None,
      },
      ServerConfig::Centralized { .. } => ServerState::Centralized,
    }
  }
}

pub struct RedisClientInner {
  /// The client ID used for logging and the default `CLIENT SETNAME` value.
  pub id:            ArcStr,
  /// The RESP version used by the underlying connections.
  pub resp_version:  ArcSwap<RespVersion>,
  /// The state of the underlying connection.
  pub state:         RwLock<ClientState>,
  /// Client configuration options.
  pub config:        Arc<RedisConfig>,
  /// Performance config options for the client.
  pub performance:   ArcSwap<PerformanceConfig>,
  /// An optional reconnect policy.
  pub policy:        RwLock<Option<ReconnectPolicy>>,
  /// Notification channels for the event interfaces.
  pub notifications: Notifications,
  /// An mpsc sender for commands to the multiplexer.
  pub command_tx:    CommandSender,
  /// Temporary storage for the receiver half of the multiplexer command channel.
  pub command_rx:    RwLock<Option<CommandReceiver>>,
  /// Shared counters.
  pub counters:      ClientCounters,
  /// The DNS resolver to use when establishing new connections.
  // TODO make this generic via the Resolve trait
  pub resolver: DefaultResolver,
  /// A backchannel that can be used to control the multiplexer connections even while the connections are blocked.
  pub backchannel:   Arc<AsyncRwLock<Backchannel>>,
  /// Server state cache for various deployment types.
  pub server_state:  RwLock<ServerState>,

  /// Command latency metrics.
  #[cfg(feature = "metrics")]
  pub latency_stats:         RwLock<MovingStats>,
  /// Network latency metrics.
  #[cfg(feature = "metrics")]
  pub network_latency_stats: RwLock<MovingStats>,
  /// Payload size metrics tracking for requests.
  #[cfg(feature = "metrics")]
  pub req_size_stats:        Arc<RwLock<MovingStats>>,
  /// Payload size metrics tracking for responses
  #[cfg(feature = "metrics")]
  pub res_size_stats:        Arc<RwLock<MovingStats>>,
}

impl RedisClientInner {
  pub fn new(config: RedisConfig, perf: PerformanceConfig, policy: Option<ReconnectPolicy>) -> Arc<RedisClientInner> {
    let id = ArcStr::from(format!("fred-{}", utils::random_string(10)));
    let resolver = DefaultResolver::new(&id);
    let (command_tx, command_rx) = unbounded_channel();
    let notifications = Notifications::new(&id);
    let (config, policy) = (Arc::new(config), RwLock::new(policy));
    let performance = ArcSwap::new(Arc::new(perf));
    let resp_version = ArcSwap::new(Arc::new(config.version.clone()));
    let (counters, state) = (ClientCounters::default(), RwLock::new(ClientState::Disconnected));
    let (command_rx, policy) = (RwLock::new(Some(command_rx)), RwLock::new(policy));
    let backchannel = Arc::new(AsyncRwLock::new(Backchannel::default()));
    let server_state = RwLock::new(ServerState::new(&config));

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
      server_state,
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

  pub fn update_cluster_state(&self, state: Option<ClusterRouting>) {
    if let ServerState::Cluster { ref mut cache } = self.server_state.write() {
      *cache = state;
    }
  }

  pub fn num_cluster_nodes(&self) -> usize {
    if let ServerState::Cluster { ref cache } = self.server_state.read() {
      cache.map(|state| state.unique_primary_nodes().len()).unwrap_or(1)
    } else {
      1
    }
  }

  pub fn with_cluster_state<F, R>(&self, func: F) -> Result<R, RedisError>
  where
    F: FnOnce(&ClusterRouting) -> Result<R, RedisError>,
  {
    if let ServerState::Cluster { ref cache } = self.server_state.read() {
      if let Some(state) = cache.as_ref() {
        func(state)
      } else {
        Err(RedisError::new(
          RedisErrorKind::Cluster,
          "Missing cluster routing state.",
        ))
      }
    } else {
      Err(RedisError::new(
        RedisErrorKind::Cluster,
        "Missing cluster routing state.",
      ))
    }
  }

  pub fn with_perf_config<F, R>(&self, func: F) -> R
  where
    F: FnOnce(&PerformanceConfig) -> R,
  {
    let guard = self.performance.load();
    func(guard.as_ref())
  }

  pub fn update_sentinel_primary(&self, server: &ArcStr) {
    if let ServerState::Sentinel { ref mut primary, .. } = self.server_state.write() {
      *primary = Some(server.clone());
    }
  }

  pub fn sentinel_primary(&self) -> Option<ArcStr> {
    if let ServerState::Sentinel { ref primary, .. } = self.server_state.read() {
      primary.clone()
    } else {
      None
    }
  }

  pub fn update_sentinel_nodes(&self, nodes: Vec<(String, u16)>) {
    if let ServerState::Sentinel { ref mut sentinels, .. } = self.server_state.write() {
      *sentinels = nodes;
    }
  }

  pub fn read_sentinel_nodes(&self) -> Option<Vec<(String, u16)>> {
    if let ServerState::Sentinel { ref sentinels, .. } = self.server_state.read() {
      if sentinels.is_empty() {
        match self.config.server {
          ServerConfig::Sentinel { ref hosts, .. } => Some(hosts.clone()),
          _ => None,
        }
      } else {
        Some(sentinels.clone())
      }
    } else {
      None
    }
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
    *self.resp_version.load().as_ref() == RespVersion::RESP3
  }

  pub fn switch_protocol_versions(&self, version: RespVersion) {
    self.resp_version.store(Arc::new(version))
  }

  pub fn update_performance_config(&self, config: PerformanceConfig) {
    self.performance.store(Arc::new(config));
  }

  pub fn performance_config(&self) -> PerformanceConfig {
    self.performance.load().as_ref().clone()
  }

  pub fn reconnect_policy(&self) -> Option<ReconnectPolicy> {
    self.policy.read().as_ref().map(|p| p.clone())
  }

  pub fn reset_protocol_version(&self) {
    let version = self.config.version.clone();
    self.resp_version.as_ref().store(Arc::new(version));
  }

  pub fn max_command_attempts(&self) -> u32 {
    self.performance.load().max_command_attempts
  }

  pub fn max_feed_count(&self) -> u64 {
    self.performance.load().max_feed_count
  }

  pub fn default_command_timeout(&self) -> u64 {
    self.performance.load().default_command_timeout_ms
  }

  pub async fn set_blocked_server(&self, server: &ArcStr) {
    self.backchannel.write().await.set_blocked(server);
  }

  pub fn should_reconnect(&self) -> bool {
    let has_policy = self
      .policy
      .read()
      .map(|policy| policy.should_reconnect())
      .unwrap_or(false);

    // do not attempt a reconnection if the client is intentionally disconnecting
    has_policy && utils::read_locked(&self.state) != ClientState::Disconnecting
  }

  pub fn send_reconnect(&self, server: Option<ArcStr>, force: bool, tx: Option<ResponseSender>) {
    debug!("{}: Sending reconnect message to multiplexer for {:?}", self.id, server);
    let result = self
      .command_tx
      .send(MultiplexerCommand::Reconnect { server, force, tx });

    if let Err(_) = result {
      warn!("{}: Error sending reconnect command to multiplexer.", self.id);
    }
  }

  pub fn reset_reconnection_attempts(&self) {
    if let Some(policy) = self.policy.write().deref_mut() {
      policy.reset_attempts();
    }
  }

  pub fn should_cluster_sync(&self, error: &RedisError) -> bool {
    self.config.server.is_clustered() && error.is_cluster_error()
  }

  pub async fn update_backchannel(&self, transport: RedisTransport) {
    self.backchannel.write().await.transport = Some(transport);
  }

  pub async fn wait_with_interrupt(&self, duration: Duration) -> Result<(), RedisError> {
    let mut rx = self.notifications.close.subscribe();
    debug!("{}: Sleeping for {} ms", self.id, duration.as_millis());
    let (sleep_ft, recv_ft) = (sleep(duration), rx.recv());
    tokio::pin!(sleep_ft);
    tokio::pin!(recv_ft);

    if let Either::Right((_, _)) = select(sleep_ft, recv_ft).await {
      Err(RedisError::new(RedisErrorKind::Canceled, "Connection(s) closed."))
    } else {
      Ok(())
    }
  }
}
