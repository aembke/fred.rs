use crate::{
  error::*,
  globals::globals,
  interfaces,
  modules::backchannel::Backchannel,
  protocol::{
    command::{ResponseSender, RouterCommand},
    connection::RedisTransport,
    types::{ClusterRouting, DefaultResolver, Resolve, Server},
  },
  types::*,
  utils,
};
use arc_swap::ArcSwap;
use futures::future::{select, Either};
use parking_lot::{Mutex, RwLock};
use semver::Version;
use std::{
  ops::DerefMut,
  sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc,
  },
  time::Duration,
};
use tokio::{
  sync::{
    broadcast::{self, Sender as BroadcastSender},
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    RwLock as AsyncRwLock,
  },
  time::sleep,
};

#[cfg(feature = "metrics")]
use crate::modules::metrics::MovingStats;
#[cfg(feature = "check-unresponsive")]
use crate::router::types::NetworkTimeout;
use bytes_utils::Str;
#[cfg(feature = "replicas")]
use std::collections::HashMap;

pub type CommandSender = UnboundedSender<RouterCommand>;
pub type CommandReceiver = UnboundedReceiver<RouterCommand>;

#[cfg(feature = "client-tracking")]
use crate::types::Invalidation;

pub struct Notifications {
  /// The client ID.
  pub id:             Str,
  /// A broadcast channel for the `on_error` interface.
  pub errors:         ArcSwap<BroadcastSender<RedisError>>,
  /// A broadcast channel for the `on_message` interface.
  pub pubsub:         ArcSwap<BroadcastSender<Message>>,
  /// A broadcast channel for the `on_keyspace_event` interface.
  pub keyspace:       ArcSwap<BroadcastSender<KeyspaceEvent>>,
  /// A broadcast channel for the `on_reconnect` interface.
  pub reconnect:      ArcSwap<BroadcastSender<Server>>,
  /// A broadcast channel for the `on_cluster_change` interface.
  pub cluster_change: ArcSwap<BroadcastSender<Vec<ClusterStateChange>>>,
  /// A broadcast channel for the `on_connect` interface.
  pub connect:        ArcSwap<BroadcastSender<Result<(), RedisError>>>,
  /// A channel for events that should close all client tasks with `Canceled` errors.
  ///
  /// Emitted when QUIT, SHUTDOWN, etc are called.
  pub close:          BroadcastSender<()>,
  /// A broadcast channel for the `on_invalidation` interface.
  #[cfg(feature = "client-tracking")]
  pub invalidations:  ArcSwap<BroadcastSender<Invalidation>>,
  /// A broadcast channel for notifying callers when servers go unresponsive.
  #[cfg(feature = "check-unresponsive")]
  pub unresponsive:   ArcSwap<BroadcastSender<Server>>,
}

impl Notifications {
  pub fn new(id: &Str) -> Self {
    let capacity = globals().default_broadcast_channel_capacity();

    Notifications {
      id:                                                  id.clone(),
      close:                                               broadcast::channel(capacity).0,
      errors:                                              ArcSwap::new(Arc::new(broadcast::channel(capacity).0)),
      pubsub:                                              ArcSwap::new(Arc::new(broadcast::channel(capacity).0)),
      keyspace:                                            ArcSwap::new(Arc::new(broadcast::channel(capacity).0)),
      reconnect:                                           ArcSwap::new(Arc::new(broadcast::channel(capacity).0)),
      cluster_change:                                      ArcSwap::new(Arc::new(broadcast::channel(capacity).0)),
      connect:                                             ArcSwap::new(Arc::new(broadcast::channel(capacity).0)),
      #[cfg(feature = "client-tracking")]
      invalidations:                                       ArcSwap::new(Arc::new(broadcast::channel(capacity).0)),
      #[cfg(feature = "check-unresponsive")]
      unresponsive:                                        ArcSwap::new(Arc::new(broadcast::channel(capacity).0)),
    }
  }

  /// Replace the senders that have public receivers, closing the receivers in the process.
  pub fn close_public_receivers(&self) {
    utils::swap_new_broadcast_channel(&self.errors);
    utils::swap_new_broadcast_channel(&self.pubsub);
    utils::swap_new_broadcast_channel(&self.keyspace);
    utils::swap_new_broadcast_channel(&self.reconnect);
    utils::swap_new_broadcast_channel(&self.cluster_change);
    utils::swap_new_broadcast_channel(&self.connect);
    #[cfg(feature = "client-tracking")]
    utils::swap_new_broadcast_channel(&self.invalidations);
    #[cfg(feature = "check-unresponsive")]
    utils::swap_new_broadcast_channel(&self.unresponsive);
  }

  pub fn broadcast_error(&self, error: RedisError) {
    if let Err(_) = self.errors.load().send(error) {
      debug!("{}: No `on_error` listener.", self.id);
    }
  }

  pub fn broadcast_pubsub(&self, message: Message) {
    if let Err(_) = self.pubsub.load().send(message) {
      debug!("{}: No `on_message` listeners.", self.id);
    }
  }

  pub fn broadcast_keyspace(&self, event: KeyspaceEvent) {
    if let Err(_) = self.keyspace.load().send(event) {
      debug!("{}: No `on_keyspace_event` listeners.", self.id);
    }
  }

  pub fn broadcast_reconnect(&self, server: Server) {
    if let Err(_) = self.reconnect.load().send(server) {
      debug!("{}: No `on_reconnect` listeners.", self.id);
    }
  }

  pub fn broadcast_cluster_change(&self, changes: Vec<ClusterStateChange>) {
    if let Err(_) = self.cluster_change.load().send(changes) {
      debug!("{}: No `on_cluster_change` listeners.", self.id);
    }
  }

  pub fn broadcast_connect(&self, result: Result<(), RedisError>) {
    if let Err(_) = self.connect.load().send(result) {
      debug!("{}: No `on_connect` listeners.", self.id);
    }
  }

  pub fn broadcast_close(&self) {
    if let Err(_) = self.close.send(()) {
      debug!("{}: No `close` listeners.", self.id);
    }
  }

  #[cfg(feature = "client-tracking")]
  pub fn broadcast_invalidation(&self, msg: Invalidation) {
    if let Err(_) = self.invalidations.load().send(msg) {
      debug!("{}: No `on_invalidation` listeners.", self.id);
    }
  }

  #[cfg(feature = "check-unresponsive")]
  pub fn broadcast_unresponsive(&self, server: Server) {
    if let Err(_) = self.unresponsive.load().send(server) {
      debug!("{}: No unresponsive listeners", self.id);
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

/// Cached state related to the server(s).
pub struct ServerState {
  pub kind:     ServerKind,
  #[cfg(feature = "replicas")]
  pub replicas: HashMap<Server, Server>,
}

impl ServerState {
  pub fn new(config: &RedisConfig) -> Self {
    ServerState {
      kind:                                  ServerKind::new(config),
      #[cfg(feature = "replicas")]
      replicas:                              HashMap::new(),
    }
  }

  #[cfg(feature = "replicas")]
  pub fn update_replicas(&mut self, map: HashMap<Server, Server>) {
    self.replicas = map;
  }
}

/// Added state associated with different server deployment types.
pub enum ServerKind {
  Sentinel {
    version:   Option<Version>,
    /// An updated set of known sentinel nodes.
    sentinels: Vec<Server>,
    /// The server host/port resolved from the sentinel nodes, if known.
    primary:   Option<Server>,
  },
  Cluster {
    version: Option<Version>,
    /// The cached cluster routing table.
    cache:   Option<ClusterRouting>,
  },
  Centralized {
    version: Option<Version>,
  },
}

impl ServerKind {
  /// Create a new, empty server state cache.
  pub fn new(config: &RedisConfig) -> Self {
    match config.server {
      ServerConfig::Clustered { .. } => ServerKind::Cluster {
        version: None,
        cache:   None,
      },
      ServerConfig::Sentinel { ref hosts, .. } => ServerKind::Sentinel {
        version:   None,
        sentinels: hosts.clone(),
        primary:   None,
      },
      ServerConfig::Centralized { .. } => ServerKind::Centralized { version: None },
    }
  }

  pub fn set_server_version(&mut self, new_version: Version) {
    match self {
      ServerKind::Cluster { ref mut version, .. } => {
        *version = Some(new_version);
      },
      ServerKind::Centralized { ref mut version, .. } => {
        *version = Some(new_version);
      },
      ServerKind::Sentinel { ref mut version, .. } => {
        *version = Some(new_version);
      },
    }
  }

  pub fn server_version(&self) -> Option<Version> {
    match self {
      ServerKind::Cluster { ref version, .. } => version.clone(),
      ServerKind::Centralized { ref version, .. } => version.clone(),
      ServerKind::Sentinel { ref version, .. } => version.clone(),
    }
  }

  pub fn update_cluster_state(&mut self, state: Option<ClusterRouting>) {
    if let ServerKind::Cluster { ref mut cache, .. } = *self {
      *cache = state;
    }
  }

  pub fn num_cluster_nodes(&self) -> usize {
    if let ServerKind::Cluster { ref cache, .. } = *self {
      cache
        .as_ref()
        .map(|state| state.unique_primary_nodes().len())
        .unwrap_or(1)
    } else {
      1
    }
  }

  pub fn with_cluster_state<F, R>(&self, func: F) -> Result<R, RedisError>
  where
    F: FnOnce(&ClusterRouting) -> Result<R, RedisError>,
  {
    if let ServerKind::Cluster { ref cache, .. } = *self {
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

  pub fn update_sentinel_primary(&mut self, server: &Server) {
    if let ServerKind::Sentinel { ref mut primary, .. } = *self {
      *primary = Some(server.clone());
    }
  }

  pub fn sentinel_primary(&self) -> Option<Server> {
    if let ServerKind::Sentinel { ref primary, .. } = *self {
      primary.clone()
    } else {
      None
    }
  }

  pub fn update_sentinel_nodes(&mut self, server: &Server, nodes: Vec<Server>) {
    if let ServerKind::Sentinel {
      ref mut sentinels,
      ref mut primary,
      ..
    } = *self
    {
      *primary = Some(server.clone());
      *sentinels = nodes;
    }
  }

  pub fn read_sentinel_nodes(&self, config: &ServerConfig) -> Option<Vec<Server>> {
    if let ServerKind::Sentinel { ref sentinels, .. } = *self {
      if sentinels.is_empty() {
        match config {
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
}

// TODO make a config option for other defaults and extend this
fn create_resolver(id: &Str) -> Arc<dyn Resolve> {
  Arc::new(DefaultResolver::new(id))
}

pub struct RedisClientInner {
  /// An internal lock used to sync certain operations that should not run concurrently across tasks.
  pub _lock:         Mutex<()>,
  /// The client ID used for logging and the default `CLIENT SETNAME` value.
  pub id:            Str,
  /// Whether the client uses RESP3.
  pub resp3:         Arc<AtomicBool>,
  /// The state of the underlying connection.
  pub state:         RwLock<ClientState>,
  /// Client configuration options.
  pub config:        Arc<RedisConfig>,
  /// Connection configuration options.
  pub connection:    Arc<ConnectionConfig>,
  /// Performance config options for the client.
  pub performance:   ArcSwap<PerformanceConfig>,
  /// An optional reconnect policy.
  pub policy:        RwLock<Option<ReconnectPolicy>>,
  /// Notification channels for the event interfaces.
  pub notifications: Arc<Notifications>,
  /// An mpsc sender for commands to the router.
  pub command_tx:    ArcSwap<CommandSender>,
  /// Temporary storage for the receiver half of the router command channel.
  pub command_rx:    RwLock<Option<CommandReceiver>>,
  /// Shared counters.
  pub counters:      ClientCounters,
  /// The DNS resolver to use when establishing new connections.
  pub resolver:      AsyncRwLock<Arc<dyn Resolve>>,
  /// A backchannel that can be used to control the router connections even while the connections are blocked.
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
  /// Shared network timeout state with the router.
  #[cfg(feature = "check-unresponsive")]
  pub network_timeouts:      NetworkTimeout,
}

#[cfg(feature = "check-unresponsive")]
impl Drop for RedisClientInner {
  fn drop(&mut self) {
    if let Some(jh) = self.network_timeouts.take_handle() {
      trace!("{}: Ending network timeout task.", self.id);
      jh.abort();
    }
  }
}

impl RedisClientInner {
  pub fn new(
    config: RedisConfig,
    perf: PerformanceConfig,
    connection: ConnectionConfig,
    policy: Option<ReconnectPolicy>,
  ) -> Arc<RedisClientInner> {
    let id = Str::from(format!("fred-{}", utils::random_string(10)));
    let resolver = AsyncRwLock::new(create_resolver(&id));
    let (command_tx, command_rx) = unbounded_channel();
    let notifications = Arc::new(Notifications::new(&id));
    let (config, policy) = (Arc::new(config), RwLock::new(policy));
    let performance = ArcSwap::new(Arc::new(perf));
    let (counters, state) = (ClientCounters::default(), RwLock::new(ClientState::Disconnected));
    let command_rx = RwLock::new(Some(command_rx));
    let backchannel = Arc::new(AsyncRwLock::new(Backchannel::default()));
    let server_state = RwLock::new(ServerState::new(&config));
    let resp3 = if config.version == RespVersion::RESP3 {
      Arc::new(AtomicBool::new(true))
    } else {
      Arc::new(AtomicBool::new(false))
    };
    let connection = Arc::new(connection);
    let command_tx = ArcSwap::new(Arc::new(command_tx));

    let inner = Arc::new(RedisClientInner {
      _lock: Mutex::new(()),
      #[cfg(feature = "metrics")]
      latency_stats: RwLock::new(MovingStats::default()),
      #[cfg(feature = "metrics")]
      network_latency_stats: RwLock::new(MovingStats::default()),
      #[cfg(feature = "metrics")]
      req_size_stats: Arc::new(RwLock::new(MovingStats::default())),
      #[cfg(feature = "metrics")]
      res_size_stats: Arc::new(RwLock::new(MovingStats::default())),
      #[cfg(feature = "check-unresponsive")]
      network_timeouts: NetworkTimeout::new(),

      backchannel,
      command_rx,
      server_state,
      command_tx,
      state,
      counters,
      config,
      performance,
      policy,
      resp3,
      notifications,
      resolver,
      connection,
      id,
    });
    inner.spawn_timeout_task();
    inner
  }

  #[cfg(feature = "check-unresponsive")]
  pub fn spawn_timeout_task(self: &Arc<RedisClientInner>) {
    self.network_timeouts.spawn_task(self);
  }

  #[cfg(not(feature = "check-unresponsive"))]
  pub fn spawn_timeout_task(self: &Arc<RedisClientInner>) {}

  pub fn is_pipelined(&self) -> bool {
    self.performance.load().as_ref().auto_pipeline
  }

  #[cfg(feature = "replicas")]
  pub fn ignore_replica_reconnect_errors(&self) -> bool {
    self.connection.replica.ignore_reconnection_errors
  }

  #[cfg(not(feature = "replicas"))]
  pub fn ignore_replica_reconnect_errors(&self) -> bool {
    true
  }

  /// Swap the command channel sender, returning the old one.
  pub fn swap_command_tx(&self, tx: CommandSender) -> Arc<CommandSender> {
    self.command_tx.swap(Arc::new(tx))
  }

  /// Whether the client has the command channel receiver stored. If not then the caller can assume another
  /// connection/router instance is using it.
  pub fn has_command_rx(&self) -> bool {
    self.command_rx.read().is_some()
  }

  pub fn shared_resp3(&self) -> Arc<AtomicBool> {
    self.resp3.clone()
  }

  pub fn log_client_name_fn<F>(&self, level: log::Level, func: F)
  where
    F: FnOnce(&str),
  {
    if log_enabled!(level) {
      func(&self.id)
    }
  }

  pub async fn set_resolver(&self, resolver: Arc<dyn Resolve>) {
    let mut guard = self.resolver.write().await;
    *guard = resolver;
  }

  pub async fn get_resolver(&self) -> Arc<dyn Resolve> {
    self.resolver.read().await.clone()
  }

  pub fn client_name(&self) -> &str {
    &self.id
  }

  pub fn num_cluster_nodes(&self) -> usize {
    self.server_state.read().kind.num_cluster_nodes()
  }

  pub fn with_cluster_state<F, R>(&self, func: F) -> Result<R, RedisError>
  where
    F: FnOnce(&ClusterRouting) -> Result<R, RedisError>,
  {
    self.server_state.read().kind.with_cluster_state(func)
  }

  pub fn with_perf_config<F, R>(&self, func: F) -> R
  where
    F: FnOnce(&PerformanceConfig) -> R,
  {
    let guard = self.performance.load();
    func(guard.as_ref())
  }

  #[cfg(feature = "partial-tracing")]
  pub fn should_trace(&self) -> bool {
    self.config.tracing.enabled
  }

  #[cfg(feature = "partial-tracing")]
  pub fn tracing_span_level(&self) -> tracing::Level {
    self.config.tracing.default_tracing_level
  }

  #[cfg(feature = "full-tracing")]
  pub fn full_tracing_span_level(&self) -> tracing::Level {
    self.config.tracing.full_tracing_level
  }

  #[cfg(not(feature = "partial-tracing"))]
  pub fn should_trace(&self) -> bool {
    false
  }

  pub fn take_command_rx(&self) -> Option<CommandReceiver> {
    self.command_rx.write().take()
  }

  pub fn store_command_rx(&self, rx: CommandReceiver, force: bool) {
    let mut guard = self.command_rx.write();
    if guard.is_none() || force {
      *guard = Some(rx);
    }
  }

  pub fn is_resp3(&self) -> bool {
    utils::read_bool_atomic(&self.resp3)
  }

  pub fn switch_protocol_versions(&self, version: RespVersion) {
    match version {
      RespVersion::RESP3 => utils::set_bool_atomic(&self.resp3, true),
      RespVersion::RESP2 => utils::set_bool_atomic(&self.resp3, false),
    };
  }

  pub fn update_performance_config(&self, config: PerformanceConfig) {
    self.performance.store(Arc::new(config));
  }

  pub fn performance_config(&self) -> PerformanceConfig {
    self.performance.load().as_ref().clone()
  }

  pub fn connection_config(&self) -> ConnectionConfig {
    self.connection.as_ref().clone()
  }

  pub fn reconnect_policy(&self) -> Option<ReconnectPolicy> {
    self.policy.read().as_ref().map(|p| p.clone())
  }

  pub fn reset_protocol_version(&self) {
    let resp3 = match self.config.version {
      RespVersion::RESP3 => true,
      RespVersion::RESP2 => false,
    };

    utils::set_bool_atomic(&self.resp3, resp3);
  }

  pub fn max_command_attempts(&self) -> u32 {
    self.connection.max_command_attempts
  }

  pub fn max_feed_count(&self) -> u64 {
    self.performance.load().max_feed_count
  }

  pub fn default_command_timeout(&self) -> Duration {
    self.performance.load().default_command_timeout.clone()
  }

  pub fn connection_timeout(&self) -> Duration {
    self.connection.connection_timeout.clone()
  }

  pub fn internal_command_timeout(&self) -> Duration {
    self.connection.internal_command_timeout.clone()
  }

  pub async fn set_blocked_server(&self, server: &Server) {
    self.backchannel.write().await.set_blocked(server);
  }

  pub fn should_reconnect(&self) -> bool {
    let has_policy = self
      .policy
      .read()
      .as_ref()
      .map(|policy| policy.should_reconnect())
      .unwrap_or(false);

    // do not attempt a reconnection if the client is intentionally disconnecting. the QUIT and SHUTDOWN commands set
    // this flag.
    let is_disconnecting = utils::read_locked(&self.state) == ClientState::Disconnecting;

    debug!(
      "{}: Checking reconnect state. Has policy: {}, Is intentionally disconnecting: {}",
      self.id, has_policy, is_disconnecting,
    );
    has_policy && !is_disconnecting
  }

  pub fn send_reconnect(
    self: &Arc<RedisClientInner>,
    server: Option<Server>,
    force: bool,
    tx: Option<ResponseSender>,
  ) {
    debug!("{}: Sending reconnect message to router for {:?}", self.id, server);

    let cmd = RouterCommand::Reconnect {
      server,
      force,
      tx,
      #[cfg(feature = "replicas")]
      replica: false,
    };
    if let Err(_) = interfaces::send_to_router(self, cmd) {
      warn!("{}: Error sending reconnect command to router.", self.id);
    }
  }

  #[cfg(feature = "replicas")]
  pub fn send_replica_reconnect(self: &Arc<RedisClientInner>, server: &Server) {
    debug!(
      "{}: Sending replica reconnect message to router for {:?}",
      self.id, server
    );

    let cmd = RouterCommand::Reconnect {
      server:  Some(server.clone()),
      force:   false,
      tx:      None,
      replica: true,
    };
    if let Err(_) = interfaces::send_to_router(self, cmd) {
      warn!("{}: Error sending reconnect command to router.", self.id);
    }
  }

  pub fn reset_reconnection_attempts(&self) {
    if let Some(policy) = self.policy.write().deref_mut() {
      policy.reset_attempts();
    }
  }

  pub fn should_cluster_sync(&self, error: &RedisError) -> bool {
    self.config.server.is_clustered() && error.is_cluster()
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
