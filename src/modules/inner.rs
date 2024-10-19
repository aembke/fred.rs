use crate::{
  error::*,
  interfaces,
  modules::backchannel::Backchannel,
  protocol::{
    command::{ResponseSender, RouterCommand},
    connection::RedisTransport,
    types::{ClusterRouting, DefaultResolver, Resolve, Server},
  },
  runtime::{
    broadcast_channel,
    broadcast_send,
    sleep,
    unbounded_channel,
    AsyncRwLock,
    AtomicBool,
    AtomicUsize,
    BroadcastSender,
    Mutex,
    RefCount,
    RefSwap,
    RwLock,
    UnboundedReceiver,
    UnboundedSender,
  },
  types::*,
  utils,
};
use bytes_utils::Str;
use futures::future::{select, Either};
use semver::Version;
use std::{ops::DerefMut, time::Duration};

#[cfg(feature = "metrics")]
use crate::modules::metrics::MovingStats;
#[cfg(feature = "credential-provider")]
use crate::{
  clients::RedisClient,
  interfaces::RedisResult,
  interfaces::{AuthInterface, ClientLike},
  runtime::{spawn, JoinHandle},
};
#[cfg(feature = "replicas")]
use std::collections::HashMap;
use std::collections::HashSet;

pub type CommandSender = UnboundedSender<RouterCommand>;
pub type CommandReceiver = UnboundedReceiver<RouterCommand>;

#[cfg(feature = "i-tracking")]
use crate::types::Invalidation;

pub struct Notifications {
  /// The client ID.
  pub id:             Str,
  /// A broadcast channel for the `on_error` interface.
  pub errors:         RefSwap<RefCount<BroadcastSender<RedisError>>>,
  /// A broadcast channel for the `on_message` interface.
  pub pubsub:         RefSwap<RefCount<BroadcastSender<Message>>>,
  /// A broadcast channel for the `on_keyspace_event` interface.
  pub keyspace:       RefSwap<RefCount<BroadcastSender<KeyspaceEvent>>>,
  /// A broadcast channel for the `on_reconnect` interface.
  pub reconnect:      RefSwap<RefCount<BroadcastSender<Server>>>,
  /// A broadcast channel for the `on_cluster_change` interface.
  pub cluster_change: RefSwap<RefCount<BroadcastSender<Vec<ClusterStateChange>>>>,
  /// A broadcast channel for the `on_connect` interface.
  pub connect:        RefSwap<RefCount<BroadcastSender<Result<(), RedisError>>>>,
  /// A channel for events that should close all client tasks with `Canceled` errors.
  ///
  /// Emitted when QUIT, SHUTDOWN, etc are called.
  pub close:          BroadcastSender<()>,
  /// A broadcast channel for the `on_invalidation` interface.
  #[cfg(feature = "i-tracking")]
  pub invalidations:  RefSwap<RefCount<BroadcastSender<Invalidation>>>,
  /// A broadcast channel for notifying callers when servers go unresponsive.
  pub unresponsive:   RefSwap<RefCount<BroadcastSender<Server>>>,
}

impl Notifications {
  pub fn new(id: &Str, capacity: usize) -> Self {
    Notifications {
      id:                                           id.clone(),
      close:                                        broadcast_channel(capacity).0,
      errors:                                       RefSwap::new(RefCount::new(broadcast_channel(capacity).0)),
      pubsub:                                       RefSwap::new(RefCount::new(broadcast_channel(capacity).0)),
      keyspace:                                     RefSwap::new(RefCount::new(broadcast_channel(capacity).0)),
      reconnect:                                    RefSwap::new(RefCount::new(broadcast_channel(capacity).0)),
      cluster_change:                               RefSwap::new(RefCount::new(broadcast_channel(capacity).0)),
      connect:                                      RefSwap::new(RefCount::new(broadcast_channel(capacity).0)),
      #[cfg(feature = "i-tracking")]
      invalidations:                                RefSwap::new(RefCount::new(broadcast_channel(capacity).0)),
      unresponsive:                                 RefSwap::new(RefCount::new(broadcast_channel(capacity).0)),
    }
  }

  /// Replace the senders that have public receivers, closing the receivers in the process.
  pub fn close_public_receivers(&self, capacity: usize) {
    utils::swap_new_broadcast_channel(&self.errors, capacity);
    utils::swap_new_broadcast_channel(&self.pubsub, capacity);
    utils::swap_new_broadcast_channel(&self.keyspace, capacity);
    utils::swap_new_broadcast_channel(&self.reconnect, capacity);
    utils::swap_new_broadcast_channel(&self.cluster_change, capacity);
    utils::swap_new_broadcast_channel(&self.connect, capacity);
    #[cfg(feature = "i-tracking")]
    utils::swap_new_broadcast_channel(&self.invalidations, capacity);
    utils::swap_new_broadcast_channel(&self.unresponsive, capacity);
  }

  pub fn broadcast_error(&self, error: RedisError) {
    broadcast_send(self.errors.load().as_ref(), &error, |err| {
      debug!("{}: No `on_error` listener. The error was: {err:?}", self.id);
    });
  }

  pub fn broadcast_pubsub(&self, message: Message) {
    broadcast_send(self.pubsub.load().as_ref(), &message, |_| {
      debug!("{}: No `on_message` listeners.", self.id);
    });
  }

  pub fn broadcast_keyspace(&self, event: KeyspaceEvent) {
    broadcast_send(self.keyspace.load().as_ref(), &event, |_| {
      debug!("{}: No `on_keyspace_event` listeners.", self.id);
    });
  }

  pub fn broadcast_reconnect(&self, server: Server) {
    broadcast_send(self.reconnect.load().as_ref(), &server, |_| {
      debug!("{}: No `on_reconnect` listeners.", self.id);
    });
  }

  pub fn broadcast_cluster_change(&self, changes: Vec<ClusterStateChange>) {
    broadcast_send(self.cluster_change.load().as_ref(), &changes, |_| {
      debug!("{}: No `on_cluster_change` listeners.", self.id);
    });
  }

  pub fn broadcast_connect(&self, result: Result<(), RedisError>) {
    broadcast_send(self.connect.load().as_ref(), &result, |_| {
      debug!("{}: No `on_connect` listeners.", self.id);
    });
  }

  /// Interrupt any tokio `sleep` calls.
  //`RedisClientInner::wait_with_interrupt` hides the subscription part from callers.
  pub fn broadcast_close(&self) {
    broadcast_send(&self.close, &(), |_| {
      debug!("{}: No `close` listeners.", self.id);
    });
  }

  #[cfg(feature = "i-tracking")]
  pub fn broadcast_invalidation(&self, msg: Invalidation) {
    broadcast_send(self.invalidations.load().as_ref(), &msg, |_| {
      debug!("{}: No `on_invalidation` listeners.", self.id);
    });
  }

  pub fn broadcast_unresponsive(&self, server: Server) {
    broadcast_send(self.unresponsive.load().as_ref(), &server, |_| {
      debug!("{}: No unresponsive listeners", self.id);
    });
  }
}

#[derive(Clone)]
pub struct ClientCounters {
  pub cmd_buffer_len:   RefCount<AtomicUsize>,
  pub redelivery_count: RefCount<AtomicUsize>,
}

impl Default for ClientCounters {
  fn default() -> Self {
    ClientCounters {
      cmd_buffer_len:   RefCount::new(AtomicUsize::new(0)),
      redelivery_count: RefCount::new(AtomicUsize::new(0)),
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
  pub kind:        ServerKind,
  pub connections: HashSet<Server>,
  #[cfg(feature = "replicas")]
  pub replicas:    HashMap<Server, Server>,
}

impl ServerState {
  pub fn new(config: &RedisConfig) -> Self {
    ServerState {
      kind:                                  ServerKind::new(config),
      connections:                           HashSet::new(),
      #[cfg(feature = "replicas")]
      replicas:                              HashMap::new(),
    }
  }

  #[cfg(feature = "replicas")]
  pub fn update_replicas(&mut self, map: HashMap<Server, Server>) {
    self.replicas = map;
  }
}

/// Added state associated with different server deployment types, synchronized by the router task.
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
      #[cfg(feature = "unix-sockets")]
      ServerConfig::Unix { .. } => ServerKind::Centralized { version: None },
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
fn create_resolver(id: &Str) -> RefCount<dyn Resolve> {
  RefCount::new(DefaultResolver::new(id))
}

#[cfg(feature = "credential-provider")]
fn spawn_credential_refresh(client: RedisClient, interval: Duration) -> JoinHandle<RedisResult<()>> {
  spawn(async move {
    loop {
      trace!(
        "{}: Waiting {} ms before refreshing credentials.",
        client.inner.id,
        interval.as_millis()
      );
      client.inner.wait_with_interrupt(interval).await?;

      let (username, password) = match client.inner.config.credential_provider {
        Some(ref provider) => match provider.fetch(None).await {
          Ok(creds) => creds,
          Err(e) => {
            warn!("{}: Failed to fetch and refresh credentials: {e:?}", client.inner.id);
            continue;
          },
        },
        None => (None, None),
      };

      if client.state() != ClientState::Connected {
        debug!("{}: Skip credential refresh when disconnected", client.inner.id);
        continue;
      }

      if let Some(password) = password {
        if client.inner.config.version == RespVersion::RESP3 {
          let username = username.unwrap_or("default".into());
          let result = client
            .hello(RespVersion::RESP3, Some((username.into(), password.into())), None)
            .await;

          if let Err(err) = result {
            warn!("{}: Failed to refresh credentials: {err}", client.inner.id);
          }
        } else if let Err(err) = client.auth(username, password).await {
          warn!("{}: Failed to refresh credentials: {err}", client.inner.id);
        }
      }
    }
  })
}

pub struct RedisClientInner {
  /// An internal lock used to sync certain select operations that should not run concurrently across tasks.
  pub _lock:         Mutex<()>,
  /// The client ID used for logging and the default `CLIENT SETNAME` value.
  pub id:            Str,
  /// Whether the client uses RESP3.
  pub resp3:         RefCount<AtomicBool>,
  /// The state of the underlying connection.
  pub state:         RwLock<ClientState>,
  /// Client configuration options.
  pub config:        RefCount<RedisConfig>,
  /// Connection configuration options.
  pub connection:    RefCount<ConnectionConfig>,
  /// Performance config options for the client.
  pub performance:   RefSwap<RefCount<PerformanceConfig>>,
  /// An optional reconnect policy.
  pub policy:        RwLock<Option<ReconnectPolicy>>,
  /// Notification channels for the event interfaces.
  pub notifications: RefCount<Notifications>,
  /// Shared counters.
  pub counters:      ClientCounters,
  /// The DNS resolver to use when establishing new connections.
  pub resolver:      AsyncRwLock<RefCount<dyn Resolve>>,
  /// A backchannel that can be used to control the router connections even while the connections are blocked.
  pub backchannel:   RefCount<AsyncRwLock<Backchannel>>,
  /// Server state cache for various deployment types.
  pub server_state:  RwLock<ServerState>,

  /// An mpsc sender for commands to the router.
  pub command_tx: RefSwap<RefCount<CommandSender>>,
  /// Temporary storage for the receiver half of the router command channel.
  pub command_rx: RwLock<Option<CommandReceiver>>,

  /// A handle to a task that refreshes credentials on an interval.
  #[cfg(feature = "credential-provider")]
  pub credentials_task:      RwLock<Option<JoinHandle<RedisResult<()>>>>,
  /// Command latency metrics.
  #[cfg(feature = "metrics")]
  pub latency_stats:         RwLock<MovingStats>,
  /// Network latency metrics.
  #[cfg(feature = "metrics")]
  pub network_latency_stats: RwLock<MovingStats>,
  /// Payload size metrics tracking for requests.
  #[cfg(feature = "metrics")]
  pub req_size_stats:        RefCount<RwLock<MovingStats>>,
  /// Payload size metrics tracking for responses
  #[cfg(feature = "metrics")]
  pub res_size_stats:        RefCount<RwLock<MovingStats>>,
}

#[cfg(feature = "credential-provider")]
impl Drop for RedisClientInner {
  fn drop(&mut self) {
    self.abort_credential_refresh_task();
  }
}

impl RedisClientInner {
  pub fn new(
    config: RedisConfig,
    perf: PerformanceConfig,
    connection: ConnectionConfig,
    policy: Option<ReconnectPolicy>,
  ) -> RefCount<RedisClientInner> {
    let id = Str::from(format!("fred-{}", utils::random_string(10)));
    let resolver = AsyncRwLock::new(create_resolver(&id));
    let (command_tx, command_rx) = unbounded_channel();
    let notifications = RefCount::new(Notifications::new(&id, perf.broadcast_channel_capacity));
    let (config, policy) = (RefCount::new(config), RwLock::new(policy));
    let performance = RefSwap::new(RefCount::new(perf));
    let (counters, state) = (ClientCounters::default(), RwLock::new(ClientState::Disconnected));
    let command_rx = RwLock::new(Some(command_rx));
    let backchannel = RefCount::new(AsyncRwLock::new(Backchannel::default()));
    let server_state = RwLock::new(ServerState::new(&config));
    let resp3 = if config.version == RespVersion::RESP3 {
      RefCount::new(AtomicBool::new(true))
    } else {
      RefCount::new(AtomicBool::new(false))
    };
    let connection = RefCount::new(connection);
    #[cfg(feature = "glommio")]
    let command_tx = command_tx.into();
    let command_tx = RefSwap::new(RefCount::new(command_tx));

    RefCount::new(RedisClientInner {
      _lock: Mutex::new(()),
      #[cfg(feature = "metrics")]
      latency_stats: RwLock::new(MovingStats::default()),
      #[cfg(feature = "metrics")]
      network_latency_stats: RwLock::new(MovingStats::default()),
      #[cfg(feature = "metrics")]
      req_size_stats: RefCount::new(RwLock::new(MovingStats::default())),
      #[cfg(feature = "metrics")]
      res_size_stats: RefCount::new(RwLock::new(MovingStats::default())),
      #[cfg(feature = "credential-provider")]
      credentials_task: RwLock::new(None),

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
    })
  }

  pub fn add_connection(&self, server: &Server) {
    self.server_state.write().connections.insert(server.clone());
  }

  pub fn remove_connection(&self, server: &Server) {
    self.server_state.write().connections.remove(server);
  }

  pub fn active_connections(&self) -> Vec<Server> {
    self.server_state.read().connections.iter().cloned().collect()
  }

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
  pub fn swap_command_tx(&self, tx: CommandSender) -> RefCount<CommandSender> {
    self.command_tx.swap(RefCount::new(tx))
  }

  /// Whether the client has the command channel receiver stored. If not then the caller can assume another
  /// connection/router instance is using it.
  pub fn has_command_rx(&self) -> bool {
    self.command_rx.read().is_some()
  }

  pub fn reset_server_state(&self) {
    #[cfg(feature = "replicas")]
    self.server_state.write().replicas.clear()
  }

  pub fn shared_resp3(&self) -> RefCount<AtomicBool> {
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

  pub async fn set_resolver(&self, resolver: RefCount<dyn Resolve>) {
    let mut guard = self.resolver.write().await;
    *guard = resolver;
  }

  pub fn cluster_discovery_policy(&self) -> Option<&ClusterDiscoveryPolicy> {
    match self.config.server {
      ServerConfig::Clustered { ref policy, .. } => Some(policy),
      _ => None,
    }
  }

  pub async fn get_resolver(&self) -> RefCount<dyn Resolve> {
    self.resolver.write().await.clone()
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
    self.performance.store(RefCount::new(config));
  }

  pub fn performance_config(&self) -> PerformanceConfig {
    self.performance.load().as_ref().clone()
  }

  pub fn connection_config(&self) -> ConnectionConfig {
    self.connection.as_ref().clone()
  }

  pub fn reconnect_policy(&self) -> Option<ReconnectPolicy> {
    self.policy.read().as_ref().cloned()
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
    self.performance.load().default_command_timeout
  }

  pub fn connection_timeout(&self) -> Duration {
    self.connection.connection_timeout
  }

  pub fn internal_command_timeout(&self) -> Duration {
    self.connection.internal_command_timeout
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
    self: &RefCount<RedisClientInner>,
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
  pub fn send_replica_reconnect(self: &RefCount<RedisClientInner>, server: &Server) {
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
    #[allow(unused_mut)]
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

  #[cfg(not(feature = "glommio"))]
  pub fn send_command(&self, command: RouterCommand) -> Result<(), RouterCommand> {
    self.command_tx.load().send(command).map_err(|e| e.0)
  }

  #[cfg(feature = "glommio")]
  pub fn send_command(&self, command: RouterCommand) -> Result<(), RouterCommand> {
    self.command_tx.load().try_send(command).map_err(|e| match e {
      glommio::GlommioError::Closed(glommio::ResourceType::Channel(v)) => v,
      glommio::GlommioError::WouldBlock(glommio::ResourceType::Channel(v)) => v,
      _ => unreachable!(),
    })
  }

  #[cfg(not(feature = "credential-provider"))]
  pub async fn read_credentials(&self, _: &Server) -> Result<(Option<String>, Option<String>), RedisError> {
    Ok((self.config.username.clone(), self.config.password.clone()))
  }

  #[cfg(feature = "credential-provider")]
  pub async fn read_credentials(&self, server: &Server) -> Result<(Option<String>, Option<String>), RedisError> {
    Ok(if let Some(ref provider) = self.config.credential_provider {
      provider.fetch(Some(server)).await?
    } else {
      (self.config.username.clone(), self.config.password.clone())
    })
  }

  #[cfg(feature = "credential-provider")]
  pub fn reset_credential_refresh_task(self: &RefCount<Self>) {
    let mut guard = self.credentials_task.write();

    if let Some(task) = guard.take() {
      task.abort();
    }
    let refresh_interval = self
      .config
      .credential_provider
      .as_ref()
      .and_then(|provider| provider.refresh_interval());

    if let Some(interval) = refresh_interval {
      *guard = Some(spawn_credential_refresh(self.into(), interval));
    }
  }

  #[cfg(feature = "credential-provider")]
  pub fn abort_credential_refresh_task(&self) {
    if let Some(task) = self.credentials_task.write().take() {
      task.abort();
    }
  }
}
