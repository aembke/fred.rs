use crate::clients::RedisClient;
use crate::error::*;
use crate::modules::backchannel::Backchannel;
use crate::multiplexer::SentCommand;
use crate::protocol::types::DefaultResolver;
use crate::protocol::types::RedisCommand;
use crate::types::*;
use crate::utils;
use arc_swap::ArcSwap;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender as OneshotSender;
use tokio::sync::RwLock as AsyncRwLock;
use tokio::task::JoinHandle;

#[cfg(feature = "metrics")]
use crate::modules::metrics::MovingStats;

pub type CommandSender = UnboundedSender<RedisCommand>;
pub type CommandReceiver = UnboundedReceiver<RedisCommand>;

/// State sent to the task that performs reconnection logic.
pub struct ClosedState {
  /// Commands that were in flight that can be retried again after reconnecting.
  pub commands: VecDeque<SentCommand>,
  /// The error that closed the last connection.
  pub error: RedisError,
}

pub type ConnectionClosedTx = UnboundedSender<ClosedState>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultiPolicy {
  /// The hash slot against which the transaction is running.
  pub hash_slot: Option<u16>,
  /// Whether or not to abort the transaction on an error.
  pub abort_on_error: bool,
  /// Whether or not the MULTI command has been sent. In clustered mode we defer sending the MULTI command until we know the hash slot.
  pub sent_multi: bool,
}

impl MultiPolicy {
  pub fn check_and_set_hash_slot(&mut self, slot: u16) -> Result<(), RedisError> {
    if let Some(old_slot) = self.hash_slot {
      if slot != old_slot {
        return Err(RedisError::new(
          RedisErrorKind::InvalidArgument,
          "Invalid hash slot. All commands inside a transaction must use the same hash slot.",
        ));
      }
    } else {
      self.hash_slot = Some(slot);
    }

    Ok(())
  }
}

/// A lock-free internal representation of the performance config options from the `RedisConfig`.
#[derive(Debug)]
pub struct InternalPerfConfig {
  pipeline: ArcSwap<bool>,
  max_command_attempts: Arc<AtomicUsize>,
  default_command_timeout: Arc<AtomicUsize>,
  max_feed_count: Arc<AtomicUsize>,
  cluster_cache_update_delay_ms: Arc<AtomicUsize>,
  disable_auto_backpressure: ArcSwap<bool>,
  disable_backpressure_scaling: ArcSwap<bool>,
  min_sleep_duration: Arc<AtomicUsize>,
  max_in_flight_commands: Arc<AtomicUsize>,
}

impl<'a> From<&'a RedisConfig> for InternalPerfConfig {
  fn from(config: &'a RedisConfig) -> Self {
    InternalPerfConfig {
      pipeline: ArcSwap::from(Arc::new(config.performance.pipeline)),
      max_command_attempts: Arc::new(AtomicUsize::new(config.performance.max_command_attempts as usize)),
      default_command_timeout: Arc::new(AtomicUsize::new(config.performance.default_command_timeout_ms as usize)),
      max_feed_count: Arc::new(AtomicUsize::new(config.performance.max_feed_count as usize)),
      cluster_cache_update_delay_ms: Arc::new(AtomicUsize::new(
        config.performance.cluster_cache_update_delay_ms as usize,
      )),
      disable_auto_backpressure: ArcSwap::from(Arc::new(config.performance.backpressure.disable_auto_backpressure)),
      disable_backpressure_scaling: ArcSwap::from(Arc::new(
        config.performance.backpressure.disable_backpressure_scaling,
      )),
      min_sleep_duration: Arc::new(AtomicUsize::new(
        config.performance.backpressure.min_sleep_duration_ms as usize,
      )),
      max_in_flight_commands: Arc::new(AtomicUsize::new(
        config.performance.backpressure.max_in_flight_commands as usize,
      )),
    }
  }
}

impl InternalPerfConfig {
  pub fn pipeline(&self) -> bool {
    *self.pipeline.load().as_ref()
  }

  pub fn max_command_attempts(&self) -> usize {
    utils::read_atomic(&self.max_command_attempts)
  }

  pub fn default_command_timeout(&self) -> usize {
    utils::read_atomic(&self.default_command_timeout)
  }

  pub fn max_feed_count(&self) -> usize {
    utils::read_atomic(&self.max_feed_count)
  }

  pub fn cluster_cache_update_delay_ms(&self) -> usize {
    utils::read_atomic(&self.cluster_cache_update_delay_ms)
  }

  pub fn disable_auto_backpressure(&self) -> bool {
    *self.disable_auto_backpressure.load().as_ref()
  }

  pub fn disable_backpressure_scaling(&self) -> bool {
    *self.disable_backpressure_scaling.load().as_ref()
  }

  pub fn min_sleep_duration(&self) -> usize {
    utils::read_atomic(&self.min_sleep_duration)
  }

  pub fn max_in_flight_commands(&self) -> usize {
    utils::read_atomic(&self.max_in_flight_commands)
  }

  pub fn update(&self, config: &PerformanceConfig) {
    self.pipeline.store(Arc::new(config.pipeline));
    self
      .disable_backpressure_scaling
      .store(Arc::new(config.backpressure.disable_backpressure_scaling));
    self
      .disable_auto_backpressure
      .store(Arc::new(config.backpressure.disable_auto_backpressure));

    utils::set_atomic(&self.max_command_attempts, config.max_command_attempts as usize);
    utils::set_atomic(
      &self.default_command_timeout,
      config.default_command_timeout_ms as usize,
    );
    utils::set_atomic(&self.max_feed_count, config.max_feed_count as usize);
    utils::set_atomic(
      &self.cluster_cache_update_delay_ms,
      config.cluster_cache_update_delay_ms as usize,
    );
    utils::set_atomic(
      &self.min_sleep_duration,
      config.backpressure.min_sleep_duration_ms as usize,
    );
    utils::set_atomic(
      &self.max_in_flight_commands,
      config.backpressure.max_in_flight_commands as usize,
    );
  }
}

pub struct RedisClientInner {
  /// The client ID as seen by the server.
  pub id: Arc<String>,
  /// The RESP version used by the underlying connections.
  pub resp_version: Arc<ArcSwap<RespVersion>>,
  /// The response policy to apply when the client is in a MULTI block.
  pub multi_block: RwLock<Option<MultiPolicy>>,
  /// The state of the underlying connection.
  pub state: RwLock<ClientState>,
  /// The redis config used for initializing connections.
  pub config: RwLock<RedisConfig>,
  /// An optional reconnect policy.
  pub policy: RwLock<Option<ReconnectPolicy>>,
  /// An mpsc sender for errors to `on_error` streams.
  pub error_tx: RwLock<VecDeque<UnboundedSender<RedisError>>>,
  /// An mpsc sender for commands to the multiplexer.
  pub command_tx: CommandSender,
  /// Temporary storage for the receiver half of the multiplexer command channel.
  pub command_rx: RwLock<Option<CommandReceiver>>,
  /// An mpsc sender for pubsub messages to `on_message` streams.
  pub message_tx: RwLock<VecDeque<UnboundedSender<(String, RedisValue)>>>,
  /// An mpsc sender for pubsub messages to `on_keyspace_event` streams.
  pub keyspace_tx: RwLock<VecDeque<UnboundedSender<KeyspaceEvent>>>,
  /// An mpsc sender for reconnection events to `on_reconnect` streams.
  pub reconnect_tx: RwLock<VecDeque<UnboundedSender<RedisClient>>>,
  /// An mpsc sender for cluster change notifications.
  pub cluster_change_tx: RwLock<VecDeque<UnboundedSender<Vec<ClusterStateChange>>>>,
  /// MPSC senders for `on_connect` futures.
  pub connect_tx: RwLock<VecDeque<OneshotSender<Result<(), RedisError>>>>,
  /// A join handle for the task that sleeps waiting to reconnect.
  pub reconnect_sleep_jh: RwLock<Option<JoinHandle<Result<(), ()>>>>,
  /// Command queue buffer size.
  pub cmd_buffer_len: Arc<AtomicUsize>,
  /// Number of message redeliveries.
  pub redeliver_count: Arc<AtomicUsize>,
  /// Channel listening to connection closed events.
  pub connection_closed_tx: RwLock<Option<ConnectionClosedTx>>,
  /// The cached view of the cluster state, if running against a clustered deployment.
  pub cluster_state: RwLock<Option<ClusterKeyCache>>,
  /// The DNS resolver to use when establishing new connections.
  pub resolver: DefaultResolver,
  /// A backchannel that can be used to control the multiplexer connections even while the connections are blocked.
  pub backchannel: Arc<AsyncRwLock<Backchannel>>,
  /// The server host/port resolved from the sentinel nodes, if known.
  pub sentinel_primary: RwLock<Option<Arc<String>>>,
  /// The internal representation of the performance config options from the `RedisConfig`.
  pub perf_config: Arc<InternalPerfConfig>,
  /// An mpsc sender for `cluster_resync` requests.
  pub cluster_resync_tx: RwLock<Option<UnboundedSender<()>>>,

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

impl RedisClientInner {
  pub fn new(config: RedisConfig) -> Arc<RedisClientInner> {
    let backchannel = Backchannel::default();
    let id = Arc::new(format!("fred-{}", utils::random_string(10)));
    let resolver = DefaultResolver::new(&id);
    let (command_tx, command_rx) = unbounded_channel();
    let version = config.version.clone();
    let perf_config = InternalPerfConfig::from(&config);

    Arc::new(RedisClientInner {
      #[cfg(feature = "metrics")]
      latency_stats: RwLock::new(MovingStats::default()),
      #[cfg(feature = "metrics")]
      network_latency_stats: RwLock::new(MovingStats::default()),
      #[cfg(feature = "metrics")]
      req_size_stats: Arc::new(RwLock::new(MovingStats::default())),
      #[cfg(feature = "metrics")]
      res_size_stats: Arc::new(RwLock::new(MovingStats::default())),

      resp_version: Arc::new(ArcSwap::from(Arc::new(version))),
      perf_config: Arc::new(perf_config),
      config: RwLock::new(config),
      policy: RwLock::new(None),
      state: RwLock::new(ClientState::Disconnected),
      error_tx: RwLock::new(VecDeque::new()),
      message_tx: RwLock::new(VecDeque::new()),
      keyspace_tx: RwLock::new(VecDeque::new()),
      reconnect_tx: RwLock::new(VecDeque::new()),
      cluster_change_tx: RwLock::new(VecDeque::new()),
      connect_tx: RwLock::new(VecDeque::new()),
      reconnect_sleep_jh: RwLock::new(None),
      cmd_buffer_len: Arc::new(AtomicUsize::new(0)),
      redeliver_count: Arc::new(AtomicUsize::new(0)),
      connection_closed_tx: RwLock::new(None),
      multi_block: RwLock::new(None),
      cluster_state: RwLock::new(None),
      backchannel: Arc::new(AsyncRwLock::new(backchannel)),
      sentinel_primary: RwLock::new(None),
      command_rx: RwLock::new(Some(command_rx)),
      command_tx,
      resolver,
      id,
      cluster_resync_tx: RwLock::new(None),
    })
  }

  pub fn is_pipelined(&self) -> bool {
    self.perf_config.pipeline()
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

  pub fn client_name_ref(&self) -> &Arc<String> {
    &self.id
  }

  pub fn update_cluster_state(&self, state: Option<ClusterKeyCache>) {
    let mut guard = self.cluster_state.write();
    *guard = state;
  }

  pub fn update_sentinel_primary(&self, server: &Arc<String>) {
    let mut guard = self.sentinel_primary.write();
    *guard = Some(server.clone());
  }

  #[cfg(feature = "partial-tracing")]
  pub fn should_trace(&self) -> bool {
    self.config.read().tracing
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

  pub fn reset_protocol_version(&self) {
    let version = self.config.read().version.clone();
    self.resp_version.as_ref().store(Arc::new(version));
  }
}
