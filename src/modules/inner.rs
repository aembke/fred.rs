use crate::client::CommandSender;
use crate::client::RedisClient;
use crate::error::*;
use crate::multiplexer::{ConnectionIDs, SentCommand};
use crate::protocol::connection::{
  create_authenticated_connection, create_authenticated_connection_tls, request_response_safe, FramedTcp, FramedTls,
  RedisTransport,
};
use crate::protocol::types::{DefaultResolver, RedisCommand};
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use parking_lot::RwLock;
use redis_protocol::resp2::types::Frame as ProtocolFrame;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Sender as OneshotSender;
use tokio::sync::RwLock as AsyncRwLock;
use tokio::task::JoinHandle;

#[cfg(feature = "metrics")]
use crate::modules::metrics::MovingStats;

/// State sent to the task that performs reconnection logic.
pub struct ClosedState {
  /// Commands that were in flight that can be retried again after reconnecting.
  pub commands: VecDeque<SentCommand>,
  /// The error that closed the last connection.
  pub error: RedisError,
}

pub type ConnectionClosedTx = UnboundedSender<ClosedState>;

async fn create_transport(
  inner: &Arc<RedisClientInner>,
  host: &str,
  port: u16,
  tls: bool,
) -> Result<RedisTransport, RedisError> {
  let addr = inner.resolver.resolve(host.to_owned(), port).await?;

  let transport = if tls {
    let transport = create_authenticated_connection_tls(&addr, host, inner).await?;
    RedisTransport::Tls(transport)
  } else {
    let transport = create_authenticated_connection(&addr, inner).await?;
    RedisTransport::Tcp(transport)
  };

  Ok(transport)
}

fn map_tcp_response(
  result: Result<(ProtocolFrame, FramedTcp), (RedisError, FramedTcp)>,
) -> Result<(ProtocolFrame, RedisTransport), (RedisError, RedisTransport)> {
  result
    .map(|(f, t)| (f, RedisTransport::Tcp(t)))
    .map_err(|(e, t)| (e, RedisTransport::Tcp(t)))
}

fn map_tls_response(
  result: Result<(ProtocolFrame, FramedTls), (RedisError, FramedTls)>,
) -> Result<(ProtocolFrame, RedisTransport), (RedisError, RedisTransport)> {
  result
    .map(|(f, t)| (f, RedisTransport::Tls(t)))
    .map_err(|(e, t)| (e, RedisTransport::Tls(t)))
}

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

/// A struct that allows for a backchannel to the server(s) even when the connections are blocked.
#[derive(Default)]
pub struct Backchannel {
  /// A connection to any of the servers, with the associated server name.
  pub transport: Option<(RedisTransport, Arc<String>)>,
  /// The server (host/port) that is blocked, if any.
  pub blocked: Option<Arc<String>>,
  /// A shared mapping of server IDs to connection IDs.
  pub connection_ids: Option<ConnectionIDs>,
}

impl Backchannel {
  /// Set the connection IDs from the multiplexer.
  pub fn set_connection_ids(&mut self, connection_ids: ConnectionIDs) {
    self.connection_ids = Some(connection_ids);
  }

  /// Read the connection ID for the provided server.
  pub fn connection_id(&self, server: &Arc<String>) -> Option<i64> {
    self
      .connection_ids
      .as_ref()
      .and_then(|connection_ids| match connection_ids {
        ConnectionIDs::Centralized(ref inner) => inner.read().clone(),
        ConnectionIDs::Clustered(ref inner) => inner.read().get(server).map(|i| *i),
      })
  }

  /// Set the blocked flag to the provided server.
  pub fn set_blocked(&mut self, server: Arc<String>) {
    self.blocked = Some(server);
  }

  /// Remove the blocked flag.
  pub fn set_unblocked(&mut self) {
    self.blocked = None;
  }

  /// Whether or not the client is blocked on a command.
  pub fn is_blocked(&self) -> bool {
    self.blocked.is_some()
  }

  /// Whether or not an open transport exists to the blocked server.
  pub fn has_blocked_transport(&self) -> bool {
    match self.blocked {
      Some(ref server) => match self.transport {
        Some((_, ref _server)) => server == _server,
        None => false,
      },
      None => false,
    }
  }

  /// Take the current transport or create a new one, returning the new transport, the server name, and whether a new connection was needed.
  pub async fn take_or_create_transport(
    &mut self,
    inner: &Arc<RedisClientInner>,
    host: &str,
    port: u16,
    uses_tls: bool,
  ) -> Result<(RedisTransport, Option<Arc<String>>, bool), RedisError> {
    if self.has_blocked_transport() {
      if let Some((transport, server)) = self.transport.take() {
        Ok((transport, Some(server), false))
      } else {
        _debug!(inner, "Creating backchannel to {}:{}", host, port);
        let transport = create_transport(inner, host, port, uses_tls).await?;
        Ok((transport, None, true))
      }
    } else {
      let _ = self.transport.take();
      _debug!(inner, "Creating backchannel to {}:{}", host, port);

      let transport = create_transport(inner, host, port, uses_tls).await?;
      Ok((transport, None, true))
    }
  }

  /// Send the provided command to the server at `host:port`.
  ///
  /// If an existing transport to the provided server is found this function will try to use it, but will automatically retry once if the connection is dead.
  /// If a new transport has to be created this function will create it, use it, and set it on `self` if the command succeeds.
  pub async fn request_response(
    &mut self,
    inner: &Arc<RedisClientInner>,
    server: &Arc<String>,
    cmd: RedisCommand,
  ) -> Result<ProtocolFrame, RedisError> {
    let uses_tls = inner.config.read().uses_tls();
    let (host, port) = protocol_utils::server_to_parts(server)?;

    let (transport, _server, try_once) = self.take_or_create_transport(inner, host, port, uses_tls).await?;
    let server = _server.unwrap_or(server.clone());
    let result = match transport {
      RedisTransport::Tcp(transport) => map_tcp_response(request_response_safe(transport, &cmd).await),
      RedisTransport::Tls(transport) => map_tls_response(request_response_safe(transport, &cmd).await),
    };

    match result {
      Ok((frame, transport)) => {
        _debug!(inner, "Created backchannel to {}", server);
        self.transport = Some((transport, server));
        Ok(frame)
      }
      Err((e, _)) => {
        if try_once {
          _warn!(inner, "Failed to create backchannel to {}", server);
          Err(e)
        } else {
          // need to avoid async recursion
          let (transport, _, _) = self.take_or_create_transport(inner, host, port, uses_tls).await?;
          let result = match transport {
            RedisTransport::Tcp(transport) => map_tcp_response(request_response_safe(transport, &cmd).await),
            RedisTransport::Tls(transport) => map_tls_response(request_response_safe(transport, &cmd).await),
          };

          match result {
            Ok((frame, transport)) => {
              self.transport = Some((transport, server));
              Ok(frame)
            }
            Err((e, _)) => Err(e),
          }
        }
      }
    }
  }
}

pub struct RedisClientInner {
  /// The client ID as seen by the server.
  pub id: Arc<String>,
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
  pub command_tx: RwLock<Option<CommandSender>>,
  /// An mpsc sender for pubsub messages to `on_message` streams.
  pub message_tx: RwLock<VecDeque<UnboundedSender<(String, RedisValue)>>>,
  /// An mpsc sender for pubsub messages to `on_keyspace_event` streams.
  pub keyspace_tx: RwLock<VecDeque<UnboundedSender<KeyspaceEvent>>>,
  /// An mpsc sender for reconnection events to `on_reconnect` streams.
  pub reconnect_tx: RwLock<VecDeque<UnboundedSender<RedisClient>>>,
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

    Arc::new(RedisClientInner {
      #[cfg(feature = "metrics")]
      latency_stats: RwLock::new(MovingStats::default()),
      #[cfg(feature = "metrics")]
      network_latency_stats: RwLock::new(MovingStats::default()),
      #[cfg(feature = "metrics")]
      req_size_stats: Arc::new(RwLock::new(MovingStats::default())),
      #[cfg(feature = "metrics")]
      res_size_stats: Arc::new(RwLock::new(MovingStats::default())),

      config: RwLock::new(config),
      policy: RwLock::new(None),
      state: RwLock::new(ClientState::Disconnected),
      error_tx: RwLock::new(VecDeque::new()),
      message_tx: RwLock::new(VecDeque::new()),
      keyspace_tx: RwLock::new(VecDeque::new()),
      reconnect_tx: RwLock::new(VecDeque::new()),
      connect_tx: RwLock::new(VecDeque::new()),
      command_tx: RwLock::new(None),
      reconnect_sleep_jh: RwLock::new(None),
      cmd_buffer_len: Arc::new(AtomicUsize::new(0)),
      redeliver_count: Arc::new(AtomicUsize::new(0)),
      connection_closed_tx: RwLock::new(None),
      multi_block: RwLock::new(None),
      cluster_state: RwLock::new(None),
      backchannel: Arc::new(AsyncRwLock::new(backchannel)),
      resolver,
      id,
    })
  }

  pub fn is_pipelined(&self) -> bool {
    self.config.read().pipeline
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

  #[cfg(feature = "partial-tracing")]
  pub fn should_trace(&self) -> bool {
    self.config.read().tracing
  }

  #[cfg(not(feature = "partial-tracing"))]
  pub fn should_trace(&self) -> bool {
    false
  }
}
