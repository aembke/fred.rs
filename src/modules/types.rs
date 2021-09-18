use crate::client::RedisClient;
use crate::error::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::connection::OK;
use crate::protocol::types::{KeyScanInner, RedisCommand, RedisCommandKind, ValueScanInner};
use crate::protocol::utils as protocol_utils;
use crate::utils;
pub use redis_protocol::resp2::types::Frame;
use redis_protocol::resp2::types::NULL;
use std::borrow::Cow;
use std::cmp;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::iter::FromIterator;
use std::mem;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::str;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub use crate::modules::response::RedisResponse;
pub use crate::protocol::tls::TlsConfig;
pub use crate::protocol::types::{ClusterKeyCache, SlotRange};

#[cfg(feature = "metrics")]
pub use crate::modules::metrics::Stats;

#[cfg(feature = "index-map")]
use indexmap::{IndexMap, IndexSet};
#[cfg(not(feature = "index-map"))]
use std::collections::HashSet;

pub(crate) static QUEUED: &'static str = "QUEUED";
pub(crate) static NIL: &'static str = "nil";

/// The ANY flag used on certain GEO commands.
pub type Any = bool;
/// The result from any of the `connect` functions showing the error that closed the connection, if any.
pub type ConnectHandle = JoinHandle<Result<(), RedisError>>;
/// A tuple of `(offset, count)` values for commands that allow paging through results.
pub type Limit = (i64, i64);

/// Arguments passed to the SHUTDOWN command.
///
/// <https://redis.io/commands/shutdown>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShutdownFlags {
  Save,
  NoSave,
}

impl ShutdownFlags {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ShutdownFlags::Save => "SAVE",
      ShutdownFlags::NoSave => "NOSAVE",
    }
  }
}

/// An event on the publish-subscribe interface describing a keyspace notification.
///
/// <https://redis.io/topics/notifications>
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct KeyspaceEvent {
  pub db: u8,
  pub operation: String,
  pub key: String,
}

/// Aggregate options for the [zinterstore](https://redis.io/commands/zinterstore) (and related) commands.
pub enum AggregateOptions {
  Sum,
  Min,
  Max,
}

impl AggregateOptions {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      AggregateOptions::Sum => "SUM",
      AggregateOptions::Min => "MIN",
      AggregateOptions::Max => "MAX",
    }
  }
}

/// The types of values supported by the [type](https://redis.io/commands/type) command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScanType {
  Set,
  String,
  ZSet,
  List,
  Hash,
  Stream,
}

impl ScanType {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ScanType::Set => "set",
      ScanType::String => "string",
      ScanType::List => "list",
      ScanType::ZSet => "zset",
      ScanType::Hash => "hash",
      ScanType::Stream => "stream",
    }
  }
}

/// The result of a SCAN operation.
pub struct ScanResult {
  pub(crate) results: Option<Vec<RedisKey>>,
  pub(crate) inner: Arc<RedisClientInner>,
  pub(crate) args: Vec<RedisValue>,
  pub(crate) scan_state: KeyScanInner,
  pub(crate) can_continue: bool,
}

impl ScanResult {
  /// Read the current cursor from the SCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the SCAN operation.
  pub fn results(&self) -> &Option<Vec<RedisKey>> {
    &self.results
  }

  /// Take ownership over the results of the SCAN operation. Calls to `results` or `take_results` will return `None` afterwards.
  pub fn take_results(&mut self) -> Option<Vec<RedisKey>> {
    self.results.take()
  }

  /// Move on to the next page of results from the SCAN operation. If no more results are available this may close the stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the background since
  /// this could cause the  buffer backing the stream to grow too large very quickly. This interface provides a mechanism
  /// for throttling the throughput of the SCAN call. If this struct is dropped without calling this function the stream will
  /// close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other fatal error
  /// has occurred. If this happens the error will appear in the stream from the original SCAN call.
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let kind = RedisCommandKind::Scan(self.scan_state);
    let cmd = RedisCommand::new(kind, self.args, None);
    utils::send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the SCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `scan` again on the client will initiate a new SCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }
}

/// The result of a HSCAN operation.
pub struct HScanResult {
  pub(crate) results: Option<RedisMap>,
  pub(crate) inner: Arc<RedisClientInner>,
  pub(crate) args: Vec<RedisValue>,
  pub(crate) scan_state: ValueScanInner,
  pub(crate) can_continue: bool,
}

impl HScanResult {
  /// Read the current cursor from the SCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the HSCAN operation.
  pub fn results(&self) -> &Option<RedisMap> {
    &self.results
  }

  /// Take ownership over the results of the HSCAN operation. Calls to `results` or `take_results` will return `None` afterwards.
  pub fn take_results(&mut self) -> Option<RedisMap> {
    self.results.take()
  }

  /// Move on to the next page of results from the HSCAN operation. If no more results are available this may close the stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the background since
  /// this could cause the  buffer backing the stream to grow too large very quickly. This interface provides a mechanism
  /// for throttling the throughput of the SCAN call. If this struct is dropped without calling this function the stream will
  /// close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other fatal error
  /// has occurred. If this happens the error will appear in the stream from the original SCAN call.
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let kind = RedisCommandKind::Hscan(self.scan_state);
    let cmd = RedisCommand::new(kind, self.args, None);
    utils::send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the HSCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `hscan` again on the client will initiate a new HSCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }
}

/// The result of a SCAN operation.
pub struct SScanResult {
  pub(crate) results: Option<Vec<RedisValue>>,
  pub(crate) inner: Arc<RedisClientInner>,
  pub(crate) args: Vec<RedisValue>,
  pub(crate) scan_state: ValueScanInner,
  pub(crate) can_continue: bool,
}

impl SScanResult {
  /// Read the current cursor from the SSCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the SCAN operation.
  pub fn results(&self) -> &Option<Vec<RedisValue>> {
    &self.results
  }

  /// Take ownership over the results of the SSCAN operation. Calls to `results` or `take_results` will return `None` afterwards.
  pub fn take_results(&mut self) -> Option<Vec<RedisValue>> {
    self.results.take()
  }

  /// Move on to the next page of results from the SSCAN operation. If no more results are available this may close the stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the background since
  /// this could cause the  buffer backing the stream to grow too large very quickly. This interface provides a mechanism
  /// for throttling the throughput of the SCAN call. If this struct is dropped without calling this function the stream will
  /// close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other fatal error
  /// has occurred. If this happens the error will appear in the stream from the original SCAN call.
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let kind = RedisCommandKind::Sscan(self.scan_state);
    let cmd = RedisCommand::new(kind, self.args, None);
    utils::send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the SSCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `sscan` again on the client will initiate a new SSCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }
}

/// The result of a SCAN operation.
pub struct ZScanResult {
  pub(crate) results: Option<Vec<(RedisValue, f64)>>,
  pub(crate) inner: Arc<RedisClientInner>,
  pub(crate) args: Vec<RedisValue>,
  pub(crate) scan_state: ValueScanInner,
  pub(crate) can_continue: bool,
}

impl ZScanResult {
  /// Read the current cursor from the ZSCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the ZSCAN operation.
  pub fn results(&self) -> &Option<Vec<(RedisValue, f64)>> {
    &self.results
  }

  /// Take ownership over the results of the ZSCAN operation. Calls to `results` or `take_results` will return `None` afterwards.
  pub fn take_results(&mut self) -> Option<Vec<(RedisValue, f64)>> {
    self.results.take()
  }

  /// Move on to the next page of results from the ZSCAN operation. If no more results are available this may close the stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the background since
  /// this could cause the  buffer backing the stream to grow too large very quickly. This interface provides a mechanism
  /// for throttling the throughput of the SCAN call. If this struct is dropped without calling this function the stream will
  /// close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other fatal error
  /// has occurred. If this happens the error will appear in the stream from the original SCAN call.
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let kind = RedisCommandKind::Zscan(self.scan_state);
    let cmd = RedisCommand::new(kind, self.args, None);
    utils::send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the ZSCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `zscan` again on the client will initiate a new ZSCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }
}

/// Options for the [info](https://redis.io/commands/info) command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InfoKind {
  Default,
  All,
  Keyspace,
  Cluster,
  CommandStats,
  Cpu,
  Replication,
  Stats,
  Persistence,
  Memory,
  Clients,
  Server,
}

impl InfoKind {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      InfoKind::Default => "default",
      InfoKind::All => "all",
      InfoKind::Keyspace => "keyspace",
      InfoKind::Cluster => "cluster",
      InfoKind::CommandStats => "commandstats",
      InfoKind::Cpu => "cpu",
      InfoKind::Replication => "replication",
      InfoKind::Stats => "stats",
      InfoKind::Persistence => "persistence",
      InfoKind::Memory => "memory",
      InfoKind::Clients => "clients",
      InfoKind::Server => "server",
    }
  }
}

/// Configuration for custom redis commands, primarily used for interacting with third party modules or extensions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomCommand {
  /// The command name, sent directly to the server.
  pub cmd: &'static str,
  /// The hash slot to use for the provided command when running against a cluster. If a hash slot is not provided the command will run against a random node in the cluster.
  pub hash_slot: Option<u16>,
  /// Whether or not the command should block the connection while waiting on a response.
  pub is_blocking: bool,
}

/// The type of reconnection policy to use. This will apply to every connection used by the client.
///
/// Use a `max_attempts` value of `0` to retry forever.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconnectPolicy {
  /// Wait a constant amount of time between reconnect attempts, in ms.
  Constant {
    attempts: u32,
    max_attempts: u32,
    delay: u32,
  },
  /// Backoff reconnection attempts linearly, adding `delay` each time.
  Linear {
    attempts: u32,
    max_attempts: u32,
    max_delay: u32,
    delay: u32,
  },
  /// Backoff reconnection attempts exponentially, multiplying the last delay by `mult` each time.
  Exponential {
    attempts: u32,
    max_attempts: u32,
    min_delay: u32,
    max_delay: u32,
    mult: u32,
  },
}

impl Default for ReconnectPolicy {
  fn default() -> Self {
    ReconnectPolicy::Constant {
      attempts: 0,
      max_attempts: 0,
      delay: 1000,
    }
  }
}

impl ReconnectPolicy {
  /// Create a new reconnect policy with a constant backoff.
  pub fn new_constant(max_attempts: u32, delay: u32) -> ReconnectPolicy {
    ReconnectPolicy::Constant {
      max_attempts,
      delay,
      attempts: 0,
    }
  }

  /// Create a new reconnect policy with a linear backoff.
  pub fn new_linear(max_attempts: u32, max_delay: u32, delay: u32) -> ReconnectPolicy {
    ReconnectPolicy::Linear {
      max_attempts,
      max_delay,
      delay,
      attempts: 0,
    }
  }

  /// Create a new reconnect policy with an exponential backoff.
  pub fn new_exponential(max_attempts: u32, min_delay: u32, max_delay: u32, mult: u32) -> ReconnectPolicy {
    ReconnectPolicy::Exponential {
      max_delay,
      max_attempts,
      min_delay,
      mult,
      attempts: 0,
    }
  }

  /// Reset the number of reconnection attempts. It's unlikely users will need to call this.
  pub fn reset_attempts(&mut self) {
    match *self {
      ReconnectPolicy::Constant { ref mut attempts, .. } => {
        *attempts = 0;
      }
      ReconnectPolicy::Linear { ref mut attempts, .. } => {
        *attempts = 0;
      }
      ReconnectPolicy::Exponential { ref mut attempts, .. } => {
        *attempts = 0;
      }
    }
  }

  /// Read the number of reconnection attempts.
  pub fn attempts(&self) -> u32 {
    match *self {
      ReconnectPolicy::Constant { ref attempts, .. } => *attempts,
      ReconnectPolicy::Linear { ref attempts, .. } => *attempts,
      ReconnectPolicy::Exponential { ref attempts, .. } => *attempts,
    }
  }

  /// Calculate the next delay, incrementing `attempts` in the process.
  pub fn next_delay(&mut self) -> Option<u64> {
    match *self {
      ReconnectPolicy::Constant {
        ref mut attempts,
        delay,
        max_attempts,
      } => {
        *attempts = match utils::incr_with_max(*attempts, max_attempts) {
          Some(a) => a,
          None => return None,
        };

        Some(delay as u64)
      }
      ReconnectPolicy::Linear {
        ref mut attempts,
        max_delay,
        max_attempts,
        delay,
      } => {
        *attempts = match utils::incr_with_max(*attempts, max_attempts) {
          Some(a) => a,
          None => return None,
        };

        Some(cmp::min(
          max_delay as u64,
          (delay as u64).saturating_mul(*attempts as u64),
        ))
      }
      ReconnectPolicy::Exponential {
        ref mut attempts,
        min_delay,
        max_delay,
        max_attempts,
        mult,
      } => {
        *attempts = match utils::incr_with_max(*attempts, max_attempts) {
          Some(a) => a,
          None => return None,
        };

        Some(cmp::min(
          max_delay as u64,
          (mult as u64).pow(*attempts - 1).saturating_mul(min_delay as u64),
        ))
      }
    }
  }
}

/// Describes how the client should respond when a command is sent while the client is in a blocked state from a blocking command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Blocking {
  /// Wait to send the command until the blocked command finishes. (Default)
  Block,
  /// Return an error to the caller.
  Error,
  /// Interrupt the blocked command by automatically sending `CLIENT UNBLOCK` for the blocked connection.
  Interrupt,
}

impl Default for Blocking {
  fn default() -> Self {
    Blocking::Block
  }
}

/// Configuration options for a `RedisClient`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisConfig {
  /// Whether or not the client should return an error if it cannot connect to the server the first time when being initialized.
  /// If `false` the client will run the reconnect logic if it cannot connect to the server the first time, but if `true` the client
  /// will return initial connection errors to the caller immediately.
  ///
  /// Normally the reconnection logic only applies to connections that close unexpectedly, but this flag can apply the same logic to
  /// the first connection as it is being created.
  ///
  /// Note: Callers should use caution setting this to `false` since it can make debugging configuration issues more difficult.
  ///
  /// Default: `true`
  pub fail_fast: bool,
  /// Whether or not the client should automatically pipeline commands when possible.
  ///
  /// Default: `true`
  pub pipeline: bool,
  /// The default behavior of the client when a command is sent while the connection is blocked on a blocking command.
  ///
  /// Default: `Blocking::Block`
  pub blocking: Blocking,
  /// An optional ACL username for the client to use when authenticating. If ACL rules are not configured this should be `None`.
  ///
  /// Default: `None`
  pub username: Option<String>,
  /// An optional password for the client to use when authenticating.
  ///
  /// Default: `None`
  pub password: Option<String>,
  /// Connection configuration for the server(s).
  ///
  /// Default: `Centralized(localhost, 6379)`
  pub server: ServerConfig,
  /// TLS configuration fields. If `None` the connection will not use TLS.
  ///
  /// Default: `None`
  #[cfg(feature = "enable-tls")]
  pub tls: Option<TlsConfig>,
  /// Whether or not to enable tracing for this client.
  ///
  /// Default: `false`
  #[cfg(feature = "partial-tracing")]
  pub tracing: bool,
}

impl Default for RedisConfig {
  fn default() -> Self {
    RedisConfig {
      fail_fast: true,
      pipeline: true,
      blocking: Blocking::default(),
      username: None,
      password: None,
      server: ServerConfig::default(),
      #[cfg(feature = "enable-tls")]
      tls: None,
      #[cfg(feature = "partial-tracing")]
      tracing: false,
    }
  }
}

impl RedisConfig {
  /// Whether or not the client uses TLS.
  #[cfg(feature = "enable-tls")]
  pub fn uses_tls(&self) -> bool {
    self.tls.is_some()
  }

  /// Whether or not the client uses TLS.
  #[cfg(not(feature = "enable-tls"))]
  pub fn uses_tls(&self) -> bool {
    false
  }
}

/// Connection configuration for the Redis server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServerConfig {
  Centralized {
    /// The hostname or IP address of the Redis server.
    host: String,
    /// The port on which the Redis server is listening.
    port: u16,
  },
  Clustered {
    /// An array of `(host, port)` tuples for nodes in the cluster. Only one node in the cluster needs to be provided here,
    /// the rest will be discovered via the `CLUSTER NODES` command.
    hosts: Vec<(String, u16)>,
  },
}

impl Default for ServerConfig {
  fn default() -> Self {
    ServerConfig::default_centralized()
  }
}

impl ServerConfig {
  /// Create a new centralized config with the provided host and port.
  pub fn new_centralized<S>(host: S, port: u16) -> ServerConfig
  where
    S: Into<String>,
  {
    ServerConfig::Centralized {
      host: host.into(),
      port,
    }
  }

  /// Create a new clustered config with the provided set of hosts and ports.
  ///
  /// Only one valid host in the cluster needs to be provided here. The client will use `CLUSTER NODES` to discover the other nodes.
  pub fn new_clustered<S>(mut hosts: Vec<(S, u16)>) -> ServerConfig
  where
    S: Into<String>,
  {
    ServerConfig::Clustered {
      hosts: hosts.drain(..).map(|(s, p)| (s.into(), p)).collect(),
    }
  }

  /// Create a centralized config with default settings for a local deployment.
  pub fn default_centralized() -> ServerConfig {
    ServerConfig::Centralized {
      host: "127.0.0.1".to_owned(),
      port: 6379,
    }
  }

  /// Create a clustered config with the same defaults as specified in the `create-cluster` script provided by Redis.
  pub fn default_clustered() -> ServerConfig {
    ServerConfig::Clustered {
      hosts: vec![
        ("127.0.0.1".to_owned(), 30001),
        ("127.0.0.1".to_owned(), 30002),
        ("127.0.0.1".to_owned(), 30003),
      ],
    }
  }

  /// Check if the config is for a clustered Redis deployment.
  pub fn is_clustered(&self) -> bool {
    match *self {
      ServerConfig::Centralized { .. } => false,
      ServerConfig::Clustered { .. } => true,
    }
  }

  /// Read the first host
  pub fn hosts(&self) -> Vec<(&str, u16)> {
    match *self {
      ServerConfig::Centralized { ref host, port } => vec![(host.as_str(), port)],
      ServerConfig::Clustered { ref hosts } => hosts.iter().map(|(h, p)| (h.as_str(), *p)).collect(),
    }
  }
}

/// Options for the [set](https://redis.io/commands/set) command.
///
/// <https://redis.io/commands/set>
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SetOptions {
  NX,
  XX,
}

impl SetOptions {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      SetOptions::NX => "NX",
      SetOptions::XX => "XX",
    }
  }
}

/// Expiration options for the [set](https://redis.io/commands/set) command.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Expiration {
  /// Expiration in seconds.
  EX(i64),
  /// Expiration in milliseconds.
  PX(i64),
  /// Expiration time, in seconds.
  EXAT(i64),
  /// Expiration time, in milliseconds.
  PXAT(i64),
  /// Do not reset the TTL.
  KEEPTTL,
}

impl Expiration {
  pub(crate) fn into_args(self) -> (&'static str, Option<i64>) {
    match self {
      Expiration::EX(i) => ("EX", Some(i)),
      Expiration::PX(i) => ("PX", Some(i)),
      Expiration::EXAT(i) => ("EXAT", Some(i)),
      Expiration::PXAT(i) => ("PXAT", Some(i)),
      Expiration::KEEPTTL => ("KEEPTTL", None),
    }
  }
}

/// The state of the underlying connection to the Redis server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientState {
  Disconnected,
  Disconnecting,
  Connected,
  Connecting,
}

impl ClientState {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ClientState::Connecting => "Connecting",
      ClientState::Connected => "Connected",
      ClientState::Disconnecting => "Disconnecting",
      ClientState::Disconnected => "Disconnected",
    }
  }
}

impl fmt::Display for ClientState {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}", self.to_str())
  }
}

/// A key in Redis.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RedisKey {
  key: Vec<u8>,
}

impl RedisKey {
  /// Create a new redis key from anything that can be read as bytes.
  pub fn new<S>(key: S) -> RedisKey
  where
    S: Into<Vec<u8>>,
  {
    RedisKey { key: key.into() }
  }

  /// Read the key as a str slice if it can be parsed as a UTF8 string.
  pub fn as_str(&self) -> Option<&str> {
    str::from_utf8(&self.key).ok()
  }

  /// Read the key as a byte slice.
  pub fn as_bytes(&self) -> &[u8] {
    &self.key
  }

  /// Read the key as a lossy UTF8 string with `String::from_utf8_lossy`.
  pub fn as_str_lossy(&self) -> Cow<str> {
    String::from_utf8_lossy(&self.key)
  }

  /// Convert the key to a UTF8 string, if possible.
  pub fn into_string(self) -> Option<String> {
    String::from_utf8(self.key).ok()
  }

  /// Read the inner bytes making up the key.
  pub fn into_bytes(self) -> Vec<u8> {
    self.key
  }

  /// Hash the key to find the associated cluster [hash slot](https://redis.io/topics/cluster-spec#keys-distribution-model).
  pub fn cluster_hash(&self) -> u16 {
    let as_str = String::from_utf8_lossy(&self.key);
    redis_protocol::redis_keyslot(&as_str)
  }

  /// Read the `host:port` of the cluster node that owns the key if the client is clustered and the cluster state is known.
  pub fn cluster_owner(&self, client: &RedisClient) -> Option<Arc<String>> {
    if utils::is_clustered(&client.inner.config) {
      let hash_slot = self.cluster_hash();
      client
        .inner
        .cluster_state
        .read()
        .as_ref()
        .and_then(|state| state.get_server(hash_slot).map(|slot| slot.server.clone()))
    } else {
      None
    }
  }

  /// Replace this key with an empty string, returning the bytes from the original key.
  pub fn take(&mut self) -> Vec<u8> {
    mem::replace(&mut self.key, Vec::new())
  }
}

impl From<String> for RedisKey {
  fn from(s: String) -> RedisKey {
    RedisKey { key: s.into_bytes() }
  }
}

impl<'a> From<&'a str> for RedisKey {
  fn from(s: &'a str) -> RedisKey {
    RedisKey {
      key: s.as_bytes().to_vec(),
    }
  }
}

impl<'a> From<&'a String> for RedisKey {
  fn from(s: &'a String) -> RedisKey {
    RedisKey {
      key: s.as_bytes().to_vec(),
    }
  }
}

impl<'a> From<&'a RedisKey> for RedisKey {
  fn from(k: &'a RedisKey) -> RedisKey {
    k.clone()
  }
}

impl<'a> From<&'a [u8]> for RedisKey {
  fn from(k: &'a [u8]) -> Self {
    RedisKey { key: k.to_vec() }
  }
}

/*
// conflicting impl with MultipleKeys when this is used
// callers should use `RedisKey::new` here
impl From<Vec<u8>> for RedisKey {
  fn from(key: Vec<u8>) -> Self {
    RedisKey { key }
  }
}
*/

/// Convenience struct for commands that take 1 or more keys.
pub struct MultipleKeys {
  keys: Vec<RedisKey>,
}

impl MultipleKeys {
  pub fn new() -> MultipleKeys {
    MultipleKeys { keys: Vec::new() }
  }

  pub fn inner(self) -> Vec<RedisKey> {
    self.keys
  }

  pub fn len(&self) -> usize {
    self.keys.len()
  }
}

impl<T> From<T> for MultipleKeys
where
  T: Into<RedisKey>,
{
  fn from(d: T) -> Self {
    MultipleKeys { keys: vec![d.into()] }
  }
}

impl<T> FromIterator<T> for MultipleKeys
where
  T: Into<RedisKey>,
{
  fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
    MultipleKeys {
      keys: iter.into_iter().map(|k| k.into()).collect(),
    }
  }
}

impl<T> From<Vec<T>> for MultipleKeys
where
  T: Into<RedisKey>,
{
  fn from(d: Vec<T>) -> Self {
    MultipleKeys {
      keys: d.into_iter().map(|k| k.into()).collect(),
    }
  }
}

impl<T> From<VecDeque<T>> for MultipleKeys
where
  T: Into<RedisKey>,
{
  fn from(d: VecDeque<T>) -> Self {
    MultipleKeys {
      keys: d.into_iter().map(|k| k.into()).collect(),
    }
  }
}

/// Convenience struct for commands that take 1 or more strings.
pub type MultipleStrings = MultipleKeys;

/// Convenience struct for commands that take 1 or more values.
pub struct MultipleValues {
  values: Vec<RedisValue>,
}

impl MultipleValues {
  pub fn inner(self) -> Vec<RedisValue> {
    self.values
  }

  pub fn len(&self) -> usize {
    self.values.len()
  }

  /// Convert this a nested `RedisValue`.
  pub fn into_values(self) -> RedisValue {
    RedisValue::Array(self.values)
  }
}

impl From<()> for MultipleValues {
  fn from(_: ()) -> Self {
    MultipleValues { values: vec![] }
  }
}

/*
// https://github.com/rust-lang/rust/issues/50133
impl<T> TryFrom<T> for MultipleValues
where
  T: TryInto<RedisValue>,
  T::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from(d: T) -> Result<Self, Self::Error> {
    Ok(MultipleValues { values: vec![to!(d)?] })
  }
}
*/

impl<T> From<T> for MultipleValues
where
  T: Into<RedisValue>,
{
  fn from(d: T) -> Self {
    MultipleValues { values: vec![d.into()] }
  }
}

impl<T> FromIterator<T> for MultipleValues
where
  T: Into<RedisValue>,
{
  fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
    MultipleValues {
      values: iter.into_iter().map(|v| v.into()).collect(),
    }
  }
}

impl<T> TryFrom<Vec<T>> for MultipleValues
where
  T: TryInto<RedisValue>,
  T::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from(d: Vec<T>) -> Result<Self, Self::Error> {
    let mut values = Vec::with_capacity(d.len());
    for value in d.into_iter() {
      values.push(to!(value)?);
    }

    Ok(MultipleValues { values })
  }
}

impl<T> TryFrom<VecDeque<T>> for MultipleValues
where
  T: TryInto<RedisValue>,
  T::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from(d: VecDeque<T>) -> Result<Self, Self::Error> {
    let mut values = Vec::with_capacity(d.len());
    for value in d.into_iter() {
      values.push(to!(value)?);
    }

    Ok(MultipleValues { values })
  }
}

/// Convenience struct for `ZINTERSTORE` and `ZUNIONSTORE` when accepting 1 or more `weights` arguments.
pub struct MultipleWeights {
  values: Vec<f64>,
}

impl MultipleWeights {
  pub fn new() -> MultipleWeights {
    MultipleWeights { values: Vec::new() }
  }

  pub fn inner(self) -> Vec<f64> {
    self.values
  }

  pub fn len(&self) -> usize {
    self.values.len()
  }
}

impl From<Option<f64>> for MultipleWeights {
  fn from(d: Option<f64>) -> Self {
    match d {
      Some(w) => w.into(),
      None => MultipleWeights::new(),
    }
  }
}

impl From<f64> for MultipleWeights {
  fn from(d: f64) -> Self {
    MultipleWeights { values: vec![d] }
  }
}

impl FromIterator<f64> for MultipleWeights {
  fn from_iter<I: IntoIterator<Item = f64>>(iter: I) -> Self {
    MultipleWeights {
      values: iter.into_iter().collect(),
    }
  }
}

impl From<Vec<f64>> for MultipleWeights {
  fn from(d: Vec<f64>) -> Self {
    MultipleWeights { values: d }
  }
}

impl From<VecDeque<f64>> for MultipleWeights {
  fn from(d: VecDeque<f64>) -> Self {
    MultipleWeights {
      values: d.into_iter().collect(),
    }
  }
}

/// Convenience struct for the `ZADD` command to accept 1 or more `(score, value)` arguments.
pub struct MultipleZaddValues {
  values: Vec<(f64, RedisValue)>,
}

impl MultipleZaddValues {
  pub fn new() -> MultipleZaddValues {
    MultipleZaddValues { values: Vec::new() }
  }

  pub fn inner(self) -> Vec<(f64, RedisValue)> {
    self.values
  }

  pub fn len(&self) -> usize {
    self.values.len()
  }
}

impl<T> TryFrom<(f64, T)> for MultipleZaddValues
where
  T: TryInto<RedisValue>,
  T::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from((f, d): (f64, T)) -> Result<Self, Self::Error> {
    Ok(MultipleZaddValues {
      values: vec![(f, to!(d)?)],
    })
  }
}

impl<T> FromIterator<(f64, T)> for MultipleZaddValues
where
  T: Into<RedisValue>,
{
  fn from_iter<I: IntoIterator<Item = (f64, T)>>(iter: I) -> Self {
    MultipleZaddValues {
      values: iter.into_iter().map(|(f, d)| (f, d.into())).collect(),
    }
  }
}

impl<T> TryFrom<Vec<(f64, T)>> for MultipleZaddValues
where
  T: TryInto<RedisValue>,
  T::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from(d: Vec<(f64, T)>) -> Result<Self, Self::Error> {
    let mut values = Vec::with_capacity(d.len());
    for (f, v) in d.into_iter() {
      values.push((f, to!(v)?));
    }

    Ok(MultipleZaddValues { values })
  }
}

impl<T> TryFrom<VecDeque<(f64, T)>> for MultipleZaddValues
where
  T: TryInto<RedisValue>,
  T::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from(d: VecDeque<(f64, T)>) -> Result<Self, Self::Error> {
    let mut values = Vec::with_capacity(d.len());
    for (f, v) in d.into_iter() {
      values.push((f, to!(v)?));
    }

    Ok(MultipleZaddValues { values })
  }
}

/// A map of `(String, RedisValue)` pairs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RedisMap {
  #[cfg(feature = "index-map")]
  pub(crate) inner: IndexMap<String, RedisValue>,
  #[cfg(not(feature = "index-map"))]
  pub(crate) inner: HashMap<String, RedisValue>,
}

impl RedisMap {
  /// Create a new empty map.
  pub fn new() -> Self {
    RedisMap {
      inner: utils::new_map(0),
    }
  }

  /// Replace the value an empty map, returning the original value.
  pub fn take(&mut self) -> Self {
    mem::replace(&mut self.inner, utils::new_map(0)).into()
  }

  /// Take the inner `IndexMap`.
  #[cfg(feature = "index-map")]
  pub fn inner(self) -> IndexMap<String, RedisValue> {
    self.inner
  }

  /// Read the number of (key, value) pairs in the map.
  pub fn len(&self) -> usize {
    self.inner.len()
  }

  /// Take the inner `HashMap`.
  #[cfg(not(feature = "index-map"))]
  pub fn inner(self) -> HashMap<String, RedisValue> {
    self.inner
  }
}

impl Deref for RedisMap {
  #[cfg(feature = "index-map")]
  type Target = IndexMap<String, RedisValue>;
  #[cfg(not(feature = "index-map"))]
  type Target = HashMap<String, RedisValue>;

  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl DerefMut for RedisMap {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.inner
  }
}

#[cfg(feature = "index-map")]
impl From<IndexMap<String, RedisValue>> for RedisMap {
  fn from(d: IndexMap<String, RedisValue>) -> Self {
    RedisMap { inner: d }
  }
}

#[cfg(feature = "index-map")]
impl From<HashMap<String, RedisValue>> for RedisMap {
  fn from(d: HashMap<String, RedisValue>) -> Self {
    let mut inner = utils::new_map(d.len());
    for (key, value) in d.into_iter() {
      inner.insert(key, value);
    }
    RedisMap { inner }
  }
}

#[cfg(not(feature = "index-map"))]
impl From<HashMap<String, RedisValue>> for RedisMap {
  fn from(d: HashMap<String, RedisValue>) -> Self {
    RedisMap { inner: d }
  }
}

impl From<BTreeMap<String, RedisValue>> for RedisMap {
  fn from(d: BTreeMap<String, RedisValue>) -> Self {
    let mut inner = utils::new_map(d.len());
    for (key, value) in d.into_iter() {
      inner.insert(key, value);
    }
    RedisMap { inner }
  }
}

impl<S: Into<String>> From<(S, RedisValue)> for RedisMap {
  fn from(d: (S, RedisValue)) -> Self {
    let mut inner = utils::new_map(1);
    inner.insert(d.0.into(), d.1);
    RedisMap { inner }
  }
}

impl<S: Into<String>> From<Vec<(S, RedisValue)>> for RedisMap {
  fn from(d: Vec<(S, RedisValue)>) -> Self {
    let mut inner = utils::new_map(d.len());
    for (key, value) in d.into_iter() {
      inner.insert(key.into(), value);
    }
    RedisMap { inner }
  }
}

impl<S: Into<String>> From<VecDeque<(S, RedisValue)>> for RedisMap {
  fn from(d: VecDeque<(S, RedisValue)>) -> Self {
    let mut inner = utils::new_map(d.len());
    for (key, value) in d.into_iter() {
      inner.insert(key.into(), value);
    }
    RedisMap { inner }
  }
}

/// The kind of value from Redis.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RedisValueKind {
  Integer,
  String,
  Bytes,
  Null,
  Queued,
  Map,
  Array,
}

impl fmt::Display for RedisValueKind {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    let s = match *self {
      RedisValueKind::Integer => "Integer",
      RedisValueKind::String => "String",
      RedisValueKind::Bytes => "Bytes",
      RedisValueKind::Null => "nil",
      RedisValueKind::Queued => "Queued",
      RedisValueKind::Map => "Map",
      RedisValueKind::Array => "Array",
    };

    write!(f, "{}", s)
  }
}

/// A value used in a Redis command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RedisValue {
  /// An integer value.
  Integer(i64),
  /// A string value.
  String(String),
  /// A binary value to represent non-UTF8 strings.
  Bytes(Vec<u8>),
  /// A `nil` value.
  Null,
  /// A special value used to indicate a MULTI block command was received by the server.
  Queued,
  /// A nested map of key/value pairs.
  Map(RedisMap),
  /// An ordered list of values.
  Array(Vec<RedisValue>),
}

impl<'a> RedisValue {
  /// Create a new `RedisValue` with the `OK` status.
  pub fn new_ok() -> Self {
    RedisValue::String(OK.into())
  }

  /// Whether or not the value is a simple string OK value.
  pub fn is_ok(&self) -> bool {
    match *self {
      RedisValue::String(ref s) => s == OK,
      _ => false,
    }
  }

  /// Attempt to convert the value into an integer, returning the original string as an error if the parsing fails.
  pub fn into_integer(self) -> Result<RedisValue, RedisValue> {
    match self {
      RedisValue::String(s) => match s.parse::<i64>() {
        Ok(i) => Ok(RedisValue::Integer(i)),
        Err(_) => Err(RedisValue::String(s)),
      },
      RedisValue::Integer(i) => Ok(RedisValue::Integer(i)),
      _ => Err(self),
    }
  }

  /// Read the type of the value without any associated data.
  pub fn kind(&self) -> RedisValueKind {
    match *self {
      RedisValue::Integer(_) => RedisValueKind::Integer,
      RedisValue::String(_) => RedisValueKind::String,
      RedisValue::Bytes(_) => RedisValueKind::Bytes,
      RedisValue::Null => RedisValueKind::Null,
      RedisValue::Queued => RedisValueKind::Queued,
      RedisValue::Map(_) => RedisValueKind::Map,
      RedisValue::Array(_) => RedisValueKind::Array,
    }
  }

  /// Check if the value is null.
  pub fn is_null(&self) -> bool {
    match *self {
      RedisValue::Null => true,
      _ => false,
    }
  }

  /// Check if the value is an integer.
  pub fn is_integer(&self) -> bool {
    match *self {
      RedisValue::Integer(_) => true,
      _ => false,
    }
  }

  /// Check if the value is a string.
  pub fn is_string(&self) -> bool {
    match *self {
      RedisValue::String(_) => true,
      _ => false,
    }
  }

  /// Check if the value is an array of bytes.
  pub fn is_bytes(&self) -> bool {
    match *self {
      RedisValue::Bytes(_) => true,
      _ => false,
    }
  }

  /// Check if the value is a `QUEUED` response.
  pub fn is_queued(&self) -> bool {
    match *self {
      RedisValue::Queued => true,
      _ => false,
    }
  }

  /// Check if the inner string value can be cast to an `f64`.
  pub fn is_float(&self) -> bool {
    match *self {
      RedisValue::String(ref s) => utils::redis_string_to_f64(s).is_ok(),
      _ => false,
    }
  }

  /// Whether or not the value is a `RedisMap`.
  pub fn is_map(&self) -> bool {
    match *self {
      RedisValue::Map(_) => true,
      _ => false,
    }
  }

  /// Whether or not the value is an array.
  pub fn is_array(&self) -> bool {
    match *self {
      RedisValue::Array(_) => true,
      _ => false,
    }
  }

  /// Read and return the inner value as a `u64`, if possible.
  pub fn as_u64(&self) -> Option<u64> {
    match self {
      RedisValue::Integer(ref i) => {
        if *i >= 0 {
          Some(*i as u64)
        } else {
          None
        }
      }
      RedisValue::String(ref s) => s.parse::<u64>().ok(),
      RedisValue::Array(ref inner) => {
        if inner.len() == 1 {
          inner.first().and_then(|v| v.as_u64())
        } else {
          None
        }
      }
      _ => None,
    }
  }

  ///  Read and return the inner value as a `i64`, if possible.
  pub fn as_i64(&self) -> Option<i64> {
    match self {
      RedisValue::Integer(ref i) => Some(*i),
      RedisValue::String(ref s) => s.parse::<i64>().ok(),
      RedisValue::Array(ref inner) => {
        if inner.len() == 1 {
          inner.first().and_then(|v| v.as_i64())
        } else {
          None
        }
      }
      _ => None,
    }
  }

  ///  Read and return the inner value as a `usize`, if possible.
  pub fn as_usize(&self) -> Option<usize> {
    match self {
      RedisValue::Integer(i) => {
        if *i >= 0 {
          Some(*i as usize)
        } else {
          None
        }
      }
      RedisValue::String(ref s) => s.parse::<usize>().ok(),
      RedisValue::Array(ref inner) => {
        if inner.len() == 1 {
          inner.first().and_then(|v| v.as_usize())
        } else {
          None
        }
      }
      _ => None,
    }
  }

  ///  Read and return the inner value as a `f64`, if possible.
  pub fn as_f64(&self) -> Option<f64> {
    match self {
      RedisValue::String(ref s) => utils::redis_string_to_f64(s).ok(),
      RedisValue::Integer(ref i) => Some(*i as f64),
      RedisValue::Array(ref inner) => {
        if inner.len() == 1 {
          inner.first().and_then(|v| v.as_f64())
        } else {
          None
        }
      }
      _ => None,
    }
  }

  /// Read and return the inner `String` if the value is a string or integer.
  pub fn into_string(self) -> Option<String> {
    match self {
      RedisValue::String(s) => Some(s),
      RedisValue::Bytes(b) => String::from_utf8(b).ok(),
      RedisValue::Integer(i) => Some(i.to_string()),
      RedisValue::Queued => Some(QUEUED.to_owned()),
      RedisValue::Array(mut inner) => {
        if inner.len() == 1 {
          inner.pop().and_then(|v| v.into_string())
        } else {
          None
        }
      }
      _ => None,
    }
  }

  /// Read and return the inner `String` if the value is a string or integer.
  ///
  /// Note: this will cast integers to strings.
  pub fn as_string(&self) -> Option<String> {
    match self {
      RedisValue::String(ref s) => Some(s.to_owned()),
      RedisValue::Bytes(ref b) => str::from_utf8(b).ok().map(|s| s.to_owned()),
      RedisValue::Integer(ref i) => Some(i.to_string()),
      RedisValue::Queued => Some(QUEUED.to_owned()),
      _ => None,
    }
  }

  /// Read the inner value as a string slice.
  ///
  /// Null is returned as "nil" and integers are cast to a string.
  pub fn as_str(&'a self) -> Option<Cow<'a, str>> {
    let s = match *self {
      RedisValue::String(ref s) => Cow::Borrowed(s.as_str()),
      RedisValue::Integer(ref i) => Cow::Owned(i.to_string()),
      RedisValue::Null => Cow::Borrowed(NIL),
      RedisValue::Queued => Cow::Borrowed(QUEUED),
      RedisValue::Bytes(ref b) => return str::from_utf8(b).ok().map(|s| Cow::Borrowed(s)),
      _ => return None,
    };

    Some(s)
  }

  /// Read the inner value as a string, using `String::from_utf8_lossy` on byte slices.
  pub fn as_str_lossy(&self) -> Option<Cow<str>> {
    let s = match *self {
      RedisValue::String(ref s) => Cow::Borrowed(s.as_str()),
      RedisValue::Integer(ref i) => Cow::Owned(i.to_string()),
      RedisValue::Null => Cow::Borrowed(NIL),
      RedisValue::Queued => Cow::Borrowed(QUEUED),
      RedisValue::Bytes(ref b) => String::from_utf8_lossy(b),
      _ => return None,
    };

    Some(s)
  }

  /// Read the inner value as an array of bytes, if possible.
  pub fn as_bytes(&self) -> Option<&[u8]> {
    match *self {
      RedisValue::String(ref s) => Some(s.as_bytes()),
      RedisValue::Bytes(ref b) => Some(b),
      RedisValue::Queued => Some(QUEUED.as_bytes()),
      _ => None,
    }
  }

  /// Attempt to convert the value to a `bool`.
  pub fn as_bool(&self) -> Option<bool> {
    match *self {
      RedisValue::Integer(ref i) => match *i {
        0 => Some(false),
        1 => Some(true),
        _ => None,
      },
      RedisValue::String(ref s) => match s.as_ref() {
        "true" | "TRUE" | "t" | "T" | "1" => Some(true),
        "false" | "FALSE" | "f" | "F" | "0" => Some(false),
        _ => None,
      },
      RedisValue::Null => Some(false),
      RedisValue::Array(ref inner) => {
        if inner.len() == 1 {
          inner.first().and_then(|v| v.as_bool())
        } else {
          None
        }
      }
      _ => None,
    }
  }

  /// Convert the value to an array of `(value, score)` tuples if the redis value is an array result from a sorted set command with scores.
  pub fn into_zset_result(self) -> Result<Vec<(RedisValue, f64)>, RedisError> {
    protocol_utils::value_to_zset_result(self)
  }

  /// Attempt to convert this value to a Redis map if it's an array with an even number of elements.
  pub fn into_map(self) -> Result<RedisMap, RedisError> {
    if let RedisValue::Map(map) = self {
      return Ok(map);
    }

    if let RedisValue::Array(mut values) = self {
      if values.len() % 2 != 0 {
        return Err(RedisError::new(
          RedisErrorKind::Unknown,
          "Expected an even number of elements.",
        ));
      }
      let mut inner = utils::new_map(values.len() / 2);
      while values.len() >= 2 {
        let value = values.pop().unwrap();
        let key = match values.pop().unwrap().into_string() {
          Some(s) => s,
          None => {
            return Err(RedisError::new(
              RedisErrorKind::Unknown,
              "Expected redis map string key.",
            ))
          }
        };

        inner.insert(key, value);
      }

      Ok(RedisMap { inner })
    } else {
      Err(RedisError::new(RedisErrorKind::Unknown, "Expected array."))
    }
  }

  /// Convert the array value to a set, if possible.
  #[cfg(not(feature = "index-map"))]
  pub fn into_set(self) -> Result<HashSet<RedisValue>, RedisError> {
    if let RedisValue::Array(values) = self {
      let mut out = HashSet::with_capacity(values.len());

      for value in values.into_iter() {
        out.insert(value);
      }
      Ok(out)
    } else {
      Err(RedisError::new(RedisErrorKind::Unknown, "Expected array."))
    }
  }

  #[cfg(feature = "index-map")]
  pub fn into_set(self) -> Result<IndexSet<RedisValue>, RedisError> {
    if let RedisValue::Array(values) = self {
      let mut out = IndexSet::with_capacity(values.len());

      for value in values.into_iter() {
        out.insert(value);
      }
      Ok(out)
    } else {
      Err(RedisError::new(RedisErrorKind::Unknown, "Expected array."))
    }
  }

  /// Convert this value to an array if it's an array or map.
  ///
  /// If the value is not an array or map this returns a single-element array containing the current value.
  pub fn into_array(self) -> Vec<RedisValue> {
    match self {
      RedisValue::Array(values) => values,
      RedisValue::Map(map) => {
        let mut out = Vec::with_capacity(map.len() * 2);

        for (key, value) in map.inner().into_iter() {
          out.push(key.into());
          out.push(value);
        }
        out
      }
      _ => vec![self],
    }
  }

  /// Convert the value to an array of bytes, if possible.
  pub fn into_bytes(self) -> Option<Vec<u8>> {
    let v = match self {
      RedisValue::String(s) => s.into_bytes(),
      RedisValue::Bytes(b) => b,
      RedisValue::Null => NULL.as_bytes().to_vec(),
      RedisValue::Queued => QUEUED.as_bytes().to_vec(),
      RedisValue::Array(mut inner) => {
        if inner.len() == 1 {
          return inner.pop().and_then(|v| v.into_bytes());
        } else {
          return None;
        }
      }
      // TODO maybe rethink this
      RedisValue::Integer(i) => i.to_string().into_bytes(),
      _ => return None,
    };

    Some(v)
  }

  /// Convert the value into a `GeoPosition`, if possible.
  ///
  /// Null values are returned as `None` to work more easily with the result of the `GEOPOS` command.
  pub fn as_geo_position(&self) -> Result<Option<GeoPosition>, RedisError> {
    utils::value_to_geo_pos(self)
  }

  /// Replace this value with `RedisValue::Null`, returning the original value.
  pub fn take(&mut self) -> RedisValue {
    mem::replace(self, RedisValue::Null)
  }

  /// Attempt to convert this value to any value that implements the [RedisResponse](crate::types::RedisResponse) trait.
  ///
  /// ```rust
  /// # use fred::types::RedisValue;
  /// # use std::collections::HashMap;
  /// let foo: usize = RedisValue::String("123".into()).convert()?;
  /// let foo: i64 = RedisValue::String("123".into()).convert()?;
  /// let foo: String = RedisValue::String("123".into()).convert()?;
  /// let foo: Vec<u8> = RedisValue::Bytes(vec![102, 111, 111]).convert()?;
  /// let foo: Vec<u8> = RedisValue::String("foo".into()).convert()?;
  /// let foo: Vec<String> = RedisValue::Array(vec!["a".into(), "b".into()]).convert()?;
  /// let foo: HashMap<String, u16> = RedisValue::Array(vec![
  ///   "a".into(), 1.into(),
  ///   "b".into(), 2.into()
  /// ])
  /// .convert()?;
  /// let foo: (String, i64) = RedisValue::Array(vec!["a".into(), 1.into()]).convert()?;
  /// let foo: Vec<(String, i64)> = RedisValue::Array(vec![
  ///   "a".into(), 1.into(),
  ///   "b".into(), 2.into()
  /// ])
  /// .convert()?;
  /// // ...
  /// ```
  pub fn convert<R>(self) -> Result<R, RedisError>
  where
    R: RedisResponse,
  {
    R::from_value(self)
  }
}

impl Hash for RedisValue {
  fn hash<H: Hasher>(&self, state: &mut H) {
    let prefix = match self.kind() {
      RedisValueKind::Integer => 'i',
      RedisValueKind::String => 's',
      RedisValueKind::Null => 'n',
      RedisValueKind::Queued => 'h',
      RedisValueKind::Array => 'a',
      RedisValueKind::Map => 'm',
      RedisValueKind::Bytes => 'b',
    };
    prefix.hash(state);

    match *self {
      RedisValue::Integer(d) => d.hash(state),
      RedisValue::String(ref s) => s.hash(state),
      RedisValue::Bytes(ref b) => b.hash(state),
      RedisValue::Null => NULL.hash(state),
      RedisValue::Queued => QUEUED.hash(state),
      RedisValue::Map(ref map) => utils::hash_map(map, state),
      RedisValue::Array(ref arr) => {
        for value in arr.iter() {
          value.hash(state);
        }
      }
    }
  }
}

impl From<u8> for RedisValue {
  fn from(d: u8) -> Self {
    RedisValue::Integer(d as i64)
  }
}

impl From<u16> for RedisValue {
  fn from(d: u16) -> Self {
    RedisValue::Integer(d as i64)
  }
}

impl From<u32> for RedisValue {
  fn from(d: u32) -> Self {
    RedisValue::Integer(d as i64)
  }
}

impl From<i8> for RedisValue {
  fn from(d: i8) -> Self {
    RedisValue::Integer(d as i64)
  }
}

impl From<i16> for RedisValue {
  fn from(d: i16) -> Self {
    RedisValue::Integer(d as i64)
  }
}

impl From<i32> for RedisValue {
  fn from(d: i32) -> Self {
    RedisValue::Integer(d as i64)
  }
}

impl From<i64> for RedisValue {
  fn from(d: i64) -> Self {
    RedisValue::Integer(d)
  }
}

impl TryFrom<f32> for RedisValue {
  type Error = RedisError;

  fn try_from(f: f32) -> Result<Self, Self::Error> {
    utils::f64_to_redis_string(f as f64)
  }
}

impl TryFrom<f64> for RedisValue {
  type Error = RedisError;

  fn try_from(f: f64) -> Result<Self, Self::Error> {
    utils::f64_to_redis_string(f)
  }
}

impl TryFrom<u64> for RedisValue {
  type Error = RedisError;

  fn try_from(d: u64) -> Result<Self, Self::Error> {
    if d >= (i64::MAX as u64) {
      return Err(RedisError::new(RedisErrorKind::Unknown, "Unsigned integer too large."));
    }

    Ok((d as i64).into())
  }
}

impl TryFrom<u128> for RedisValue {
  type Error = RedisError;

  fn try_from(d: u128) -> Result<Self, Self::Error> {
    if d >= (i64::MAX as u128) {
      return Err(RedisError::new(RedisErrorKind::Unknown, "Unsigned integer too large."));
    }

    Ok((d as i64).into())
  }
}

impl TryFrom<i128> for RedisValue {
  type Error = RedisError;

  fn try_from(d: i128) -> Result<Self, Self::Error> {
    if d >= (i64::MAX as i128) {
      return Err(RedisError::new(RedisErrorKind::Unknown, "Signed integer too large."));
    }

    Ok((d as i64).into())
  }
}

impl TryFrom<usize> for RedisValue {
  type Error = RedisError;

  fn try_from(d: usize) -> Result<Self, Self::Error> {
    if d >= (i64::MAX as usize) {
      return Err(RedisError::new(RedisErrorKind::Unknown, "Unsigned integer too large."));
    }

    Ok((d as i64).into())
  }
}

impl From<String> for RedisValue {
  fn from(d: String) -> Self {
    RedisValue::String(d)
  }
}

impl<'a> From<&'a str> for RedisValue {
  fn from(d: &'a str) -> Self {
    RedisValue::String(d.to_owned())
  }
}

impl<'a> From<&'a String> for RedisValue {
  fn from(s: &'a String) -> Self {
    RedisValue::String(s.clone())
  }
}

impl<'a> From<&'a [u8]> for RedisValue {
  fn from(b: &'a [u8]) -> Self {
    RedisValue::Bytes(b.to_vec())
  }
}

impl From<bool> for RedisValue {
  fn from(d: bool) -> Self {
    RedisValue::from(match d {
      true => "true",
      false => "false",
    })
  }
}

impl<T> TryFrom<Option<T>> for RedisValue
where
  T: TryInto<RedisValue>,
  T::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from(d: Option<T>) -> Result<Self, Self::Error> {
    match d {
      Some(i) => to!(i),
      None => Ok(RedisValue::Null),
    }
  }
}

impl FromIterator<RedisValue> for RedisValue {
  fn from_iter<I: IntoIterator<Item = RedisValue>>(iter: I) -> Self {
    RedisValue::Array(iter.into_iter().collect())
  }
}

#[cfg(feature = "index-map")]
impl From<IndexMap<String, RedisValue>> for RedisValue {
  fn from(d: IndexMap<String, RedisValue>) -> Self {
    RedisValue::Map(d.into())
  }
}

impl From<HashMap<String, RedisValue>> for RedisValue {
  fn from(d: HashMap<String, RedisValue>) -> Self {
    RedisValue::Map(d.into())
  }
}

impl From<BTreeMap<String, RedisValue>> for RedisValue {
  fn from(d: BTreeMap<String, RedisValue>) -> Self {
    RedisValue::Map(d.into())
  }
}

impl From<RedisKey> for RedisValue {
  fn from(d: RedisKey) -> Self {
    RedisValue::Bytes(d.key)
  }
}

/// The parsed result of the MEMORY STATS command for a specific database.
///
/// <https://redis.io/commands/memory-stats>
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseMemoryStats {
  pub overhead_hashtable_main: u64,
  pub overhead_hashtable_expires: u64,
}

impl Default for DatabaseMemoryStats {
  fn default() -> Self {
    DatabaseMemoryStats {
      overhead_hashtable_expires: 0,
      overhead_hashtable_main: 0,
    }
  }
}

/// The parsed result of the MEMORY STATS command.
///
/// <https://redis.io/commands/memory-stats>
#[derive(Clone, Debug)]
pub struct MemoryStats {
  pub peak_allocated: u64,
  pub total_allocated: u64,
  pub startup_allocated: u64,
  pub replication_backlog: u64,
  pub clients_slaves: u64,
  pub clients_normal: u64,
  pub aof_buffer: u64,
  pub lua_caches: u64,
  pub overhead_total: u64,
  pub keys_count: u64,
  pub keys_bytes_per_key: u64,
  pub dataset_bytes: u64,
  pub dataset_percentage: f64,
  pub peak_percentage: f64,
  pub fragmentation: f64,
  pub fragmentation_bytes: u64,
  pub rss_overhead_ratio: f64,
  pub rss_overhead_bytes: u64,
  pub allocator_allocated: u64,
  pub allocator_active: u64,
  pub allocator_resident: u64,
  pub allocator_fragmentation_ratio: f64,
  pub allocator_fragmentation_bytes: u64,
  pub allocator_rss_ratio: f64,
  pub allocator_rss_bytes: u64,
  pub db: HashMap<u16, DatabaseMemoryStats>,
}

impl Default for MemoryStats {
  fn default() -> Self {
    MemoryStats {
      peak_allocated: 0,
      total_allocated: 0,
      startup_allocated: 0,
      replication_backlog: 0,
      clients_normal: 0,
      clients_slaves: 0,
      aof_buffer: 0,
      lua_caches: 0,
      overhead_total: 0,
      keys_count: 0,
      keys_bytes_per_key: 0,
      dataset_bytes: 0,
      dataset_percentage: 0.0,
      peak_percentage: 0.0,
      fragmentation: 0.0,
      fragmentation_bytes: 0,
      rss_overhead_ratio: 0.0,
      rss_overhead_bytes: 0,
      allocator_allocated: 0,
      allocator_active: 0,
      allocator_resident: 0,
      allocator_fragmentation_ratio: 0.0,
      allocator_fragmentation_bytes: 0,
      allocator_rss_bytes: 0,
      allocator_rss_ratio: 0.0,
      db: HashMap::new(),
    }
  }
}

impl PartialEq for MemoryStats {
  fn eq(&self, other: &Self) -> bool {
    self.peak_allocated == other.peak_allocated
      && self.total_allocated == other.total_allocated
      && self.startup_allocated == other.startup_allocated
      && self.replication_backlog == other.replication_backlog
      && self.clients_normal == other.clients_normal
      && self.clients_slaves == other.clients_slaves
      && self.aof_buffer == other.aof_buffer
      && self.lua_caches == other.lua_caches
      && self.overhead_total == other.overhead_total
      && self.keys_count == other.keys_count
      && self.keys_bytes_per_key == other.keys_bytes_per_key
      && self.dataset_bytes == other.dataset_bytes
      && utils::f64_eq(self.dataset_percentage, other.dataset_percentage)
      && utils::f64_eq(self.peak_percentage, other.peak_percentage)
      && utils::f64_eq(self.fragmentation, other.fragmentation)
      && self.fragmentation_bytes == other.fragmentation_bytes
      && utils::f64_eq(self.rss_overhead_ratio, other.rss_overhead_ratio)
      && self.rss_overhead_bytes == other.rss_overhead_bytes
      && self.allocator_allocated == other.allocator_allocated
      && self.allocator_active == other.allocator_active
      && self.allocator_resident == other.allocator_resident
      && utils::f64_eq(self.allocator_fragmentation_ratio, other.allocator_fragmentation_ratio)
      && self.allocator_fragmentation_bytes == other.allocator_fragmentation_bytes
      && self.allocator_rss_bytes == other.allocator_rss_bytes
      && utils::f64_eq(self.allocator_rss_ratio, other.allocator_rss_ratio)
      && self.db == other.db
  }
}

impl Eq for MemoryStats {}

/// ACL rules describing the keys a user can access.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AclKeyPattern {
  AllKeys,
  Custom(String),
}

impl AclKeyPattern {
  pub(crate) fn to_string(&self) -> String {
    match *self {
      AclKeyPattern::AllKeys => "allkeys".into(),
      AclKeyPattern::Custom(ref pat) => format!("~{}", pat),
    }
  }
}

/// ACL rules describing the channels a user can access.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AclChannelPattern {
  AllChannels,
  Custom(String),
}

impl AclChannelPattern {
  pub(crate) fn to_string(&self) -> String {
    match *self {
      AclChannelPattern::AllChannels => "allchannels".into(),
      AclChannelPattern::Custom(ref pat) => format!("&{}", pat),
    }
  }
}

/// ACL rules describing the commands a user can access.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AclCommandPattern {
  AllCommands,
  NoCommands,
  Custom {
    command: String,
    subcommand: Option<String>,
  },
}

impl AclCommandPattern {
  pub(crate) fn to_string(&self, prefix: &'static str) -> String {
    match *self {
      AclCommandPattern::AllCommands => "allcommands".into(),
      AclCommandPattern::NoCommands => "nocommands".into(),
      AclCommandPattern::Custom {
        ref command,
        ref subcommand,
      } => {
        if let Some(subcommand) = subcommand {
          format!("{}{}|{}", prefix, command, subcommand)
        } else {
          format!("{}{}", prefix, command)
        }
      }
    }
  }
}

/// ACL rules associated with a user.
///
/// <https://redis.io/commands/acl-setuser#list-of-rules>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AclRule {
  On,
  Off,
  Reset,
  ResetChannels,
  ResetKeys,
  AddKeys(AclKeyPattern),
  AddChannels(AclChannelPattern),
  AddCommands(AclCommandPattern),
  RemoveCommands(AclCommandPattern),
  AddCategory(String),
  RemoveCategory(String),
  NoPass,
  AddPassword(String),
  AddHashedPassword(String),
  RemovePassword(String),
  RemoveHashedPassword(String),
}

impl AclRule {
  pub(crate) fn to_string(&self) -> String {
    match self {
      AclRule::On => "on".into(),
      AclRule::Off => "off".into(),
      AclRule::Reset => "reset".into(),
      AclRule::ResetChannels => "resetchannels".into(),
      AclRule::ResetKeys => "resetkeys".into(),
      AclRule::NoPass => "nopass".into(),
      AclRule::AddPassword(ref pass) => format!(">{}", pass),
      AclRule::RemovePassword(ref pass) => format!("<{}", pass),
      AclRule::AddHashedPassword(ref pass) => format!("#{}", pass),
      AclRule::RemoveHashedPassword(ref pass) => format!("!{}", pass),
      AclRule::AddCategory(ref cat) => format!("+@{}", cat),
      AclRule::RemoveCategory(ref cat) => format!("-@{}", cat),
      AclRule::AddKeys(ref pat) => pat.to_string(),
      AclRule::AddChannels(ref pat) => pat.to_string(),
      AclRule::AddCommands(ref pat) => pat.to_string("+"),
      AclRule::RemoveCommands(ref pat) => pat.to_string("-"),
    }
  }
}

/// A flag from the ACL GETUSER command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AclUserFlag {
  On,
  Off,
  AllKeys,
  AllChannels,
  AllCommands,
  NoPass,
}

/// An ACL user from the ACL GETUSER command.
///
/// <https://redis.io/commands/acl-getuser>
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct AclUser {
  pub flags: Vec<AclUserFlag>,
  pub passwords: Vec<String>,
  pub commands: Vec<String>,
  pub keys: Vec<String>,
  pub channels: Vec<String>,
}

/// The output of an entry in the slow queries log.
///
/// <https://redis.io/commands/slowlog#output-format>
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlowlogEntry {
  pub id: i64,
  pub timestamp: i64,
  pub duration: u64,
  pub args: Vec<String>,
  pub ip: Option<String>,
  pub name: Option<String>,
}

/// The direction to move elements in a *LMOVE command.
///
/// <https://redis.io/commands/blmove>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LMoveDirection {
  Left,
  Right,
}

impl LMoveDirection {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      LMoveDirection::Left => "LEFT",
      LMoveDirection::Right => "RIGHT",
    }
  }
}

/// The type of clients to close.
///
/// <https://redis.io/commands/client-kill>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientKillType {
  Normal,
  Master,
  Replica,
  Pubsub,
}

impl ClientKillType {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ClientKillType::Normal => "normal",
      ClientKillType::Master => "master",
      ClientKillType::Replica => "replica",
      ClientKillType::Pubsub => "pubsub",
    }
  }
}

/// Filters provided to the CLIENT KILL command.
///
/// <https://redis.io/commands/client-kill>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientKillFilter {
  ID(String),
  Type(ClientKillType),
  User(String),
  Addr(String),
  LAddr(String),
  SkipMe(bool),
}

impl ClientKillFilter {
  pub(crate) fn to_str(&self) -> (&'static str, &str) {
    match *self {
      ClientKillFilter::ID(ref id) => ("ID", id),
      ClientKillFilter::Type(ref kind) => ("TYPE", kind.to_str()),
      ClientKillFilter::User(ref user) => ("USER", user),
      ClientKillFilter::Addr(ref addr) => ("ADDR", addr),
      ClientKillFilter::LAddr(ref addr) => ("LADDR", addr),
      ClientKillFilter::SkipMe(ref b) => (
        "SKIPME",
        match *b {
          true => "yes",
          false => "no",
        },
      ),
    }
  }
}

/// Filters for the CLIENT PAUSE command.
///
/// <https://redis.io/commands/client-pause>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientPauseKind {
  Write,
  All,
}

impl ClientPauseKind {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ClientPauseKind::Write => "WRITE",
      ClientPauseKind::All => "ALL",
    }
  }
}

/// Arguments for the CLIENT REPLY command.
///
/// <https://redis.io/commands/client-reply>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientReplyFlag {
  On,
  Off,
  Skip,
}

impl ClientReplyFlag {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ClientReplyFlag::On => "ON",
      ClientReplyFlag::Off => "OFF",
      ClientReplyFlag::Skip => "SKIP",
    }
  }
}

/// Arguments to the CLIENT UNBLOCK command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientUnblockFlag {
  Timeout,
  Error,
}

impl ClientUnblockFlag {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ClientUnblockFlag::Timeout => "TIMEOUT",
      ClientUnblockFlag::Error => "ERROR",
    }
  }
}

/// The state of the cluster from the CLUSTER INFO command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterState {
  Ok,
  Fail,
}

impl Default for ClusterState {
  fn default() -> Self {
    ClusterState::Ok
  }
}

/// A parsed response from the CLUSTER INFO command.
///
/// <https://redis.io/commands/cluster-info>
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ClusterInfo {
  pub cluster_state: ClusterState,
  pub cluster_slots_assigned: u16,
  pub cluster_slots_ok: u16,
  pub cluster_slots_pfail: u16,
  pub cluster_slots_fail: u16,
  pub cluster_known_nodes: u16,
  pub cluster_size: u32,
  pub cluster_current_epoch: u64,
  pub cluster_my_epoch: u64,
  pub cluster_stats_messages_sent: u64,
  pub cluster_stats_messages_received: u64,
}

/// A convenience struct for functions that take one or more hash slot values.
pub struct MultipleHashSlots {
  inner: Vec<u16>,
}

impl MultipleHashSlots {
  pub fn inner(self) -> Vec<u16> {
    self.inner
  }

  pub fn len(&self) -> usize {
    self.inner.len()
  }
}

impl From<u16> for MultipleHashSlots {
  fn from(d: u16) -> Self {
    MultipleHashSlots { inner: vec![d] }
  }
}

impl From<Vec<u16>> for MultipleHashSlots {
  fn from(d: Vec<u16>) -> Self {
    MultipleHashSlots { inner: d }
  }
}

impl From<VecDeque<u16>> for MultipleHashSlots {
  fn from(d: VecDeque<u16>) -> Self {
    MultipleHashSlots {
      inner: d.into_iter().collect(),
    }
  }
}

impl FromIterator<u16> for MultipleHashSlots {
  fn from_iter<I: IntoIterator<Item = u16>>(iter: I) -> Self {
    MultipleHashSlots {
      inner: iter.into_iter().collect(),
    }
  }
}

/// Options for the CLUSTER FAILOVER command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterFailoverFlag {
  Force,
  Takeover,
}

impl ClusterFailoverFlag {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ClusterFailoverFlag::Force => "FORCE",
      ClusterFailoverFlag::Takeover => "TAKEOVER",
    }
  }
}

/// Flags for the CLUSTER RESET command.
///
/// <https://redis.io/commands/cluster-reset>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterResetFlag {
  Hard,
  Soft,
}

impl ClusterResetFlag {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ClusterResetFlag::Hard => "HARD",
      ClusterResetFlag::Soft => "SOFT",
    }
  }
}

/// Flags for the CLUSTER SETSLOT command.
///
/// <https://redis.io/commands/cluster-setslot>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterSetSlotState {
  Importing,
  Migrating,
  Stable,
  Node(String),
}

impl ClusterSetSlotState {
  pub(crate) fn to_str(&self) -> (&'static str, Option<&str>) {
    match *self {
      ClusterSetSlotState::Importing => ("IMPORTING", None),
      ClusterSetSlotState::Migrating => ("MIGRATING", None),
      ClusterSetSlotState::Stable => ("STABLE", None),
      ClusterSetSlotState::Node(ref n) => ("NODE", Some(n)),
    }
  }
}

/// A struct describing the longitude and latitude coordinates of a GEO command.
#[derive(Clone, Debug)]
pub struct GeoPosition {
  pub longitude: f64,
  pub latitude: f64,
}

impl PartialEq for GeoPosition {
  fn eq(&self, other: &Self) -> bool {
    utils::f64_eq(self.longitude, other.longitude) && utils::f64_eq(self.latitude, other.latitude)
  }
}

impl Eq for GeoPosition {}

impl From<(f64, f64)> for GeoPosition {
  fn from(d: (f64, f64)) -> Self {
    GeoPosition {
      longitude: d.0,
      latitude: d.1,
    }
  }
}

/// Units for the GEO DIST command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GeoUnit {
  Meters,
  Kilometers,
  Miles,
  Feet,
}

impl GeoUnit {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      GeoUnit::Meters => "m",
      GeoUnit::Kilometers => "km",
      GeoUnit::Feet => "ft",
      GeoUnit::Miles => "mi",
    }
  }
}

/// A struct describing the value inside a GEO data structure.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GeoValue {
  pub coordinates: GeoPosition,
  pub member: RedisValue,
}

impl GeoValue {
  pub fn new<V: Into<RedisValue>>(coordinates: GeoPosition, member: V) -> Self {
    let member = member.into();
    GeoValue { coordinates, member }
  }
}

impl<T> TryFrom<(f64, f64, T)> for GeoValue
where
  T: TryInto<RedisValue>,
  T::Error: Into<RedisError>,
{
  type Error = RedisError;

  fn try_from(v: (f64, f64, T)) -> Result<Self, Self::Error> {
    Ok(GeoValue {
      coordinates: GeoPosition {
        longitude: v.0,
        latitude: v.1,
      },
      member: utils::try_into(v.2)?,
    })
  }
}

/// A convenience struct for commands that take one or more GEO values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MultipleGeoValues {
  inner: Vec<GeoValue>,
}

impl MultipleGeoValues {
  pub fn len(&self) -> usize {
    self.inner.len()
  }

  pub fn inner(self) -> Vec<GeoValue> {
    self.inner
  }
}

impl From<GeoValue> for MultipleGeoValues {
  fn from(d: GeoValue) -> Self {
    MultipleGeoValues { inner: vec![d] }
  }
}

impl From<Vec<GeoValue>> for MultipleGeoValues {
  fn from(d: Vec<GeoValue>) -> Self {
    MultipleGeoValues { inner: d }
  }
}

impl From<VecDeque<GeoValue>> for MultipleGeoValues {
  fn from(d: VecDeque<GeoValue>) -> Self {
    MultipleGeoValues {
      inner: d.into_iter().collect(),
    }
  }
}

/// The sort order for redis commands that take or return a sorted list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SortOrder {
  Asc,
  Desc,
}

impl SortOrder {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      SortOrder::Asc => "ASC",
      SortOrder::Desc => "DESC",
    }
  }
}

/// A typed struct representing the full output of the GEORADIUS (or similar) command.
#[derive(Clone, Debug)]
pub struct GeoRadiusInfo {
  pub member: RedisValue,
  pub position: Option<GeoPosition>,
  pub distance: Option<f64>,
  pub hash: Option<i64>,
}

impl Default for GeoRadiusInfo {
  fn default() -> Self {
    GeoRadiusInfo {
      member: RedisValue::Null,
      position: None,
      distance: None,
      hash: None,
    }
  }
}

impl PartialEq for GeoRadiusInfo {
  fn eq(&self, other: &Self) -> bool {
    self.member == other.member
      && self.position == other.position
      && self.hash == other.hash
      && utils::f64_opt_eq(&self.distance, &other.distance)
  }
}

impl Eq for GeoRadiusInfo {}

/// Flags for the SCRIPT DEBUG command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScriptDebugFlag {
  Yes,
  No,
  Sync,
}

impl ScriptDebugFlag {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ScriptDebugFlag::Yes => "YES",
      ScriptDebugFlag::No => "NO",
      ScriptDebugFlag::Sync => "SYNC",
    }
  }
}

/// Location flag for the `LINSERT` command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListLocation {
  Before,
  After,
}

impl ListLocation {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ListLocation::Before => "BEFORE",
      ListLocation::After => "AFTER",
    }
  }
}

/// Ordering options for the ZADD (and related) commands.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Ordering {
  GreaterThan,
  LessThan,
}

impl Ordering {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      Ordering::GreaterThan => "GT",
      Ordering::LessThan => "LT",
    }
  }
}

/// Options for the ZRANGE (and related) commands.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ZSort {
  ByScore,
  ByLex,
}

impl ZSort {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ZSort::ByScore => "BYSCORE",
      ZSort::ByLex => "BYLEX",
    }
  }
}

/// An index, score, lexicographical, or +|-|+inf|-inf range bound for the ZRANGE command.
#[derive(Clone, Debug)]
pub enum ZRangeBound {
  /// Index ranges (<https://redis.io/commands/zrange#index-ranges>)
  Index(i64),
  /// Score ranges (<https://redis.io/commands/zrange#score-ranges>)
  Score(f64),
  /// Lexicographical ranges (<https://redis.io/commands/zrange#lexicographical-ranges>)
  Lex(String),
  /// Shortcut for the `+` character.
  InfiniteLex,
  /// Shortcut for the `-` character.
  NegInfinityLex,
  /// Shortcut for the `+inf` range bound.
  InfiniteScore,
  /// Shortcut for the `-inf` range bound.
  NegInfiniteScore,
}

impl From<i64> for ZRangeBound {
  fn from(i: i64) -> Self {
    ZRangeBound::Index(i)
  }
}

impl<'a> From<&'a str> for ZRangeBound {
  fn from(s: &'a str) -> Self {
    if s == "+inf" {
      ZRangeBound::InfiniteScore
    } else if s == "-inf" {
      ZRangeBound::NegInfiniteScore
    } else {
      ZRangeBound::Lex(s.to_owned())
    }
  }
}

impl From<String> for ZRangeBound {
  fn from(s: String) -> Self {
    if s == "+inf" {
      ZRangeBound::InfiniteScore
    } else if s == "-inf" {
      ZRangeBound::NegInfiniteScore
    } else {
      ZRangeBound::Lex(s)
    }
  }
}

impl<'a> From<&'a String> for ZRangeBound {
  fn from(s: &'a String) -> Self {
    s.as_str().into()
  }
}

impl TryFrom<f64> for ZRangeBound {
  type Error = RedisError;

  fn try_from(f: f64) -> Result<Self, Self::Error> {
    let value = if f.is_infinite() && f.is_sign_negative() {
      ZRangeBound::NegInfiniteScore
    } else if f.is_infinite() {
      ZRangeBound::InfiniteScore
    } else if f.is_nan() {
      return Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Cannot use NaN as zrange field.",
      ));
    } else {
      ZRangeBound::Score(f)
    };

    Ok(value)
  }
}

/// The type of range interval bound.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ZRangeKind {
  Inclusive,
  Exclusive,
}

impl Default for ZRangeKind {
  fn default() -> Self {
    ZRangeKind::Inclusive
  }
}

/// A wrapper struct for a range bound in a sorted set command.
#[derive(Clone, Debug)]
pub struct ZRange {
  pub kind: ZRangeKind,
  pub range: ZRangeBound,
}

impl ZRange {
  pub(crate) fn into_value(self) -> Result<RedisValue, RedisError> {
    let value = if self.kind == ZRangeKind::Exclusive {
      match self.range {
        ZRangeBound::Index(i) => format!("({}", i).into(),
        ZRangeBound::Score(f) => utils::f64_to_zrange_bound(f, &self.kind)?.into(),
        ZRangeBound::Lex(s) => utils::check_lex_str(s, &self.kind).into(),
        ZRangeBound::InfiniteLex => "+".into(),
        ZRangeBound::NegInfinityLex => "-".into(),
        ZRangeBound::InfiniteScore => "+inf".into(),
        ZRangeBound::NegInfiniteScore => "-inf".into(),
      }
    } else {
      match self.range {
        ZRangeBound::Index(i) => i.into(),
        ZRangeBound::Score(f) => f.try_into()?,
        ZRangeBound::Lex(s) => utils::check_lex_str(s, &self.kind).into(),
        ZRangeBound::InfiniteLex => "+".into(),
        ZRangeBound::NegInfinityLex => "-".into(),
        ZRangeBound::InfiniteScore => "+inf".into(),
        ZRangeBound::NegInfiniteScore => "-inf".into(),
      }
    };

    Ok(value)
  }
}

impl From<i64> for ZRange {
  fn from(i: i64) -> Self {
    ZRange {
      kind: ZRangeKind::default(),
      range: i.into(),
    }
  }
}

impl<'a> From<&'a str> for ZRange {
  fn from(s: &'a str) -> Self {
    ZRange {
      kind: ZRangeKind::default(),
      range: s.into(),
    }
  }
}

impl From<String> for ZRange {
  fn from(s: String) -> Self {
    ZRange {
      kind: ZRangeKind::default(),
      range: s.into(),
    }
  }
}

impl<'a> From<&'a String> for ZRange {
  fn from(s: &'a String) -> Self {
    ZRange {
      kind: ZRangeKind::default(),
      range: s.as_str().into(),
    }
  }
}

impl TryFrom<f64> for ZRange {
  type Error = RedisError;

  fn try_from(f: f64) -> Result<Self, Self::Error> {
    Ok(ZRange {
      kind: ZRangeKind::default(),
      range: f.try_into()?,
    })
  }
}

impl<'a> From<&'a ZRange> for ZRange {
  fn from(range: &'a ZRange) -> Self {
    range.clone()
  }
}

/// A trait that can be used to override DNS resolution logic for a client.
///
/// Note: using this requires [async-trait](https://crates.io/crates/async-trait).
// TODO expose this to callers so they can do their own DNS resolution
#[async_trait]
pub(crate) trait Resolve: Send + Sync + 'static {
  /// Resolve a hostname.
  async fn resolve(&self, host: String, port: u16) -> Result<SocketAddr, RedisError>;
}
