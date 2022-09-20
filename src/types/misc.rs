use crate::utils;
use bytes_utils::Str;
use std::{collections::HashMap, fmt};

/// Arguments passed to the SHUTDOWN command.
///
/// <https://redis.io/commands/shutdown>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShutdownFlags {
  Save,
  NoSave,
}

impl ShutdownFlags {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      ShutdownFlags::Save => "SAVE",
      ShutdownFlags::NoSave => "NOSAVE",
    })
  }
}

/// An event on the publish-subscribe interface describing a keyspace notification.
///
/// <https://redis.io/topics/notifications>
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct KeyspaceEvent {
  pub db:        u8,
  pub operation: String,
  pub key:       String,
}

/// Aggregate options for the [zinterstore](https://redis.io/commands/zinterstore) (and related) commands.
pub enum AggregateOptions {
  Sum,
  Min,
  Max,
}

impl AggregateOptions {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      AggregateOptions::Sum => "SUM",
      AggregateOptions::Min => "MIN",
      AggregateOptions::Max => "MAX",
    })
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
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
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
    })
  }
}

/// Configuration for custom redis commands, primarily used for interacting with third party modules or extensions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomCommand {
  /// The command name, sent directly to the server.
  pub cmd:         Str,
  /// The hash slot to use for the provided command when running against a cluster. If a hash slot is not provided
  /// the command will run against a random node in the cluster.
  pub hash_slot:   Option<u16>,
  /// Whether or not the command should block the connection while waiting on a response.
  pub is_blocking: bool,
}

impl CustomCommand {
  /// create a new custom command.
  ///
  /// see the [custom](crate::interfaces::ClientLike::custom) command for more information.
  pub fn new<C>(cmd: C, hash_slot: Option<u16>, is_blocking: bool) -> Self
  where
    C: Into<Str>, {
    CustomCommand {
      cmd: cmd.into(),
      hash_slot,
      is_blocking,
    }
  }

  /// Create a new custom command specified by a `&'static str`.
  pub fn new_static(cmd: &'static str, hash_slot: Option<u16>, is_blocking: bool) -> Self {
    CustomCommand {
      cmd: utils::static_str(cmd),
      hash_slot,
      is_blocking,
    }
  }
}

/// An enum describing the possible ways in which a Redis cluster can change state.
///
/// See [on_cluster_change](crate::clients::RedisClient::on_cluster_change) for more information.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterStateChange {
  /// A node was added to the cluster.
  ///
  /// This implies that hash slots were also probably rebalanced.
  Add((String, u16)),
  /// A node was removed from the cluster.
  ///
  /// This implies that hash slots were also probably rebalanced.
  Remove((String, u16)),
  /// Hash slots were rebalanced across the cluster.
  Rebalance,
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
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      SetOptions::NX => "NX",
      SetOptions::XX => "XX",
    })
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
  pub(crate) fn into_args(self) -> (Str, Option<i64>) {
    let (prefix, value) = match self {
      Expiration::EX(i) => ("EX", Some(i)),
      Expiration::PX(i) => ("PX", Some(i)),
      Expiration::EXAT(i) => ("EXAT", Some(i)),
      Expiration::PXAT(i) => ("PXAT", Some(i)),
      Expiration::KEEPTTL => ("KEEPTTL", None),
    };

    (utils::static_str(prefix), value)
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
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      ClientState::Connecting => "Connecting",
      ClientState::Connected => "Connected",
      ClientState::Disconnecting => "Disconnecting",
      ClientState::Disconnected => "Disconnected",
    })
  }
}

impl fmt::Display for ClientState {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}", self.to_str())
  }
}

/// The parsed result of the MEMORY STATS command for a specific database.
///
/// <https://redis.io/commands/memory-stats>
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseMemoryStats {
  pub overhead_hashtable_main:    u64,
  pub overhead_hashtable_expires: u64,
}

impl Default for DatabaseMemoryStats {
  fn default() -> Self {
    DatabaseMemoryStats {
      overhead_hashtable_expires: 0,
      overhead_hashtable_main:    0,
    }
  }
}

/// The parsed result of the MEMORY STATS command.
///
/// <https://redis.io/commands/memory-stats>
#[derive(Clone, Debug)]
pub struct MemoryStats {
  pub peak_allocated:                u64,
  pub total_allocated:               u64,
  pub startup_allocated:             u64,
  pub replication_backlog:           u64,
  pub clients_slaves:                u64,
  pub clients_normal:                u64,
  pub aof_buffer:                    u64,
  pub lua_caches:                    u64,
  pub overhead_total:                u64,
  pub keys_count:                    u64,
  pub keys_bytes_per_key:            u64,
  pub dataset_bytes:                 u64,
  pub dataset_percentage:            f64,
  pub peak_percentage:               f64,
  pub fragmentation:                 f64,
  pub fragmentation_bytes:           u64,
  pub rss_overhead_ratio:            f64,
  pub rss_overhead_bytes:            u64,
  pub allocator_allocated:           u64,
  pub allocator_active:              u64,
  pub allocator_resident:            u64,
  pub allocator_fragmentation_ratio: f64,
  pub allocator_fragmentation_bytes: u64,
  pub allocator_rss_ratio:           f64,
  pub allocator_rss_bytes:           u64,
  pub db:                            HashMap<u16, DatabaseMemoryStats>,
}

impl Default for MemoryStats {
  fn default() -> Self {
    MemoryStats {
      peak_allocated:                0,
      total_allocated:               0,
      startup_allocated:             0,
      replication_backlog:           0,
      clients_normal:                0,
      clients_slaves:                0,
      aof_buffer:                    0,
      lua_caches:                    0,
      overhead_total:                0,
      keys_count:                    0,
      keys_bytes_per_key:            0,
      dataset_bytes:                 0,
      dataset_percentage:            0.0,
      peak_percentage:               0.0,
      fragmentation:                 0.0,
      fragmentation_bytes:           0,
      rss_overhead_ratio:            0.0,
      rss_overhead_bytes:            0,
      allocator_allocated:           0,
      allocator_active:              0,
      allocator_resident:            0,
      allocator_fragmentation_ratio: 0.0,
      allocator_fragmentation_bytes: 0,
      allocator_rss_bytes:           0,
      allocator_rss_ratio:           0.0,
      db:                            HashMap::new(),
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

/// The output of an entry in the slow queries log.
///
/// <https://redis.io/commands/slowlog#output-format>
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlowlogEntry {
  pub id:        i64,
  pub timestamp: i64,
  pub duration:  u64,
  pub args:      Vec<String>,
  pub ip:        Option<String>,
  pub name:      Option<String>,
}

/// Flags for the SCRIPT DEBUG command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScriptDebugFlag {
  Yes,
  No,
  Sync,
}

impl ScriptDebugFlag {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      ScriptDebugFlag::Yes => "YES",
      ScriptDebugFlag::No => "NO",
      ScriptDebugFlag::Sync => "SYNC",
    })
  }
}

/// Arguments for the `SENTINEL SIMULATE-FAILURE` command.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "sentinel-client")]
#[cfg_attr(docsrs, doc(cfg(feature = "sentinel-client")))]
pub enum SentinelFailureKind {
  CrashAfterElection,
  CrashAfterPromotion,
  Help,
}

#[cfg(feature = "sentinel-client")]
impl SentinelFailureKind {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match self {
      SentinelFailureKind::CrashAfterElection => "crash-after-election",
      SentinelFailureKind::CrashAfterPromotion => "crash-after-promotion",
      SentinelFailureKind::Help => "help",
    })
  }
}

/// The sort order for redis commands that take or return a sorted list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SortOrder {
  Asc,
  Desc,
}

impl SortOrder {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      SortOrder::Asc => "ASC",
      SortOrder::Desc => "DESC",
    })
  }
}
