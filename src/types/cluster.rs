use crate::utils;
use bytes_utils::Str;

pub use crate::protocol::types::ClusterRouting;

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
  pub cluster_state:                   ClusterState,
  pub cluster_slots_assigned:          u16,
  pub cluster_slots_ok:                u16,
  pub cluster_slots_pfail:             u16,
  pub cluster_slots_fail:              u16,
  pub cluster_known_nodes:             u16,
  pub cluster_size:                    u32,
  pub cluster_current_epoch:           u64,
  pub cluster_my_epoch:                u64,
  pub cluster_stats_messages_sent:     u64,
  pub cluster_stats_messages_received: u64,
}

/// Options for the CLUSTER FAILOVER command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterFailoverFlag {
  Force,
  Takeover,
}

impl ClusterFailoverFlag {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      ClusterFailoverFlag::Force => "FORCE",
      ClusterFailoverFlag::Takeover => "TAKEOVER",
    })
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
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      ClusterResetFlag::Hard => "HARD",
      ClusterResetFlag::Soft => "SOFT",
    })
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
  pub(crate) fn to_str(&self) -> (Str, Option<Str>) {
    let (prefix, value) = match *self {
      ClusterSetSlotState::Importing => ("IMPORTING", None),
      ClusterSetSlotState::Migrating => ("MIGRATING", None),
      ClusterSetSlotState::Stable => ("STABLE", None),
      ClusterSetSlotState::Node(ref n) => ("NODE", Some(n.into())),
    };

    (utils::static_str(prefix), value)
  }
}
