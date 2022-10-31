use super::utils as protocol_utils;
use crate::{
  clients::RedisClient,
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::cluster,
  types::*,
  utils,
  utils::{set_locked, take_locked},
};
use arcstr::ArcStr;
use bytes_utils::Str;
use parking_lot::{Mutex, RwLock};
use rand::Rng;
pub use redis_protocol::{redis_keyslot, resp2::types::NULL, types::CRLF};
use redis_protocol::{resp2::types::Frame as Resp2Frame, resp2_frame_to_resp3, resp3::types::Frame as Resp3Frame};
use std::{
  collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
  convert::TryInto,
  fmt,
  net::{SocketAddr, ToSocketAddrs},
  sync::{atomic::AtomicUsize, Arc},
  time::Instant,
};
use tokio::sync::{mpsc::UnboundedSender, oneshot::Sender as OneshotSender};

#[cfg(feature = "blocking-encoding")]
use crate::globals::globals;

#[cfg(not(feature = "full-tracing"))]
use crate::trace::disabled::Span as FakeSpan;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use crate::trace::CommandTraces;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use crate::trace::Span;

pub const REDIS_CLUSTER_SLOTS: u16 = 16384;

#[derive(Debug)]
pub enum ProtocolFrame {
  Resp2(Resp2Frame),
  Resp3(Resp3Frame),
}

impl ProtocolFrame {
  pub fn into_resp3(self) -> Resp3Frame {
    // the `RedisValue::convert` logic already accounts for different encodings of maps and sets, so
    // we can just change everything to RESP3 above the protocol layer
    match self {
      ProtocolFrame::Resp2(frame) => resp2_frame_to_resp3(frame),
      ProtocolFrame::Resp3(frame) => frame,
    }
  }
}

impl From<Resp2Frame> for ProtocolFrame {
  fn from(frame: Resp2Frame) -> Self {
    ProtocolFrame::Resp2(frame)
  }
}

impl From<Resp3Frame> for ProtocolFrame {
  fn from(frame: Resp3Frame) -> Self {
    ProtocolFrame::Resp3(frame)
  }
}

pub struct KeyScanInner {
  /// The hash slot for the command.
  pub hash_slot:  Option<u16>,
  /// The index of the cursor in `args`.
  pub cursor_idx: usize,
  /// The arguments sent in each scan command.
  pub args:       Vec<RedisValue>,
  /// The sender half of the results channel.
  pub tx:         UnboundedSender<Result<ScanResult, RedisError>>,
}

impl PartialEq for KeyScanInner {
  fn eq(&self, other: &KeyScanInner) -> bool {
    self.cursor == other.cursor
  }
}

impl Eq for KeyScanInner {}

impl KeyScanInner {
  /// Update the cursor in place in the arguments.
  pub fn update_cursor(&mut self, cursor: Str) {
    self.args[self.cursor_idx] = cursor.into();
  }

  /// Send an error on the response stream.
  pub fn send_error(&self, error: RedisError) {
    self.tx.send(Err(error));
  }
}

pub enum ValueScanResult {
  SScan(SScanResult),
  HScan(HScanResult),
  ZScan(ZScanResult),
}

pub struct ValueScanInner {
  /// The index of the cursor argument in `args`.
  pub cursor_idx: usize,
  /// The arguments sent in each scan command.
  pub args:       Vec<RedisValue>,
  /// The sender half of the results channel.
  pub tx:         UnboundedSender<Result<ValueScanResult, RedisError>>,
}

impl PartialEq for ValueScanInner {
  fn eq(&self, other: &ValueScanInner) -> bool {
    self.cursor == other.cursor
  }
}

impl Eq for ValueScanInner {}

impl ValueScanInner {
  /// Update the cursor in place in the arguments.
  pub fn update_cursor(&mut self, cursor: Str) {
    self.args[self.cursor_idx] = cursor.into();
  }

  /// Send an error on the response stream.
  pub fn send_error(&self, error: RedisError) {
    self.tx.send(Err(error));
  }

  pub fn transform_hscan_result(mut data: Vec<RedisValue>) -> Result<RedisMap, RedisError> {
    if data.is_empty() {
      return Ok(RedisMap::new());
    }
    if data.len() % 2 != 0 {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Invalid HSCAN result. Expected array with an even number of elements.",
      ));
    }

    let mut out = HashMap::with_capacity(data.len() / 2);
    while data.len() >= 2 {
      let value = data.pop().unwrap();
      let key: RedisKey = match data.pop().unwrap() {
        RedisValue::String(s) => s.into(),
        RedisValue::Bytes(b) => b.into(),
        _ => {
          return Err(RedisError::new(
            RedisErrorKind::ProtocolError,
            "Invalid HSCAN result. Expected string.",
          ))
        },
      };

      out.insert(key, value);
    }

    Ok(out.try_into()?)
  }

  pub fn transform_zscan_result(mut data: Vec<RedisValue>) -> Result<Vec<(RedisValue, f64)>, RedisError> {
    if data.is_empty() {
      return Ok(Vec::new());
    }
    if data.len() % 2 != 0 {
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Invalid ZSCAN result. Expected array with an even number of elements.",
      ));
    }

    let mut out = Vec::with_capacity(data.len() / 2);

    for chunk in data.chunks_exact_mut(2) {
      let value = chunk[0].take();
      let score = match chunk[1].take() {
        RedisValue::String(s) => utils::redis_string_to_f64(&s)?,
        RedisValue::Integer(i) => i as f64,
        RedisValue::Double(f) => f,
        _ => {
          return Err(RedisError::new(
            RedisErrorKind::ProtocolError,
            "Invalid HSCAN result. Expected a string or number score.",
          ))
        },
      };

      out.push((value, score));
    }

    Ok(out)
  }
}

/// A container for replica server IDs that can also act as an unbounded iterator.
#[cfg(feature = "replicas")]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ReplicaSet {
  servers: Vec<ArcStr>,
  next:    usize,
}

#[cfg(feature = "replicas")]
#[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
impl ReplicaSet {
  /// Read the replica server ID that should handle the next command.
  pub fn next(&mut self) -> Option<&ArcStr> {
    if self.servers.is_empty() {
      return None;
    }

    let val = self.next;
    self.next = self.next.wrapping_add(1);
    Some(&self.servers[val % self.servers.len()])
  }
}

/// A slot range and associated cluster node information from the `CLUSTER SLOTS` command.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SlotRange {
  pub start:    u16,
  pub end:      u16,
  pub primary:  ArcStr,
  pub id:       ArcStr,
  #[cfg(feature = "replicas")]
  #[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
  pub replicas: Option<ReplicaSet>,
}

/// The cached view of the cluster used by the client to route commands to the correct cluster nodes.
#[derive(Debug, Clone)]
pub struct ClusterRouting {
  data: Vec<SlotRange>,
}

impl From<Vec<SlotRange>> for ClusterRouting {
  fn from(data: Vec<SlotRange>) -> Self {
    ClusterRouting { data }
  }
}

impl ClusterRouting {
  /// Create a new, empty cache.
  pub fn new() -> Self {
    ClusterRouting { data: Vec::new() }
  }

  /// Read a set of unique hash slots that each map to a different primary/main node in the cluster.
  pub fn unique_hash_slots(&self) -> Vec<u16> {
    let mut out = BTreeMap::new();

    for slot in self.data.iter() {
      out.insert(&slot.server, slot.start);
    }

    out.into_iter().map(|(_, v)| v).collect()
  }

  /// Read the set of unique primary nodes in the cluster.
  pub fn unique_primary_nodes(&self) -> Vec<ArcStr> {
    let mut out = BTreeSet::new();

    for slot in self.data.iter() {
      out.insert(slot.server.clone());
    }

    out.into_iter().collect()
  }

  /// Clear the cached state of the cluster.
  pub fn clear(&mut self) {
    self.data.clear();
  }

  /// Rebuild the cache in place with the output of a CLUSTER NODES command.
  pub fn rebuild(&mut self, cluster_slots: RedisValue, default_host: &str) -> Result<(), RedisError> {
    self.data = cluster::parse_cluster_slots(cluster_slots, default_host)?;
    self.data.sort_by(|a, b| a.start.cmp(&b.start));
    Ok(())
  }

  /// Calculate the cluster hash slot for the provided key.
  pub fn hash_key(key: &[u8]) -> u16 {
    redis_protocol::redis_keyslot(key)
  }

  /// Find the server that owns the provided hash slot.
  pub fn get_server(&self, slot: u16) -> Option<&ArcStr> {
    if self.data.is_empty() {
      return None;
    }

    protocol_utils::binary_search(&self.data, slot).map(|idx| &self.data[idx].server)
  }

  /// Read the set of replicas that own the provided hash slot.
  #[cfg(feature = "replicas")]
  #[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
  pub fn read_replicas(&mut self, slot: u16) -> Option<&mut ReplicaSet> {
    if self.data.is_empty() {
      return None;
    }

    protocol_utils::binary_search(&self.data, slot).and_then(|idx| self.data[idx].replicas.as_mut())
  }

  /// Read the replica server ID that should handle the next command.
  #[cfg(feature = "replicas")]
  #[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
  pub fn next_replica(&mut self, slot: u16) -> Option<&ArcStr> {
    self.read_replicas(slot).and_then(|r| r.next())
  }

  /// Read the number of hash slot ranges in the cluster.
  pub fn len(&self) -> usize {
    self.data.len()
  }

  /// Read the hash slot ranges in the cluster.
  pub fn slots(&self) -> &Vec<SlotRange> {
    &self.data
  }

  /// Read a random primary node hash slot range from the cluster cache.
  pub fn random_slot(&self) -> Option<&SlotRange> {
    if self.data.len() > 0 {
      let idx = rand::thread_rng().gen_range(0 .. self.data.len());
      Some(&self.data[idx])
    } else {
      None
    }
  }

  /// Read a random primary node from the cluster cache.
  pub fn random_node(&self) -> Option<&ArcStr> {
    self.random_slot().map(|slot| &slot.id)
  }
}

// TODO support custom DNS resolution logic by exposing this in the client.
/// Default DNS resolver that just uses `to_socket_addrs` under the hood.
#[derive(Clone, Debug)]
pub struct DefaultResolver {
  id: ArcStr,
}

impl DefaultResolver {
  /// Create a new resolver using the system's default DNS resolution.
  pub fn new(id: &ArcStr) -> Self {
    DefaultResolver { id: id.clone() }
  }
}

#[async_trait]
impl Resolve for DefaultResolver {
  async fn resolve(&self, host: String, port: u16) -> Result<SocketAddr, RedisError> {
    let client_id = self.id.clone();

    tokio::task::spawn_blocking(move || {
      let ips: Vec<SocketAddr> = format!("{}:{}", host, port).to_socket_addrs()?.into_iter().collect();

      if ips.is_empty() {
        Err(RedisError::new(
          RedisErrorKind::IO,
          format!("Failed to resolve {}:{}", host, port),
        ))
      } else {
        let possible_addrs = ips.len();
        let addr = ips[0];

        trace!(
          "{}: Using {} among {} possible socket addresses for {}:{}",
          client_id,
          addr.ip(),
          possible_addrs,
          host,
          port
        );
        Ok(addr)
      }
    })
    .await?
  }
}
