use super::utils as protocol_utils;
use crate::clients::RedisClient;
use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::protocol::cluster;
use crate::types::*;
use crate::utils;
use crate::utils::{set_locked, take_locked};
use arcstr::ArcStr;
use bytes_utils::Str;
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use redis_protocol::resp2::types::Frame as Resp2Frame;
use redis_protocol::resp2_frame_to_resp3;
use redis_protocol::resp3::types::Frame as Resp3Frame;
pub use redis_protocol::{redis_keyslot, resp2::types::NULL, types::CRLF};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::convert::TryInto;
use std::fmt;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Sender as OneshotSender;

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
    // since the `RedisValue::convert` logic already accounts for different encodings of maps and sets we can just
    // change everything to RESP3 above the protocol layer. resp2->resp3 is lossless so this is safe.
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomKeySlot {
  pub key_slot: Option<u16>,
}

#[derive(Clone)]
pub struct SplitCommand {
  // TODO change to mutex
  pub tx: Arc<Mutex<Option<OneshotSender<Result<Vec<RedisClient>, RedisError>>>>>,
  pub config: Option<RedisConfig>,
}

impl fmt::Debug for SplitCommand {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[SplitCommand]")
  }
}

impl PartialEq for SplitCommand {
  fn eq(&self, other: &SplitCommand) -> bool {
    self.config == other.config
  }
}

impl Eq for SplitCommand {}

pub struct KeyScanInner {
  pub key_slot: Option<u16>,
  pub cursor: Str,
  pub tx: UnboundedSender<Result<ScanResult, RedisError>>,
}

impl PartialEq for KeyScanInner {
  fn eq(&self, other: &KeyScanInner) -> bool {
    self.cursor == other.cursor
  }
}

impl Eq for KeyScanInner {}

pub enum ValueScanResult {
  SScan(SScanResult),
  HScan(HScanResult),
  ZScan(ZScanResult),
}

pub struct ValueScanInner {
  pub cursor: Str,
  pub tx: UnboundedSender<Result<ValueScanResult, RedisError>>,
}

impl PartialEq for ValueScanInner {
  fn eq(&self, other: &ValueScanInner) -> bool {
    self.cursor == other.cursor
  }
}

impl Eq for ValueScanInner {}

impl ValueScanInner {
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

/// A container for replica server IDs with some added state for round-robin requests.
#[cfg(feature = "replicas")]
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ReplicaSet {
  servers: Vec<ArcStr>,
  next: usize,
}

#[cfg(feature = "replicas")]
impl ReplicaSet {
  pub fn next(&mut self) -> Option<&ArcStr> {
    if self.servers.is_empty() {
      return None;
    }

    let val = self.next;
    self.next += 1;
    Some(&self.servers[val % self.servers.len()])
  }
}

#[cfg(feature = "replicas")]
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ReplicaNodes {
  replicas: HashMap<ArcStr, ReplicaSet>,
}

#[cfg(feature = "replicas")]
impl From<HashMap<ArcStr, ReplicaSet>> for ReplicaNodes {
  fn from(replicas: HashMap<ArcStr, ReplicaSet>) -> Self {
    ReplicaNodes { replicas }
  }
}

#[cfg(feature = "replicas")]
impl ReplicaNodes {
  pub fn add(&mut self, primary: &ArcStr, replica: &ArcStr) {
    self
      .replicas
      .entry(primary.clone())
      .or_insert(ReplicaSet::default())
      .servers
      .push(replica.clone());
  }

  pub fn clear(&mut self) {
    self.replicas.clear();
  }

  pub fn next(&mut self, primary: &ArcStr) -> Option<&ArcStr> {
    self.replicas.get_mut(primary).and_then(|r| r.next())
  }
}

/// A slot range and associated cluster node information from the CLUSTER NODES command.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SlotRange {
  pub start: u16,
  pub end: u16,
  pub server: ArcStr,
  pub id: ArcStr,
  #[cfg(feature = "replicas")]
  pub(crate) replicas: Option<Arc<ReplicaNodes>>,
}

/// The cached view of the cluster used by the client to route commands to the correct cluster nodes.
#[derive(Debug, Clone)]
pub struct ClusterKeyCache {
  data: Vec<SlotRange>,
}

impl From<Vec<SlotRange>> for ClusterKeyCache {
  fn from(data: Vec<SlotRange>) -> Self {
    ClusterKeyCache { data }
  }
}

impl ClusterKeyCache {
  /// Create a new, empty cache.
  pub fn new() -> ClusterKeyCache {
    ClusterKeyCache { data: Vec::new() }
  }

  /// Read a set of unique hash slots that each map to a primary/main node in the cluster.
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
  pub fn get_server(&self, slot: u16) -> Option<&SlotRange> {
    if self.data.is_empty() {
      return None;
    }

    protocol_utils::binary_search(&self.data, slot)
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
      let idx = rand::thread_rng().gen_range(0..self.data.len());
      Some(&self.data[idx])
    } else {
      None
    }
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
