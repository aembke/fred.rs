use super::utils as protocol_utils;
use crate::client::RedisClient;
use crate::error::{RedisError, RedisErrorKind};
use crate::globals::globals;
use crate::types::*;
use crate::utils;
use crate::utils::{set_locked, take_locked};
use parking_lot::RwLock;
pub use redis_protocol::{redis_keyslot, resp2::types::NULL, types::CRLF};
use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Sender as OneshotSender;

#[cfg(not(feature = "full-tracing"))]
use crate::trace::disabled::Span as FakeSpan;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use crate::trace::CommandTraces;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use crate::trace::Span;
use rand::Rng;
use std::borrow::Cow;

pub const REDIS_CLUSTER_SLOTS: u16 = 16384;

#[derive(Clone)]
pub struct AllNodesResponse {
  num_nodes: Arc<RwLock<usize>>,
  resp_tx: Arc<RwLock<Option<OneshotSender<Result<(), RedisError>>>>>,
}

impl fmt::Debug for AllNodesResponse {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[AllNodesResponse]")
  }
}

impl PartialEq for AllNodesResponse {
  fn eq(&self, _other: &Self) -> bool {
    // this doesnt matter, it's here to make the derive(Eq) on RedisCommandKind easier
    true
  }
}

impl Eq for AllNodesResponse {}

impl AllNodesResponse {
  pub fn new(tx: OneshotSender<Result<(), RedisError>>) -> Self {
    AllNodesResponse {
      num_nodes: Arc::new(RwLock::new(0)),
      resp_tx: Arc::new(RwLock::new(Some(tx))),
    }
  }

  pub fn set_num_nodes(&self, num_nodes: usize) {
    let mut guard = self.num_nodes.write();
    *guard = num_nodes;
  }

  pub fn num_nodes(&self) -> usize {
    self.num_nodes.read().clone()
  }

  pub fn decr_num_nodes(&self) -> usize {
    let mut guard = self.num_nodes.write();
    *guard = guard.saturating_sub(1);
    *guard
  }

  pub fn take_tx(&self) -> Option<OneshotSender<Result<(), RedisError>>> {
    self.resp_tx.write().take()
  }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomKeySlot {
  pub key_slot: Option<u16>,
}

#[derive(Clone)]
pub struct SplitCommand {
  pub tx: Arc<RwLock<Option<OneshotSender<Result<Vec<RedisClient>, RedisError>>>>>,
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

#[derive(Clone)]
pub enum ResponseKind {
  Blocking { tx: Option<UnboundedSender<Frame>> },
  Multiple { count: usize, buffer: VecDeque<Frame> },
}

impl fmt::Debug for ResponseKind {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[Response Kind]")
  }
}

impl PartialEq for ResponseKind {
  fn eq(&self, other: &ResponseKind) -> bool {
    match *self {
      ResponseKind::Blocking { .. } => match *other {
        ResponseKind::Blocking { .. } => true,
        ResponseKind::Multiple { .. } => false,
      },
      ResponseKind::Multiple { .. } => match *other {
        ResponseKind::Blocking { .. } => false,
        ResponseKind::Multiple { .. } => true,
      },
    }
  }
}

impl Eq for ResponseKind {}

pub struct KeyScanInner {
  pub key_slot: Option<u16>,
  pub cursor: String,
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
  pub cursor: String,
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

    let mut out = utils::new_map(data.len() / 2);
    while data.len() >= 2 {
      let value = data.pop().unwrap();
      let key = match data.pop().unwrap() {
        RedisValue::String(s) => s,
        _ => {
          return Err(RedisError::new(
            RedisErrorKind::ProtocolError,
            "Invalid HSCAN result. Expected string.",
          ))
        }
      };

      out.insert(key, value);
    }

    Ok(out.into())
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
        _ => {
          return Err(RedisError::new(
            RedisErrorKind::ProtocolError,
            "Invalid HSCAN result. Expected a string or integer score.",
          ))
        }
      };

      out.push((value, score));
    }

    Ok(out)
  }
}

#[derive(Eq, PartialEq)]
pub enum RedisCommandKind {
  AclLoad,
  AclSave,
  AclList,
  AclUsers,
  AclGetUser,
  AclSetUser,
  AclDelUser,
  AclCat,
  AclGenPass,
  AclWhoAmI,
  AclLog,
  AclHelp,
  Append,
  Auth,
  BgreWriteAof,
  BgSave,
  BitCount,
  BitField,
  BitOp,
  BitPos,
  BlPop,
  BlMove,
  BrPop,
  BrPopLPush,
  BzPopMin,
  BzPopMax,
  ClientID,
  ClientInfo,
  ClientKill,
  ClientList,
  ClientGetName,
  ClientGetRedir,
  ClientPause,
  ClientUnpause,
  ClientUnblock,
  ClientReply,
  ClientSetname,
  ClusterAddSlots,
  ClusterCountFailureReports,
  ClusterCountKeysInSlot,
  ClusterDelSlots,
  ClusterFailOver,
  ClusterForget,
  ClusterFlushSlots,
  ClusterGetKeysInSlot,
  ClusterInfo,
  ClusterKeySlot,
  ClusterMeet,
  ClusterMyID,
  ClusterNodes,
  ClusterReplicate,
  ClusterReset,
  ClusterSaveConfig,
  ClusterSetConfigEpoch,
  ClusterBumpEpoch,
  ClusterSetSlot,
  ClusterReplicas,
  ClusterSlots,
  ConfigGet,
  ConfigRewrite,
  ConfigSet,
  ConfigResetStat,
  Copy,
  DBSize,
  Decr,
  DecrBy,
  Del,
  Discard,
  Dump,
  Echo,
  Eval(CustomKeySlot),
  EvalSha(CustomKeySlot),
  Exec,
  Exists,
  Expire,
  ExpireAt,
  Failover,
  FlushAll,
  FlushDB,
  GeoAdd,
  GeoHash,
  GeoPos,
  GeoDist,
  GeoRadius,
  GeoRadiusByMember,
  GeoSearch,
  GeoSearchStore,
  Get,
  GetBit,
  GetDel,
  GetRange,
  GetSet,
  HDel,
  HExists,
  HGet,
  HGetAll,
  HIncrBy,
  HIncrByFloat,
  HKeys,
  HLen,
  HMGet,
  HMSet,
  HSet,
  HSetNx,
  HStrLen,
  HVals,
  HRandField,
  Incr,
  IncrBy,
  IncrByFloat,
  Info,
  Keys,
  LastSave,
  LIndex,
  LInsert,
  LLen,
  LMove,
  LPop,
  LPos,
  LPush,
  LPushX,
  LRange,
  LRem,
  LSet,
  LTrim,
  MemoryDoctor,
  MemoryHelp,
  MemoryMallocStats,
  MemoryPurge,
  MemoryStats,
  MemoryUsage,
  Mget,
  Migrate,
  Monitor,
  Move,
  Mset,
  Msetnx,
  Multi,
  Object,
  Persist,
  Pexpire,
  Pexpireat,
  Pfadd,
  Pfcount,
  Pfmerge,
  Ping,
  Psetex,
  Psubscribe(ResponseKind),
  Pubsub,
  Pttl,
  Publish,
  Punsubscribe(ResponseKind),
  Quit,
  Randomkey,
  Readonly,
  Readwrite,
  Rename,
  Renamenx,
  Restore,
  Role,
  Rpop,
  Rpoplpush,
  Rpush,
  Rpushx,
  Sadd,
  Save,
  Scard,
  Sdiff,
  Sdiffstore,
  Select,
  Set,
  Setbit,
  Setex,
  Setnx,
  Setrange,
  Shutdown,
  Sinter,
  Sinterstore,
  Sismember,
  Replicaof,
  Slowlog,
  Smembers,
  Smismember,
  Smove,
  Sort,
  Spop,
  Srandmember,
  Srem,
  Strlen,
  Subscribe,
  Sunion,
  Sunionstore,
  Swapdb,
  Sync,
  Time,
  Touch,
  Ttl,
  Type,
  Unsubscribe,
  Unlink,
  Unwatch,
  Wait,
  Watch,
  Zadd,
  Zcard,
  Zcount,
  Zdiff,
  Zdiffstore,
  Zincrby,
  Zinter,
  Zinterstore,
  Zlexcount,
  Zrandmember,
  Zrange,
  Zrangestore,
  Zrangebylex,
  Zrangebyscore,
  Zrank,
  Zrem,
  Zremrangebylex,
  Zremrangebyrank,
  Zremrangebyscore,
  Zrevrange,
  Zrevrangebylex,
  Zrevrangebyscore,
  Zrevrank,
  Zscore,
  Zmscore,
  Zunion,
  Zunionstore,
  Zpopmax,
  Zpopmin,
  ScriptLoad,
  ScriptDebug,
  ScriptExists,
  ScriptFlush,
  ScriptKill,
  Scan(KeyScanInner),
  Sscan(ValueScanInner),
  Hscan(ValueScanInner),
  Zscan(ValueScanInner),
  #[doc(hidden)]
  _Close,
  #[doc(hidden)]
  _Split(SplitCommand),
  #[doc(hidden)]
  _FlushAllCluster(AllNodesResponse),
  _ScriptFlushCluster(AllNodesResponse),
  _ScriptLoadCluster(AllNodesResponse),
  _ScriptKillCluster(AllNodesResponse),
  _Custom(CustomCommand),
}

impl fmt::Debug for RedisCommandKind {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}", self.to_str_debug())
  }
}

impl RedisCommandKind {
  pub fn is_scan(&self) -> bool {
    match *self {
      RedisCommandKind::Scan(_) => true,
      _ => false,
    }
  }

  pub fn is_hscan(&self) -> bool {
    match *self {
      RedisCommandKind::Hscan(_) => true,
      _ => false,
    }
  }

  pub fn is_sscan(&self) -> bool {
    match *self {
      RedisCommandKind::Sscan(_) => true,
      _ => false,
    }
  }

  pub fn is_zscan(&self) -> bool {
    match *self {
      RedisCommandKind::Zscan(_) => true,
      _ => false,
    }
  }

  pub fn is_value_scan(&self) -> bool {
    match *self {
      RedisCommandKind::Zscan(_) | RedisCommandKind::Hscan(_) | RedisCommandKind::Sscan(_) => true,
      _ => false,
    }
  }

  pub fn has_response_kind(&self) -> bool {
    match *self {
      RedisCommandKind::Punsubscribe(_) | RedisCommandKind::Psubscribe(_) => true,
      _ => false,
    }
  }

  pub fn has_multiple_response_kind(&self) -> bool {
    match *self {
      RedisCommandKind::Punsubscribe(_) | RedisCommandKind::Psubscribe(_) => true,
      _ => false,
    }
  }

  pub fn has_blocking_response_kind(&self) -> bool {
    match *self {
      RedisCommandKind::_Custom(ref kind) => kind.is_blocking,
      _ => false,
    }
  }

  pub fn response_kind(&self) -> Option<&ResponseKind> {
    match *self {
      RedisCommandKind::Punsubscribe(ref k) | RedisCommandKind::Psubscribe(ref k) => Some(k),
      _ => None,
    }
  }

  pub fn response_kind_mut(&mut self) -> Option<&mut ResponseKind> {
    match *self {
      RedisCommandKind::Punsubscribe(ref mut k) | RedisCommandKind::Psubscribe(ref mut k) => Some(k),
      _ => None,
    }
  }

  pub fn is_multi(&self) -> bool {
    match *self {
      RedisCommandKind::Multi => true,
      _ => false,
    }
  }

  pub fn is_exec(&self) -> bool {
    match *self {
      RedisCommandKind::Exec => true,
      _ => false,
    }
  }

  pub fn is_discard(&self) -> bool {
    match *self {
      RedisCommandKind::Discard => true,
      _ => false,
    }
  }

  pub fn ends_transaction(&self) -> bool {
    match *self {
      RedisCommandKind::Exec | RedisCommandKind::Discard => true,
      _ => false,
    }
  }

  pub fn is_mset(&self) -> bool {
    match *self {
      RedisCommandKind::Mset | RedisCommandKind::Msetnx => true,
      _ => false,
    }
  }

  pub fn is_split(&self) -> bool {
    match *self {
      RedisCommandKind::_Split(_) => true,
      _ => false,
    }
  }

  pub fn is_close(&self) -> bool {
    match *self {
      RedisCommandKind::_Close => true,
      _ => false,
    }
  }

  pub fn is_custom(&self) -> bool {
    match *self {
      RedisCommandKind::_Custom(_) => true,
      _ => false,
    }
  }

  /// Read the command's protocol string without panicking.
  pub fn to_str_debug(&self) -> &'static str {
    match *self {
      RedisCommandKind::AclLoad => "ACL LOAD",
      RedisCommandKind::AclSave => "ACL SAVE",
      RedisCommandKind::AclList => "ACL LIST",
      RedisCommandKind::AclUsers => "ACL USERS",
      RedisCommandKind::AclGetUser => "ACL GETUSER",
      RedisCommandKind::AclSetUser => "ACL SETUSER",
      RedisCommandKind::AclDelUser => "ACL DELUSER",
      RedisCommandKind::AclCat => "ACL CAT",
      RedisCommandKind::AclGenPass => "ACL GENPASS",
      RedisCommandKind::AclWhoAmI => "ACL WHOAMI",
      RedisCommandKind::AclLog => "ACL LOG",
      RedisCommandKind::AclHelp => "ACL HELP",
      RedisCommandKind::Append => "APPEND",
      RedisCommandKind::Auth => "AUTH",
      RedisCommandKind::BgreWriteAof => "BGREWRITEAOF",
      RedisCommandKind::BgSave => "BGSAVE",
      RedisCommandKind::BitCount => "BITCOUNT",
      RedisCommandKind::BitField => "BITFIELD",
      RedisCommandKind::BitOp => "BITOP",
      RedisCommandKind::BitPos => "BITPOS",
      RedisCommandKind::BlPop => "BLPOP",
      RedisCommandKind::BlMove => "BLMOVE",
      RedisCommandKind::BrPop => "BRPOP",
      RedisCommandKind::BrPopLPush => "BRPOPLPUSH",
      RedisCommandKind::BzPopMin => "BZPOPMIN",
      RedisCommandKind::BzPopMax => "BZPOPMAX",
      RedisCommandKind::ClientID => "CLIENT ID",
      RedisCommandKind::ClientInfo => "CLIENT INFO",
      RedisCommandKind::ClientKill => "CLIENT KILL",
      RedisCommandKind::ClientList => "CLIENT LIST",
      RedisCommandKind::ClientGetRedir => "CLIENT GETREDIR",
      RedisCommandKind::ClientGetName => "CLIENT GETNAME",
      RedisCommandKind::ClientPause => "CLIENT PAUSE",
      RedisCommandKind::ClientUnpause => "CLIENT UNPAUSE",
      RedisCommandKind::ClientUnblock => "CLIENT UNBLOCK",
      RedisCommandKind::ClientReply => "CLIENT REPLY",
      RedisCommandKind::ClientSetname => "CLIENT SETNAME",
      RedisCommandKind::ClusterAddSlots => "CLUSTER ADDSLOTS",
      RedisCommandKind::ClusterCountFailureReports => "CLUSTER COUNT-FAILURE-REPORTS",
      RedisCommandKind::ClusterCountKeysInSlot => "CLUSTER COUNTKEYSINSLOT",
      RedisCommandKind::ClusterDelSlots => "CLUSTER DEL SLOTS",
      RedisCommandKind::ClusterFailOver => "CLUSTER FAILOVER",
      RedisCommandKind::ClusterForget => "CLUSTER FORGET",
      RedisCommandKind::ClusterGetKeysInSlot => "CLUSTER GETKEYSINSLOTS",
      RedisCommandKind::ClusterInfo => "CLUSTER INFO",
      RedisCommandKind::ClusterKeySlot => "CLUSTER KEYSLOT",
      RedisCommandKind::ClusterMeet => "CLUSTER MEET",
      RedisCommandKind::ClusterNodes => "CLUSTER NODES",
      RedisCommandKind::ClusterReplicate => "CLUSTER REPLICATE",
      RedisCommandKind::ClusterReset => "CLUSTER RESET",
      RedisCommandKind::ClusterSaveConfig => "CLUSTER SAVECONFIG",
      RedisCommandKind::ClusterSetConfigEpoch => "CLUSTER SET-CONFIG-EPOCH",
      RedisCommandKind::ClusterSetSlot => "CLUSTER SETSLOT",
      RedisCommandKind::ClusterReplicas => "CLUSTER REPLICAS",
      RedisCommandKind::ClusterSlots => "CLUSTER SLOTS",
      RedisCommandKind::ClusterBumpEpoch => "CLUSTER BUMPEPOCH",
      RedisCommandKind::ClusterFlushSlots => "CLUSTER FLUSHSLOTS",
      RedisCommandKind::ClusterMyID => "CLUSTER MYID",
      RedisCommandKind::ConfigGet => "CONFIG GET",
      RedisCommandKind::ConfigRewrite => "CONFIG REWRITE",
      RedisCommandKind::ConfigSet => "CONFIG SET",
      RedisCommandKind::ConfigResetStat => "CONFIG RESETSTAT",
      RedisCommandKind::Copy => "COPY",
      RedisCommandKind::DBSize => "DBSIZE",
      RedisCommandKind::Decr => "DECR",
      RedisCommandKind::DecrBy => "DECRBY",
      RedisCommandKind::Del => "DEL",
      RedisCommandKind::Discard => "DISCARD",
      RedisCommandKind::Dump => "DUMP",
      RedisCommandKind::Echo => "ECHO",
      RedisCommandKind::Eval(_) => "EVAL",
      RedisCommandKind::EvalSha(_) => "EVALSHA",
      RedisCommandKind::Exec => "EXEC",
      RedisCommandKind::Exists => "EXISTS",
      RedisCommandKind::Expire => "EXPIRE",
      RedisCommandKind::ExpireAt => "EXPIREAT",
      RedisCommandKind::Failover => "FAILOVER",
      RedisCommandKind::FlushAll => "FLUSHALL",
      RedisCommandKind::FlushDB => "FLUSHDB",
      RedisCommandKind::GeoAdd => "GEOADD",
      RedisCommandKind::GeoHash => "GEOHASH",
      RedisCommandKind::GeoPos => "GEOPOS",
      RedisCommandKind::GeoDist => "GEODIST",
      RedisCommandKind::GeoRadius => "GEORADIUS",
      RedisCommandKind::GeoRadiusByMember => "GEORADIUSBYMEMBER",
      RedisCommandKind::GeoSearch => "GEOSEARCH",
      RedisCommandKind::GeoSearchStore => "GEOSEARCHSTORE",
      RedisCommandKind::Get => "GET",
      RedisCommandKind::GetDel => "GETDEL",
      RedisCommandKind::GetBit => "GETBIT",
      RedisCommandKind::GetRange => "GETRANGE",
      RedisCommandKind::GetSet => "GETSET",
      RedisCommandKind::HDel => "HDEL",
      RedisCommandKind::HExists => "HEXISTS",
      RedisCommandKind::HGet => "HGET",
      RedisCommandKind::HGetAll => "HGETALL",
      RedisCommandKind::HIncrBy => "HINCRBY",
      RedisCommandKind::HIncrByFloat => "HINCRBYFLOAT",
      RedisCommandKind::HKeys => "HKEYS",
      RedisCommandKind::HLen => "HLEN",
      RedisCommandKind::HMGet => "HMGET",
      RedisCommandKind::HMSet => "HMSET",
      RedisCommandKind::HSet => "HSET",
      RedisCommandKind::HSetNx => "HSETNX",
      RedisCommandKind::HStrLen => "HSTRLEN",
      RedisCommandKind::HRandField => "HRANDFIELD",
      RedisCommandKind::HVals => "HVALS",
      RedisCommandKind::Incr => "INCR",
      RedisCommandKind::IncrBy => "INCRBY",
      RedisCommandKind::IncrByFloat => "INCRBYFLOAT",
      RedisCommandKind::Info => "INFO",
      RedisCommandKind::Keys => "KEYS",
      RedisCommandKind::LastSave => "LASTSAVE",
      RedisCommandKind::LIndex => "LINDEX",
      RedisCommandKind::LInsert => "LINSERT",
      RedisCommandKind::LLen => "LLEN",
      RedisCommandKind::LMove => "LMOVE",
      RedisCommandKind::LPop => "LPOP",
      RedisCommandKind::LPos => "LPOS",
      RedisCommandKind::LPush => "LPUSH",
      RedisCommandKind::LPushX => "LPUSHX",
      RedisCommandKind::LRange => "LRANGE",
      RedisCommandKind::LRem => "LREM",
      RedisCommandKind::LSet => "LSET",
      RedisCommandKind::LTrim => "LTRIM",
      RedisCommandKind::MemoryDoctor => "MEMORY DOCTOR",
      RedisCommandKind::MemoryHelp => "MEMORY HELP",
      RedisCommandKind::MemoryMallocStats => "MEMORY MALLOC-STATS",
      RedisCommandKind::MemoryPurge => "MEMORY PURGE",
      RedisCommandKind::MemoryStats => "MEMORY STATS",
      RedisCommandKind::MemoryUsage => "MEMORY USAGE",
      RedisCommandKind::Mget => "MGET",
      RedisCommandKind::Migrate => "MIGRATE",
      RedisCommandKind::Monitor => "MONITOR",
      RedisCommandKind::Move => "MOVE",
      RedisCommandKind::Mset => "MSET",
      RedisCommandKind::Msetnx => "MSETNX",
      RedisCommandKind::Multi => "MULTI",
      RedisCommandKind::Object => "OBJECT",
      RedisCommandKind::Persist => "PERSIST",
      RedisCommandKind::Pexpire => "PEXPIRE",
      RedisCommandKind::Pexpireat => "PEXPIREAT",
      RedisCommandKind::Pfadd => "PFADD",
      RedisCommandKind::Pfcount => "PFCOUNT",
      RedisCommandKind::Pfmerge => "PFMERGE",
      RedisCommandKind::Ping => "PING",
      RedisCommandKind::Psetex => "PSETEX",
      RedisCommandKind::Psubscribe(_) => "PSUBSCRIBE",
      RedisCommandKind::Pubsub => "PUBSUB",
      RedisCommandKind::Pttl => "PTTL",
      RedisCommandKind::Publish => "PUBLISH",
      RedisCommandKind::Punsubscribe(_) => "PUNSUBSCRIBE",
      RedisCommandKind::Quit => "QUIT",
      RedisCommandKind::Randomkey => "RANDOMKEY",
      RedisCommandKind::Readonly => "READONLY",
      RedisCommandKind::Readwrite => "READWRITE",
      RedisCommandKind::Rename => "RENAME",
      RedisCommandKind::Renamenx => "RENAMENX",
      RedisCommandKind::Restore => "RESTORE",
      RedisCommandKind::Role => "ROLE",
      RedisCommandKind::Rpop => "RPOP",
      RedisCommandKind::Rpoplpush => "RPOPLPUSH",
      RedisCommandKind::Rpush => "RPUSH",
      RedisCommandKind::Rpushx => "RPUSHX",
      RedisCommandKind::Sadd => "SADD",
      RedisCommandKind::Save => "SAVE",
      RedisCommandKind::Scard => "SCARD",
      RedisCommandKind::Sdiff => "SDIFF",
      RedisCommandKind::Sdiffstore => "SDIFFSTORE",
      RedisCommandKind::Select => "SELECT",
      RedisCommandKind::Set => "SET",
      RedisCommandKind::Setbit => "SETBIT",
      RedisCommandKind::Setex => "SETEX",
      RedisCommandKind::Setnx => "SETNX",
      RedisCommandKind::Setrange => "SETRANGE",
      RedisCommandKind::Shutdown => "SHUTDOWN",
      RedisCommandKind::Sinter => "SINTER",
      RedisCommandKind::Sinterstore => "SINTERSTORE",
      RedisCommandKind::Sismember => "SISMEMBER",
      RedisCommandKind::Replicaof => "REPLICAOF",
      RedisCommandKind::Slowlog => "SLOWLOG",
      RedisCommandKind::Smembers => "SMEMBERS",
      RedisCommandKind::Smismember => "SMISMEMBER",
      RedisCommandKind::Smove => "SMOVE",
      RedisCommandKind::Sort => "SORT",
      RedisCommandKind::Spop => "SPOP",
      RedisCommandKind::Srandmember => "SRANDMEMBER",
      RedisCommandKind::Srem => "SREM",
      RedisCommandKind::Strlen => "STRLEN",
      RedisCommandKind::Subscribe => "SUBSCRIBE",
      RedisCommandKind::Sunion => "SUNION",
      RedisCommandKind::Sunionstore => "SUNIONSTORE",
      RedisCommandKind::Swapdb => "SWAPDB",
      RedisCommandKind::Sync => "SYNC",
      RedisCommandKind::Time => "TIME",
      RedisCommandKind::Touch => "TOUCH",
      RedisCommandKind::Ttl => "TTL",
      RedisCommandKind::Type => "TYPE",
      RedisCommandKind::Unsubscribe => "UNSUBSCRIBE",
      RedisCommandKind::Unlink => "UNLINK",
      RedisCommandKind::Unwatch => "UNWATCH",
      RedisCommandKind::Wait => "WAIT",
      RedisCommandKind::Watch => "WATCH",
      RedisCommandKind::Zadd => "ZADD",
      RedisCommandKind::Zcard => "ZCARD",
      RedisCommandKind::Zcount => "ZCOUNT",
      RedisCommandKind::Zdiff => "ZDIFF",
      RedisCommandKind::Zdiffstore => "ZDIFFSTORE",
      RedisCommandKind::Zincrby => "ZINCRBY",
      RedisCommandKind::Zinter => "ZINTER",
      RedisCommandKind::Zinterstore => "ZINTERSTORE",
      RedisCommandKind::Zlexcount => "ZLEXCOUNT",
      RedisCommandKind::Zrandmember => "ZRANDMEMBER",
      RedisCommandKind::Zrange => "ZRANGE",
      RedisCommandKind::Zrangestore => "ZRANGESTORE",
      RedisCommandKind::Zrangebylex => "ZRANGEBYLEX",
      RedisCommandKind::Zrangebyscore => "ZRANGEBYSCORE",
      RedisCommandKind::Zrank => "ZRANK",
      RedisCommandKind::Zrem => "ZREM",
      RedisCommandKind::Zremrangebylex => "ZREMRANGEBYLEX",
      RedisCommandKind::Zremrangebyrank => "ZREMRANGEBYRANK",
      RedisCommandKind::Zremrangebyscore => "ZREMRANGEBYSCORE",
      RedisCommandKind::Zrevrange => "ZREVRANGE",
      RedisCommandKind::Zrevrangebylex => "ZREVRANGEBYLEX",
      RedisCommandKind::Zrevrangebyscore => "ZREVRANGEBYSCORE",
      RedisCommandKind::Zrevrank => "ZREVRANK",
      RedisCommandKind::Zscore => "ZSCORE",
      RedisCommandKind::Zmscore => "ZMSCORE",
      RedisCommandKind::Zunion => "ZUNION",
      RedisCommandKind::Zunionstore => "ZUNIONSTORE",
      RedisCommandKind::Zpopmax => "ZPOPMAX",
      RedisCommandKind::Zpopmin => "ZPOPMIN",
      RedisCommandKind::Scan(_) => "SCAN",
      RedisCommandKind::Sscan(_) => "SSCAN",
      RedisCommandKind::Hscan(_) => "HSCAN",
      RedisCommandKind::Zscan(_) => "ZSCAN",
      RedisCommandKind::ScriptDebug => "SCRIPT DEBUG",
      RedisCommandKind::ScriptExists => "SCRIPT EXISTS",
      RedisCommandKind::ScriptFlush => "SCRIPT FLUSH",
      RedisCommandKind::ScriptKill => "SCRIPT KILL",
      RedisCommandKind::ScriptLoad => "SCRIPT LOAD",
      RedisCommandKind::_Close => "CLOSE",
      RedisCommandKind::_Split(_) => "SPLIT",
      RedisCommandKind::_FlushAllCluster(_) => "FLUSHALL CLUSTER",
      RedisCommandKind::_ScriptFlushCluster(_) => "SCRIPT FLUSH CLUSTER",
      RedisCommandKind::_ScriptLoadCluster(_) => "SCRIPT LOAD CLUSTER",
      RedisCommandKind::_ScriptKillCluster(_) => "SCRIPT Kill CLUSTER",
      RedisCommandKind::_Custom(ref kind) => kind.cmd,
    }
  }

  /// Read the protocol string for a command, panicking for internal commands that don't map directly to redis command.
  pub(crate) fn cmd_str(&self) -> &'static str {
    match *self {
      RedisCommandKind::AclLoad => "ACL",
      RedisCommandKind::AclSave => "ACL",
      RedisCommandKind::AclList => "ACL",
      RedisCommandKind::AclUsers => "ACL",
      RedisCommandKind::AclGetUser => "ACL",
      RedisCommandKind::AclSetUser => "ACL",
      RedisCommandKind::AclDelUser => "ACL",
      RedisCommandKind::AclCat => "ACL",
      RedisCommandKind::AclGenPass => "ACL",
      RedisCommandKind::AclWhoAmI => "ACL",
      RedisCommandKind::AclLog => "ACL",
      RedisCommandKind::AclHelp => "ACL",
      RedisCommandKind::Append => "APPEND",
      RedisCommandKind::Auth => "AUTH",
      RedisCommandKind::BgreWriteAof => "BGREWRITEAOF",
      RedisCommandKind::BgSave => "BGSAVE",
      RedisCommandKind::BitCount => "BITCOUNT",
      RedisCommandKind::BitField => "BITFIELD",
      RedisCommandKind::BitOp => "BITOP",
      RedisCommandKind::BitPos => "BITPOS",
      RedisCommandKind::BlPop => "BLPOP",
      RedisCommandKind::BlMove => "BLMOVE",
      RedisCommandKind::BrPop => "BRPOP",
      RedisCommandKind::BrPopLPush => "BRPOPLPUSH",
      RedisCommandKind::BzPopMin => "BZPOPMIN",
      RedisCommandKind::BzPopMax => "BZPOPMAX",
      RedisCommandKind::ClientID => "CLIENT",
      RedisCommandKind::ClientInfo => "CLIENT",
      RedisCommandKind::ClientKill => "CLIENT",
      RedisCommandKind::ClientList => "CLIENT",
      RedisCommandKind::ClientGetName => "CLIENT",
      RedisCommandKind::ClientGetRedir => "CLIENT",
      RedisCommandKind::ClientPause => "CLIENT",
      RedisCommandKind::ClientUnpause => "CLIENT",
      RedisCommandKind::ClientUnblock => "CLIENT",
      RedisCommandKind::ClientReply => "CLIENT",
      RedisCommandKind::ClientSetname => "CLIENT",
      RedisCommandKind::ClusterAddSlots => "CLUSTER",
      RedisCommandKind::ClusterCountFailureReports => "CLUSTER",
      RedisCommandKind::ClusterCountKeysInSlot => "CLUSTER",
      RedisCommandKind::ClusterDelSlots => "CLUSTER",
      RedisCommandKind::ClusterFailOver => "CLUSTER",
      RedisCommandKind::ClusterForget => "CLUSTER",
      RedisCommandKind::ClusterGetKeysInSlot => "CLUSTER",
      RedisCommandKind::ClusterInfo => "CLUSTER",
      RedisCommandKind::ClusterKeySlot => "CLUSTER",
      RedisCommandKind::ClusterMeet => "CLUSTER",
      RedisCommandKind::ClusterNodes => "CLUSTER",
      RedisCommandKind::ClusterReplicate => "CLUSTER",
      RedisCommandKind::ClusterReset => "CLUSTER",
      RedisCommandKind::ClusterSaveConfig => "CLUSTER",
      RedisCommandKind::ClusterSetConfigEpoch => "CLUSTER",
      RedisCommandKind::ClusterSetSlot => "CLUSTER",
      RedisCommandKind::ClusterReplicas => "CLUSTER",
      RedisCommandKind::ClusterSlots => "CLUSTER",
      RedisCommandKind::ClusterBumpEpoch => "CLUSTER",
      RedisCommandKind::ClusterFlushSlots => "CLUSTER",
      RedisCommandKind::ClusterMyID => "CLUSTER",
      RedisCommandKind::ConfigGet => "CONFIG",
      RedisCommandKind::ConfigRewrite => "CONFIG",
      RedisCommandKind::ConfigSet => "CONFIG",
      RedisCommandKind::ConfigResetStat => "CONFIG",
      RedisCommandKind::Copy => "COPY",
      RedisCommandKind::DBSize => "DBSIZE",
      RedisCommandKind::Decr => "DECR",
      RedisCommandKind::DecrBy => "DECRBY",
      RedisCommandKind::Del => "DEL",
      RedisCommandKind::Discard => "DISCARD",
      RedisCommandKind::Dump => "DUMP",
      RedisCommandKind::Echo => "ECHO",
      RedisCommandKind::Eval(_) => "EVAL",
      RedisCommandKind::EvalSha(_) => "EVALSHA",
      RedisCommandKind::Exec => "EXEC",
      RedisCommandKind::Exists => "EXISTS",
      RedisCommandKind::Expire => "EXPIRE",
      RedisCommandKind::ExpireAt => "EXPIREAT",
      RedisCommandKind::Failover => "FAILOVER",
      RedisCommandKind::FlushAll => "FLUSHALL",
      RedisCommandKind::_FlushAllCluster(_) => "FLUSHALL",
      RedisCommandKind::FlushDB => "FLUSHDB",
      RedisCommandKind::GeoAdd => "GEOADD",
      RedisCommandKind::GeoHash => "GEOHASH",
      RedisCommandKind::GeoPos => "GEOPOS",
      RedisCommandKind::GeoDist => "GEODIST",
      RedisCommandKind::GeoRadius => "GEORADIUS",
      RedisCommandKind::GeoRadiusByMember => "GEORADIUSBYMEMBER",
      RedisCommandKind::GeoSearch => "GEOSEARCH",
      RedisCommandKind::GeoSearchStore => "GEOSEARCHSTORE",
      RedisCommandKind::Get => "GET",
      RedisCommandKind::GetDel => "GETDEL",
      RedisCommandKind::GetBit => "GETBIT",
      RedisCommandKind::GetRange => "GETRANGE",
      RedisCommandKind::GetSet => "GETSET",
      RedisCommandKind::HDel => "HDEL",
      RedisCommandKind::HExists => "HEXISTS",
      RedisCommandKind::HGet => "HGET",
      RedisCommandKind::HGetAll => "HGETALL",
      RedisCommandKind::HIncrBy => "HINCRBY",
      RedisCommandKind::HIncrByFloat => "HINCRBYFLOAT",
      RedisCommandKind::HKeys => "HKEYS",
      RedisCommandKind::HLen => "HLEN",
      RedisCommandKind::HMGet => "HMGET",
      RedisCommandKind::HMSet => "HMSET",
      RedisCommandKind::HSet => "HSET",
      RedisCommandKind::HSetNx => "HSETNX",
      RedisCommandKind::HStrLen => "HSTRLEN",
      RedisCommandKind::HRandField => "HRANDFIELD",
      RedisCommandKind::HVals => "HVALS",
      RedisCommandKind::Incr => "INCR",
      RedisCommandKind::IncrBy => "INCRBY",
      RedisCommandKind::IncrByFloat => "INCRBYFLOAT",
      RedisCommandKind::Info => "INFO",
      RedisCommandKind::Keys => "KEYS",
      RedisCommandKind::LastSave => "LASTSAVE",
      RedisCommandKind::LIndex => "LINDEX",
      RedisCommandKind::LInsert => "LINSERT",
      RedisCommandKind::LLen => "LLEN",
      RedisCommandKind::LMove => "LMOVE",
      RedisCommandKind::LPop => "LPOP",
      RedisCommandKind::LPos => "LPOS",
      RedisCommandKind::LPush => "LPUSH",
      RedisCommandKind::LPushX => "LPUSHX",
      RedisCommandKind::LRange => "LRANGE",
      RedisCommandKind::LRem => "LREM",
      RedisCommandKind::LSet => "LSET",
      RedisCommandKind::LTrim => "LTRIM",
      RedisCommandKind::MemoryDoctor => "MEMORY",
      RedisCommandKind::MemoryHelp => "MEMORY",
      RedisCommandKind::MemoryMallocStats => "MEMORY",
      RedisCommandKind::MemoryPurge => "MEMORY",
      RedisCommandKind::MemoryStats => "MEMORY",
      RedisCommandKind::MemoryUsage => "MEMORY",
      RedisCommandKind::Mget => "MGET",
      RedisCommandKind::Migrate => "MIGRATE",
      RedisCommandKind::Monitor => "MONITOR",
      RedisCommandKind::Move => "MOVE",
      RedisCommandKind::Mset => "MSET",
      RedisCommandKind::Msetnx => "MSETNX",
      RedisCommandKind::Multi => "MULTI",
      RedisCommandKind::Object => "OBJECT",
      RedisCommandKind::Persist => "PERSIST",
      RedisCommandKind::Pexpire => "PEXPIRE",
      RedisCommandKind::Pexpireat => "PEXPIREAT",
      RedisCommandKind::Pfadd => "PFADD",
      RedisCommandKind::Pfcount => "PFCOUNT",
      RedisCommandKind::Pfmerge => "PFMERGE",
      RedisCommandKind::Ping => "PING",
      RedisCommandKind::Psetex => "PSETEX",
      RedisCommandKind::Psubscribe(_) => "PSUBSCRIBE",
      RedisCommandKind::Pubsub => "PUBSUB",
      RedisCommandKind::Pttl => "PTTL",
      RedisCommandKind::Publish => "PUBLISH",
      RedisCommandKind::Punsubscribe(_) => "PUNSUBSCRIBE",
      RedisCommandKind::Quit => "QUIT",
      RedisCommandKind::Randomkey => "RANDOMKEY",
      RedisCommandKind::Readonly => "READONLY",
      RedisCommandKind::Readwrite => "READWRITE",
      RedisCommandKind::Rename => "RENAME",
      RedisCommandKind::Renamenx => "RENAMENX",
      RedisCommandKind::Restore => "RESTORE",
      RedisCommandKind::Role => "ROLE",
      RedisCommandKind::Rpop => "RPOP",
      RedisCommandKind::Rpoplpush => "RPOPLPUSH",
      RedisCommandKind::Rpush => "RPUSH",
      RedisCommandKind::Rpushx => "RPUSHX",
      RedisCommandKind::Sadd => "SADD",
      RedisCommandKind::Save => "SAVE",
      RedisCommandKind::Scard => "SCARD",
      RedisCommandKind::Sdiff => "SDIFF",
      RedisCommandKind::Sdiffstore => "SDIFFSTORE",
      RedisCommandKind::Select => "SELECT",
      RedisCommandKind::Set => "SET",
      RedisCommandKind::Setbit => "SETBIT",
      RedisCommandKind::Setex => "SETEX",
      RedisCommandKind::Setnx => "SETNX",
      RedisCommandKind::Setrange => "SETRANGE",
      RedisCommandKind::Shutdown => "SHUTDOWN",
      RedisCommandKind::Sinter => "SINTER",
      RedisCommandKind::Sinterstore => "SINTERSTORE",
      RedisCommandKind::Sismember => "SISMEMBER",
      RedisCommandKind::Replicaof => "REPLICAOF",
      RedisCommandKind::Slowlog => "SLOWLOG",
      RedisCommandKind::Smembers => "SMEMBERS",
      RedisCommandKind::Smismember => "SMISMEMBER",
      RedisCommandKind::Smove => "SMOVE",
      RedisCommandKind::Sort => "SORT",
      RedisCommandKind::Spop => "SPOP",
      RedisCommandKind::Srandmember => "SRANDMEMBER",
      RedisCommandKind::Srem => "SREM",
      RedisCommandKind::Strlen => "STRLEN",
      RedisCommandKind::Subscribe => "SUBSCRIBE",
      RedisCommandKind::Sunion => "SUNION",
      RedisCommandKind::Sunionstore => "SUNIONSTORE",
      RedisCommandKind::Swapdb => "SWAPDB",
      RedisCommandKind::Sync => "SYNC",
      RedisCommandKind::Time => "TIME",
      RedisCommandKind::Touch => "TOUCH",
      RedisCommandKind::Ttl => "TTL",
      RedisCommandKind::Type => "TYPE",
      RedisCommandKind::Unsubscribe => "UNSUBSCRIBE",
      RedisCommandKind::Unlink => "UNLINK",
      RedisCommandKind::Unwatch => "UNWATCH",
      RedisCommandKind::Wait => "WAIT",
      RedisCommandKind::Watch => "WATCH",
      RedisCommandKind::Zadd => "ZADD",
      RedisCommandKind::Zcard => "ZCARD",
      RedisCommandKind::Zcount => "ZCOUNT",
      RedisCommandKind::Zdiff => "ZDIFF",
      RedisCommandKind::Zdiffstore => "ZDIFFSTORE",
      RedisCommandKind::Zincrby => "ZINCRBY",
      RedisCommandKind::Zinter => "ZINTER",
      RedisCommandKind::Zinterstore => "ZINTERSTORE",
      RedisCommandKind::Zlexcount => "ZLEXCOUNT",
      RedisCommandKind::Zrandmember => "ZRANDMEMBER",
      RedisCommandKind::Zrange => "ZRANGE",
      RedisCommandKind::Zrangestore => "ZRANGESTORE",
      RedisCommandKind::Zrangebylex => "ZRANGEBYLEX",
      RedisCommandKind::Zrangebyscore => "ZRANGEBYSCORE",
      RedisCommandKind::Zrank => "ZRANK",
      RedisCommandKind::Zrem => "ZREM",
      RedisCommandKind::Zremrangebylex => "ZREMRANGEBYLEX",
      RedisCommandKind::Zremrangebyrank => "ZREMRANGEBYRANK",
      RedisCommandKind::Zremrangebyscore => "ZREMRANGEBYSCORE",
      RedisCommandKind::Zrevrange => "ZREVRANGE",
      RedisCommandKind::Zrevrangebylex => "ZREVRANGEBYLEX",
      RedisCommandKind::Zrevrangebyscore => "ZREVRANGEBYSCORE",
      RedisCommandKind::Zrevrank => "ZREVRANK",
      RedisCommandKind::Zscore => "ZSCORE",
      RedisCommandKind::Zmscore => "ZMSCORE",
      RedisCommandKind::Zunion => "ZUNION",
      RedisCommandKind::Zunionstore => "ZUNIONSTORE",
      RedisCommandKind::Zpopmax => "ZPOPMAX",
      RedisCommandKind::Zpopmin => "ZPOPMIN",
      RedisCommandKind::ScriptDebug => "SCRIPT",
      RedisCommandKind::ScriptExists => "SCRIPT",
      RedisCommandKind::ScriptFlush => "SCRIPT",
      RedisCommandKind::ScriptKill => "SCRIPT",
      RedisCommandKind::ScriptLoad => "SCRIPT",
      RedisCommandKind::_ScriptFlushCluster(_) => "SCRIPT",
      RedisCommandKind::_ScriptLoadCluster(_) => "SCRIPT",
      RedisCommandKind::_ScriptKillCluster(_) => "SCRIPT",
      RedisCommandKind::Scan(_) => "SCAN",
      RedisCommandKind::Sscan(_) => "SSCAN",
      RedisCommandKind::Hscan(_) => "HSCAN",
      RedisCommandKind::Zscan(_) => "ZSCAN",
      RedisCommandKind::_Custom(ref kind) => kind.cmd,
      RedisCommandKind::_Close | RedisCommandKind::_Split(_) => {
        panic!("unreachable (redis command)")
      }
    }
  }

  /// Read the optional subcommand string for a command.
  pub fn subcommand_str(&self) -> Option<&'static str> {
    let s = match *self {
      RedisCommandKind::ScriptDebug => "DEBUG",
      RedisCommandKind::ScriptLoad => "LOAD",
      RedisCommandKind::ScriptKill => "KILL",
      RedisCommandKind::ScriptFlush => "FLUSH",
      RedisCommandKind::ScriptExists => "EXISTS",
      RedisCommandKind::_ScriptFlushCluster(_) => "FLUSH",
      RedisCommandKind::_ScriptLoadCluster(_) => "LOAD",
      RedisCommandKind::_ScriptKillCluster(_) => "KILL",
      RedisCommandKind::AclLoad => "LOAD",
      RedisCommandKind::AclSave => "SAVE",
      RedisCommandKind::AclList => "LIST",
      RedisCommandKind::AclUsers => "USERS",
      RedisCommandKind::AclGetUser => "GETUSER",
      RedisCommandKind::AclSetUser => "SETUSER",
      RedisCommandKind::AclDelUser => "DELUSER",
      RedisCommandKind::AclCat => "CAT",
      RedisCommandKind::AclGenPass => "GENPASS",
      RedisCommandKind::AclWhoAmI => "WHOAMI",
      RedisCommandKind::AclLog => "LOG",
      RedisCommandKind::AclHelp => "HELP",
      RedisCommandKind::ClusterAddSlots => "ADDSLOTS",
      RedisCommandKind::ClusterCountFailureReports => "COUNT-FAILURE-REPORTS",
      RedisCommandKind::ClusterCountKeysInSlot => "COUNTKEYSINSLOT",
      RedisCommandKind::ClusterDelSlots => "DELSLOTS",
      RedisCommandKind::ClusterFailOver => "FAILOVER",
      RedisCommandKind::ClusterForget => "FORGET",
      RedisCommandKind::ClusterGetKeysInSlot => "GETKEYSINSLOT",
      RedisCommandKind::ClusterInfo => "INFO",
      RedisCommandKind::ClusterKeySlot => "KEYSLOT",
      RedisCommandKind::ClusterMeet => "MEET",
      RedisCommandKind::ClusterNodes => "NODES",
      RedisCommandKind::ClusterReplicate => "REPLICATE",
      RedisCommandKind::ClusterReset => "RESET",
      RedisCommandKind::ClusterSaveConfig => "SAVECONFIG",
      RedisCommandKind::ClusterSetConfigEpoch => "SET-CONFIG-EPOCH",
      RedisCommandKind::ClusterSetSlot => "SETSLOT",
      RedisCommandKind::ClusterReplicas => "REPLICAS",
      RedisCommandKind::ClusterSlots => "SLOTS",
      RedisCommandKind::ClusterBumpEpoch => "BUMPEPOCH",
      RedisCommandKind::ClusterFlushSlots => "FLUSHSLOTS",
      RedisCommandKind::ClusterMyID => "MYID",
      RedisCommandKind::ClientID => "ID",
      RedisCommandKind::ClientInfo => "INFO",
      RedisCommandKind::ClientKill => "KILL",
      RedisCommandKind::ClientList => "LIST",
      RedisCommandKind::ClientGetRedir => "GETREDIR",
      RedisCommandKind::ClientGetName => "GETNAME",
      RedisCommandKind::ClientPause => "PAUSE",
      RedisCommandKind::ClientUnpause => "UNPAUSE",
      RedisCommandKind::ClientUnblock => "UNBLOCK",
      RedisCommandKind::ClientReply => "REPLY",
      RedisCommandKind::ClientSetname => "SETNAME",
      RedisCommandKind::ConfigGet => "GET",
      RedisCommandKind::ConfigRewrite => "REWRITE",
      RedisCommandKind::ConfigSet => "SET",
      RedisCommandKind::ConfigResetStat => "RESETSTAT",
      RedisCommandKind::MemoryDoctor => "DOCTOR",
      RedisCommandKind::MemoryHelp => "HELP",
      RedisCommandKind::MemoryUsage => "USAGE",
      RedisCommandKind::MemoryMallocStats => "MALLOC-STATS",
      RedisCommandKind::MemoryStats => "STATS",
      RedisCommandKind::MemoryPurge => "PURGE",
      _ => return None,
    };

    Some(s)
  }

  pub fn is_script_command(&self) -> bool {
    match *self {
      RedisCommandKind::ScriptDebug
      | RedisCommandKind::ScriptExists
      | RedisCommandKind::ScriptFlush
      | RedisCommandKind::ScriptKill
      | RedisCommandKind::_ScriptFlushCluster(_)
      | RedisCommandKind::_ScriptLoadCluster(_)
      | RedisCommandKind::_ScriptKillCluster(_)
      | RedisCommandKind::ScriptLoad => true,
      _ => false,
    }
  }

  pub fn is_acl_command(&self) -> bool {
    match *self {
      RedisCommandKind::AclLoad
      | RedisCommandKind::AclSave
      | RedisCommandKind::AclList
      | RedisCommandKind::AclUsers
      | RedisCommandKind::AclGetUser
      | RedisCommandKind::AclSetUser
      | RedisCommandKind::AclDelUser
      | RedisCommandKind::AclCat
      | RedisCommandKind::AclGenPass
      | RedisCommandKind::AclWhoAmI
      | RedisCommandKind::AclLog
      | RedisCommandKind::AclHelp => true,
      _ => false,
    }
  }

  pub fn is_cluster_command(&self) -> bool {
    match *self {
      RedisCommandKind::ClusterAddSlots
      | RedisCommandKind::ClusterCountFailureReports
      | RedisCommandKind::ClusterCountKeysInSlot
      | RedisCommandKind::ClusterDelSlots
      | RedisCommandKind::ClusterFailOver
      | RedisCommandKind::ClusterForget
      | RedisCommandKind::ClusterGetKeysInSlot
      | RedisCommandKind::ClusterInfo
      | RedisCommandKind::ClusterKeySlot
      | RedisCommandKind::ClusterMeet
      | RedisCommandKind::ClusterNodes
      | RedisCommandKind::ClusterReplicate
      | RedisCommandKind::ClusterReset
      | RedisCommandKind::ClusterSaveConfig
      | RedisCommandKind::ClusterSetConfigEpoch
      | RedisCommandKind::ClusterSetSlot
      | RedisCommandKind::ClusterReplicas
      | RedisCommandKind::ClusterBumpEpoch
      | RedisCommandKind::ClusterFlushSlots
      | RedisCommandKind::ClusterMyID
      | RedisCommandKind::ClusterSlots => true,
      _ => false,
    }
  }

  pub fn is_client_command(&self) -> bool {
    match *self {
      RedisCommandKind::ClientGetName
      | RedisCommandKind::ClientGetRedir
      | RedisCommandKind::ClientInfo
      | RedisCommandKind::ClientID
      | RedisCommandKind::ClientKill
      | RedisCommandKind::ClientList
      | RedisCommandKind::ClientPause
      | RedisCommandKind::ClientUnpause
      | RedisCommandKind::ClientUnblock
      | RedisCommandKind::ClientReply
      | RedisCommandKind::ClientSetname => true,
      _ => false,
    }
  }

  pub fn is_config_command(&self) -> bool {
    match *self {
      RedisCommandKind::ConfigGet
      | RedisCommandKind::ConfigRewrite
      | RedisCommandKind::ConfigSet
      | RedisCommandKind::ConfigResetStat => true,
      _ => false,
    }
  }

  pub fn is_memory_command(&self) -> bool {
    match *self {
      RedisCommandKind::MemoryUsage
      | RedisCommandKind::MemoryStats
      | RedisCommandKind::MemoryPurge
      | RedisCommandKind::MemoryMallocStats
      | RedisCommandKind::MemoryHelp
      | RedisCommandKind::MemoryDoctor => true,
      _ => false,
    }
  }

  pub fn is_blocking(&self) -> bool {
    match *self {
      RedisCommandKind::BlPop
      | RedisCommandKind::BrPop
      | RedisCommandKind::BrPopLPush
      | RedisCommandKind::BlMove
      | RedisCommandKind::BzPopMin
      | RedisCommandKind::BzPopMax
      | RedisCommandKind::Wait => true,
      RedisCommandKind::_Custom(ref kind) => kind.is_blocking,
      _ => false,
    }
  }

  pub fn custom_key_slot(&self) -> Option<u16> {
    match *self {
      RedisCommandKind::Scan(ref inner) => inner.key_slot.clone(),
      RedisCommandKind::_Custom(ref kind) => kind.hash_slot.clone(),
      RedisCommandKind::EvalSha(ref slot) => slot.key_slot.clone(),
      RedisCommandKind::Eval(ref slot) => slot.key_slot.clone(),
      _ => None,
    }
  }

  pub fn is_all_cluster_nodes(&self) -> bool {
    match *self {
      RedisCommandKind::_FlushAllCluster(_)
      | RedisCommandKind::_ScriptFlushCluster(_)
      | RedisCommandKind::_ScriptKillCluster(_)
      | RedisCommandKind::_ScriptLoadCluster(_) => true,
      _ => false,
    }
  }

  pub fn is_eval(&self) -> bool {
    match *self {
      RedisCommandKind::EvalSha(_) | RedisCommandKind::Eval(_) => true,
      _ => false,
    }
  }

  pub fn all_nodes_response(&self) -> Option<&AllNodesResponse> {
    match *self {
      RedisCommandKind::_FlushAllCluster(ref inner) => Some(inner),
      RedisCommandKind::_ScriptFlushCluster(ref inner) => Some(inner),
      RedisCommandKind::_ScriptLoadCluster(ref inner) => Some(inner),
      RedisCommandKind::_ScriptKillCluster(ref inner) => Some(inner),
      _ => None,
    }
  }

  pub fn clone_all_nodes(&self) -> Option<Self> {
    match *self {
      RedisCommandKind::_FlushAllCluster(ref inner) => Some(RedisCommandKind::_FlushAllCluster(inner.clone())),
      RedisCommandKind::_ScriptFlushCluster(ref inner) => Some(RedisCommandKind::_ScriptFlushCluster(inner.clone())),
      RedisCommandKind::_ScriptLoadCluster(ref inner) => Some(RedisCommandKind::_ScriptLoadCluster(inner.clone())),
      RedisCommandKind::_ScriptKillCluster(ref inner) => Some(RedisCommandKind::_ScriptKillCluster(inner.clone())),
      _ => None,
    }
  }

  pub fn is_read(&self) -> bool {
    // TODO finish this and use for sending reads to replicas
    match *self {
      _ => false,
    }
  }
}

/// Alias for a sender to notify the caller that a response was received.
pub type ResponseSender = Option<OneshotSender<Result<Frame, RedisError>>>;

/// An arbitrary Redis command.
pub struct RedisCommand {
  pub kind: RedisCommandKind,
  pub args: Vec<RedisValue>,
  /// Sender for notifying the caller that a response was received.
  pub tx: ResponseSender,
  /// Number of times the request was sent to the server.
  pub attempted: usize,
  /// Time when the command was first initialized.
  pub sent: Instant,
  /// Sender for notifying the command processing loop that the command received a response.
  pub resp_tx: Arc<RwLock<Option<OneshotSender<()>>>>,
  #[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
  pub traces: CommandTraces,
}

impl fmt::Debug for RedisCommand {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[RedisCommand Kind: {:?}, Args: {:?}]", &self.kind, &self.args)
  }
}

impl fmt::Display for RedisCommand {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "[RedisCommand: {}]", self.kind.to_str_debug())
  }
}

impl RedisCommand {
  #[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
  pub fn new(kind: RedisCommandKind, args: Vec<RedisValue>, tx: ResponseSender) -> RedisCommand {
    RedisCommand {
      kind,
      args,
      tx,
      traces: CommandTraces::default(),
      attempted: 0,
      sent: Instant::now(),
      resp_tx: Arc::new(RwLock::new(None)),
    }
  }

  #[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
  pub fn new(kind: RedisCommandKind, args: Vec<RedisValue>, tx: ResponseSender) -> RedisCommand {
    RedisCommand {
      kind,
      args,
      tx,
      attempted: 0,
      sent: Instant::now(),
      resp_tx: Arc::new(RwLock::new(None)),
    }
  }

  #[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
  pub fn duplicate(&self, kind: RedisCommandKind) -> RedisCommand {
    RedisCommand {
      kind,
      attempted: 0,
      tx: None,
      args: self.args.clone(),
      sent: self.sent.clone(),
      resp_tx: self.resp_tx.clone(),
      traces: CommandTraces::default(),
    }
  }

  #[cfg(not(any(feature = "full-tracing", feature = "partial-tracing")))]
  pub fn duplicate(&self, kind: RedisCommandKind) -> RedisCommand {
    RedisCommand {
      kind,
      attempted: 0,
      tx: None,
      args: self.args.clone(),
      sent: self.sent.clone(),
      resp_tx: self.resp_tx.clone(),
    }
  }

  #[cfg(feature = "full-tracing")]
  pub fn take_queued_span(&mut self) -> Option<Span> {
    self.traces.queued.take()
  }

  #[cfg(not(feature = "full-tracing"))]
  pub fn take_queued_span(&mut self) -> Option<FakeSpan> {
    None
  }

  pub fn add_resp_tx(&self, tx: OneshotSender<()>) {
    set_locked(&self.resp_tx, Some(tx));
  }

  pub fn take_resp_tx(&self) -> Option<OneshotSender<()>> {
    take_locked(&self.resp_tx)
  }

  pub fn incr_attempted(&mut self) {
    self.attempted += 1;
  }

  pub fn max_attempts_exceeded(&self) -> bool {
    self.attempted >= globals().max_command_attempts()
  }

  /// Convert to a single frame with an array of bulk strings (or null).
  #[cfg(not(feature = "blocking-encoding"))]
  pub fn to_frame(&self) -> Result<Frame, RedisError> {
    protocol_utils::command_to_frame(self)
  }

  /// Convert to a single frame with an array of bulk strings (or null), using a blocking task.
  #[cfg(feature = "blocking-encoding")]
  pub fn to_frame(&self) -> Result<Frame, RedisError> {
    let cmd_size = protocol_utils::args_size(&self.args);

    if cmd_size >= globals().blocking_encode_threshold() {
      trace!("Using blocking task to convert command to frame with size {}", cmd_size);
      tokio::task::block_in_place(|| protocol_utils::command_to_frame(self))
    } else {
      protocol_utils::command_to_frame(self)
    }
  }

  /// Commands that do not need to run on a specific host in a cluster.
  pub fn no_cluster(&self) -> bool {
    match self.kind {
      RedisCommandKind::Publish
      | RedisCommandKind::Subscribe
      | RedisCommandKind::Unsubscribe
      | RedisCommandKind::Psubscribe(_)
      | RedisCommandKind::Punsubscribe(_)
      | RedisCommandKind::Ping
      | RedisCommandKind::Info
      | RedisCommandKind::Scan(_)
      | RedisCommandKind::FlushAll
      | RedisCommandKind::FlushDB => true,
      _ => false,
    }
  }

  /// Read the first key in the command, if any.
  pub fn extract_key(&self) -> Option<Cow<str>> {
    if self.no_cluster() {
      return None;
    }

    match self.args.first() {
      Some(RedisValue::String(ref s)) => Some(Cow::Borrowed(s)),
      Some(RedisValue::Bytes(ref b)) => Some(String::from_utf8_lossy(b)),
      Some(_) => match self.args.get(1) {
        // some commands take a `num_keys` argument first, followed by keys
        Some(RedisValue::String(ref s)) => Some(Cow::Borrowed(s)),
        Some(RedisValue::Bytes(ref b)) => Some(String::from_utf8_lossy(b)),
        _ => None,
      },
      None => None,
    }
  }

  pub fn key_slot(&self) -> Option<u16> {
    self.kind.custom_key_slot()
  }

  pub fn is_quit(&self) -> bool {
    match self.kind {
      RedisCommandKind::Quit => true,
      _ => false,
    }
  }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaNodes {
  servers: Vec<Arc<String>>,
  next: usize,
}

impl ReplicaNodes {
  // TODO remove when replica support is added
  #![allow(dead_code)]

  pub fn new(servers: Vec<Arc<String>>) -> ReplicaNodes {
    ReplicaNodes { servers, next: 0 }
  }

  pub fn add(&mut self, server: Arc<String>) {
    self.servers.push(server);
  }

  pub fn clear(&mut self) {
    self.servers.clear();
    self.next = 0;
  }

  pub fn next(&mut self) -> Option<Arc<String>> {
    if self.servers.len() == 0 {
      return None;
    }

    let last = self.next;
    self.next = (self.next + 1) % self.servers.len();

    self.servers.get(last).cloned()
  }
}

/// A slot range and associated cluster node information from the CLUSTER NODES command.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SlotRange {
  pub start: u16,
  pub end: u16,
  pub server: Arc<String>,
  pub id: Arc<String>,
  // TODO cache replicas for each primary and round-robin reads to the replicas + primary, and only send writes to the primary
  //pub replicas: Option<ReplicaNodes>,
}

/// The cached view of the cluster used by the client to route commands to the correct cluster nodes.
#[derive(Debug, Clone)]
pub struct ClusterKeyCache {
  data: Vec<Arc<SlotRange>>,
}

impl From<Vec<Arc<SlotRange>>> for ClusterKeyCache {
  fn from(data: Vec<Arc<SlotRange>>) -> Self {
    ClusterKeyCache { data }
  }
}

impl ClusterKeyCache {
  /// Create a new cache from the output of CLUSTER NODES, if available.
  pub fn new(status: Option<String>) -> Result<ClusterKeyCache, RedisError> {
    let mut cache = ClusterKeyCache { data: Vec::new() };

    if let Some(status) = status {
      cache.rebuild(status)?;
    }

    Ok(cache)
  }

  /// Read the set of unique primary/main nodes in the cluster.
  pub fn unique_main_nodes(&self) -> Vec<Arc<String>> {
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
  pub fn rebuild(&mut self, status: String) -> Result<(), RedisError> {
    if status.trim().is_empty() {
      error!("Invalid empty CLUSTER NODES response.");
      return Err(RedisError::new(
        RedisErrorKind::ProtocolError,
        "Invalid empty CLUSTER NODES response.",
      ));
    }

    let mut parsed = protocol_utils::parse_cluster_nodes(status)?;
    self.data.clear();

    for (_, ranges) in parsed.drain() {
      for slot in ranges {
        self.data.push(Arc::new(slot));
      }
    }
    self.data.sort_by(|lhs, rhs| lhs.start.cmp(&rhs.start));

    self.data.shrink_to_fit();
    Ok(())
  }

  /// Calculate the cluster hash slot for the provided key.
  pub fn hash_key(key: &str) -> u16 {
    redis_protocol::redis_keyslot(key)
  }

  /// Find the server that owns the provided hash slot.
  pub fn get_server(&self, slot: u16) -> Option<Arc<SlotRange>> {
    protocol_utils::binary_search(&self.data, slot)
  }

  /// Read the number of hash slot ranges in the cluster.
  pub fn len(&self) -> usize {
    self.data.len()
  }

  /// Read the hash slot ranges in the cluster.
  pub fn slots(&self) -> &Vec<Arc<SlotRange>> {
    &self.data
  }

  /// Read a random primary node hash slot range from the cluster cache.
  pub fn random_slot(&self) -> Option<Arc<SlotRange>> {
    if self.data.len() > 0 {
      let idx = rand::thread_rng().gen_range(0..self.data.len());
      Some(self.data[idx].clone())
    } else {
      None
    }
  }
}

// TODO support custom DNS resolution logic by exposing this in the client.
/// Default DNS resolver that just uses `to_socket_addrs` under the hood.
#[derive(Clone, Debug)]
pub struct DefaultResolver {
  id: Arc<String>,
}

impl DefaultResolver {
  /// Create a new resolver using the system's default DNS resolution.
  pub fn new(id: &Arc<String>) -> Self {
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
