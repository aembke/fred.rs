use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces::Resp3Frame,
  modules::inner::RedisClientInner,
  protocol::{
    hashers::ClusterHash,
    responders::ResponseKind,
    types::{ProtocolFrame, Server},
    utils as protocol_utils,
  },
  trace,
  types::{CustomCommand, RedisValue},
  utils as client_utils,
  utils,
};
use bytes_utils::Str;
use redis_protocol::resp3::types::RespVersion;
use std::{
  convert::TryFrom,
  fmt,
  fmt::Formatter,
  mem,
  str,
  sync::{atomic::AtomicBool, Arc},
};
use tokio::sync::oneshot::{channel as oneshot_channel, Receiver as OneshotReceiver, Sender as OneshotSender};

#[cfg(feature = "blocking-encoding")]
use crate::globals::globals;
#[cfg(feature = "mocks")]
use crate::modules::mocks::MockCommand;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use crate::trace::CommandTraces;
#[cfg(any(feature = "metrics", feature = "partial-tracing"))]
use std::time::Instant;

#[cfg(feature = "debug-ids")]
use lazy_static::lazy_static;
use parking_lot::Mutex;
#[cfg(feature = "debug-ids")]
use std::sync::atomic::AtomicUsize;
#[cfg(feature = "debug-ids")]
lazy_static! {
  static ref COMMAND_COUNTER: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}
#[cfg(feature = "debug-ids")]
pub fn command_counter() -> usize {
  utils::incr_atomic(&COMMAND_COUNTER)
}

/// A command interface for communication between connection reader tasks and the multiplexer.
///
/// Use of this interface assumes that a command was **not** pipelined. The reader task may instead
/// choose to communicate with the multiplexer via the shared command queue if no channel exists on
/// which to send this command.
#[derive(Debug)]
pub enum MultiplexerResponse {
  /// Continue with the next command.
  Continue,
  /// Retry the command immediately against the provided server, but with an `ASKING` prefix.
  ///
  /// Typically used with transactions to retry the entire transaction against a different node.
  ///
  /// Reader tasks will attempt to use the multiplexer channel first when handling cluster errors, but
  /// may fall back to communication via the command channel in the context of pipelined commands.
  Ask((u16, Server, RedisCommand)),
  /// Retry the command immediately against the provided server, updating the cached routing table first.
  ///
  /// Reader tasks will attempt to use the multiplexer channel first when handling cluster errors, but
  /// may fall back to communication via the command channel in the context of pipelined commands.
  Moved((u16, Server, RedisCommand)),
  /// Indicate to the multiplexer that the provided transaction command failed with the associated error.
  ///
  /// The multiplexer is responsible for responding to the caller with the error, if needed. Transaction commands are
  /// never pipelined.
  TransactionError((RedisError, RedisCommand)),
  /// Indicates to the multiplexer that the transaction finished with the associated result.
  TransactionResult(Resp3Frame),
  /// Indicates that the connection closed while the command was in-flight.
  ///
  /// This is only used for non-pipelined commands where the multiplexer task is blocked on a response before
  /// checking the next command.
  ConnectionClosed((RedisError, RedisCommand)),
}

/// A channel for communication between connection reader tasks and futures returned to the caller.
pub type ResponseSender = OneshotSender<Result<Resp3Frame, RedisError>>;
/// A sender channel for communication between connection reader tasks and the multiplexer.
pub type MultiplexerSender = OneshotSender<MultiplexerResponse>;
/// A receiver channel for communication between connection reader tasks and the multiplexer.
pub type MultiplexerReceiver = OneshotReceiver<MultiplexerResponse>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterErrorKind {
  Moved,
  Ask,
}

impl fmt::Display for ClusterErrorKind {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match self {
      ClusterErrorKind::Moved => write!(f, "MOVED"),
      ClusterErrorKind::Ask => write!(f, "ASK"),
    }
  }
}

impl<'a> TryFrom<&'a str> for ClusterErrorKind {
  type Error = RedisError;

  fn try_from(value: &'a str) -> Result<Self, Self::Error> {
    match value.as_ref() {
      "MOVED" => Ok(ClusterErrorKind::Moved),
      "ASK" => Ok(ClusterErrorKind::Ask),
      _ => Err(RedisError::new(
        RedisErrorKind::Protocol,
        "Expected MOVED or ASK error.",
      )),
    }
  }
}

#[derive(Clone, Eq, PartialEq)]
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
  Asking,
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
  Eval,
  EvalSha,
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
  Lcs,
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
  Psubscribe,
  Pubsub,
  Pttl,
  Publish,
  Punsubscribe,
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
  Sentinel,
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
  XinfoConsumers,
  XinfoGroups,
  XinfoStream,
  Xadd,
  Xtrim,
  Xdel,
  Xrange,
  Xrevrange,
  Xlen,
  Xread,
  Xgroupcreate,
  XgroupCreateConsumer,
  XgroupDelConsumer,
  XgroupDestroy,
  XgroupSetId,
  Xreadgroup,
  Xack,
  Xclaim,
  Xautoclaim,
  Xpending,
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
  Scan,
  Sscan,
  Hscan,
  Zscan,
  Spublish,
  Ssubscribe,
  Sunsubscribe,
  Fcall,
  FcallRO,
  FunctionDelete,
  FunctionDump,
  FunctionFlush,
  FunctionKill,
  FunctionList,
  FunctionLoad,
  FunctionRestore,
  FunctionStats,
  // Commands with custom state or commands that don't map directly to the server's command interface.
  _Hello(RespVersion),
  _AuthAllCluster,
  _HelloAllCluster(RespVersion),
  _FlushAllCluster,
  _ScriptFlushCluster,
  _ScriptLoadCluster,
  _ScriptKillCluster,
  _FunctionLoadCluster,
  _FunctionFlushCluster,
  _FunctionDeleteCluster,
  _FunctionRestoreCluster,
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
      RedisCommandKind::Scan => true,
      _ => false,
    }
  }

  pub fn is_hscan(&self) -> bool {
    match *self {
      RedisCommandKind::Hscan => true,
      _ => false,
    }
  }

  pub fn is_sscan(&self) -> bool {
    match *self {
      RedisCommandKind::Sscan => true,
      _ => false,
    }
  }

  pub fn is_zscan(&self) -> bool {
    match *self {
      RedisCommandKind::Zscan => true,
      _ => false,
    }
  }

  pub fn is_hello(&self) -> bool {
    match *self {
      RedisCommandKind::_Hello(_) | RedisCommandKind::_HelloAllCluster(_) => true,
      _ => false,
    }
  }

  pub fn is_auth(&self) -> bool {
    match *self {
      RedisCommandKind::Auth => true,
      _ => false,
    }
  }

  pub fn is_value_scan(&self) -> bool {
    match *self {
      RedisCommandKind::Zscan | RedisCommandKind::Hscan | RedisCommandKind::Sscan => true,
      _ => false,
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

  pub fn is_custom(&self) -> bool {
    match *self {
      RedisCommandKind::_Custom(_) => true,
      _ => false,
    }
  }

  pub fn closes_connection(&self) -> bool {
    match *self {
      RedisCommandKind::Quit | RedisCommandKind::Shutdown => true,
      _ => false,
    }
  }

  pub fn custom_hash_slot(&self) -> Option<u16> {
    match self {
      RedisCommandKind::_Custom(ref cmd) => match cmd.cluster_hash {
        ClusterHash::Custom(ref val) => Some(*val),
        _ => None,
      },
      _ => None,
    }
  }

  /// Read the command's protocol string without panicking.
  ///
  /// Typically used for logging or debugging.
  pub fn to_str_debug(&self) -> &str {
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
      RedisCommandKind::Asking => "ASKING",
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
      RedisCommandKind::Eval => "EVAL",
      RedisCommandKind::EvalSha => "EVALSHA",
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
      RedisCommandKind::_Hello(_) => "HELLO",
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
      RedisCommandKind::Lcs => "LCS",
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
      RedisCommandKind::Psubscribe => "PSUBSCRIBE",
      RedisCommandKind::Pubsub => "PUBSUB",
      RedisCommandKind::Pttl => "PTTL",
      RedisCommandKind::Publish => "PUBLISH",
      RedisCommandKind::Punsubscribe => "PUNSUBSCRIBE",
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
      RedisCommandKind::Sentinel => "SENTINEL",
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
      RedisCommandKind::XinfoConsumers => "XINFO CONSUMERS",
      RedisCommandKind::XinfoGroups => "XINFO GROUPS",
      RedisCommandKind::XinfoStream => "XINFO STREAM",
      RedisCommandKind::Xadd => "XADD",
      RedisCommandKind::Xtrim => "XTRIM",
      RedisCommandKind::Xdel => "XDEL",
      RedisCommandKind::Xrange => "XRANGE",
      RedisCommandKind::Xrevrange => "XREVRANGE",
      RedisCommandKind::Xlen => "XLEN",
      RedisCommandKind::Xread => "XREAD",
      RedisCommandKind::Xgroupcreate => "XGROUP CREATE",
      RedisCommandKind::XgroupCreateConsumer => "XGROUP CREATECONSUMER",
      RedisCommandKind::XgroupDelConsumer => "XGROUP DELCONSUMER",
      RedisCommandKind::XgroupDestroy => "XGROUP DESTROY",
      RedisCommandKind::XgroupSetId => "XGROUP SETID",
      RedisCommandKind::Xreadgroup => "XREADGROUP",
      RedisCommandKind::Xack => "XACK",
      RedisCommandKind::Xclaim => "XCLAIM",
      RedisCommandKind::Xautoclaim => "XAUTOCLAIM",
      RedisCommandKind::Xpending => "XPENDING",
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
      RedisCommandKind::Scan => "SCAN",
      RedisCommandKind::Sscan => "SSCAN",
      RedisCommandKind::Hscan => "HSCAN",
      RedisCommandKind::Zscan => "ZSCAN",
      RedisCommandKind::ScriptDebug => "SCRIPT DEBUG",
      RedisCommandKind::ScriptExists => "SCRIPT EXISTS",
      RedisCommandKind::ScriptFlush => "SCRIPT FLUSH",
      RedisCommandKind::ScriptKill => "SCRIPT KILL",
      RedisCommandKind::ScriptLoad => "SCRIPT LOAD",
      RedisCommandKind::Spublish => "SPUBLISH",
      RedisCommandKind::Ssubscribe => "SSUBSCRIBE",
      RedisCommandKind::Sunsubscribe => "SUNSUBSCRIBE",
      RedisCommandKind::_AuthAllCluster => "AUTH ALL CLUSTER",
      RedisCommandKind::_HelloAllCluster(_) => "HELLO ALL CLUSTER",
      RedisCommandKind::_FlushAllCluster => "FLUSHALL CLUSTER",
      RedisCommandKind::_ScriptFlushCluster => "SCRIPT FLUSH CLUSTER",
      RedisCommandKind::_ScriptLoadCluster => "SCRIPT LOAD CLUSTER",
      RedisCommandKind::_ScriptKillCluster => "SCRIPT Kill CLUSTER",
      RedisCommandKind::_FunctionLoadCluster => "FUNCTION LOAD CLUSTER",
      RedisCommandKind::_FunctionFlushCluster => "FUNCTION FLUSH CLUSTER",
      RedisCommandKind::_FunctionDeleteCluster => "FUNCTION DELETE CLUSTER",
      RedisCommandKind::_FunctionRestoreCluster => "FUNCTION RESTORE CLUSTER",
      RedisCommandKind::Fcall => "FCALL",
      RedisCommandKind::FcallRO => "FCALL_RO",
      RedisCommandKind::FunctionDelete => "FUNCTION DELETE",
      RedisCommandKind::FunctionDump => "FUNCTION DUMP",
      RedisCommandKind::FunctionFlush => "FUNCTION FLUSH",
      RedisCommandKind::FunctionKill => "FUNCTION KILL",
      RedisCommandKind::FunctionList => "FUNCTION LIST",
      RedisCommandKind::FunctionLoad => "FUNCTION LOAD",
      RedisCommandKind::FunctionRestore => "FUNCTION RESTORE",
      RedisCommandKind::FunctionStats => "FUNCTION STATS",
      RedisCommandKind::_Custom(ref kind) => &kind.cmd,
    }
  }

  /// Read the protocol string for a command, panicking for internal commands that don't map directly to redis
  /// command.
  pub(crate) fn cmd_str(&self) -> Str {
    let s = match *self {
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
      | RedisCommandKind::AclHelp => "ACL",
      RedisCommandKind::Append => "APPEND",
      RedisCommandKind::Auth => "AUTH",
      RedisCommandKind::Asking => "ASKING",
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
      RedisCommandKind::ClientID
      | RedisCommandKind::ClientInfo
      | RedisCommandKind::ClientKill
      | RedisCommandKind::ClientList
      | RedisCommandKind::ClientGetName
      | RedisCommandKind::ClientGetRedir
      | RedisCommandKind::ClientPause
      | RedisCommandKind::ClientUnpause
      | RedisCommandKind::ClientUnblock
      | RedisCommandKind::ClientReply
      | RedisCommandKind::ClientSetname => "CLIENT",
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
      | RedisCommandKind::ClusterSlots
      | RedisCommandKind::ClusterBumpEpoch
      | RedisCommandKind::ClusterFlushSlots
      | RedisCommandKind::ClusterMyID => "CLUSTER",
      RedisCommandKind::ConfigGet
      | RedisCommandKind::ConfigRewrite
      | RedisCommandKind::ConfigSet
      | RedisCommandKind::ConfigResetStat => "CONFIG",
      RedisCommandKind::Copy => "COPY",
      RedisCommandKind::DBSize => "DBSIZE",
      RedisCommandKind::Decr => "DECR",
      RedisCommandKind::DecrBy => "DECRBY",
      RedisCommandKind::Del => "DEL",
      RedisCommandKind::Discard => "DISCARD",
      RedisCommandKind::Dump => "DUMP",
      RedisCommandKind::Echo => "ECHO",
      RedisCommandKind::Eval => "EVAL",
      RedisCommandKind::EvalSha => "EVALSHA",
      RedisCommandKind::Exec => "EXEC",
      RedisCommandKind::Exists => "EXISTS",
      RedisCommandKind::Expire => "EXPIRE",
      RedisCommandKind::ExpireAt => "EXPIREAT",
      RedisCommandKind::Failover => "FAILOVER",
      RedisCommandKind::FlushAll => "FLUSHALL",
      RedisCommandKind::_FlushAllCluster => "FLUSHALL",
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
      RedisCommandKind::_Hello(_) => "HELLO",
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
      RedisCommandKind::Lcs => "LCS",
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
      RedisCommandKind::Psubscribe => "PSUBSCRIBE",
      RedisCommandKind::Pubsub => "PUBSUB",
      RedisCommandKind::Pttl => "PTTL",
      RedisCommandKind::Publish => "PUBLISH",
      RedisCommandKind::Punsubscribe => "PUNSUBSCRIBE",
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
      RedisCommandKind::Sentinel => "SENTINEL",
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
      RedisCommandKind::XinfoConsumers | RedisCommandKind::XinfoGroups | RedisCommandKind::XinfoStream => "XINFO",
      RedisCommandKind::Xadd => "XADD",
      RedisCommandKind::Xtrim => "XTRIM",
      RedisCommandKind::Xdel => "XDEL",
      RedisCommandKind::Xrange => "XRANGE",
      RedisCommandKind::Xrevrange => "XREVRANGE",
      RedisCommandKind::Xlen => "XLEN",
      RedisCommandKind::Xread => "XREAD",
      RedisCommandKind::Xgroupcreate
      | RedisCommandKind::XgroupCreateConsumer
      | RedisCommandKind::XgroupDelConsumer
      | RedisCommandKind::XgroupDestroy
      | RedisCommandKind::XgroupSetId => "XGROUP",
      RedisCommandKind::Xreadgroup => "XREADGROUP",
      RedisCommandKind::Xack => "XACK",
      RedisCommandKind::Xclaim => "XCLAIM",
      RedisCommandKind::Xautoclaim => "XAUTOCLAIM",
      RedisCommandKind::Xpending => "XPENDING",
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
      RedisCommandKind::ScriptDebug
      | RedisCommandKind::ScriptExists
      | RedisCommandKind::ScriptFlush
      | RedisCommandKind::ScriptKill
      | RedisCommandKind::ScriptLoad
      | RedisCommandKind::_ScriptFlushCluster
      | RedisCommandKind::_ScriptKillCluster
      | RedisCommandKind::_ScriptLoadCluster => "SCRIPT",
      RedisCommandKind::Spublish => "SPUBLISH",
      RedisCommandKind::Ssubscribe => "SSUBSCRIBE",
      RedisCommandKind::Sunsubscribe => "SUNSUBSCRIBE",
      RedisCommandKind::Scan => "SCAN",
      RedisCommandKind::Sscan => "SSCAN",
      RedisCommandKind::Hscan => "HSCAN",
      RedisCommandKind::Zscan => "ZSCAN",
      RedisCommandKind::Fcall => "FCALL",
      RedisCommandKind::FcallRO => "FCALL_RO",
      RedisCommandKind::FunctionDelete
      | RedisCommandKind::FunctionDump
      | RedisCommandKind::FunctionFlush
      | RedisCommandKind::FunctionKill
      | RedisCommandKind::FunctionList
      | RedisCommandKind::FunctionLoad
      | RedisCommandKind::FunctionRestore
      | RedisCommandKind::FunctionStats
      | RedisCommandKind::_FunctionFlushCluster
      | RedisCommandKind::_FunctionRestoreCluster
      | RedisCommandKind::_FunctionDeleteCluster
      | RedisCommandKind::_FunctionLoadCluster => "FUNCTION",
      RedisCommandKind::_AuthAllCluster => "AUTH",
      RedisCommandKind::_HelloAllCluster(_) => "HELLO",
      RedisCommandKind::_Custom(ref kind) => return kind.cmd.clone(),
    };

    client_utils::static_str(s)
  }

  /// Read the optional subcommand string for a command.
  pub fn subcommand_str(&self) -> Option<Str> {
    let s = match *self {
      RedisCommandKind::ScriptDebug => "DEBUG",
      RedisCommandKind::ScriptLoad => "LOAD",
      RedisCommandKind::ScriptKill => "KILL",
      RedisCommandKind::ScriptFlush => "FLUSH",
      RedisCommandKind::ScriptExists => "EXISTS",
      RedisCommandKind::_ScriptFlushCluster => "FLUSH",
      RedisCommandKind::_ScriptLoadCluster => "LOAD",
      RedisCommandKind::_ScriptKillCluster => "KILL",
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
      RedisCommandKind::XinfoConsumers => "CONSUMERS",
      RedisCommandKind::XinfoGroups => "GROUPS",
      RedisCommandKind::XinfoStream => "STREAM",
      RedisCommandKind::Xgroupcreate => "CREATE",
      RedisCommandKind::XgroupCreateConsumer => "CREATECONSUMER",
      RedisCommandKind::XgroupDelConsumer => "DELCONSUMER",
      RedisCommandKind::XgroupDestroy => "DESTROY",
      RedisCommandKind::XgroupSetId => "SETID",
      RedisCommandKind::FunctionDelete => "DELETE",
      RedisCommandKind::FunctionDump => "DUMP",
      RedisCommandKind::FunctionFlush => "FLUSH",
      RedisCommandKind::FunctionKill => "KILL",
      RedisCommandKind::FunctionList => "LIST",
      RedisCommandKind::FunctionLoad => "LOAD",
      RedisCommandKind::FunctionRestore => "RESTORE",
      RedisCommandKind::FunctionStats => "STATS",
      RedisCommandKind::_FunctionLoadCluster => "LOAD",
      RedisCommandKind::_FunctionFlushCluster => "FLUSH",
      RedisCommandKind::_FunctionDeleteCluster => "DELETE",
      RedisCommandKind::_FunctionRestoreCluster => "RESTORE",
      _ => return None,
    };

    Some(utils::static_str(s))
  }

  pub fn use_random_cluster_node(&self) -> bool {
    match self {
      RedisCommandKind::Publish
      | RedisCommandKind::Subscribe
      | RedisCommandKind::Unsubscribe
      | RedisCommandKind::Psubscribe
      | RedisCommandKind::Punsubscribe
      | RedisCommandKind::Ping
      | RedisCommandKind::Info
      | RedisCommandKind::Scan
      | RedisCommandKind::FlushAll
      | RedisCommandKind::FlushDB => true,
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
      | RedisCommandKind::Fcall
      | RedisCommandKind::FcallRO
      | RedisCommandKind::Wait => true,
      // can be changed by the BLOCKING args
      RedisCommandKind::Xread | RedisCommandKind::Xreadgroup => false,
      RedisCommandKind::_Custom(ref kind) => kind.is_blocking,
      _ => false,
    }
  }

  pub fn is_all_cluster_nodes(&self) -> bool {
    match *self {
      RedisCommandKind::_FlushAllCluster
      | RedisCommandKind::_AuthAllCluster
      | RedisCommandKind::_ScriptFlushCluster
      | RedisCommandKind::_ScriptKillCluster
      | RedisCommandKind::_HelloAllCluster(_)
      | RedisCommandKind::_ScriptLoadCluster
      | RedisCommandKind::_FunctionFlushCluster
      | RedisCommandKind::_FunctionDeleteCluster
      | RedisCommandKind::_FunctionRestoreCluster
      | RedisCommandKind::_FunctionLoadCluster => true,
      _ => false,
    }
  }

  pub fn should_flush(&self) -> bool {
    match self {
      RedisCommandKind::Quit
      | RedisCommandKind::Shutdown
      | RedisCommandKind::Ping
      | RedisCommandKind::Auth
      | RedisCommandKind::_Hello(_)
      | RedisCommandKind::Exec
      | RedisCommandKind::Discard
      | RedisCommandKind::Eval
      | RedisCommandKind::EvalSha
      | RedisCommandKind::Fcall
      | RedisCommandKind::FcallRO
      | RedisCommandKind::_Custom(_) => true,
      _ => false,
    }
  }

  pub fn can_pipeline(&self) -> bool {
    if self.is_blocking() {
      false
    } else {
      match self {
        // make it easier to handle multiple potentially out-of-band responses
        RedisCommandKind::Psubscribe
        | RedisCommandKind::Punsubscribe
        | RedisCommandKind::Ssubscribe
        | RedisCommandKind::Sunsubscribe
        // https://redis.io/commands/eval#evalsha-in-the-context-of-pipelining
        | RedisCommandKind::Eval
        | RedisCommandKind::EvalSha
        | RedisCommandKind::Auth
        | RedisCommandKind::Fcall
        | RedisCommandKind::FcallRO
        // makes it easier to avoid decoding in-flight responses with the wrong codec logic
        | RedisCommandKind::_Hello(_) => false,
        _ => true,
      }
    }
  }

  pub fn is_eval(&self) -> bool {
    match *self {
      RedisCommandKind::EvalSha | RedisCommandKind::Eval | RedisCommandKind::Fcall | RedisCommandKind::FcallRO => {
        true
      },
      _ => false,
    }
  }
}

pub struct RedisCommand {
  /// The command and optional subcommand name.
  pub kind:              RedisCommandKind,
  /// The policy to apply when handling the response.
  pub response:          ResponseKind,
  /// The policy to use when hashing the arguments for cluster routing.
  pub hasher:            ClusterHash,
  /// The provided arguments.
  ///
  /// Some commands store arguments differently. Callers should use `self.args()` to account for this.
  pub arguments:         Vec<RedisValue>,
  /// A oneshot sender used to communicate with the multiplexer.
  pub multiplexer_tx:    Arc<Mutex<Option<MultiplexerSender>>>,
  /// The number of times the command was sent to the server.
  pub attempted:         u32,
  /// Whether or not the command can be pipelined.
  ///
  /// Also used for commands like XREAD that block based on an argument.
  pub can_pipeline:      bool,
  /// Whether or not to skip backpressure checks.
  pub skip_backpressure: bool,
  /// The internal ID of a transaction.
  pub transaction_id:    Option<u64>,
  /// Whether the command has timed out from the perspective of the caller.
  pub timed_out:         Arc<AtomicBool>,
  /// A timestamp of when the command was last written to the socket.
  pub network_start:     Option<Instant>,
  /// Whether to route the command to a replica, if possible.
  #[cfg(feature = "replicas")]
  pub use_replica:       bool,
  /// A timestamp of when the command was first created from the public interface.
  #[cfg(feature = "metrics")]
  pub created:           Instant,
  /// Tracing state that has to carry over across writer/reader tasks to track certain fields (response size, etc).
  #[cfg(feature = "partial-tracing")]
  pub traces:            CommandTraces,
  /// A counter to differentiate unique commands.
  #[cfg(feature = "debug-ids")]
  pub counter:           usize,
}

impl fmt::Debug for RedisCommand {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("RedisCommand")
      .field("command", &self.kind.to_str_debug())
      .field("attempted", &self.attempted)
      .field("can_pipeline", &self.can_pipeline)
      .field("arguments", &self.args())
      .finish()
  }
}

impl fmt::Display for RedisCommand {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.kind.to_str_debug())
  }
}

impl From<RedisCommandKind> for RedisCommand {
  fn from(kind: RedisCommandKind) -> Self {
    (kind, Vec::new()).into()
  }
}

impl From<(RedisCommandKind, Vec<RedisValue>)> for RedisCommand {
  fn from((kind, arguments): (RedisCommandKind, Vec<RedisValue>)) -> Self {
    RedisCommand {
      kind,
      arguments,
      timed_out: Arc::new(AtomicBool::new(false)),
      response: ResponseKind::Respond(None),
      hasher: ClusterHash::default(),
      multiplexer_tx: Arc::new(Mutex::new(None)),
      attempted: 0,
      can_pipeline: true,
      skip_backpressure: false,
      transaction_id: None,
      #[cfg(feature = "replicas")]
      use_replica: false,
      #[cfg(feature = "metrics")]
      created: Instant::now(),
      #[cfg(any(feature = "metrics", feature = "partial-tracing"))]
      network_start: None,
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
      #[cfg(feature = "debug-ids")]
      counter: command_counter(),
    }
  }
}

impl From<(RedisCommandKind, Vec<RedisValue>, ResponseSender)> for RedisCommand {
  fn from((kind, arguments, tx): (RedisCommandKind, Vec<RedisValue>, ResponseSender)) -> Self {
    RedisCommand {
      kind,
      arguments,
      timed_out: Arc::new(AtomicBool::new(false)),
      response: ResponseKind::Respond(Some(tx)),
      hasher: ClusterHash::default(),
      multiplexer_tx: Arc::new(Mutex::new(None)),
      attempted: 0,
      can_pipeline: true,
      skip_backpressure: false,
      transaction_id: None,
      #[cfg(feature = "replicas")]
      use_replica: false,
      #[cfg(feature = "metrics")]
      created: Instant::now(),
      #[cfg(any(feature = "metrics", feature = "partial-tracing"))]
      network_start: None,
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
      #[cfg(feature = "debug-ids")]
      counter: command_counter(),
    }
  }
}

impl From<(RedisCommandKind, Vec<RedisValue>, ResponseKind)> for RedisCommand {
  fn from((kind, arguments, response): (RedisCommandKind, Vec<RedisValue>, ResponseKind)) -> Self {
    RedisCommand {
      kind,
      arguments,
      response,
      timed_out: Arc::new(AtomicBool::new(false)),
      hasher: ClusterHash::default(),
      multiplexer_tx: Arc::new(Mutex::new(None)),
      attempted: 0,
      can_pipeline: true,
      skip_backpressure: false,
      transaction_id: None,
      #[cfg(feature = "replicas")]
      use_replica: false,
      #[cfg(feature = "metrics")]
      created: Instant::now(),
      #[cfg(any(feature = "metrics", feature = "partial-tracing"))]
      network_start: None,
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
      #[cfg(feature = "debug-ids")]
      counter: command_counter(),
    }
  }
}

impl RedisCommand {
  /// Create a new command without a response handling policy.
  pub fn new(kind: RedisCommandKind, args: Vec<RedisValue>) -> Self {
    RedisCommand {
      kind,
      arguments: args,
      timed_out: Arc::new(AtomicBool::new(false)),
      response: ResponseKind::Skip,
      hasher: ClusterHash::FirstKey,
      multiplexer_tx: Arc::new(Mutex::new(None)),
      attempted: 0,
      can_pipeline: true,
      skip_backpressure: false,
      transaction_id: None,
      #[cfg(feature = "replicas")]
      use_replica: false,
      #[cfg(feature = "metrics")]
      created: Instant::now(),
      #[cfg(any(feature = "metrics", feature = "partial-tracing"))]
      network_start: None,
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
      #[cfg(feature = "debug-ids")]
      counter: command_counter(),
    }
  }

  /// Create a new empty `ASKING` command.
  pub fn new_asking(hash_slot: u16) -> Self {
    RedisCommand {
      kind: RedisCommandKind::Asking,
      arguments: Vec::new(),
      response: ResponseKind::Skip,
      hasher: ClusterHash::Custom(hash_slot),
      timed_out: Arc::new(AtomicBool::new(false)),
      multiplexer_tx: Arc::new(Mutex::new(None)),
      attempted: 0,
      can_pipeline: false,
      skip_backpressure: true,
      transaction_id: None,
      #[cfg(feature = "replicas")]
      use_replica: false,
      #[cfg(feature = "metrics")]
      created: Instant::now(),
      #[cfg(any(feature = "metrics", feature = "partial-tracing"))]
      network_start: None,
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
      #[cfg(feature = "debug-ids")]
      counter: command_counter(),
    }
  }

  /// Whether or not to pipeline the command.
  pub fn should_auto_pipeline(&self, inner: &Arc<RedisClientInner>, force: bool) -> bool {
    let should_pipeline = force
      || (inner.is_pipelined()
      && self.can_pipeline
      && self.kind.can_pipeline()
      && !self.kind.is_all_cluster_nodes()
      // disable pipelining for transactions to handle ASK errors or support the `abort_on_error` logic
      && self.transaction_id.is_none());

    _trace!(
      inner,
      "Pipeline check {}: {}",
      self.kind.to_str_debug(),
      should_pipeline
    );
    should_pipeline
  }

  /// Whether errors writing the command should be returned to the caller.
  pub fn should_send_write_error(&self, inner: &Arc<RedisClientInner>) -> bool {
    self.attempted >= inner.max_command_attempts() || inner.policy.read().is_none()
  }

  /// Mark the command to only run once, returning connection write errors to the caller immediately.
  pub fn set_try_once(&mut self, inner: &Arc<RedisClientInner>) {
    self.attempted = inner.max_command_attempts();
  }

  /// Increment and check the number of write attempts.
  pub fn incr_check_attempted(&mut self, max: u32) -> Result<(), RedisError> {
    self.attempted += 1;
    if max > 0 && self.attempted > max {
      Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Too many failed write attempts.",
      ))
    } else {
      Ok(())
    }
  }

  /// Read the arguments associated with the command.
  pub fn args(&self) -> &Vec<RedisValue> {
    match self.response {
      ResponseKind::ValueScan(ref inner) => &inner.args,
      ResponseKind::KeyScan(ref inner) => &inner.args,
      _ => &self.arguments,
    }
  }

  /// Whether the command blocks the connection.
  pub fn blocks_connection(&self) -> bool {
    self.transaction_id.is_none()
      && (self.kind.is_blocking()
        || match self.kind {
          RedisCommandKind::Xread | RedisCommandKind::Xreadgroup => !self.can_pipeline,
          _ => false,
        })
  }

  /// Take the arguments from this command.
  pub fn take_args(&mut self) -> Vec<RedisValue> {
    match self.response {
      ResponseKind::ValueScan(ref mut inner) => inner.args.drain(..).collect(),
      ResponseKind::KeyScan(ref mut inner) => inner.args.drain(..).collect(),
      _ => self.arguments.drain(..).collect(),
    }
  }

  /// Take the response handler, replacing it with `ResponseKind::Skip`.
  pub fn take_response(&mut self) -> ResponseKind {
    mem::replace(&mut self.response, ResponseKind::Skip)
  }

  /// Create a channel on which to block the multiplexer, returning the receiver.
  pub fn create_multiplexer_channel(&self) -> OneshotReceiver<MultiplexerResponse> {
    let (tx, rx) = oneshot_channel();
    let mut guard = self.multiplexer_tx.lock();
    *guard = Some(tx);
    rx
  }

  /// Send a message to unblock the multiplexer loop, if necessary.
  pub fn respond_to_multiplexer(&self, inner: &Arc<RedisClientInner>, cmd: MultiplexerResponse) {
    if let Some(tx) = self.multiplexer_tx.lock().take() {
      if tx.send(cmd).is_err() {
        _warn!(inner, "Failed to unblock multiplexer loop.");
      }
    }
  }

  /// Take the multiplexer sender from the command.
  pub fn take_multiplexer_tx(&self) -> Option<MultiplexerSender> {
    self.multiplexer_tx.lock().take()
  }

  /// Whether the command has a channel to the multiplexer.
  pub fn has_multiplexer_channel(&self) -> bool {
    self.multiplexer_tx.lock().is_some()
  }

  /// Clone the command, supporting commands with shared response state.
  ///
  /// Note: this will **not** clone the multiplexer channel.
  pub fn duplicate(&self, response: ResponseKind) -> Self {
    RedisCommand {
      timed_out: self.timed_out.clone(),
      kind: self.kind.clone(),
      arguments: self.arguments.clone(),
      hasher: self.hasher.clone(),
      transaction_id: self.transaction_id.clone(),
      attempted: self.attempted,
      can_pipeline: self.can_pipeline,
      skip_backpressure: self.skip_backpressure,
      multiplexer_tx: self.multiplexer_tx.clone(),
      response,
      #[cfg(feature = "replicas")]
      use_replica: self.use_replica,
      #[cfg(feature = "metrics")]
      created: self.created.clone(),
      #[cfg(any(feature = "metrics", feature = "partial-tracing"))]
      network_start: self.network_start.clone(),
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
      #[cfg(feature = "debug-ids")]
      counter: self.counter,
    }
  }

  /// Take the command tracing state for the `queued` span.
  #[cfg(feature = "full-tracing")]
  pub fn take_queued_span(&mut self) -> Option<trace::Span> {
    self.traces.queued.take()
  }

  /// Take the command tracing state for the `queued` span.
  #[cfg(not(feature = "full-tracing"))]
  pub fn take_queued_span(&mut self) -> Option<trace::disabled::Span> {
    None
  }

  /// Take the response sender from the command.
  ///
  /// Usually used for responding early without sending the command.
  pub fn take_responder(&mut self) -> Option<ResponseSender> {
    match self.response {
      ResponseKind::Respond(ref mut tx) => tx.take(),
      ResponseKind::Multiple { ref mut tx, .. } => tx.lock().take(),
      ResponseKind::Buffer { ref mut tx, .. } => tx.lock().take(),
      _ => None,
    }
  }

  /// Whether the command has a channel for sending responses to the caller.
  pub fn has_response_tx(&self) -> bool {
    match self.response {
      ResponseKind::Respond(ref r) => r.is_some(),
      ResponseKind::Multiple { ref tx, .. } => tx.lock().is_some(),
      ResponseKind::Buffer { ref tx, .. } => tx.lock().is_some(),
      _ => false,
    }
  }

  /// Respond to the caller, taking the response channel in the process.
  pub fn respond_to_caller(&mut self, result: Result<Resp3Frame, RedisError>) {
    if let Some(tx) = self.take_responder() {
      let _ = tx.send(result);
    }
  }

  /// Read the first key in the arguments according to the `FirstKey` cluster hash policy.
  pub fn first_key(&self) -> Option<&[u8]> {
    ClusterHash::FirstKey.find_key(self.args())
  }

  /// Hash the arguments according to the command's cluster hash policy.
  pub fn cluster_hash(&self) -> Option<u16> {
    self
      .kind
      .custom_hash_slot()
      .or(self.scan_hash_slot())
      .or(self.hasher.hash(self.args()))
  }

  /// Read the custom hash slot assigned to a scan operation.
  pub fn scan_hash_slot(&self) -> Option<u16> {
    match self.response {
      ResponseKind::KeyScan(ref inner) => inner.hash_slot.clone(),
      _ => None,
    }
  }

  /// Convert to a single frame with an array of bulk strings (or null).
  #[cfg(not(feature = "blocking-encoding"))]
  pub fn to_frame(&self, is_resp3: bool) -> Result<ProtocolFrame, RedisError> {
    protocol_utils::command_to_frame(self, is_resp3)
  }

  /// Convert to a single frame with an array of bulk strings (or null), using a blocking task.
  #[cfg(feature = "blocking-encoding")]
  pub fn to_frame(&self, is_resp3: bool) -> Result<ProtocolFrame, RedisError> {
    let cmd_size = protocol_utils::args_size(self.args());

    if cmd_size >= globals().blocking_encode_threshold() {
      trace!("Using blocking task to convert command to frame with size {}", cmd_size);
      tokio::task::block_in_place(|| protocol_utils::command_to_frame(self, is_resp3))
    } else {
      protocol_utils::command_to_frame(self, is_resp3)
    }
  }

  #[cfg(feature = "mocks")]
  pub fn to_mocked(&self) -> MockCommand {
    MockCommand {
      cmd:        self.kind.cmd_str(),
      subcommand: self.kind.subcommand_str(),
      args:       self.args().clone(),
    }
  }

  #[cfg(not(feature = "debug-ids"))]
  pub fn debug_id(&self) -> usize {
    0
  }

  #[cfg(feature = "debug-ids")]
  pub fn debug_id(&self) -> usize {
    self.counter
  }
}

/// A message sent from the front-end client to the multiplexer.
pub enum MultiplexerCommand {
  /// Send a command to the server.
  Command(RedisCommand),
  /// Send a pipelined series of commands to the server.
  ///
  /// Commands may finish out of order in the following cluster scenario:
  /// 1. The client sends `GET foo`.
  /// 2. The client sends `GET bar`.
  /// 3. The client sends `GET baz`.
  /// 4. The client receives a successful response from `GET foo`.
  /// 5. The client receives `MOVED` or `ASK` from `GET bar`.
  /// 6. The client receives a successful response from `GET baz`.
  ///
  /// In this scenario the client will retry `GET bar` against the correct node, but after `GET baz` has already
  /// finished. Callers should use a transaction if they require commands to always finish in order across
  /// arbitrary keys in a cluster. Both a `Pipeline` and `Transaction` will run a series of commands without
  /// interruption, but only a `Transaction` can guarantee in-order execution while accounting for cluster errors.
  ///
  /// Note: if the third command also operated on the `bar` key (such as `TTL bar` instead of `GET baz`) then the
  /// commands **would** finish in order, since the server would respond with `MOVED` or `ASK` to both commands,
  /// and the client would retry them in the same order.
  Pipeline { commands: Vec<RedisCommand> },
  /// Send a transaction to the server.
  ///
  /// Notes:
  /// * The inner command buffer will not contain the initial `MULTI` or trailing `EXEC` command.
  /// * Transactions are never pipelined in order to handle ASK responses.
  /// * IDs must be unique w/r/t other transactions buffered in memory.
  ///
  /// There is one special failure mode that must be considered:
  /// 1. The client sends `MULTI` and we receive an `OK` response.
  /// 2. The caller sends `GET foo{1}` and we receive a `QUEUED` response.
  /// 3. The caller sends `GET bar{1}` and we receive an `ASK` response.
  ///
  /// According to the cluster spec the client should retry the entire transaction against the node in the `ASK`
  /// response, but with an `ASKING` command before `MULTI`. However, the future returned to the caller from `GET
  /// foo{1}` will have already finished at this point. To account for this the client will never pipeline
  /// transactions against a cluster, and may clone commands before sending them in order to replay them later with
  /// a different cluster node mapping.
  Transaction {
    id:             u64,
    commands:       Vec<RedisCommand>,
    abort_on_error: bool,
    tx:             ResponseSender,
  },
  /// Retry a command after a `MOVED` error.
  ///
  /// This will trigger a call to `CLUSTER SLOTS` before the command is retried. Additionally,
  /// the client will **not** increment the command's write attempt counter.
  Moved {
    slot:    u16,
    server:  Server,
    command: RedisCommand,
  },
  /// Retry a command after an `ASK` error.
  ///
  /// The client will **not** increment the command's write attempt counter.
  ///
  /// This is typically used instead of `MultiplexerResponse::Ask` when a command was pipelined.
  Ask {
    slot:    u16,
    server:  Server,
    command: RedisCommand,
  },
  /// Initiate a reconnection to the provided server, or all servers.
  ///
  /// The client may not perform a reconnection if a healthy connection exists to `server`, unless `force` is `true`.
  Reconnect {
    server: Option<Server>,
    force:  bool,
    tx:     Option<ResponseSender>,
  },
  /// Sync the cached cluster state with the server via `CLUSTER SLOTS`.
  SyncCluster { tx: OneshotSender<Result<(), RedisError>> },
  /// Read the set of active connections managed by the client.
  Connections { tx: OneshotSender<Vec<Server>> },
}

impl fmt::Debug for MultiplexerCommand {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let mut formatter = f.debug_struct("MultiplexerCommand");

    match self {
      MultiplexerCommand::Ask { server, slot, command } => {
        formatter
          .field("kind", &"Ask")
          .field("server", &server)
          .field("slot", &slot)
          .field("command", &command.kind.to_str_debug());
      },
      MultiplexerCommand::Moved { server, slot, command } => {
        formatter
          .field("kind", &"Moved")
          .field("server", &server)
          .field("slot", &slot)
          .field("command", &command.kind.to_str_debug());
      },
      MultiplexerCommand::Reconnect { server, force, .. } => {
        formatter
          .field("kind", &"Reconnect")
          .field("server", &server)
          .field("force", &force);
      },
      MultiplexerCommand::SyncCluster { .. } => {
        formatter.field("kind", &"Sync Cluster");
      },
      MultiplexerCommand::Transaction { .. } => {
        formatter.field("kind", &"Transaction");
      },
      MultiplexerCommand::Pipeline { .. } => {
        formatter.field("kind", &"Pipeline");
      },
      MultiplexerCommand::Connections { .. } => {
        formatter.field("kind", &"Connections");
      },
      MultiplexerCommand::Command(command) => {
        formatter
          .field("kind", &"Command")
          .field("command", &command.kind.to_str_debug());
      },
    };

    formatter.finish()
  }
}

impl From<RedisCommand> for MultiplexerCommand {
  fn from(cmd: RedisCommand) -> Self {
    MultiplexerCommand::Command(cmd)
  }
}
