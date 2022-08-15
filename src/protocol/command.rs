use crate::error::{RedisError, RedisErrorKind};
use crate::interfaces::Resp3Frame;
use crate::modules::inner::RedisClientInner;
use crate::protocol::command::ResponseKind::Respond;
use crate::protocol::connection::{SentCommand, SharedBuffer};
use crate::protocol::hashers::ClusterHash;
use crate::protocol::responders::ResponseKind;
use crate::protocol::types::{KeyScanInner, ProtocolFrame, SplitCommand, ValueScanInner};
use crate::protocol::utils as protocol_utils;
use crate::types::{CustomCommand, RedisValue};
use crate::utils as client_utils;
use bytes_utils::Str;
use lazy_static::lazy_static;
use parking_lot::Mutex;
use redis_protocol::resp3::types::RespVersion;
use semver::Op;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::env::args;
use std::fmt;
use std::fmt::Formatter;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot::{channel as oneshot_channel, Receiver as OneshotReceiver, Sender as OneshotSender};

#[cfg(feature = "blocking-encoding")]
use crate::globals::globals;

#[cfg(not(feature = "full-tracing"))]
use crate::trace::disabled::Span as FakeSpan;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use crate::trace::CommandTraces;
#[cfg(any(feature = "full-tracing", feature = "partial-tracing"))]
use crate::trace::Span;

pub type ResponseSender = OneshotSender<Result<Resp3Frame, RedisError>>;
pub type MultiplexerSender = OneshotSender<()>;

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
  // Commands with custom state or commands that don't map directly to the server's command interface.
  _Scan(KeyScanInner),
  _Sscan(ValueScanInner),
  _Hscan(ValueScanInner),
  _Zscan(ValueScanInner),
  /// Close all connections and reset the client.
  _Close,
  /// Force sync the cluster state.
  _Sync,
  /// Force a reconnection
  _Reconnect,
  _Hello(RespVersion),
  _Split(SplitCommand),
  _AuthAllCluster,
  _HelloAllCluster(RespVersion),
  _FlushAllCluster,
  _ScriptFlushCluster,
  _ScriptLoadCluster,
  _ScriptKillCluster,
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
      RedisCommandKind::_Scan(_) => true,
      _ => false,
    }
  }

  pub fn is_hscan(&self) -> bool {
    match *self {
      RedisCommandKind::_Hscan(_) => true,
      _ => false,
    }
  }

  pub fn is_sscan(&self) -> bool {
    match *self {
      RedisCommandKind::_Sscan(_) => true,
      _ => false,
    }
  }

  pub fn is_zscan(&self) -> bool {
    match *self {
      RedisCommandKind::_Zscan(_) => true,
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
      RedisCommandKind::_Zscan(_) | RedisCommandKind::_Hscan(_) | RedisCommandKind::_Sscan(_) => true,
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

  pub fn closes_connection(&self) -> bool {
    match *self {
      RedisCommandKind::Quit | RedisCommandKind::Shutdown => true,
      _ => false,
    }
  }

  /// Read the command's protocol string without panicking.
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
      RedisCommandKind::_Scan(_) => "SCAN",
      RedisCommandKind::_Sscan(_) => "SSCAN",
      RedisCommandKind::_Hscan(_) => "HSCAN",
      RedisCommandKind::_Zscan(_) => "ZSCAN",
      RedisCommandKind::ScriptDebug => "SCRIPT DEBUG",
      RedisCommandKind::ScriptExists => "SCRIPT EXISTS",
      RedisCommandKind::ScriptFlush => "SCRIPT FLUSH",
      RedisCommandKind::ScriptKill => "SCRIPT KILL",
      RedisCommandKind::ScriptLoad => "SCRIPT LOAD",
      RedisCommandKind::_Close => "CLOSE",
      RedisCommandKind::_Split(_) => "SPLIT",
      RedisCommandKind::_Sync => "SYNC",
      RedisCommandKind::_Reconnect => "RECONNECT",
      RedisCommandKind::_AuthAllCluster => "AUTH ALL CLUSTER",
      RedisCommandKind::_HelloAllCluster(_) => "HELLO ALL CLUSTER",
      RedisCommandKind::_FlushAllCluster => "FLUSHALL CLUSTER",
      RedisCommandKind::_ScriptFlushCluster => "SCRIPT FLUSH CLUSTER",
      RedisCommandKind::_ScriptLoadCluster => "SCRIPT LOAD CLUSTER",
      RedisCommandKind::_ScriptKillCluster => "SCRIPT Kill CLUSTER",
      RedisCommandKind::_Custom(ref kind) => &kind.cmd,
    }
  }

  /// Read the protocol string for a command, panicking for internal commands that don't map directly to redis command.
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
      RedisCommandKind::_Scan(_) => "SCAN",
      RedisCommandKind::_Sscan(_) => "SSCAN",
      RedisCommandKind::_Hscan(_) => "HSCAN",
      RedisCommandKind::_Zscan(_) => "ZSCAN",
      RedisCommandKind::_AuthAllCluster => "AUTH",
      RedisCommandKind::_HelloAllCluster(_) => "HELLO",
      RedisCommandKind::_Custom(ref kind) => return kind.cmd.clone(),
      RedisCommandKind::_Close
      | RedisCommandKind::_Split(_)
      | RedisCommandKind::_Reconnect
      | RedisCommandKind::_Sync => {
        panic!("unreachable (redis command)")
      },
    };

    client_utils::static_str(s)
  }

  /// Read the optional subcommand string for a command.
  pub fn subcommand_str(&self) -> Option<&'static str> {
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
      | RedisCommandKind::_ScriptFlushCluster
      | RedisCommandKind::_ScriptLoadCluster
      | RedisCommandKind::_ScriptKillCluster
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

  pub fn use_random_cluster_node(&self) -> bool {
    match self {
      RedisCommandKind::Publish
      | RedisCommandKind::Subscribe
      | RedisCommandKind::Unsubscribe
      | RedisCommandKind::Psubscribe
      | RedisCommandKind::Punsubscribe
      | RedisCommandKind::Ping
      | RedisCommandKind::Info
      | RedisCommandKind::_Scan(_)
      | RedisCommandKind::FlushAll
      | RedisCommandKind::FlushDB => true,
      _ => false,
    }
  }

  pub fn is_stream_command(&self) -> bool {
    match *self {
      RedisCommandKind::XinfoConsumers
      | RedisCommandKind::XinfoGroups
      | RedisCommandKind::XinfoStream
      | RedisCommandKind::Xadd
      | RedisCommandKind::Xtrim
      | RedisCommandKind::Xdel
      | RedisCommandKind::Xrange
      | RedisCommandKind::Xrevrange
      | RedisCommandKind::Xlen
      | RedisCommandKind::Xread
      | RedisCommandKind::Xgroupcreate
      | RedisCommandKind::XgroupCreateConsumer
      | RedisCommandKind::XgroupDelConsumer
      | RedisCommandKind::XgroupDestroy
      | RedisCommandKind::XgroupSetId
      | RedisCommandKind::Xreadgroup
      | RedisCommandKind::Xack
      | RedisCommandKind::Xclaim
      | RedisCommandKind::Xautoclaim
      | RedisCommandKind::Xpending => true,
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
      // can be changed by the BLOCKING args
      RedisCommandKind::Xread | RedisCommandKind::Xreadgroup => false,
      RedisCommandKind::_Custom(ref kind) => kind.is_blocking,
      _ => false,
    }
  }

  pub fn custom_key_slot(&self) -> Option<u16> {
    match *self {
      RedisCommandKind::_Custom(ref kind) => kind.hash_slot.clone(),
      _ => None,
    }
  }

  pub fn is_all_cluster_nodes(&self) -> bool {
    match *self {
      RedisCommandKind::_FlushAllCluster
      | RedisCommandKind::_AuthAllCluster
      | RedisCommandKind::_ScriptFlushCluster
      | RedisCommandKind::_ScriptKillCluster
      | RedisCommandKind::_HelloAllCluster(_)
      | RedisCommandKind::_ScriptLoadCluster => true,
      _ => false,
    }
  }

  pub fn should_flush(&self) -> bool {
    match self {
      RedisCommandKind::Quit
      | RedisCommandKind::Ping
      | RedisCommandKind::Auth
      | RedisCommandKind::_Hello(_)
      | RedisCommandKind::Shutdown
      | RedisCommandKind::Exec
      | RedisCommandKind::Eval
      | RedisCommandKind::EvalSha => true,
      _ => false,
    }
  }

  pub fn is_eval(&self) -> bool {
    match *self {
      RedisCommandKind::EvalSha | RedisCommandKind::Eval => true,
      _ => false,
    }
  }
}

pub struct RedisCommand {
  /// The command and optional subcommand name.
  pub kind: RedisCommandKind,
  /// The policy to apply when handling the response.
  pub response: ResponseKind,
  /// The policy to use when hashing the arguments for cluster routing.
  pub hasher: ClusterHash,
  /// The provided arguments.
  pub args: Vec<RedisValue>,
  /// A oneshot sender used to communicate with the multiplexer.
  pub multiplexer_tx: Option<MultiplexerSender>,
  /// The number of times the command was sent to the server.
  pub attempted: usize,
  /// Whether or not the command can be pipelined.
  ///
  /// Also used for commands like XREAD that block based on an argument.
  pub can_pipeline: bool,
  /// Whether or not to skip backpressure checks.
  pub skip_backpressure: bool,
  /// A timestamp of when the command was first created from the public interface.
  #[cfg(feature = "metrics")]
  pub created: Instant,
  /// A timestamp of when the command was last written to the socket.
  #[cfg(any(feature = "metrics", feature = "partial-tracing"))]
  pub network_start: Option<Instant>,
  /// Tracing state that has to carry over across writer/reader tasks to track certain fields (response size, etc).
  #[cfg(feature = "partial-tracing")]
  pub traces: CommandTraces,
}

impl fmt::Debug for RedisCommand {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("RedisCommand")
      .field("command", self.kind.to_str_debug())
      .field("attempted", &self.attempted)
      .field("can_pipeline", &self.can_pipeline)
      .field("arguments", &self.args)
      .finish()
  }
}

impl fmt::Display for RedisCommand {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{}", self.kind.to_str_debug())
  }
}

impl From<(RedisCommandKind, Vec<RedisValue>)> for RedisCommand {
  fn from((kind, args): (RedisCommandKind, Vec<RedisValue>)) -> Self {
    RedisCommand {
      kind,
      args,
      response: ResponseKind::Respond(None),
      hasher: ClusterHash::FirstKey,
      multiplexer_tx: None,
      attempted: 0,
      can_pipeline: true,
      skip_backpressure: false,
      #[cfg(feature = "metrics")]
      created: Instant::now(),
      #[cfg(any(feature = "metrics", feature = "partial-tracing"))]
      network_start: None,
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
    }
  }
}

impl From<(RedisCommandKind, Vec<RedisValue>, ResponseSender)> for RedisCommand {
  fn from((kind, args, tx): (RedisCommandKind, Vec<RedisValue>, ResponseSender)) -> Self {
    RedisCommand {
      kind,
      args,
      response: ResponseKind::Respond(Some(tx)),
      hasher: ClusterHash::FirstKey,
      multiplexer_tx: None,
      attempted: 0,
      can_pipeline: true,
      skip_backpressure: false,
      #[cfg(feature = "metrics")]
      created: Instant::now(),
      #[cfg(any(feature = "metrics", feature = "partial-tracing"))]
      network_start: None,
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
    }
  }
}

impl From<(RedisCommandKind, Vec<RedisValue>, ResponseKind)> for RedisCommand {
  fn from((kind, args, response): (RedisCommandKind, Vec<RedisValue>, ResponseKind)) -> Self {
    RedisCommand {
      kind,
      args,
      response,
      hasher: ClusterHash::FirstKey,
      multiplexer_tx: None,
      attempted: 0,
      can_pipeline: true,
      skip_backpressure: false,
      #[cfg(feature = "metrics")]
      created: Instant::now(),
      #[cfg(any(feature = "metrics", feature = "partial-tracing"))]
      network_start: None,
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
    }
  }
}

impl RedisCommand {
  /// Whether or not to pipeline the command.
  pub fn should_auto_pipeline(&self, inner: &Arc<RedisClientInner>) -> bool {
    // https://redis.io/commands/eval#evalsha-in-the-context-of-pipelining
    let force_no_pipeline = command.kind.is_eval()
      // we also disable pipelining on the HELLO command so that we don't try to decode any in-flight responses with the wrong codec logic
      || command.kind.is_hello()
      || command.kind.is_blocking();

    // TODO blocking commands do not block in a transaction

    let should_pipeline = inner.is_pipelined() && self.can_pipeline && !force_no_pipeline;
    _trace!(
      inner,
      "Pipeline check {}: {}",
      self.kind.to_debug_str(),
      should_pipeline
    );
    should_pipeline
  }

  /// Increment and check the number of write attempts.
  pub fn incr_check_attempted(&mut self, max: usize) -> Result<(), RedisError> {
    self.attempted += 1;
    if self.attempted > max {
      Err(RedisError::new(
        RedisErrorKind::Unknown,
        "Exceeded max failed write attempts.",
      ))
    } else {
      Ok(())
    }
  }

  /// Create a channel on which to block the multiplexer, returning the receiver.
  pub fn create_multiplexer_channel(&mut self) -> OneshotReceiver<()> {
    let (tx, rx) = oneshot_channel();
    self.multiplexer_tx = Some(tx);
    rx
  }

  /// Send a message to unblock the multiplexer loop, if necessary.
  pub fn unblock_multiplexer(&mut self, name: &str) {
    if let Some(tx) = self.multiplexer_tx.take() {
      if tx.send(()).is_err() {
        warn!("{}: Failed to unblock multiplexer loop.", name);
      }
    }
  }

  /// Clone the command, supporting commands with shared response state.
  pub fn duplicate(&self, response: ResponseKind) -> Self {
    RedisCommand {
      kind: self.kind.clone(),
      args: self.args.clone(),
      hasher: self.hasher.clone(),
      attempted: self.attempted,
      can_pipeline: self.can_pipeline,
      skip_backpressure: self.skip_backpressure,
      multiplexer_tx: None,
      response,
      #[cfg(feature = "metrics")]
      created: self.created.clone(),
      #[cfg(any(feature = "metrics", feature = "partial-tracing"))]
      network_start: self.network_start.clone(),
      #[cfg(feature = "partial-tracing")]
      traces: CommandTraces::default(),
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

  /// Read the first key in the arguments according to the `FirstKey` cluster hash policy.
  pub fn first_key(&self) -> Option<&[u8]> {
    ClusterHash::FirstKey.find_key(&self.args)
  }

  /// Hash the arguments according to the command's cluster hash policy.
  pub fn cluster_hash(&self) -> Option<u16> {
    self.hasher.hash(&self.args)
  }

  /// Return the custom hash slot for custom commands.
  pub fn custom_hash_slot(&self) -> Option<u16> {
    self.kind.custom_key_slot()
  }

  /// Convert to a single frame with an array of bulk strings (or null).
  #[cfg(not(feature = "blocking-encoding"))]
  pub fn to_frame(&self, is_resp3: bool) -> Result<ProtocolFrame, RedisError> {
    protocol_utils::command_to_frame(self, is_resp3)
  }

  /// Convert to a single frame with an array of bulk strings (or null), using a blocking task.
  #[cfg(feature = "blocking-encoding")]
  pub fn to_frame(&self, is_resp3: bool) -> Result<ProtocolFrame, RedisError> {
    let cmd_size = protocol_utils::args_size(&self.args);

    if cmd_size >= globals().blocking_encode_threshold() {
      trace!("Using blocking task to convert command to frame with size {}", cmd_size);
      tokio::task::block_in_place(|| protocol_utils::command_to_frame(self, is_resp3))
    } else {
      protocol_utils::command_to_frame(self, is_resp3)
    }
  }
}

/// A message or pipeline queued in memory before being sent to the server.
#[derive(Debug)]
pub enum QueuedCommand {
  Command(RedisCommand),
  Pipeline(Vec<RedisCommand>),
}
