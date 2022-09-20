use crate::utils;
use bytes_utils::Str;

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
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      ClientKillType::Normal => "normal",
      ClientKillType::Master => "master",
      ClientKillType::Replica => "replica",
      ClientKillType::Pubsub => "pubsub",
    })
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
  pub(crate) fn to_str(&self) -> (Str, Str) {
    let (prefix, value) = match *self {
      ClientKillFilter::ID(ref id) => ("ID", id.into()),
      ClientKillFilter::Type(ref kind) => ("TYPE", kind.to_str()),
      ClientKillFilter::User(ref user) => ("USER", user.into()),
      ClientKillFilter::Addr(ref addr) => ("ADDR", addr.into()),
      ClientKillFilter::LAddr(ref addr) => ("LADDR", addr.into()),
      ClientKillFilter::SkipMe(ref b) => ("SKIPME", match *b {
        true => utils::static_str("yes"),
        false => utils::static_str("no"),
      }),
    };

    (utils::static_str(prefix), value)
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
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      ClientPauseKind::Write => "WRITE",
      ClientPauseKind::All => "ALL",
    })
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
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      ClientReplyFlag::On => "ON",
      ClientReplyFlag::Off => "OFF",
      ClientReplyFlag::Skip => "SKIP",
    })
  }
}

/// Arguments to the CLIENT UNBLOCK command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientUnblockFlag {
  Timeout,
  Error,
}

impl ClientUnblockFlag {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      ClientUnblockFlag::Timeout => "TIMEOUT",
      ClientUnblockFlag::Error => "ERROR",
    })
  }
}
