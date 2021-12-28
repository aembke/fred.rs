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
