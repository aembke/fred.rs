use crate::types::RedisValue;

/// ACL rules describing the keys a user can access.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AclKeyPattern {
  AllKeys,
  Custom(String),
}

impl AclKeyPattern {
  pub(crate) fn to_value(&self) -> RedisValue {
    match *self {
      AclKeyPattern::AllKeys => RedisValue::from_static_str("allkeys"),
      AclKeyPattern::Custom(ref pat) => format!("~{}", pat).into(),
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
  pub(crate) fn to_value(&self) -> RedisValue {
    match *self {
      AclChannelPattern::AllChannels => RedisValue::from_static_str("allchannels"),
      AclChannelPattern::Custom(ref pat) => format!("&{}", pat).into(),
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
  pub(crate) fn to_value(&self, prefix: &'static str) -> RedisValue {
    match *self {
      AclCommandPattern::AllCommands => RedisValue::from_static_str("allcommands"),
      AclCommandPattern::NoCommands => RedisValue::from_static_str("nocommands"),
      AclCommandPattern::Custom {
        ref command,
        ref subcommand,
      } => {
        if let Some(subcommand) = subcommand {
          format!("{}{}|{}", prefix, command, subcommand).into()
        } else {
          format!("{}{}", prefix, command).into()
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
  pub(crate) fn to_value(&self) -> RedisValue {
    match self {
      AclRule::On => RedisValue::from_static_str("on"),
      AclRule::Off => RedisValue::from_static_str("off"),
      AclRule::Reset => RedisValue::from_static_str("reset"),
      AclRule::ResetChannels => RedisValue::from_static_str("resetchannels"),
      AclRule::ResetKeys => RedisValue::from_static_str("resetkeys"),
      AclRule::NoPass => RedisValue::from_static_str("nopass"),
      AclRule::AddPassword(ref pass) => format!(">{}", pass).into(),
      AclRule::RemovePassword(ref pass) => format!("<{}", pass).into(),
      AclRule::AddHashedPassword(ref pass) => format!("#{}", pass).into(),
      AclRule::RemoveHashedPassword(ref pass) => format!("!{}", pass).into(),
      AclRule::AddCategory(ref cat) => format!("+@{}", cat).into(),
      AclRule::RemoveCategory(ref cat) => format!("-@{}", cat).into(),
      AclRule::AddKeys(ref pat) => pat.to_value(),
      AclRule::AddChannels(ref pat) => pat.to_value(),
      AclRule::AddCommands(ref pat) => pat.to_value("+"),
      AclRule::RemoveCommands(ref pat) => pat.to_value("-"),
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
