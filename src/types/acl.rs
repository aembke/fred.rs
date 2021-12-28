/// ACL rules describing the keys a user can access.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AclKeyPattern {
  AllKeys,
  Custom(String),
}

impl AclKeyPattern {
  pub(crate) fn to_string(&self) -> String {
    match *self {
      AclKeyPattern::AllKeys => "allkeys".into(),
      AclKeyPattern::Custom(ref pat) => format!("~{}", pat),
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
  pub(crate) fn to_string(&self) -> String {
    match *self {
      AclChannelPattern::AllChannels => "allchannels".into(),
      AclChannelPattern::Custom(ref pat) => format!("&{}", pat),
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
  pub(crate) fn to_string(&self, prefix: &'static str) -> String {
    match *self {
      AclCommandPattern::AllCommands => "allcommands".into(),
      AclCommandPattern::NoCommands => "nocommands".into(),
      AclCommandPattern::Custom {
        ref command,
        ref subcommand,
      } => {
        if let Some(subcommand) = subcommand {
          format!("{}{}|{}", prefix, command, subcommand)
        } else {
          format!("{}{}", prefix, command)
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
  pub(crate) fn to_string(&self) -> String {
    match self {
      AclRule::On => "on".into(),
      AclRule::Off => "off".into(),
      AclRule::Reset => "reset".into(),
      AclRule::ResetChannels => "resetchannels".into(),
      AclRule::ResetKeys => "resetkeys".into(),
      AclRule::NoPass => "nopass".into(),
      AclRule::AddPassword(ref pass) => format!(">{}", pass),
      AclRule::RemovePassword(ref pass) => format!("<{}", pass),
      AclRule::AddHashedPassword(ref pass) => format!("#{}", pass),
      AclRule::RemoveHashedPassword(ref pass) => format!("!{}", pass),
      AclRule::AddCategory(ref cat) => format!("+@{}", cat),
      AclRule::RemoveCategory(ref cat) => format!("-@{}", cat),
      AclRule::AddKeys(ref pat) => pat.to_string(),
      AclRule::AddChannels(ref pat) => pat.to_string(),
      AclRule::AddCommands(ref pat) => pat.to_string("+"),
      AclRule::RemoveCommands(ref pat) => pat.to_string("-"),
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
