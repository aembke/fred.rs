/// The direction to move elements in a *LMOVE command.
///
/// <https://redis.io/commands/blmove>
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LMoveDirection {
  Left,
  Right,
}

impl LMoveDirection {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      LMoveDirection::Left => "LEFT",
      LMoveDirection::Right => "RIGHT",
    }
  }
}

/// Location flag for the `LINSERT` command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListLocation {
  Before,
  After,
}

impl ListLocation {
  pub(crate) fn to_str(&self) -> &'static str {
    match *self {
      ListLocation::Before => "BEFORE",
      ListLocation::After => "AFTER",
    }
  }
}
