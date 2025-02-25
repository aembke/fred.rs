//! An interface for mocking commands.

use crate::{error::Error, types::values::Value};
use bytes_utils::Str;
use fred_macros::rm_send_if;
use std::fmt::Debug;

/// A wrapper type for the parts of an internal command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MockCommand {
  /// The first word in the command string. For example:
  /// * `SET` - `"SET"`
  /// * `XGROUP CREATE` - `"XGROUP"`
  /// * `INCRBY` - `"INCRBY"`
  pub cmd:        Str,
  /// The optional subcommand string (or second word) in the command string. For example:
  /// * `SET` - `None`
  /// * `XGROUP CREATE` - `Some("CREATE")`
  /// * `INCRBY` - `None`
  pub subcommand: Option<Str>,
  /// The ordered list of arguments to the command.
  pub args:       Vec<Value>,
}

/// An interface for intercepting and processing commands in a mocking layer.
#[allow(unused_variables)]
#[rm_send_if(feature = "glommio")]
pub trait Mocks: Debug + Send + Sync + 'static {
  /// Intercept and process a command, returning any `Value`.
  ///
  /// # Important
  ///
  /// The caller must ensure the response value makes sense in the context of the specific command(s) being mocked.
  /// The parsing logic following each command on the public interface will still be applied. __Most__ commands
  /// perform minimal parsing on the response, but some may require specific response formats to function correctly.
  ///
  /// `Value::Queued` can be used to return a value that will work almost anywhere.
  fn process_command(&self, command: MockCommand) -> Result<Value, Error>;

  /// Intercept and process an entire transaction. The provided commands will **not** include `EXEC`.
  ///
  /// Note: The default implementation redirects each command to the [process_command](Self::process_command)
  /// function. The results of each call are buffered and returned as an array.
  fn process_transaction(&self, commands: Vec<MockCommand>) -> Result<Value, Error> {
    let mut out = Vec::with_capacity(commands.len());

    for command in commands.into_iter() {
      out.push(self.process_command(command)?);
    }
    Ok(Value::Array(out))
  }
}
