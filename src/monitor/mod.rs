use crate::error::RedisError;
use crate::protocol::tls::TlsConfig;
use crate::types::RedisValue;
use crate::utils as client_utils;
use futures::Stream;
use std::fmt;

mod parser;
mod utils;

/// A command parsed from a [MONITOR](https://redis.io/commands/monitor) stream.
///
/// Formatting with the [Display](https://doc.rust-lang.org/std/fmt/trait.Display.html) trait will print the same output as `redis-cli`.
#[derive(Clone, Debug)]
pub struct Command {
  /// The command run by the server.
  pub command: String,
  /// Arguments passed to the command.
  pub args: Vec<RedisValue>,
  /// When the command was run on the server.
  pub timestamp: f64,
  /// The database against which the command was run.
  pub db: u8,
  /// The host and port of the client that ran the command, or `lua` when run from a script.
  pub client: String,
}

impl PartialEq for Command {
  fn eq(&self, other: &Self) -> bool {
    client_utils::f64_eq(self.timestamp, other.timestamp)
      && self.client == other.client
      && self.db == other.db
      && self.command == other.command
      && self.args == other.args
  }
}

impl Eq for Command {}

impl fmt::Display for Command {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "{:.6} [{} {}] \"{}\"",
      self.timestamp, self.db, self.client, self.command
    )?;

    for arg in self.args.iter() {
      write!(f, " \"{}\"", arg.as_str().unwrap_or("unknown".into()))?;
    }

    Ok(())
  }
}

/// Configuration options for the `MONITOR` command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
  pub host: String,
  pub port: u16,
  pub username: Option<String>,
  pub password: Option<String>,
  #[cfg(feature = "enable-native-tls")]
  pub tls: Option<TlsConfig>,
}

impl Default for Config {
  #[cfg(feature = "enable-native-tls")]
  fn default() -> Self {
    Config {
      host: "127.0.0.1".into(),
      port: 6379,
      username: None,
      password: None,
      tls: None,
    }
  }

  #[cfg(not(feature = "enable-native-tls"))]
  fn default() -> Self {
    Config {
      host: "127.0.0.1".into(),
      port: 6379,
      username: None,
      password: None,
    }
  }
}

/// Run the [MONITOR](https://redis.io/commands/monitor) command against the provided server.
pub async fn run(config: Config) -> Result<impl Stream<Item = Command>, RedisError> {
  utils::start(config).await
}
