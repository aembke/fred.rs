use crate::error::RedisError;
use crate::protocol::tls::TlsConfig;
use crate::types::RedisValue;
use futures::Stream;
use std::fmt;

mod parser;
mod utils;

/// A command parsed from a [MONITOR](https://redis.io/commands/monitor) stream.
///
/// Formatting with the [Display](https://doc.rust-lang.org/std/fmt/trait.Display.html) trait will print the same output as `redis-cli`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Command {
  /// The command run by the server.
  pub command: String,
  /// Arguments passed to the command.
  pub args: Vec<RedisValue>,
  /// When the command was run on the server.
  pub timestamp: String,
  /// The database against which the command was run.
  pub db: u8,
  /// The host and port of the server that ran the command.
  pub server: String,
}

impl fmt::Display for Command {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "{} [{} {}] \"{}\"",
      self.timestamp, self.db, self.server, self.command
    )?;

    for arg in self.args.iter() {
      write!(f, " \"{}\"", arg.as_str().unwrap_or("unknown".into()))?;
    }

    write!(f, "\n")
  }
}

/// Configuration options for the `MONITOR` command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
  pub host: String,
  pub port: u16,
  pub username: Option<String>,
  pub password: Option<String>,
  #[cfg(feature = "enable-tls")]
  pub tls: Option<TlsConfig>,
}

impl Default for Config {
  #[cfg(feature = "enable-tls")]
  fn default() -> Self {
    Config {
      host: "127.0.0.1".into(),
      port: 6379,
      username: None,
      password: None,
      tls: None,
    }
  }

  #[cfg(not(feature = "enable-tls"))]
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
