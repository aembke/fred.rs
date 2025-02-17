use crate::{error::Error, runtime::JoinHandle, utils};
use bytes_utils::Str;
pub use redis_protocol::resp3::types::{BytesFrame as Resp3Frame, RespVersion};
use std::{
  cmp::Ordering,
  fmt,
  fmt::{Display, Formatter},
  hash::{Hash, Hasher},
  net::IpAddr,
  str::FromStr,
};

pub(crate) static QUEUED: &str = "QUEUED";
/// The ANY flag used on certain GEO commands.
pub type Any = bool;
/// The result from any of the `connect` functions showing the error that closed the connection, if any.
pub type ConnectHandle = JoinHandle<Result<(), Error>>;
/// A tuple of `(offset, count)` values for commands that allow paging through results.
pub type Limit = (i64, i64);
/// An argument type equivalent to "[LIMIT count]".
pub type LimitCount = Option<i64>;

/// Types used to configure clients or commands.
pub mod config;
pub mod events;
pub(crate) mod from_tuple;
pub mod hashers;
pub mod metrics;
pub mod mocks;
#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))))]
pub mod tls;
pub mod values;
use crate::{error::ErrorKind, types::tls::TlsHostMapping};
pub use semver::Version;

/// The state of the underlying connection to the Redis server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientState {
  Disconnected,
  Disconnecting,
  Connected,
  Connecting,
}

impl ClientState {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      ClientState::Connecting => "Connecting",
      ClientState::Connected => "Connected",
      ClientState::Disconnecting => "Disconnecting",
      ClientState::Disconnected => "Disconnected",
    })
  }
}

impl fmt::Display for ClientState {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}", self.to_str())
  }
}

/// State necessary to identify or connect to a server.
#[derive(Debug, Clone)]
pub struct Server {
  /// The hostname or IP address for the server.
  pub host:            Str,
  /// The port for the server.
  pub port:            u16,
  /// The server name used during the TLS handshake.
  #[cfg(any(
    feature = "enable-rustls",
    feature = "enable-native-tls",
    feature = "enable-rustls-ring"
  ))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "enable-rustls",
      feature = "enable-native-tls",
      feature = "enable-rustls-ring"
    )))
  )]
  pub tls_server_name: Option<Str>,
}

impl Server {
  /// Create a new `Server` from parts with a TLS server name.
  #[cfg(any(
    feature = "enable-rustls",
    feature = "enable-native-tls",
    feature = "enable-rustls-ring"
  ))]
  #[cfg_attr(
    docsrs,
    doc(cfg(any(
      feature = "enable-rustls",
      feature = "enable-native-tls",
      feature = "enable-rustls-ring"
    )))
  )]
  pub fn new_with_tls<S: Into<Str>>(host: S, port: u16, tls_server_name: Option<String>) -> Self {
    Server {
      host: host.into(),
      port,
      tls_server_name: tls_server_name.map(|s| s.into()),
    }
  }

  /// Create a new `Server` from parts.
  pub fn new<S: Into<Str>>(host: S, port: u16) -> Self {
    Server {
      host: host.into(),
      port,
      #[cfg(any(
        feature = "enable-rustls",
        feature = "enable-native-tls",
        feature = "enable-rustls-ring"
      ))]
      tls_server_name: None,
    }
  }

  #[cfg(any(
    feature = "enable-rustls",
    feature = "enable-native-tls",
    feature = "enable-rustls-ring"
  ))]
  pub(crate) fn set_tls_server_name(&mut self, policy: &TlsHostMapping, default_host: &str) {
    if *policy == TlsHostMapping::None {
      return;
    }

    let ip = match IpAddr::from_str(&self.host) {
      Ok(ip) => ip,
      Err(_) => return,
    };
    if let Some(tls_server_name) = policy.map(&ip, default_host) {
      self.tls_server_name = Some(Str::from(tls_server_name));
    }
  }

  /// Attempt to parse a `host:port` string.
  pub(crate) fn from_str(s: &str) -> Option<Server> {
    let parts: Vec<&str> = s.trim().split(':').collect();
    if parts.len() == 2 {
      if let Ok(port) = parts[1].parse::<u16>() {
        Some(Server {
          host: parts[0].into(),
          port,
          #[cfg(any(
            feature = "enable-rustls",
            feature = "enable-native-tls",
            feature = "enable-rustls-ring"
          ))]
          tls_server_name: None,
        })
      } else {
        None
      }
    } else {
      None
    }
  }

  /// Create a new server struct from a `host:port` string and the default host that sent the last command.
  pub(crate) fn from_parts(server: &str, default_host: &str) -> Option<Server> {
    utils::server_to_parts(server).ok().map(|(host, port)| {
      let host = if host.is_empty() {
        Str::from(default_host)
      } else {
        Str::from(host)
      };

      Server {
        host,
        port,
        #[cfg(any(
          feature = "enable-rustls",
          feature = "enable-native-tls",
          feature = "enable-rustls-ring"
        ))]
        tls_server_name: None,
      }
    })
  }
}

#[cfg(feature = "unix-sockets")]
impl From<&std::path::Path> for Server {
  fn from(value: &std::path::Path) -> Self {
    Server {
      host:            utils::path_to_string(value).into(),
      port:            0,
      #[cfg(any(
        feature = "enable-rustls",
        feature = "enable-native-tls",
        feature = "enable-rustls-ring"
      ))]
      tls_server_name: None,
    }
  }
}

impl TryFrom<String> for Server {
  type Error = Error;

  fn try_from(value: String) -> Result<Self, Self::Error> {
    Server::from_str(&value).ok_or(Error::new(ErrorKind::Config, "Invalid `host:port` server."))
  }
}

impl TryFrom<&str> for Server {
  type Error = Error;

  fn try_from(value: &str) -> Result<Self, Self::Error> {
    Server::from_str(value).ok_or(Error::new(ErrorKind::Config, "Invalid `host:port` server."))
  }
}

impl From<(String, u16)> for Server {
  fn from((host, port): (String, u16)) -> Self {
    Server {
      host: host.into(),
      port,
      #[cfg(any(
        feature = "enable-native-tls",
        feature = "enable-rustls",
        feature = "enable-rustls-ring"
      ))]
      tls_server_name: None,
    }
  }
}

impl From<(&str, u16)> for Server {
  fn from((host, port): (&str, u16)) -> Self {
    Server {
      host: host.into(),
      port,
      #[cfg(any(
        feature = "enable-native-tls",
        feature = "enable-rustls",
        feature = "enable-rustls-ring"
      ))]
      tls_server_name: None,
    }
  }
}

impl From<&Server> for Server {
  fn from(value: &Server) -> Self {
    value.clone()
  }
}

impl Display for Server {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(f, "{}:{}", self.host, self.port)
  }
}

impl PartialEq for Server {
  fn eq(&self, other: &Self) -> bool {
    self.host == other.host && self.port == other.port
  }
}

impl Eq for Server {}

impl Hash for Server {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.host.hash(state);
    self.port.hash(state);
  }
}

impl PartialOrd for Server {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for Server {
  fn cmp(&self, other: &Self) -> Ordering {
    let host_ord = self.host.cmp(&other.host);
    if host_ord == Ordering::Equal {
      self.port.cmp(&other.port)
    } else {
      host_ord
    }
  }
}
