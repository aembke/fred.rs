use bytes_utils::string::Utf8Error as BytesUtf8Error;
use futures::channel::oneshot::Canceled;
use redis_protocol::{error::RedisProtocolError, resp2::types::BytesFrame as Resp2Frame};
use semver::Error as SemverError;
use std::{
  borrow::{Borrow, Cow},
  convert::Infallible,
  error::Error,
  fmt,
  io::Error as IoError,
  num::{ParseFloatError, ParseIntError},
  str,
  str::Utf8Error,
  string::FromUtf8Error,
};
use tokio::task::JoinError;
use url::ParseError;

#[cfg(feature = "dns")]
use trust_dns_resolver::error::ResolveError;

/// An enum representing the type of error from Redis.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RedisErrorKind {
  /// A fatal client configuration error. These errors will shutdown a client and break out of any reconnection
  /// attempts.
  Config,
  /// An authentication error.
  Auth,
  /// An IO error with the underlying connection.
  IO,
  /// An invalid command, such as trying to perform a `set` command on a client after calling `subscribe`.
  InvalidCommand,
  /// An invalid argument or set of arguments to a command.
  InvalidArgument,
  /// An invalid URL error.
  Url,
  /// A protocol error such as an invalid or unexpected frame from the server.
  Protocol,
  /// A TLS error.
  #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
  #[cfg_attr(docsrs, doc(cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))))]
  Tls,
  /// An error indicating the request was canceled.
  Canceled,
  /// An unknown error.
  Unknown,
  /// A timeout error.
  Timeout,
  /// An error used to indicate that the cluster's state has changed. These errors will show up on the `on_error`
  /// error stream even though the client will automatically attempt to recover.
  Cluster,
  /// A parser error.
  Parse,
  /// An error communicating with redis sentinel.
  Sentinel,
  /// An error indicating a value was not found, often used when trying to cast a `nil` response from the server to a
  /// non-nullable type.
  NotFound,
  /// An error indicating that the caller should apply backpressure and retry the command.
  Backpressure,
  /// An error associated with a replica node.
  #[cfg(feature = "replicas")]
  #[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
  Replica,
}

impl RedisErrorKind {
  pub fn to_str(&self) -> &'static str {
    match *self {
      RedisErrorKind::Auth => "Authentication Error",
      RedisErrorKind::IO => "IO Error",
      RedisErrorKind::InvalidArgument => "Invalid Argument",
      RedisErrorKind::InvalidCommand => "Invalid Command",
      RedisErrorKind::Url => "Url Error",
      RedisErrorKind::Protocol => "Protocol Error",
      RedisErrorKind::Unknown => "Unknown Error",
      RedisErrorKind::Canceled => "Canceled",
      RedisErrorKind::Cluster => "Cluster Error",
      RedisErrorKind::Timeout => "Timeout Error",
      #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
      RedisErrorKind::Tls => "TLS Error",
      RedisErrorKind::Config => "Config Error",
      RedisErrorKind::Parse => "Parse Error",
      RedisErrorKind::Sentinel => "Sentinel Error",
      RedisErrorKind::NotFound => "Not Found",
      RedisErrorKind::Backpressure => "Backpressure",
      #[cfg(feature = "replicas")]
      RedisErrorKind::Replica => "Replica",
    }
  }
}

/// An error from Redis.
pub struct RedisError {
  /// Details about the specific error condition.
  details: Cow<'static, str>,
  /// The kind of error.
  kind: RedisErrorKind,
}

impl Clone for RedisError {
  fn clone(&self) -> Self {
    RedisError::new(self.kind.clone(), self.details.clone())
  }
}

impl PartialEq for RedisError {
  fn eq(&self, other: &Self) -> bool {
    self.kind == other.kind && self.details == other.details
  }
}

impl Eq for RedisError {}

impl fmt::Debug for RedisError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "Redis Error - kind: {:?}, details: {}", self.kind, self.details)
  }
}

impl fmt::Display for RedisError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}: {}", self.kind.to_str(), self.details)
  }
}

#[doc(hidden)]
impl From<RedisProtocolError> for RedisError {
  fn from(e: RedisProtocolError) -> Self {
    RedisError::new(RedisErrorKind::Protocol, format!("{}", e))
  }
}

#[doc(hidden)]
impl From<()> for RedisError {
  fn from(_: ()) -> Self {
    RedisError::new(RedisErrorKind::Canceled, "Empty error.")
  }
}

#[doc(hidden)]
impl From<futures::channel::mpsc::SendError> for RedisError {
  fn from(e: futures::channel::mpsc::SendError) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("{}", e))
  }
}

#[doc(hidden)]
impl From<tokio::sync::oneshot::error::RecvError> for RedisError {
  fn from(e: tokio::sync::oneshot::error::RecvError) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("{}", e))
  }
}

#[doc(hidden)]
impl From<tokio::sync::broadcast::error::RecvError> for RedisError {
  fn from(e: tokio::sync::broadcast::error::RecvError) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("{}", e))
  }
}

#[doc(hidden)]
impl<T: fmt::Display> From<tokio::sync::broadcast::error::SendError<T>> for RedisError {
  fn from(e: tokio::sync::broadcast::error::SendError<T>) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("{}", e))
  }
}

#[doc(hidden)]
impl From<IoError> for RedisError {
  fn from(e: IoError) -> Self {
    RedisError::new(RedisErrorKind::IO, format!("{:?}", e))
  }
}

#[doc(hidden)]
impl From<ParseError> for RedisError {
  fn from(e: ParseError) -> Self {
    RedisError::new(RedisErrorKind::Url, format!("{:?}", e))
  }
}

#[doc(hidden)]
impl From<ParseFloatError> for RedisError {
  fn from(_: ParseFloatError) -> Self {
    RedisError::new(RedisErrorKind::Parse, "Invalid floating point number.")
  }
}

#[doc(hidden)]
impl From<ParseIntError> for RedisError {
  fn from(_: ParseIntError) -> Self {
    RedisError::new(RedisErrorKind::Parse, "Invalid integer string.")
  }
}

#[doc(hidden)]
impl From<FromUtf8Error> for RedisError {
  fn from(_: FromUtf8Error) -> Self {
    RedisError::new(RedisErrorKind::Parse, "Invalid UTF-8 string.")
  }
}

#[doc(hidden)]
impl From<Utf8Error> for RedisError {
  fn from(_: Utf8Error) -> Self {
    RedisError::new(RedisErrorKind::Parse, "Invalid UTF-8 string.")
  }
}

#[doc(hidden)]
impl<S> From<BytesUtf8Error<S>> for RedisError {
  fn from(e: BytesUtf8Error<S>) -> Self {
    e.utf8_error().into()
  }
}

#[doc(hidden)]
impl From<fmt::Error> for RedisError {
  fn from(e: fmt::Error) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("{:?}", e))
  }
}

#[doc(hidden)]
impl From<Canceled> for RedisError {
  fn from(e: Canceled) -> Self {
    RedisError::new(RedisErrorKind::Canceled, format!("{}", e))
  }
}

#[doc(hidden)]
impl From<JoinError> for RedisError {
  fn from(e: JoinError) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("Spawn Error: {:?}", e))
  }
}

#[doc(hidden)]
impl From<SemverError> for RedisError {
  fn from(e: SemverError) -> Self {
    RedisError::new(RedisErrorKind::Protocol, format!("Invalid Redis version: {:?}", e))
  }
}

#[doc(hidden)]
impl From<Infallible> for RedisError {
  fn from(e: Infallible) -> Self {
    warn!("Infallible error: {:?}", e);
    RedisError::new(RedisErrorKind::Unknown, "Unknown error.")
  }
}

#[doc(hidden)]
impl From<Resp2Frame> for RedisError {
  fn from(e: Resp2Frame) -> Self {
    match e {
      Resp2Frame::SimpleString(s) => match str::from_utf8(&s).ok() {
        Some("Canceled") => RedisError::new_canceled(),
        _ => RedisError::new(RedisErrorKind::Unknown, "Unknown frame error."),
      },
      _ => RedisError::new(RedisErrorKind::Unknown, "Unknown frame error."),
    }
  }
}

#[doc(hidden)]
#[cfg(feature = "enable-native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
impl From<native_tls::Error> for RedisError {
  fn from(e: native_tls::Error) -> Self {
    RedisError::new(RedisErrorKind::Tls, format!("{:?}", e))
  }
}

#[doc(hidden)]
#[cfg(feature = "enable-rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
impl From<rustls::pki_types::InvalidDnsNameError> for RedisError {
  fn from(e: rustls::pki_types::InvalidDnsNameError) -> Self {
    RedisError::new(RedisErrorKind::Tls, format!("{:?}", e))
  }
}

#[doc(hidden)]
#[cfg(feature = "enable-rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
impl From<rustls::Error> for RedisError {
  fn from(e: rustls::Error) -> Self {
    RedisError::new(RedisErrorKind::Tls, format!("{:?}", e))
  }
}

#[doc(hidden)]
#[cfg(feature = "dns")]
#[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
impl From<ResolveError> for RedisError {
  fn from(e: ResolveError) -> Self {
    RedisError::new(RedisErrorKind::IO, format!("{:?}", e))
  }
}

#[cfg(feature = "serde-json")]
#[cfg_attr(docsrs, doc(cfg(feature = "serde-json")))]
impl From<serde_json::Error> for RedisError {
  fn from(e: serde_json::Error) -> Self {
    RedisError::new(RedisErrorKind::Parse, format!("{:?}", e))
  }
}

impl RedisError {
  /// Create a new Redis error with the provided details.
  pub fn new<T>(kind: RedisErrorKind, details: T) -> RedisError
  where
    T: Into<Cow<'static, str>>,
  {
    RedisError {
      kind,
      details: details.into(),
    }
  }

  /// Read the type of error without any associated data.
  pub fn kind(&self) -> &RedisErrorKind {
    &self.kind
  }

  /// Change the kind of the error.
  pub fn change_kind(&mut self, kind: RedisErrorKind) {
    self.kind = kind;
  }

  /// Read details about the error.
  pub fn details(&self) -> &str {
    self.details.borrow()
  }

  /// Create a new empty Canceled error.
  pub fn new_canceled() -> Self {
    RedisError::new(RedisErrorKind::Canceled, "Canceled.")
  }

  /// Create a new parse error with the provided details.
  pub(crate) fn new_parse<T>(details: T) -> Self
  where
    T: Into<Cow<'static, str>>,
  {
    RedisError::new(RedisErrorKind::Parse, details)
  }

  /// Create a new default backpressure error.
  pub(crate) fn new_backpressure() -> Self {
    RedisError::new(RedisErrorKind::Backpressure, "Max in-flight commands reached.")
  }

  /// Whether reconnection logic should be skipped in all cases.
  pub(crate) fn should_not_reconnect(&self) -> bool {
    matches!(self.kind, RedisErrorKind::Config | RedisErrorKind::Url)
  }

  /// Whether the error is a `Cluster` error.
  pub fn is_cluster(&self) -> bool {
    matches!(self.kind, RedisErrorKind::Cluster)
  }

  /// Whether the error is a `Canceled` error.
  pub fn is_canceled(&self) -> bool {
    matches!(self.kind, RedisErrorKind::Canceled)
  }

  /// Whether the error is a `Replica` error.
  #[cfg(feature = "replicas")]
  #[cfg_attr(docsrs, doc(cfg(feature = "replicas")))]
  pub fn is_replica(&self) -> bool {
    matches!(self.kind, RedisErrorKind::Replica)
  }

  /// Whether the error is a `NotFound` error.
  pub fn is_not_found(&self) -> bool {
    matches!(self.kind, RedisErrorKind::NotFound)
  }
}

impl Error for RedisError {
  fn source(&self) -> Option<&(dyn Error + 'static)> {
    None
  }
}
