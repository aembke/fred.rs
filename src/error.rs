use crate::protocol::types::RedisCommand;
use bytes_utils::string::Utf8Error as BytesUtf8Error;
use futures::channel::oneshot::Canceled;
use redis_protocol::resp2::types::Frame as Resp2Frame;
use redis_protocol::types::RedisProtocolError;
use semver::Error as SemverError;
use std::borrow::{Borrow, Cow};
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::io::Error as IoError;
use std::num::ParseFloatError;
use std::num::ParseIntError;
use std::str;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use tokio::task::JoinError;
use url::ParseError;

/// An enum representing the type of error from Redis.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RedisErrorKind {
  /// A fatal client configuration error. These errors will shutdown a client and break out of any reconnection attempts.
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
  UrlError,
  /// A protocol error such as an invalid or unexpected frame from the server.
  ProtocolError,
  /// A TLS error. The `enable-native-tls` feature must be enabled for this to be used.
  Tls,
  /// An error indicating the request was canceled.
  Canceled,
  /// An unknown error.
  Unknown,
  /// A timeout error.
  Timeout,
  /// An error used to indicate that the cluster's state has changed. These errors will show up on the `on_error` error stream even though the client will automatically attempt to recover.
  Cluster,
  /// A parser error.
  Parse,
  /// An error communicating with redis sentinel.
  Sentinel,
  /// An error indicating a value was not found, often used when trying to cast a `nil` response from the server to a non-nullable type.
  NotFound,
  /// An error indicating that the caller should apply backpressure and retry the command.
  Backpressure,
}

impl RedisErrorKind {
  pub fn to_str(&self) -> &'static str {
    match *self {
      RedisErrorKind::Auth => "Authentication Error",
      RedisErrorKind::IO => "IO Error",
      RedisErrorKind::InvalidArgument => "Invalid Argument",
      RedisErrorKind::InvalidCommand => "Invalid Command",
      RedisErrorKind::UrlError => "Url Error",
      RedisErrorKind::ProtocolError => "Protocol Error",
      RedisErrorKind::Unknown => "Unknown Error",
      RedisErrorKind::Canceled => "Canceled",
      RedisErrorKind::Cluster => "Cluster Error",
      RedisErrorKind::Timeout => "Timeout Error",
      RedisErrorKind::Tls => "TLS Error",
      RedisErrorKind::Config => "Config Error",
      RedisErrorKind::Parse => "Parse Error",
      RedisErrorKind::Sentinel => "Sentinel Error",
      RedisErrorKind::NotFound => "Not Found",
      RedisErrorKind::Backpressure => "Backpressure",
    }
  }
}

/// An error from Redis.
pub struct RedisError {
  /// Details about the specific error condition.
  details: Cow<'static, str>,
  /// The kind of error.
  kind: RedisErrorKind,
  /// Command context for the error.
  context: Option<RedisCommand>,
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

impl From<RedisProtocolError> for RedisError {
  fn from(e: RedisProtocolError) -> Self {
    RedisError::new(RedisErrorKind::ProtocolError, format!("{}", e))
  }
}

impl From<()> for RedisError {
  fn from(_: ()) -> Self {
    RedisError::new(RedisErrorKind::Canceled, "Empty error.")
  }
}

impl From<futures::channel::mpsc::SendError> for RedisError {
  fn from(e: futures::channel::mpsc::SendError) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("{}", e))
  }
}

impl From<tokio::sync::oneshot::error::RecvError> for RedisError {
  fn from(e: tokio::sync::oneshot::error::RecvError) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("{}", e))
  }
}

impl From<tokio::sync::broadcast::error::RecvError> for RedisError {
  fn from(e: tokio::sync::broadcast::error::RecvError) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("{}", e))
  }
}

impl<T: Display> From<tokio::sync::broadcast::error::SendError<T>> for RedisError {
  fn from(e: tokio::sync::broadcast::error::SendError<T>) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("{}", e))
  }
}

impl From<IoError> for RedisError {
  fn from(e: IoError) -> Self {
    RedisError::new(RedisErrorKind::IO, format!("{:?}", e))
  }
}

impl From<ParseError> for RedisError {
  fn from(e: ParseError) -> Self {
    RedisError::new(RedisErrorKind::UrlError, format!("{:?}", e))
  }
}

impl From<ParseFloatError> for RedisError {
  fn from(_: ParseFloatError) -> Self {
    RedisError::new(RedisErrorKind::Parse, "Invalid floating point number.")
  }
}

impl From<ParseIntError> for RedisError {
  fn from(_: ParseIntError) -> Self {
    RedisError::new(RedisErrorKind::Parse, "Invalid integer string.")
  }
}

impl From<FromUtf8Error> for RedisError {
  fn from(_: FromUtf8Error) -> Self {
    RedisError::new(RedisErrorKind::Parse, "Invalid UTF-8 string.")
  }
}

impl From<Utf8Error> for RedisError {
  fn from(_: Utf8Error) -> Self {
    RedisError::new(RedisErrorKind::Parse, "Invalid UTF-8 string.")
  }
}

impl<S> From<BytesUtf8Error<S>> for RedisError {
  fn from(e: BytesUtf8Error<S>) -> Self {
    e.utf8_error().into()
  }
}

impl From<fmt::Error> for RedisError {
  fn from(e: fmt::Error) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("{:?}", e))
  }
}

impl From<Canceled> for RedisError {
  fn from(e: Canceled) -> Self {
    RedisError::new(RedisErrorKind::Canceled, format!("{}", e))
  }
}

impl From<JoinError> for RedisError {
  fn from(e: JoinError) -> Self {
    RedisError::new(RedisErrorKind::Unknown, format!("Spawn Error: {:?}", e))
  }
}

impl From<SemverError> for RedisError {
  fn from(e: SemverError) -> Self {
    RedisError::new(RedisErrorKind::ProtocolError, format!("Invalid Redis version: {:?}", e))
  }
}

impl From<Infallible> for RedisError {
  fn from(e: Infallible) -> Self {
    warn!("Infallible error: {:?}", e);
    RedisError::new(RedisErrorKind::Unknown, "Unknown error.")
  }
}

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

#[cfg(feature = "enable-native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-native-tls")))]
impl From<native_tls::Error> for RedisError {
  fn from(e: native_tls::Error) -> Self {
    RedisError::new(RedisErrorKind::Tls, format!("{:?}", e))
  }
}

#[cfg(feature = "enable-rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "enable-rustls")))]
impl From<tokio_rustls::rustls::Error> for RedisError {
  fn from(e: tokio_rustls::rustls::Error) -> Self {
    RedisError::new(RedisErrorKind::Tls, format!("{:?}", e))
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
      context: None,
    }
  }

  /// Create a new error with the provided command context.
  pub(crate) fn new_context<T>(kind: RedisErrorKind, details: T, cmd: RedisCommand) -> RedisError
  where
    T: Into<Cow<'static, str>>,
  {
    RedisError {
      kind,
      details: details.into(),
      context: Some(cmd),
    }
  }

  /// Take the command context off the error.
  pub(crate) fn take_context(&mut self) -> Option<RedisCommand> {
    self.context.take()
  }

  /// Whether or not the error is a Cluster error.
  pub fn is_cluster_error(&self) -> bool {
    match self.kind {
      RedisErrorKind::Cluster => true,
      _ => false,
    }
  }

  /// Whether or not the error is from a sentinel node.
  pub fn is_sentinel_error(&self) -> bool {
    match self.kind {
      RedisErrorKind::Sentinel => true,
      _ => false,
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

  pub fn to_string(&self) -> String {
    format!("{}: {}", &self.kind.to_str(), &self.details)
  }

  /// Create a new empty Canceled error.
  pub fn new_canceled() -> RedisError {
    RedisError::new(RedisErrorKind::Canceled, "Canceled.")
  }

  /// Create a new empty Timeout error.
  pub fn new_timeout() -> RedisError {
    RedisError::new(RedisErrorKind::Timeout, "")
  }

  /// Create a new parse error with the provided details.
  pub(crate) fn new_parse<T>(details: T) -> RedisError
  where
    T: Into<Cow<'static, str>>,
  {
    RedisError::new(RedisErrorKind::Parse, details)
  }

  /// Whether or not the error is a `Canceled` error.
  pub fn is_canceled(&self) -> bool {
    match self.kind {
      RedisErrorKind::Canceled => true,
      _ => false,
    }
  }

  /// Whether or not the error is a `NotFound` error.
  pub fn is_not_found(&self) -> bool {
    match self.kind {
      RedisErrorKind::NotFound => true,
      _ => false,
    }
  }
}

impl Error for RedisError {
  fn source(&self) -> Option<&(dyn Error + 'static)> {
    None
  }
}
