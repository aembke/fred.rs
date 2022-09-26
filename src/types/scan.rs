use crate::{
  clients::RedisClient,
  error::RedisError,
  interfaces,
  modules::inner::RedisClientInner,
  protocol::{
    command::RedisCommandKind,
    responders::ResponseKind,
    types::{KeyScanInner, RedisCommand, RedisCommandKind, ValueScanInner},
  },
  types::{RedisKey, RedisMap, RedisValue},
  utils,
};
use bytes_utils::Str;
use pretty_env_logger::env_logger::fmt::Color::Red;
use std::{borrow::Cow, sync::Arc};

/// The types of values supported by the [type](https://redis.io/commands/type) command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScanType {
  Set,
  String,
  ZSet,
  List,
  Hash,
  Stream,
}

impl ScanType {
  pub(crate) fn to_str(&self) -> Str {
    utils::static_str(match *self {
      ScanType::Set => "set",
      ScanType::String => "string",
      ScanType::List => "list",
      ScanType::ZSet => "zset",
      ScanType::Hash => "hash",
      ScanType::Stream => "stream",
    })
  }
}

/// An interface for interacting with the results of a scan operation.
pub trait Scanner {
  /// The type of results from the scan operation.
  type Page;

  /// Read the cursor returned from the last scan operation.
  fn cursor(&self) -> Option<Cow<str>>;

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set
  /// returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each
  /// result.
  fn has_more(&self) -> bool;

  /// Return a reference to the last page of results.
  fn results(&self) -> &Option<Self::Page>;

  /// Take ownership over the results of the SCAN operation. Calls to `results` or `take_results` will return `None`
  /// afterwards.
  fn take_results(&mut self) -> Option<Self::Page>;

  /// A lightweight function to create a Redis client from the SCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `scan` again on the client will
  /// initiate a new SCAN call starting with a cursor of 0.
  fn create_client(&self) -> RedisClient;

  /// Move on to the next page of results from the SCAN operation. If no more results are available this may close the
  /// stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the
  /// background since this could cause the  buffer backing the stream to grow too large very quickly. This
  /// interface provides a mechanism for throttling the throughput of the SCAN call. If this struct is dropped
  /// without calling this function the stream will close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other
  /// fatal error has occurred. If this happens the error will appear in the stream from the original SCAN call.
  fn next(self) -> Result<(), RedisError>;
}

/// The result of a SCAN operation.
pub struct ScanResult {
  pub(crate) results:      Option<Vec<RedisKey>>,
  pub(crate) inner:        Arc<RedisClientInner>,
  pub(crate) scan_state:   KeyScanInner,
  pub(crate) can_continue: bool,
}

impl Scanner for ScanResult {
  type Page = Vec<RedisKey>;

  fn cursor(&self) -> Option<Cow<str>> {
    self.scan_state.args[self.scan_state.cursor_idx].as_str()
  }

  fn has_more(&self) -> bool {
    self.can_continue
  }

  fn results(&self) -> &Option<Self::Page> {
    &self.results
  }

  fn take_results(&mut self) -> Option<Self::Page> {
    self.results.take()
  }

  fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }

  fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let response = ResponseKind::KeyScan(self.scan_state);
    let cmd = (RedisCommandKind::Scan, Vec::new(), response).into();
    interfaces::default_send_command(&self.inner, cmd)
  }
}

/// The result of a HSCAN operation.
pub struct HScanResult {
  pub(crate) results:      Option<RedisMap>,
  pub(crate) inner:        Arc<RedisClientInner>,
  pub(crate) scan_state:   ValueScanInner,
  pub(crate) can_continue: bool,
}

impl Scanner for HScanResult {
  type Page = RedisMap;

  fn cursor(&self) -> Option<Cow<str>> {
    self.scan_state.args[self.scan_state.cursor_idx].as_str()
  }

  fn has_more(&self) -> bool {
    self.can_continue
  }

  fn results(&self) -> &Option<Self::Page> {
    &self.results
  }

  fn take_results(&mut self) -> Option<Self::Page> {
    self.results.take()
  }

  fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }

  fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let response = ResponseKind::ValueScan(self.scan_state);
    let cmd = (RedisCommandKind::Hscan, Vec::new(), response).into();
    interfaces::default_send_command(&self.inner, cmd)
  }
}

/// The result of a SSCAN operation.
pub struct SScanResult {
  pub(crate) results:      Option<Vec<RedisValue>>,
  pub(crate) inner:        Arc<RedisClientInner>,
  pub(crate) scan_state:   ValueScanInner,
  pub(crate) can_continue: bool,
}

impl Scanner for SScanResult {
  type Page = Vec<RedisValue>;

  fn cursor(&self) -> Option<Cow<str>> {
    self.scan_state.args[self.scan_state.cursor_idx].as_str()
  }

  fn results(&self) -> &Option<Self::Page> {
    &self.results
  }

  fn take_results(&mut self) -> Option<Self::Page> {
    self.results.take()
  }

  fn has_more(&self) -> bool {
    self.can_continue
  }

  fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }

  fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let response = ResponseKind::ValueScan(self.scan_state);
    let cmd = (RedisCommandKind::Sscan, Vec::new(), response).into();
    interfaces::default_send_command(&self.inner, cmd)
  }
}

/// The result of a ZSCAN operation.
pub struct ZScanResult {
  pub(crate) results:      Option<Vec<(RedisValue, f64)>>,
  pub(crate) inner:        Arc<RedisClientInner>,
  pub(crate) scan_state:   ValueScanInner,
  pub(crate) can_continue: bool,
}

impl Scanner for ZScanResult {
  type Page = Vec<(RedisValue, f64)>;

  fn cursor(&self) -> Option<Cow<str>> {
    self.scan_state.args[self.scan_state.cursor_idx].as_str()
  }

  fn has_more(&self) -> bool {
    self.can_continue
  }

  fn results(&self) -> &Option<Self::Page> {
    &self.results
  }

  fn take_results(&mut self) -> Option<Self::Page> {
    self.results.take()
  }

  fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }

  fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let response = ResponseKind::ValueScan(self.scan_state);
    let cmd = (RedisCommandKind::Zscan, Vec::new(), response).into();
    interfaces::default_send_command(&self.inner, cmd)
  }
}
