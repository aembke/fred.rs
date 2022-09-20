use crate::{
  clients::RedisClient,
  error::RedisError,
  modules::inner::RedisClientInner,
  protocol::types::{KeyScanInner, RedisCommand, RedisCommandKind, ValueScanInner},
  types::{RedisKey, RedisMap, RedisValue},
  utils,
};
use bytes_utils::Str;
use std::sync::Arc;

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

/// The result of a SCAN operation.
pub struct ScanResult {
  pub(crate) results:      Option<Vec<RedisKey>>,
  pub(crate) inner:        Arc<RedisClientInner>,
  pub(crate) args:         Vec<RedisValue>,
  pub(crate) scan_state:   KeyScanInner,
  pub(crate) can_continue: bool,
}

impl ScanResult {
  /// Read the current cursor from the SCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set
  /// returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each
  /// result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the SCAN operation.
  pub fn results(&self) -> &Option<Vec<RedisKey>> {
    &self.results
  }

  /// Take ownership over the results of the SCAN operation. Calls to `results` or `take_results` will return `None`
  /// afterwards.
  pub fn take_results(&mut self) -> Option<Vec<RedisKey>> {
    self.results.take()
  }

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
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let kind = RedisCommandKind::Scan(self.scan_state);
    let cmd = RedisCommand::new(kind, self.args, None);
    utils::send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the SCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `scan` again on the client will
  /// initiate a new SCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }
}

/// The result of a HSCAN operation.
pub struct HScanResult {
  pub(crate) results:      Option<RedisMap>,
  pub(crate) inner:        Arc<RedisClientInner>,
  pub(crate) args:         Vec<RedisValue>,
  pub(crate) scan_state:   ValueScanInner,
  pub(crate) can_continue: bool,
}

impl HScanResult {
  /// Read the current cursor from the SCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set
  /// returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each
  /// result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the HSCAN operation.
  pub fn results(&self) -> &Option<RedisMap> {
    &self.results
  }

  /// Take ownership over the results of the HSCAN operation. Calls to `results` or `take_results` will return `None`
  /// afterwards.
  pub fn take_results(&mut self) -> Option<RedisMap> {
    self.results.take()
  }

  /// Move on to the next page of results from the HSCAN operation. If no more results are available this may close
  /// the stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the
  /// background since this could cause the  buffer backing the stream to grow too large very quickly. This
  /// interface provides a mechanism for throttling the throughput of the SCAN call. If this struct is dropped
  /// without calling this function the stream will close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other
  /// fatal error has occurred. If this happens the error will appear in the stream from the original SCAN call.
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let kind = RedisCommandKind::Hscan(self.scan_state);
    let cmd = RedisCommand::new(kind, self.args, None);
    utils::send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the HSCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `hscan` again on the client will
  /// initiate a new HSCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }
}

/// The result of a SCAN operation.
pub struct SScanResult {
  pub(crate) results:      Option<Vec<RedisValue>>,
  pub(crate) inner:        Arc<RedisClientInner>,
  pub(crate) args:         Vec<RedisValue>,
  pub(crate) scan_state:   ValueScanInner,
  pub(crate) can_continue: bool,
}

impl SScanResult {
  /// Read the current cursor from the SSCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set
  /// returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each
  /// result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the SCAN operation.
  pub fn results(&self) -> &Option<Vec<RedisValue>> {
    &self.results
  }

  /// Take ownership over the results of the SSCAN operation. Calls to `results` or `take_results` will return `None`
  /// afterwards.
  pub fn take_results(&mut self) -> Option<Vec<RedisValue>> {
    self.results.take()
  }

  /// Move on to the next page of results from the SSCAN operation. If no more results are available this may close
  /// the stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the
  /// background since this could cause the  buffer backing the stream to grow too large very quickly. This
  /// interface provides a mechanism for throttling the throughput of the SCAN call. If this struct is dropped
  /// without calling this function the stream will close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other
  /// fatal error has occurred. If this happens the error will appear in the stream from the original SCAN call.
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let kind = RedisCommandKind::Sscan(self.scan_state);
    let cmd = RedisCommand::new(kind, self.args, None);
    utils::send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the SSCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `sscan` again on the client will
  /// initiate a new SSCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }
}

/// The result of a SCAN operation.
pub struct ZScanResult {
  pub(crate) results:      Option<Vec<(RedisValue, f64)>>,
  pub(crate) inner:        Arc<RedisClientInner>,
  pub(crate) args:         Vec<RedisValue>,
  pub(crate) scan_state:   ValueScanInner,
  pub(crate) can_continue: bool,
}

impl ZScanResult {
  /// Read the current cursor from the ZSCAN operation.
  pub fn cursor(&self) -> &str {
    &self.scan_state.cursor
  }

  /// Whether or not the scan call will continue returning results. If `false` this will be the last result set
  /// returned on the stream.
  ///
  /// Calling `next` when this returns `false` will return `Ok(())`, so this does not need to be checked on each
  /// result.
  pub fn has_more(&self) -> bool {
    self.can_continue
  }

  /// A reference to the results of the ZSCAN operation.
  pub fn results(&self) -> &Option<Vec<(RedisValue, f64)>> {
    &self.results
  }

  /// Take ownership over the results of the ZSCAN operation. Calls to `results` or `take_results` will return `None`
  /// afterwards.
  pub fn take_results(&mut self) -> Option<Vec<(RedisValue, f64)>> {
    self.results.take()
  }

  /// Move on to the next page of results from the ZSCAN operation. If no more results are available this may close
  /// the stream.
  ///
  /// **This must be called to continue scanning the keyspace.** Results are not automatically scanned in the
  /// background since this could cause the  buffer backing the stream to grow too large very quickly. This
  /// interface provides a mechanism for throttling the throughput of the SCAN call. If this struct is dropped
  /// without calling this function the stream will close without an error.
  ///
  /// If this function returns an error the scan call cannot continue as the client has been closed, or some other
  /// fatal error has occurred. If this happens the error will appear in the stream from the original SCAN call.
  pub fn next(self) -> Result<(), RedisError> {
    if !self.can_continue {
      return Ok(());
    }

    let kind = RedisCommandKind::Zscan(self.scan_state);
    let cmd = RedisCommand::new(kind, self.args, None);
    utils::send_command(&self.inner, cmd)
  }

  /// A lightweight function to create a Redis client from the ZSCAN result.
  ///
  /// To continue scanning the caller should call `next` on this struct. Calling `zscan` again on the client will
  /// initiate a new ZSCAN call starting with a cursor of 0.
  pub fn create_client(&self) -> RedisClient {
    RedisClient {
      inner: self.inner.clone(),
    }
  }
}
