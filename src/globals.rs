use crate::utils::{read_atomic, set_atomic};
use lazy_static::lazy_static;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

#[cfg(feature = "custom-reconnect-errors")]
use parking_lot::RwLock;

/// Special errors that can trigger reconnection logic, which can also retry the failing command if possible.
///
/// `MOVED`, `ASK`, and `NOAUTH` errors are handled separately by the client.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(feature = "custom-reconnect-errors")]
pub enum ReconnectError {
  /// The CLUSTERDOWN prefix.
  ClusterDown,
  /// The LOADING prefix.
  Loading,
  /// The MASTERDOWN prefix.
  MasterDown,
  /// The READONLY prefix, which can happen if a primary node is switched to a replica without any connection interruption.
  ReadOnly,
  /// The MISCONF prefix.
  Misconf,
  /// The BUSY prefix.
  Busy,
  /// The NOREPLICAS prefix.
  NoReplicas,
  /// A case-sensitive prefix on an error message.
  ///
  /// See [the source](https://github.com/redis/redis/blob/unstable/src/server.c#L2506-L2538) for examples.
  Custom(&'static str),
}

#[cfg(feature = "custom-reconnect-errors")]
impl ReconnectError {
  pub(crate) fn to_str(&self) -> &'static str {
    use ReconnectError::*;

    match self {
      ClusterDown => "CLUSTERDOWN",
      Loading => "LOADING",
      MasterDown => "MASTERDOWN",
      ReadOnly => "READONLY",
      Misconf => "MISCONF",
      Busy => "BUSY",
      NoReplicas => "NOREPLICAS",
      Custom(prefix) => prefix,
    }
  }
}

/// Mutable globals that can be configured by the caller.
pub(crate) struct Globals {
  /// Max number of times a command can be written to the wire before returning an error.
  pub(crate) max_command_attempts: Arc<AtomicUsize>,
  /// Max number of in-flight commands per socket before the caller receives backpressure.
  pub(crate) backpressure_count: Arc<AtomicUsize>,
  /// Number of frames that can be fed into a socket before the socket must be flushed.
  pub(crate) feed_count: Arc<AtomicUsize>,
  /// Minimum amount of time to wait when applying backpressure.
  pub(crate) min_backpressure_time_ms: Arc<AtomicUsize>,
  /// Amount of time to wait before re-caching the cluster state when a MOVED or ASK error is detected.
  pub(crate) cluster_error_cache_delay: Arc<AtomicUsize>,
  /// The default timeout to apply to commands, in ms. A value of 0 means no timeout.
  pub(crate) default_command_timeout: Arc<AtomicUsize>,
  #[cfg(feature = "blocking-encoding")]
  /// The minimum size, in bytes, of frames that should be encoded or decoded with a blocking task.
  pub(crate) blocking_encode_threshold: Arc<AtomicUsize>,
  /// Any special errors that should trigger reconnection logic.
  #[cfg(feature = "custom-reconnect-errors")]
  pub(crate) reconnect_errors: Arc<RwLock<Vec<ReconnectError>>>,
}

impl Default for Globals {
  fn default() -> Self {
    Globals {
      max_command_attempts: Arc::new(AtomicUsize::new(3)),
      backpressure_count: Arc::new(AtomicUsize::new(500)),
      feed_count: Arc::new(AtomicUsize::new(500)),
      min_backpressure_time_ms: Arc::new(AtomicUsize::new(100)),
      cluster_error_cache_delay: Arc::new(AtomicUsize::new(100)),
      default_command_timeout: Arc::new(AtomicUsize::new(0)),
      #[cfg(feature = "blocking-encoding")]
      blocking_encode_threshold: Arc::new(AtomicUsize::new(500_000)),
      #[cfg(feature = "custom-reconnect-errors")]
      reconnect_errors: Arc::new(RwLock::new(vec![
        ReconnectError::ClusterDown,
        ReconnectError::Loading,
        ReconnectError::ReadOnly,
      ])),
    }
  }
}

impl Globals {
  pub fn max_command_attempts(&self) -> usize {
    read_atomic(&self.max_command_attempts)
  }

  pub fn backpressure_count(&self) -> usize {
    read_atomic(&self.backpressure_count)
  }

  pub fn feed_count(&self) -> usize {
    read_atomic(&self.feed_count)
  }

  pub fn min_backpressure_time_ms(&self) -> usize {
    read_atomic(&self.min_backpressure_time_ms)
  }

  pub fn cluster_error_cache_delay(&self) -> usize {
    read_atomic(&self.cluster_error_cache_delay)
  }

  pub fn default_command_timeout(&self) -> usize {
    read_atomic(&self.default_command_timeout)
  }

  #[cfg(feature = "blocking-encoding")]
  pub fn blocking_encode_threshold(&self) -> usize {
    read_atomic(&self.blocking_encode_threshold)
  }
}

lazy_static! {
  static ref GLOBALS: Globals = Globals::default();
}

pub(crate) fn globals() -> &'static Globals {
  &GLOBALS
}

/// Read the case-sensitive list of error prefixes (without the leading `-`) that will trigger the client to reconnect and retry the last command.
///
/// Default: CLUSTERDOWN, READONLY, LOADING
#[cfg(feature = "custom-reconnect-errors")]
pub fn get_custom_reconnect_errors() -> Vec<ReconnectError> {
  globals().reconnect_errors.read().clone()
}

/// See [get_custom_reconnect_errors] for more information.
#[cfg(feature = "custom-reconnect-errors")]
pub fn set_custom_reconnect_errors(prefixes: Vec<ReconnectError>) {
  let mut guard = globals().reconnect_errors.write();
  *guard = prefixes;
}

/// Read the default timeout applied to commands, in ms. A value of 0 means no timeout.
///
/// Default: 0
pub fn get_default_command_timeout() -> usize {
  read_atomic(&globals().default_command_timeout)
}

/// See [get_default_command_timeout] for more information.
pub fn set_default_command_timeout(val: usize) -> usize {
  set_atomic(&globals().default_command_timeout, val)
}

/// Read the amount of time the client will wait before caching the new layout of the cluster slots when it detects a MOVED or ASK error.
///
/// Default: 100 ms
pub fn get_cluster_error_cache_delay_ms() -> usize {
  read_atomic(&globals().cluster_error_cache_delay)
}

/// See [get_cluster_error_cache_delay_ms] for more information.
pub fn set_cluster_error_cache_delay_ms(val: usize) -> usize {
  set_atomic(&globals().cluster_error_cache_delay, val)
}

/// Read the max number of attempts the client will make when attempting to write a command to the socket.
///
/// A connection closing while a command is in flight can result in a command being written multiple times.
///
/// Default: 3
pub fn get_max_command_attempts() -> usize {
  read_atomic(&globals().max_command_attempts)
}

/// See [get_max_command_attempts] for more information.
pub fn set_max_command_attempts(val: usize) -> usize {
  set_atomic(&globals().max_command_attempts, val)
}

/// Read the maximum allowed number of in-flight commands per connection before backpressure is put on callers.
///
/// Backpressure is handled automatically by the client without returning errors. This setting can drastically affect performance.
/// See [get_min_backpressure_time_ms] for more information on how to configure the backpressure `sleep` duration.
///
/// The client will automatically [pipeline](https://redis.io/topics/pipelining) all commands to the server, and this setting can
/// be used to effectively disable pipelining by setting this to `1`. However, If the caller wants to avoid pipelining commands it's recommended
/// to use the `disable_pipeline` flag on the [connect](crate::client::RedisClient::connect) function instead since backpressure is probabilistic
/// while the `no_pipeline` flag is not.
///
/// Default: 500
pub fn get_backpressure_count() -> usize {
  read_atomic(&globals().backpressure_count)
}

/// See [get_backpressure_count] for more information.
pub fn set_backpressure_count(val: usize) -> usize {
  set_atomic(&globals().backpressure_count, val)
}

/// Read the number of protocol frames that can be written to a socket with [feed](https://docs.rs/futures/0.3.14/futures/sink/trait.SinkExt.html#method.feed)
/// before the socket will be flushed once by using [send](https://docs.rs/futures/0.3.14/futures/sink/trait.SinkExt.html#method.send).
///
/// Default: 500
pub fn get_feed_count() -> usize {
  read_atomic(&globals().feed_count)
}

/// See [get_feed_count] for more information.
pub fn set_feed_count(val: usize) -> usize {
  set_atomic(&globals().feed_count, val)
}

/// Read the minimum amount of time the client will wait between writing commands when applying backpressure.
///
/// Backpressure is only applied while the number of in-flight commands exceeds the [get_backpressure_count] value.
///
/// Default: 100 ms
pub fn get_min_backpressure_time_ms() -> usize {
  read_atomic(&globals().min_backpressure_time_ms)
}

/// See [get_min_backpressure_time_ms] for more information.
pub fn set_min_backpressure_time_ms(val: usize) -> usize {
  set_atomic(&globals().min_backpressure_time_ms, val)
}

/// The minimum size, in bytes, of frames that should be encoded or decoded with a blocking task.
///
/// See [block_in_place](https://docs.rs/tokio/1.9.0/tokio/task/fn.block_in_place.html) for more information.
///
/// Default: 500 Kb
#[cfg(feature = "blocking-encoding")]
pub fn get_blocking_encode_threshold() -> usize {
  read_atomic(&globals().blocking_encode_threshold)
}

/// See [get_blocking_encode_threshold] for more information.
#[cfg(feature = "blocking-encoding")]
pub fn set_blocking_encode_threshold(val: usize) -> usize {
  set_atomic(&globals().blocking_encode_threshold, val)
}
