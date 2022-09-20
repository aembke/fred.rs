use crate::{
  error::RedisError,
  modules::inner::RedisClientInner,
  protocol::{types::RedisCommandKind, utils as protocol_utils},
  types::RedisValue,
  utils,
};
use std::sync::Arc;

pub static MATCH: &str = "MATCH";
pub static COUNT: &str = "COUNT";
pub static TYPE: &str = "TYPE";
pub static CHANGED: &str = "CH";
pub static INCR: &str = "INCR";
pub static WITH_SCORES: &str = "WITHSCORES";
pub static LIMIT: &str = "LIMIT";
pub static AGGREGATE: &str = "AGGREGATE";
pub static WEIGHTS: &str = "WEIGHTS";
pub static GET: &str = "GET";
pub static RESET: &str = "RESET";
pub static TO: &str = "TO";
pub static FORCE: &str = "FORCE";
pub static ABORT: &str = "ABORT";
pub static TIMEOUT: &str = "TIMEOUT";
pub static LEN: &str = "LEN";
pub static DB: &str = "DB";
pub static REPLACE: &str = "REPLACE";
pub static ID: &str = "ID";
pub static ANY: &str = "ANY";
pub static STORE: &str = "STORE";
pub static WITH_VALUES: &str = "WITHVALUES";
pub static SYNC: &str = "SYNC";
pub static ASYNC: &str = "ASYNC";
pub static RANK: &str = "RANK";
pub static MAXLEN: &str = "MAXLEN";
pub static REV: &str = "REV";
pub static ABSTTL: &str = "ABSTTL";
pub static IDLE_TIME: &str = "IDLETIME";
pub static FREQ: &str = "FREQ";
pub static FULL: &str = "FULL";
pub static NOMKSTREAM: &str = "NOMKSTREAM";
pub static MINID: &str = "MINID";
pub static BLOCK: &str = "BLOCK";
pub static STREAMS: &str = "STREAMS";
pub static MKSTREAM: &str = "MKSTREAM";
pub static GROUP: &str = "GROUP";
pub static NOACK: &str = "NOACK";
pub static IDLE: &str = "IDLE";
pub static TIME: &str = "TIME";
pub static RETRYCOUNT: &str = "RETRYCOUNT";
pub static JUSTID: &str = "JUSTID";
pub static SAMPLES: &str = "SAMPLES";

/// Macro to generate a command function that takes no arguments and expects an OK response - returning `()` to the
/// caller.
macro_rules! ok_cmd(
  ($name:ident, $cmd:tt) => {
    pub async fn $name(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
      let frame = crate::utils::request_response(inner, || Ok((RedisCommandKind::$cmd, vec![]))).await?;
      let response = crate::protocol::utils::frame_to_single_result(frame)?;
      crate::protocol::utils::expect_ok(&response)
    }
  }
);

/// Macro to generate a command function that takes no arguments and returns a single `RedisValue` to the caller.
macro_rules! simple_cmd(
  ($name:ident, $cmd:tt, $res:ty) => {
    pub async fn $name(inner: &Arc<RedisClientInner>) -> Result<$res, RedisError> {
      let frame = crate::utils::request_response(inner, || Ok((RedisCommandKind::$cmd, vec![]))).await?;
      crate::protocol::utils::frame_to_single_result(frame)
    }
  }
);

/// Macro to generate a command function that takes no arguments and returns a single `RedisValue` to the caller.
macro_rules! value_cmd(
  ($name:ident, $cmd:tt) => {
    simple_cmd!($name, $cmd, RedisValue);
  }
);

/// Macro to generate a command function that takes no arguments and returns a potentially nested `RedisValue` to the
/// caller.
macro_rules! values_cmd(
  ($name:ident, $cmd:tt) => {
    pub async fn $name(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
      let frame = crate::utils::request_response(inner, || Ok((RedisCommandKind::$cmd, vec![]))).await?;
      crate::protocol::utils::frame_to_results(frame)
    }
  }
);

/// A function that issues a command that only takes one argument and returns a single `RedisValue`.
pub async fn one_arg_value_cmd(
  inner: &Arc<RedisClientInner>,
  kind: RedisCommandKind,
  arg: RedisValue,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || Ok((kind, vec![arg]))).await?;
  protocol_utils::frame_to_single_result(frame)
}

/// A function that issues a command that only takes one argument and returns a potentially nested `RedisValue`.
pub async fn one_arg_values_cmd(
  inner: &Arc<RedisClientInner>,
  kind: RedisCommandKind,
  arg: RedisValue,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || Ok((kind, vec![arg]))).await?;
  protocol_utils::frame_to_results(frame)
}

/// A function that issues a command that only takes one argument and expects an OK response - returning `()` to the
/// caller.
pub async fn one_arg_ok_cmd(
  inner: &Arc<RedisClientInner>,
  kind: RedisCommandKind,
  arg: RedisValue,
) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || Ok((kind, vec![arg]))).await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

/// A function that issues a command that takes any number of arguments and returns a single `RedisValue` to the
/// caller.
pub async fn args_value_cmd(
  inner: &Arc<RedisClientInner>,
  kind: RedisCommandKind,
  args: Vec<RedisValue>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || Ok((kind, args))).await?;
  protocol_utils::frame_to_single_result(frame)
}

/// A function that issues a command that takes any number of arguments and returns a potentially nested `RedisValue`
/// to the caller.
pub async fn args_values_cmd(
  inner: &Arc<RedisClientInner>,
  kind: RedisCommandKind,
  args: Vec<RedisValue>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || Ok((kind, args))).await?;
  protocol_utils::frame_to_results(frame)
}

/// A function that issues a command that takes any number of arguments and expects an OK response - returning `()` to
/// the caller.
pub async fn args_ok_cmd(
  inner: &Arc<RedisClientInner>,
  kind: RedisCommandKind,
  args: Vec<RedisValue>,
) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || Ok((kind, args))).await?;
  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub mod acl;
pub mod client;
pub mod cluster;
pub mod config;
pub mod geo;
pub mod hashes;
pub mod hyperloglog;
pub mod keys;
pub mod lists;
pub mod lua;
pub mod memory;
pub mod pubsub;
pub mod scan;
pub mod server;
pub mod sets;
pub mod slowlog;
pub mod sorted_sets;
pub mod streams;
pub mod strings;

#[cfg(feature = "sentinel-client")]
pub mod sentinel;
