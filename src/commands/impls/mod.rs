use crate::error::RedisError;
use crate::interfaces::ClientLike;
use crate::modules::inner::RedisClientInner;
use crate::protocol::command::RedisCommandKind;
use crate::protocol::utils as protocol_utils;
use crate::types::RedisValue;
use crate::utils;
use std::sync::Arc;

pub static MATCH: &'static str = "MATCH";
pub static COUNT: &'static str = "COUNT";
pub static TYPE: &'static str = "TYPE";
pub static CHANGED: &'static str = "CH";
pub static INCR: &'static str = "INCR";
pub static WITH_SCORES: &'static str = "WITHSCORES";
pub static LIMIT: &'static str = "LIMIT";
pub static AGGREGATE: &'static str = "AGGREGATE";
pub static WEIGHTS: &'static str = "WEIGHTS";
pub static GET: &'static str = "GET";
pub static RESET: &'static str = "RESET";
pub static TO: &'static str = "TO";
pub static FORCE: &'static str = "FORCE";
pub static ABORT: &'static str = "ABORT";
pub static TIMEOUT: &'static str = "TIMEOUT";
pub static LEN: &'static str = "LEN";
pub static DB: &'static str = "DB";
pub static REPLACE: &'static str = "REPLACE";
pub static ID: &'static str = "ID";
pub static ANY: &'static str = "ANY";
pub static STORE: &'static str = "STORE";
pub static WITH_VALUES: &'static str = "WITHVALUES";
pub static SYNC: &'static str = "SYNC";
pub static ASYNC: &'static str = "ASYNC";
pub static RANK: &'static str = "RANK";
pub static MAXLEN: &'static str = "MAXLEN";
pub static REV: &'static str = "REV";
pub static ABSTTL: &'static str = "ABSTTL";
pub static IDLE_TIME: &'static str = "IDLETIME";
pub static FREQ: &'static str = "FREQ";
pub static FULL: &'static str = "FULL";
pub static NOMKSTREAM: &'static str = "NOMKSTREAM";
pub static MINID: &'static str = "MINID";
pub static BLOCK: &'static str = "BLOCK";
pub static STREAMS: &'static str = "STREAMS";
pub static MKSTREAM: &'static str = "MKSTREAM";
pub static GROUP: &'static str = "GROUP";
pub static NOACK: &'static str = "NOACK";
pub static IDLE: &'static str = "IDLE";
pub static TIME: &'static str = "TIME";
pub static RETRYCOUNT: &'static str = "RETRYCOUNT";
pub static JUSTID: &'static str = "JUSTID";
pub static SAMPLES: &'static str = "SAMPLES";

/// Macro to generate a command function that takes no arguments and expects an OK response - returning `()` to the caller.
macro_rules! ok_cmd(
  ($name:ident, $cmd:tt) => {
    pub async fn $name<C: ClientLike>(client: C) -> Result<(), RedisError> {
      let frame = crate::utils::request_response(client, || Ok((RedisCommandKind::$cmd, vec![]))).await?;
      let response = crate::protocol::utils::frame_to_single_result(frame)?;
      crate::protocol::utils::expect_ok(&response)
    }
  }
);

/// Macro to generate a command function that takes no arguments and returns a single `RedisValue` to the caller.
macro_rules! simple_cmd(
  ($name:ident, $cmd:tt, $res:ty) => {
    pub async fn $name<C: ClientLike>(client: C) -> Result<$res, RedisError> {
      let frame = crate::utils::request_response(client, || Ok((RedisCommandKind::$cmd, vec![]))).await?;
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

/// Macro to generate a command function that takes no arguments and returns a potentially nested `RedisValue` to the caller.
macro_rules! values_cmd(
  ($name:ident, $cmd:tt) => {
    pub async fn $name<C: ClientLike>(client: C) -> Result<RedisValue, RedisError> {
      let frame = crate::utils::request_response(client, || Ok((RedisCommandKind::$cmd, vec![]))).await?;
      crate::protocol::utils::frame_to_results(frame)
    }
  }
);

/// A function that issues a command that only takes one argument and returns a single `RedisValue`.
pub async fn one_arg_value_cmd<C: ClientLike>(
  client: C,
  kind: RedisCommandKind,
  arg: RedisValue,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || Ok((kind, vec![arg]))).await?;
  protocol_utils::frame_to_single_result(frame)
}

/// A function that issues a command that only takes one argument and returns a potentially nested `RedisValue`.
pub async fn one_arg_values_cmd<C: ClientLike>(
  client: C,
  kind: RedisCommandKind,
  arg: RedisValue,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || Ok((kind, vec![arg]))).await?;
  protocol_utils::frame_to_results(frame)
}

/// A function that issues a command that only takes one argument and expects an OK response - returning `()` to the caller.
pub async fn one_arg_ok_cmd<C: ClientLike>(
  client: C,
  kind: RedisCommandKind,
  arg: RedisValue,
) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || Ok((kind, vec![arg]))).await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

/// A function that issues a command that takes any number of arguments and returns a single `RedisValue` to the caller.
pub async fn args_value_cmd<C: ClientLike>(
  client: C,
  kind: RedisCommandKind,
  args: Vec<RedisValue>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || Ok((kind, args))).await?;
  protocol_utils::frame_to_single_result(frame)
}

/// A function that issues a command that takes any number of arguments and returns a potentially nested `RedisValue` to the caller.
pub async fn args_values_cmd<C: ClientLike>(
  client: C,
  kind: RedisCommandKind,
  args: Vec<RedisValue>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || Ok((kind, args))).await?;
  protocol_utils::frame_to_results(frame)
}

/// A function that issues a command that takes any number of arguments and expects an OK response - returning `()` to the caller.
pub async fn args_ok_cmd<C: ClientLike>(
  client: C,
  kind: RedisCommandKind,
  args: Vec<RedisValue>,
) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || Ok((kind, args))).await?;
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
