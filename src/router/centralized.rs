use crate::{
  error::RedisErrorKind,
  modules::inner::RedisClientInner,
  prelude::RedisError,
  protocol::{
    command::RedisCommand,
    connection,
    connection::{Counters, RedisConnection},
    responders::{self, ResponseKind},
    types::Server,
    utils as protocol_utils,
  },
  router::{responses, utils as router_utils, Connections},
  runtime::{spawn, JoinHandle, RefCount},
  types::ServerConfig,
  utils,
};
use redis_protocol::resp3::types::{BytesFrame as Resp3Frame, Resp3Frame as _Resp3Frame};
use std::collections::VecDeque;

/// Process the response frame in the context of the last command.
///
/// Errors returned here will be logged, but will not close the socket or initiate a reconnect.
#[inline(always)]
pub async fn process_response_frame(
  inner: &RefCount<RedisClientInner>,
  conn: &mut RedisConnection,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  _trace!(inner, "Parsing response frame from {}", conn.server);
  let mut command = match conn.buffer.pop_front() {
    Some(command) => command,
    None => {
      _debug!(
        inner,
        "Missing last command from {}. Dropping {:?}.",
        conn.server,
        frame.kind()
      );
      return Ok(());
    },
  };
  _trace!(
    inner,
    "Checking response to {} ({})",
    command.kind.to_str_debug(),
    command.debug_id()
  );
  conn.counters.decr_in_flight();
  if command.blocks_connection() {
    conn.blocked = false;
  }
  responses::check_and_set_unblocked(inner, &command).await;

  _trace!(inner, "Handling centralized response kind: {:?}", command.response);
  match command.take_response() {
    ResponseKind::Skip | ResponseKind::Respond(None) => Ok(()),
    ResponseKind::Respond(Some(tx)) => responders::respond_to_caller(inner, &conn.server, command, tx, frame),
    ResponseKind::Buffer {
      received,
      expected,
      frames,
      tx,
      index,
      error_early,
    } => responders::respond_buffer(
      inner,
      &conn.server,
      command,
      received,
      expected,
      error_early,
      frames,
      index,
      tx,
      frame,
    ),
    ResponseKind::KeyScan(scanner) => {
      responders::respond_key_scan(inner, &conn.server, command, scanner, frame).await
    },
    ResponseKind::ValueScan(scanner) => {
      responders::respond_value_scan(inner, &conn.server, command, scanner, frame).await
    },
    ResponseKind::KeyScanBuffered(scanner) => {
      responders::respond_key_scan_buffered(inner, &conn.server, command, scanner, frame).await
    },
  }
}

/// Initialize fresh connections to the server, dropping any old connections and saving in-flight commands on
/// `buffer`.
pub async fn initialize_connection(
  inner: &RefCount<RedisClientInner>,
  connections: &mut Connections,
  buffer: &mut VecDeque<RedisCommand>,
) -> Result<(), RedisError> {
  _debug!(inner, "Initializing centralized connection.");
  todo!();
  // drop old connection, drain in-flight buffer
  // add in-flight buffer to router retry queue
  // connect to the server, set up the connection
  // split the connection, set on router
  // broadcast reconnect
}
