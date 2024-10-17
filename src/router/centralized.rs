use crate::{
  error::RedisErrorKind,
  modules::inner::RedisClientInner,
  prelude::RedisError,
  protocol::{
    command::{RedisCommand, RouterResponse},
    connection,
    connection::{Counters, RedisWriter, SharedBuffer, SplitStreamKind},
    responders::{self, ResponseKind},
    types::Server,
    utils as protocol_utils,
  },
  router::{responses, utils, Connections, Written},
  runtime::{spawn, JoinHandle, RefCount},
  types::ServerConfig,
};
use redis_protocol::resp3::types::{BytesFrame as Resp3Frame, Resp3Frame as _Resp3Frame};
use std::collections::VecDeque;

pub async fn write(
  inner: &RefCount<RedisClientInner>,
  writer: &mut Option<RedisWriter>,
  command: RedisCommand,
  force_flush: bool,
) -> Written {
  if let Some(writer) = writer.as_mut() {
    utils::write_command(inner, writer, command, force_flush).await
  } else {
    _debug!(inner, "Failed to read connection for {}", command.kind.to_str_debug());
    Written::Disconnected((
      None,
      Some(command),
      RedisError::new(RedisErrorKind::IO, "Missing connection."),
    ))
  }
}

/// Spawn a task to read response frames from the reader half of the socket.
#[allow(unused_assignments)]
pub fn spawn_reader_task(
  inner: &RefCount<RedisClientInner>,
  mut reader: SplitStreamKind,
  server: &Server,
  buffer: &SharedBuffer,
  counters: &Counters,
  is_replica: bool,
) -> JoinHandle<Result<(), RedisError>> {
  let (inner, server) = (inner.clone(), server.clone());
  let (buffer, counters) = (buffer.clone(), counters.clone());
  #[cfg(feature = "glommio")]
  let tq = inner.connection.connection_task_queue;

  let reader_ft = async move {
    let mut last_error = None;

    loop {
      let frame = match utils::next_frame(&inner, &mut reader, &server, &buffer).await {
        Ok(Some(frame)) => frame.into_resp3(),
        Ok(None) => {
          last_error = None;
          break;
        },
        Err(error) => {
          last_error = Some(error);
          break;
        },
      };

      if let Some(error) = responses::check_special_errors(&inner, &frame) {
        last_error = Some(error);
        break;
      }
      if let Some(frame) = responses::check_pubsub_message(&inner, &server, frame) {
        if let Err(e) = process_response_frame(&inner, &server, &buffer, &counters, frame).await {
          _debug!(inner, "Error processing response frame from {}: {:?}", server, e);
          last_error = Some(e);
          break;
        }
      }
    }

    // at this point the order of the shared buffer no longer matters since we can't know which commands actually made
    // it to the server, just that the connection closed. the shared buffer will be drained when the writer notices
    // that this task finished, but here we need to first filter out any commands that have exceeded their max write
    // attempts.
    utils::check_blocked_router(&inner, &buffer, &last_error);
    utils::check_final_write_attempt(&inner, &buffer, &last_error);
    if is_replica {
      responses::broadcast_replica_error(&inner, &server, last_error);
    } else {
      responses::broadcast_reader_error(&inner, &server, last_error);
    }
    utils::remove_cached_connection_id(&inner, &server).await;

    _debug!(inner, "Ending reader task from {}", server);
    Ok(())
  };

  #[cfg(feature = "glommio")]
  if let Some(tq) = tq {
    crate::runtime::spawn_into(reader_ft, tq)
  } else {
    spawn(reader_ft)
  }
  #[cfg(not(feature = "glommio"))]
  spawn(reader_ft)
}

/// Process the response frame in the context of the last command.
///
/// Errors returned here will be logged, but will not close the socket or initiate a reconnect.
pub async fn process_response_frame(
  inner: &RefCount<RedisClientInner>,
  server: &Server,
  buffer: &SharedBuffer,
  counters: &Counters,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  _trace!(inner, "Parsing response frame from {}", server);
  let mut command = match buffer.pop() {
    Some(command) => command,
    None => {
      _debug!(
        inner,
        "Missing last command from {}. Dropping {:?}.",
        server,
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
  counters.decr_in_flight();
  if command.blocks_connection() {
    buffer.set_unblocked();
  }
  responses::check_and_set_unblocked_flag(inner, &command).await;

  // non-pipelined transactions use ResponseKind::Skip, pipelined ones use a buffer. non-pipelined transactions
  // need to retry commands in a special way so this logic forwards the result via the latest command's router
  // response channel and exits early. pipelined transactions use the normal buffered response process below.
  if command.in_non_pipelined_transaction() {
    if let Some(error) = protocol_utils::frame_to_error(&frame) {
      #[allow(unused_mut)]
      if let Some(mut tx) = command.take_router_tx() {
        let _ = tx.send(RouterResponse::TransactionError((error, command)));
      }
      return Ok(());
    } else if command.kind.ends_transaction() {
      command.respond_to_router(inner, RouterResponse::TransactionResult(frame));
      return Ok(());
    } else {
      command.respond_to_router(inner, RouterResponse::Continue);
      return Ok(());
    }
  }

  _trace!(inner, "Handling centralized response kind: {:?}", command.response);
  match command.take_response() {
    ResponseKind::Skip | ResponseKind::Respond(None) => {
      command.respond_to_router(inner, RouterResponse::Continue);
      Ok(())
    },
    ResponseKind::Respond(Some(tx)) => responders::respond_to_caller(inner, server, command, tx, frame),
    ResponseKind::Buffer {
      received,
      expected,
      frames,
      tx,
      index,
      error_early,
    } => responders::respond_buffer(
      inner,
      server,
      command,
      received,
      expected,
      error_early,
      frames,
      index,
      tx,
      frame,
    ),
    ResponseKind::KeyScan(scanner) => responders::respond_key_scan(inner, server, command, scanner, frame),
    ResponseKind::ValueScan(scanner) => responders::respond_value_scan(inner, server, command, scanner, frame),
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
  let commands = connections.disconnect_all(inner).await;
  buffer.extend(commands);

  match connections {
    Connections::Centralized { writer, .. } => {
      let server = match inner.config.server {
        ServerConfig::Centralized { ref server } => server.clone(),
        #[cfg(feature = "unix-sockets")]
        ServerConfig::Unix { ref path } => path.as_path().into(),
        _ => return Err(RedisError::new(RedisErrorKind::Config, "Expected centralized config.")),
      };
      let mut transport = connection::create(inner, &server, None).await?;
      transport.setup(inner, None).await?;
      let (server, _writer) = connection::split(inner, transport, false, spawn_reader_task)?;
      inner.notifications.broadcast_reconnect(server);

      *writer = Some(_writer);
      Ok(())
    },
    _ => Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected centralized connection.",
    )),
  }
}
