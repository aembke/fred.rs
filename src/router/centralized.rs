use crate::{
  error::RedisErrorKind,
  modules::inner::RedisClientInner,
  prelude::{RedisError, Resp3Frame},
  protocol::{
    command::{RedisCommand, RouterResponse},
    connection,
    connection::{CommandBuffer, Counters, RedisWriter, SharedBuffer, SplitStreamKind},
    responders::{self, ResponseKind},
    types::Server,
    utils as protocol_utils,
  },
  router::{responses, utils, Connections, Written},
  types::ServerConfig,
};
use std::sync::Arc;
use tokio::task::JoinHandle;

pub async fn send_command(
  inner: &Arc<RedisClientInner>,
  writer: &mut Option<RedisWriter>,
  command: RedisCommand,
  force_flush: bool,
) -> Result<Written, (RedisError, RedisCommand)> {
  if let Some(writer) = writer.as_mut() {
    Ok(utils::write_command(inner, writer, command, force_flush).await)
  } else {
    _debug!(inner, "Failed to read connection for {}", command.kind.to_str_debug());
    Ok(Written::Disconnect((
      None,
      Some(command),
      RedisError::new(RedisErrorKind::IO, "Missing connection."),
    )))
  }
}

/// Spawn a task to read response frames from the reader half of the socket.
#[allow(unused_assignments)]
pub fn spawn_reader_task(
  inner: &Arc<RedisClientInner>,
  mut reader: SplitStreamKind,
  server: &Server,
  buffer: &SharedBuffer,
  counters: &Counters,
  is_replica: bool,
) -> JoinHandle<Result<(), RedisError>> {
  let (inner, server) = (inner.clone(), server.clone());
  let (buffer, counters) = (buffer.clone(), counters.clone());

  tokio::spawn(async move {
    let mut last_error = None;
    let mut rx = utils::reader_subscribe(&inner, &server);

    loop {
      let frame = match utils::next_frame(&inner, &mut reader, &server, &mut rx).await {
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

    utils::reader_unsubscribe(&inner, &server);
    utils::check_blocked_router(&inner, &buffer, &last_error);
    utils::check_final_write_attempt(&buffer, &last_error);
    if is_replica {
      responses::broadcast_replica_error(&inner, &server, last_error);
    } else {
      responses::broadcast_reader_error(&inner, &server, last_error);
    }

    _debug!(inner, "Ending reader task from {}", server);
    Ok(())
  })
}

/// Process the response frame in the context of the last command.
///
/// Errors returned here will be logged, but will not close the socket or initiate a reconnect.
pub async fn process_response_frame(
  inner: &Arc<RedisClientInner>,
  server: &Server,
  buffer: &SharedBuffer,
  counters: &Counters,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  _trace!(inner, "Parsing response frame from {}", server);
  let mut command = {
    let mut guard = buffer.lock();

    let command = match guard.pop_front() {
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

    if utils::should_drop_extra_pubsub_frame(inner, &command, &frame) {
      guard.push_front(command);
      return Ok(());
    } else {
      command
    }
  };
  _trace!(
    inner,
    "Checking response to {} ({})",
    command.kind.to_str_debug(),
    command.debug_id()
  );
  counters.decr_in_flight();
  responses::check_and_set_unblocked_flag(inner, &command).await;

  if command.transaction_id.is_some() {
    if let Some(error) = protocol_utils::frame_to_error(&frame) {
      if let Some(tx) = command.take_router_tx() {
        let _ = tx.send(RouterResponse::TransactionError((error, command)));
      }
      return Ok(());
    } else {
      if command.kind.ends_transaction() {
        command.respond_to_router(inner, RouterResponse::TransactionResult(frame));
        return Ok(());
      } else {
        command.respond_to_router(inner, RouterResponse::Continue);
        return Ok(());
      }
    }
  }

  _trace!(inner, "Handling centralized response kind: {:?}", command.response);
  match command.take_response() {
    ResponseKind::Skip | ResponseKind::Respond(None) => {
      command.respond_to_router(inner, RouterResponse::Continue);
      Ok(())
    },
    ResponseKind::Respond(Some(tx)) => responders::respond_to_caller(inner, server, command, tx, frame),
    ResponseKind::Multiple { received, expected, tx } => {
      if let Some(command) = responders::respond_multiple(inner, server, command, received, expected, tx, frame)? {
        // the `Multiple` policy works by processing a series of responses on the same connection, so we put it back
        // at the front of the queue. the inner logic reconstructs the responder state if necessary.
        buffer.lock().push_front(command);
        counters.incr_in_flight();
      }

      Ok(())
    },
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
  inner: &Arc<RedisClientInner>,
  connections: &mut Connections,
  buffer: &mut CommandBuffer,
) -> Result<(), RedisError> {
  _debug!(inner, "Initializing centralized connection.");
  let commands = connections.disconnect_all(inner).await;
  buffer.extend(commands);

  match connections {
    Connections::Centralized { writer, .. } => {
      let server = match inner.config.server {
        ServerConfig::Centralized { ref server } => server.clone(),
        _ => return Err(RedisError::new(RedisErrorKind::Config, "Expected centralized config.")),
      };
      let mut transport = connection::create(
        inner,
        server.host.as_str().to_owned(),
        server.port,
        None,
        server.get_tls_server_name(),
      )
      .await?;
      let _ = transport.setup(inner, None).await?;

      let (_, _writer) = connection::split_and_initialize(inner, transport, false, spawn_reader_task)?;
      *writer = Some(_writer);
      Ok(())
    },
    _ => Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected centralized connection.",
    )),
  }
}
