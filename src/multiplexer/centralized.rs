use crate::{
  error::RedisErrorKind,
  modules::inner::RedisClientInner,
  multiplexer::{responses, utils, Connections, Written},
  prelude::{RedisError, Resp3Frame},
  protocol::{
    command::{MultiplexerResponse, RedisCommand, ResponseSender},
    connection,
    connection::{
      CommandBuffer,
      Counters,
      RedisReader,
      RedisWriter,
      SharedBuffer,
      SplitRedisStream,
      SplitStreamKind,
    },
    responders::{self, ResponseKind},
    types::{KeyScanInner, ValueScanInner},
    utils as protocol_utils,
  },
  types::ServerConfig,
};
use arcstr::ArcStr;
use futures::TryStreamExt;
use parking_lot::Mutex;
use std::sync::{atomic::AtomicUsize, Arc};
use tokio::{
  io::{AsyncRead, AsyncWrite},
  task::JoinHandle,
};

pub async fn send_command(
  inner: &Arc<RedisClientInner>,
  writer: &mut Option<RedisWriter>,
  mut command: RedisCommand,
) -> Result<Written, (RedisError, RedisCommand)> {
  if let Some(writer) = writer.as_mut() {
    Ok(utils::write_command(inner, writer, command, false).await)
  } else {
    _debug!(
      inner,
      "Failed to read connection {} for {}",
      server,
      command.kind.to_str_debug()
    );
    Err((RedisError::new(RedisErrorKind::IO, "Missing connection."), command))
  }
}

/// Spawn a task to read response frames from the reader half of the socket.
pub fn spawn_reader_task(
  inner: &Arc<RedisClientInner>,
  mut reader: SplitStreamKind,
  server: &ArcStr,
  buffer: &SharedBuffer,
  counters: &Counters,
) -> JoinHandle<Result<(), RedisError>> {
  let (inner, server) = (inner.clone(), server.clone());
  let (buffer, counters) = (buffer.clone(), counters.clone());

  tokio::spawn(async move {
    let mut last_error = None;
    loop {
      let frame = match reader.try_next().await {
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
      if let Some(frame) = responses::check_pubsub_message(&inner, frame) {
        if let Err(e) = process_response_frame(&inner, &server, &buffer, &counters, frame).await {
          _debug!(inner, "Error processing response frame from {}: {:?}", server, e);
          last_error = Some(e);
          break;
        }
      }
    }

    utils::check_blocked_multiplexer(&inner, &buffer, &last_error);
    utils::check_final_write_attempt(&inner, &buffer, &last_error);
    responses::handle_reader_error(&inner, &server, last_error);

    _debug!(inner, "Ending reader task from {}", server);
    Ok(())
  })
}

/// Process the response frame in the context of the last command.
///
/// Errors returned here will be logged, but will not close the socket or initiate a reconnect.
pub async fn process_response_frame(
  inner: &Arc<RedisClientInner>,
  server: &ArcStr,
  buffer: &SharedBuffer,
  counters: &Counters,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  let mut command = {
    match buffer.lock().pop_front() {
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
    }
  };
  counters.decr_in_flight();

  if command.transaction_id.is_some() {
    if let Some(error) = protocol_utils::frame_to_error(&frame) {
      command.respond_to_multiplexer(inner, MultiplexerResponse::TransactionError((error, command)));
      return Ok(());
    } else {
      if command.kind.ends_transaction() {
        command.respond_to_multiplexer(inner, MultiplexerResponse::TransactionResult(frame));
        return Ok(());
      } else {
        command.respond_to_multiplexer(inner, MultiplexerResponse::Continue);
        return Ok(());
      }
    }
  }

  match command.take_response() {
    ResponseKind::Skip | ResponseKind::Respond(None) => {
      command.respond_to_multiplexer(inner, MultiplexerResponse::Continue);
      Ok(())
    },
    ResponseKind::Respond(Some(tx)) => responders::respond_to_caller(inner, server, command, tx, frame),
    ResponseKind::Multiple { received, expected, tx } => {
      if let Some(command) = responders::respond_multiple(inner, server, command, received, expected, tx, frame)? {
        // the `Multiple` policy works by processing a series of responses on the same connection. the response
        // channel is not shared across commands (since there's only one), so we re-queue it while waiting on
        // response frames.
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
    } => responders::respond_buffer(inner, server, command, received, expected, frames, index, tx, frame),
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
      let (host, port) = match inner.config.server {
        ServerConfig::Centralized { ref host, ref port } => (host.to_owned(), *port),
        _ => return Err(RedisError::new(RedisErrorKind::Config, "Expected centralized config.")),
      };
      let mut transport = connection::create(inner, host, port, None).await?;
      let _ = transport.setup(inner).await?;
      let (_, _writer) = connection::split_and_initialize(inner, transport, spawn_reader_task)?;

      *writer = Some(_writer);
      Ok(())
    },
    _ => Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected centralized connection.",
    )),
  }
}
