use crate::error::RedisErrorKind;
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::responses;
use crate::multiplexer::utils;
use crate::multiplexer::Written;
use crate::prelude::{RedisError, Resp3Frame};
use crate::protocol::command::{MultiplexerResponse, RedisCommand, ResponseSender};
use crate::protocol::connection::{
  Counters, RedisReader, RedisWriter, SharedBuffer, SplitRedisStream, SplitStreamKind,
};
use crate::protocol::responders::{self, ResponseKind};
use crate::protocol::types::{KeyScanInner, ValueScanInner};
use crate::protocol::utils as protocol_utils;
use arcstr::ArcStr;
use futures::StreamExt;
use parking_lot::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;

pub async fn send_command(
  inner: &Arc<RedisClientInner>,
  writer: &mut Option<RedisWriter>,
  mut command: RedisCommand,
) -> Result<Written, (RedisError, RedisCommand)> {
  if let Some(writer) = writer.as_mut() {
    Ok(utils::write_command(inner, writer, command).await)
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
    while let Some(frame) = reader.next().await {
      let frame = match frame {
        Ok(frame) => frame.into_resp3(),
        Err(error) => {
          responses::handle_reader_error(&inner, &server, error);
          return Err(RedisError::new_canceled());
        },
      };

      if let Some(error) = responses::check_special_errors(&inner, &frame) {
        responses::handle_reader_error(&inner, &server, error);
        return Err(RedisError::new_canceled());
      }
      if let Some(frame) = responses::check_pubsub_message(&inner, frame) {
        if let Err(e) = process_response_frame(&inner, &server, &buffer, &counters, frame).await {
          _debug!(inner, "Error processing response frame from {}: {:?}", server, e);
        }
      }
    }

    // check and emit a reconnection if the reader stream closes without an error
    if inner.should_reconnect() {
      inner.send_reconnect(Some(server.clone()), false);
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
  server: &ArcStr,
  buffer: &SharedBuffer,
  counters: &Counters,
  frame: Resp3Frame,
) -> Result<(), RedisError> {
  let mut command = {
    match buffer.lock().await.pop_front() {
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

  if command.transaction_id.is_some() && frame.is_error() && !command.kind.ends_transaction() {
    if let Some(error) = protocol_utils::frame_to_error(&frame) {
      if let Some(tx) = command.multiplexer_tx.take() {
        _debug!(
          inner,
          "Send transaction error to multiplexer for {}",
          command.kind.to_str_debug()
        );
        let _ = tx.send(MultiplexerResponse::TransactionError((error, command)));
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
        // the `Multiple` policy works by processing a series of responses on the same connection. the response channel
        // is not shared across commands (since there's only one), so we re-queue it while waiting on response frames.
        buffer.lock().await.push_front(command);
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
