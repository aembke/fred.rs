use crate::error::{RedisError, RedisErrorKind};
use crate::interfaces;
use crate::interfaces::Resp3Frame;
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::types::ClusterChange;
use crate::multiplexer::Written;
use crate::multiplexer::{responses, utils};
use crate::protocol::command::{ClusterErrorKind, MultiplexerCommand, MultiplexerResponse, RedisCommand};
use crate::protocol::connection::{Counters, RedisWriter, SharedBuffer, SplitRedisStream};
use crate::protocol::responders;
use crate::protocol::responders::ResponseKind;
use crate::protocol::types::ClusterRouting;
use crate::protocol::utils as protocol_utils;
use crate::utils as client_utils;
use arcstr::ArcStr;
use futures::StreamExt;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinHandle;

pub fn find_cluster_node<'a>(
  inner: &Arc<RedisClientInner>,
  state: &'a ClusterRouting,
  command: &RedisCommand,
) -> Option<&'a ArcStr> {
  command
    .cluster_hash()
    .and_then(|slot| state.get_server(slot))
    .or_else(|| {
      let node = state.random_node();
      _trace!(
        inner,
        "Using random cluster node `{:?}` for {}",
        node,
        command.kind.to_str_debug()
      );
      node
    })
}

pub async fn send_command<'a, T>(
  inner: &Arc<RedisClientInner>,
  writers: &mut HashMap<ArcStr, RedisWriter<T>>,
  state: &'a ClusterRouting,
  mut command: RedisCommand,
) -> Result<Written<'a>, (RedisError, RedisCommand)>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  let server = match find_cluster_node(inner, state, &command) {
    Some(server) => server,
    None => {
      // these errors almost always mean the cluster is misconfigured
      _warn!(
        inner,
        "Possible cluster misconfiguration. Missing hash slot owner for {:?}",
        command.cluster_hash()
      );
      command.respond_to_caller(Err(RedisError::new(
        RedisErrorKind::Cluster,
        "Missing cluster hash slot owner.",
      )));
      return Ok(Written::Ignore);
    },
  };

  if let Some(writer) = writers.get_mut(server) {
    _debug!(inner, "Writing command `{}` to {}", command.kind.to_str_debug(), server);
    Ok(utils::write_command(inner, writer, command).await)
  } else {
    // a reconnect message should already be queued from the reader task
    _debug!(
      inner,
      "Failed to read connection {} for {}",
      server,
      command.kind.to_str_debug()
    );
    Err((RedisError::new(RedisErrorKind::IO, "Missing connection."), command))
  }
}

pub fn create_cluster_change<T>(
  cluster_state: &ClusterRouting,
  writers: &HashMap<ArcStr, RedisWriter<T>>,
) -> ClusterChange
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  let mut old_servers = BTreeSet::new();
  let mut new_servers = BTreeSet::new();
  for server in cluster_state.unique_primary_nodes().into_iter() {
    new_servers.insert(server);
  }
  for server in writers.keys() {
    old_servers.insert(server.clone());
  }
  let add = new_servers.difference(&old_servers).map(|s| s.clone()).collect();
  let remove = old_servers.difference(&new_servers).map(|s| s.clone()).collect();

  ClusterChange { add, remove }
}

/// Spawn a task to read response frames from the reader half of the socket.
pub fn spawn_reader_task<T>(
  inner: &Arc<RedisClientInner>,
  mut reader: SplitRedisStream<T>,
  server: &ArcStr,
  buffer: &SharedBuffer,
  counters: &Counters,
) -> JoinHandle<Result<(), RedisError>>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
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
          _debug!(
            inner,
            "Error processing clustered response frame from {}: {:?}",
            server,
            e
          );
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

/// Send a MOVED or ASK command to the multiplexer.
fn process_cluster_error(inner: &Arc<RedisClientInner>, mut command: RedisCommand, frame: Resp3Frame) {
  let (kind, slot, server) = match frame.as_str() {
    Some(data) => match protocol_utils::parse_cluster_error(data) {
      Ok(result) => result,
      Err(e) => {
        command.respond_to_multiplexer(inner, MultiplexerResponse::Continue);
        command.respond_to_caller(Err(e));
        return;
      },
    },
    None => {
      command.respond_to_multiplexer(inner, MultiplexerResponse::Continue);
      command.respond_to_caller(Err(RedisError::new(RedisErrorKind::Protocol, "Invalid cluster error.")));
      return;
    },
  };

  if let Some(tx) = command.multiplexer_tx.take() {
    let response = match kind {
      ClusterErrorKind::Ask => MultiplexerResponse::Ask((slot, server, command)),
      ClusterErrorKind::Moved => MultiplexerResponse::Moved((slot, server, command)),
    };

    _debug!(inner, "Sending cluster error to multiplexer channel.");
    if let Err(response) = tx.send(response) {
      // if it could not be sent on the multiplexer tx then send it on the command channel
      let command = match response {
        MultiplexerResponse::Ask((slot, server, command)) => {
          if command.transaction_id.is_some() {
            _debug!(
              inner,
              "Failed sending ASK cluster error to multiplexer in transaction: {}",
              command.kind.to_str_debug()
            );
            // do not send the command to the command queue
            return;
          } else {
            MultiplexerCommand::Ask { slot, server, command }
          }
        },
        MultiplexerResponse::Moved((slot, server, command)) => {
          if command.transaction_id.is_some() {
            _debug!(
              inner,
              "Failed sending MOVED cluster error to multiplexer in transaction: {}",
              command.kind.to_str_debug()
            );
            // do not send the command to the command queue
            return;
          } else {
            MultiplexerCommand::Moved { slot, server, command }
          }
        },
        _ => {
          _error!(inner, "Invalid cluster error multiplexer response type.");
          return;
        },
      };

      _debug!(inner, "Sending cluster error to command queue.");
      interfaces::send_to_multiplexer(inner, command);
    }
  } else {
    let command = match kind {
      ClusterErrorKind::Ask => MultiplexerCommand::Ask { slot, server, command },
      ClusterErrorKind::Moved => MultiplexerCommand::Moved { slot, server, command },
    };

    _debug!(inner, "Sending cluster error to command queue.");
    interfaces::send_to_multiplexer(inner, command);
  }
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

  if frame.is_moved_or_ask_error() {
    _debug!(
      inner,
      "Recv MOVED or ASK error for `{}` from {}",
      command.kind.to_str_debug(),
      server
    );
    process_cluster_error(inner, command, frame);
    return Ok(());
  }
  if command.transaction_id.is_some() && frame.is_error() {
    // TODO handle errors within a transaction
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
