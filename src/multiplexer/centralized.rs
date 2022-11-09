use crate::{
  error::RedisErrorKind,
  modules::inner::RedisClientInner,
  multiplexer::{responses, utils, Connections, Written},
  prelude::{RedisError, Resp3Frame},
  protocol::{
    command::{MultiplexerResponse, RedisCommand},
    connection,
    connection::{CommandBuffer, Counters, RedisTransport, RedisWriter, SharedBuffer, SplitStreamKind},
    responders::{self, ResponseKind},
    utils as protocol_utils,
  },
  types::ServerConfig,
};
use arcstr::ArcStr;
use futures::TryStreamExt;
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;

#[cfg(feature = "replicas")]
use crate::protocol::command::RedisCommandKind;

pub async fn send_command(
  inner: &Arc<RedisClientInner>,
  writer: &mut Option<RedisWriter>,
  command: RedisCommand,
) -> Result<Written, (RedisError, RedisCommand)> {
  if let Some(writer) = writer.as_mut() {
    Ok(utils::write_command(inner, writer, command, false).await)
  } else {
    _debug!(inner, "Failed to read connection for {}", command.kind.to_str_debug());
    Err((RedisError::new(RedisErrorKind::IO, "Missing connection."), command))
  }
}

/// Spawn a task to read response frames from the reader half of the socket.
#[allow(unused_assignments)]
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
  counters.decr_in_flight();
  responses::check_and_set_unblocked_flag(inner, &command).await;

  if command.transaction_id.is_some() {
    if let Some(error) = protocol_utils::frame_to_error(&frame) {
      if let Some(tx) = command.take_multiplexer_tx() {
        let _ = tx.send(MultiplexerResponse::TransactionError((error, command)));
      }
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

  _trace!(inner, "Handling centralized response kind: {:?}", command.response);
  match command.take_response() {
    ResponseKind::Skip | ResponseKind::Respond(None) => {
      command.respond_to_multiplexer(inner, MultiplexerResponse::Continue);
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
    } => responders::respond_buffer(inner, server, command, received, expected, frames, index, tx, frame),
    ResponseKind::KeyScan(scanner) => responders::respond_key_scan(inner, server, command, scanner, frame),
    ResponseKind::ValueScan(scanner) => responders::respond_value_scan(inner, server, command, scanner, frame),
  }
}

/// Read the mapping of replica nodes to primary nodes via the `INFO replication` command.
#[cfg(feature = "replicas")]
pub async fn sync_replicas(
  inner: &Arc<RedisClientInner>,
  transport: &mut RedisTransport,
) -> Result<HashMap<ArcStr, ArcStr>, RedisError> {
  if !inner.config.use_replicas {
    _debug!(inner, "Skip syncing replicas.");
    return Ok(HashMap::new());
  }
  _debug!(inner, "Syncing replicas for {}", transport.server);

  let command = RedisCommand::new(RedisCommandKind::Info, vec!["replication".into()]);
  let frame = match transport.request_response(command, inner.is_resp3()).await {
    Ok(frame) => match frame.as_str() {
      Some(s) => s.to_owned(),
      None => {
        _debug!(inner, "Invalid non-string response syncing replicas.");
        return Ok(HashMap::new());
      },
    },
    Err(e) => {
      _debug!(inner, "Failed reading replicas from {}: {}", transport.server, e);
      return Ok(HashMap::new());
    },
  };

  let mut replicas = HashMap::new();
  for line in frame.lines() {
    if line.trim().starts_with("slave") {
      let values = match line.split(":").last() {
        Some(values) => values,
        None => continue,
      };

      let parts: Vec<&str> = values.split(",").collect();
      if parts.len() < 2 {
        continue;
      }

      let (mut host, mut port) = (None, None);
      for kv in parts.into_iter() {
        let parts: Vec<&str> = kv.split("=").collect();
        if parts.len() != 2 {
          continue;
        }

        if &parts[0] == "ip" {
          host = Some(parts[1].to_owned());
        } else if &parts[0] == "port" {
          port = parts[1].parse::<u16>().ok();
        }
      }

      if let Some(host) = host {
        if let Some(port) = port {
          replicas.insert(ArcStr::from(format!("{}:{}", host, port)), transport.server.clone());
        }
      }
    }
  }

  _debug!(
    inner,
    "Read centralized replicas from {}: {:?}",
    transport.server,
    replicas
  );
  Ok(replicas)
}

// TODO finish implementing replica support
#[allow(dead_code)]
#[cfg(not(feature = "replicas"))]
pub async fn sync_replicas(
  inner: &Arc<RedisClientInner>,
  _: &mut RedisTransport,
) -> Result<HashMap<ArcStr, ArcStr>, RedisError> {
  _trace!(inner, "Skip syncing replicas.");
  Ok(HashMap::new())
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

      // let replicas = sync_replicas(inner, &mut transport).await?;
      // TODO set up replicas on multiplexer
      // inner.update_replicas(replicas);

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
