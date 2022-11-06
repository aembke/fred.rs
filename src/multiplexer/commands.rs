use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::{CommandReceiver, RedisClientInner},
  multiplexer::{transactions, utils, Backpressure, Multiplexer, Written},
  protocol::command::{MultiplexerCommand, MultiplexerReceiver, MultiplexerResponse, RedisCommand, ResponseSender},
  types::{ClientState, ClusterHash},
  utils as client_utils,
};
use arcstr::ArcStr;
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::sync::Arc;

#[cfg(feature = "partial-tracing")]
use tracing_futures::Instrument;

/// Wait for the response from the reader task, handling cluster redirections if needed.
///
/// Returns the command to be retried later if needed.
///
/// Note: This does **not** handle transaction errors.
async fn handle_multiplexer_response(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  rx: Option<MultiplexerReceiver>,
) -> Result<Option<RedisCommand>, RedisError> {
  if let Some(rx) = rx {
    _trace!(inner, "Waiting on multiplexer channel.");
    let response = match rx.await {
      Ok(response) => response,
      Err(e) => {
        _warn!(inner, "Dropped multiplexer response channel with error: {:?}", e);
        return Ok(None);
      },
    };

    match response {
      MultiplexerResponse::Continue => Ok(None),
      MultiplexerResponse::Ask((slot, server, mut command)) => {
        let _ = utils::send_asking_with_policy(inner, multiplexer, &server, slot).await?;
        command.hasher = ClusterHash::Custom(slot);
        Ok(Some(command))
      },
      MultiplexerResponse::Moved((slot, server, mut command)) => {
        // check if slot belongs to server, if not then run sync cluster
        if !multiplexer.cluster_node_owns_slot(slot, &server) {
          let _ = utils::sync_cluster_with_policy(inner, multiplexer).await?;
        }
        command.hasher = ClusterHash::Custom(slot);

        Ok(Some(command))
      },
      MultiplexerResponse::ConnectionClosed((error, mut command)) => {
        let command = if command.attempted >= inner.max_command_attempts() {
          command.respond_to_caller(Err(error.clone()));
          None
        } else {
          Some(command)
        };

        let _ = utils::reconnect_with_policy(inner, multiplexer).await?;
        Ok(command)
      },
      MultiplexerResponse::TransactionError(_) | MultiplexerResponse::TransactionResult(_) => {
        _error!(inner, "Unexpected transaction response. This is a bug.");
        Err(RedisError::new(
          RedisErrorKind::Unknown,
          "Invalid transaction response.",
        ))
      },
    }
  } else {
    Ok(None)
  }
}

/// Continuously write the command until it is sent or fails with a fatal error.
///
/// If the connection closes the command will be queued to run later. The reader task will send a command to reconnect
/// some time later.
async fn write_with_backpressure(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  command: RedisCommand,
  force_pipeline: bool,
) -> Result<(), RedisError> {
  let mut _command: Option<RedisCommand> = Some(command);
  let mut _backpressure: Option<Backpressure> = None;
  loop {
    let mut command = match _command.take() {
      Some(command) => command,
      None => return Err(RedisError::new(RedisErrorKind::Unknown, "Missing command.")),
    };
    // TODO clean this up
    let rx = match _backpressure {
      Some(backpressure) => match backpressure.wait(inner, &mut command).await {
        Ok(Some(rx)) => Some(rx),
        Ok(None) => {
          if command.should_auto_pipeline(inner, force_pipeline) {
            Some(command.create_multiplexer_channel())
          } else {
            None
          }
        },
        Err(e) => {
          command.respond_to_caller(Err(e));
          return Ok(());
        },
      },
      None => {
        if command.should_auto_pipeline(inner, force_pipeline) {
          Some(command.create_multiplexer_channel())
        } else {
          None
        }
      },
    };
    let is_blocking = command.blocks_connection();

    match multiplexer.write_command(command).await {
      Ok(Written::Backpressure((command, backpressure))) => {
        _debug!(inner, "Recv backpressure again for {}.", command.kind.to_str_debug());
        _command = Some(command);
        _backpressure = Some(backpressure);

        continue;
      },
      Ok(Written::Disconnect((server, command, error))) => {
        _debug!(inner, "Handle disconnect backpressure for {} from {:?}", server, error);
        let commands = multiplexer.connections.disconnect(inner, Some(&server)).await;
        multiplexer.buffer.extend(commands);
        if let Some(command) = command {
          multiplexer.buffer.push_back(command);
        }

        break;
      },
      Ok(Written::Ignore) => {
        _trace!(inner, "Ignore `Written` response.");
        break;
      },
      Ok(Written::Sent((server, flushed))) => {
        _trace!(inner, "Sent command to {}. Flushed: {}", server, flushed);
        if is_blocking {
          inner.backchannel.write().await.set_blocked(&server);
        }
        if flushed {
          let _ = multiplexer.check_and_flush().await;
        }

        if let Some(command) = handle_multiplexer_response(inner, multiplexer, rx).await? {
          _command = Some(command);
          _backpressure = None;
          continue;
        } else {
          break;
        }
      },
      Err(e) => return Err(e),
    }
  }

  Ok(())
}

#[cfg(feature = "full-tracing")]
async fn write_with_backpressure_t(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  mut command: RedisCommand,
  force_pipeline: bool,
) -> Result<(), RedisError> {
  if inner.should_trace() {
    command.take_queued_span();
    let span = fspan!(command, "process_command");
    write_with_backpressure(inner, multiplexer, command, force_pipeline)
      .instrument(span)
      .await
  } else {
    write_with_backpressure(inner, multiplexer, command, force_pipeline).await
  }
}

#[cfg(not(feature = "full-tracing"))]
async fn write_with_backpressure_t(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  command: RedisCommand,
  force_pipeline: bool,
) -> Result<(), RedisError> {
  write_with_backpressure(inner, multiplexer, command, force_pipeline).await
}

/// Run a pipelined series of commands, queueing commands to run later if needed.
async fn process_pipeline(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  commands: Vec<RedisCommand>,
) -> Result<(), RedisError> {
  _debug!(inner, "Writing pipeline with {} commands", commands.len());

  for mut command in commands.into_iter() {
    command.can_pipeline = true;
    command.skip_backpressure = true;

    if let Err(e) = write_with_backpressure_t(inner, multiplexer, command, true).await {
      // if the command cannot be written it will be queued to run later.
      // if a connection is dropped due to an error the reader will send a command to reconnect and retry later.
      _debug!(inner, "Error writing command in pipeline: {:?}", e);
    }
  }

  Ok(())
}

/// Send ASKING to the provided server, then retry the provided command.
async fn process_ask(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  server: String,
  slot: u16,
  mut command: RedisCommand,
) -> Result<(), RedisError> {
  command.hasher = ClusterHash::Custom(slot);

  let mut _command = Some(command);
  loop {
    let mut command = match _command.take() {
      Some(command) => command,
      None => {
        _warn!(inner, "Missing command following an ASKING redirect.");
        return Ok(());
      },
    };

    if let Err(e) = utils::send_asking_with_policy(inner, multiplexer, &server, slot).await {
      command.respond_to_caller(Err(e.clone()));
      return Err(e);
    }

    if let Err(e) = command.incr_check_attempted(inner.max_command_attempts()) {
      command.respond_to_caller(Err(e));
      break;
    }
    // TODO fix this for blocking commands
    if let Err((error, command)) = multiplexer.write_direct(command, &server).await {
      _warn!(inner, "Error retrying command after ASKING: {:?}", error);
      _command = Some(command);
      continue;
    } else {
      break;
    }
  }

  Ok(())
}

/// Sync the cluster state then retry the command.
async fn process_moved(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  server: String,
  slot: u16,
  mut command: RedisCommand,
) -> Result<(), RedisError> {
  command.hasher = ClusterHash::Custom(slot);

  let mut _command = Some(command);
  loop {
    let mut command = match _command.take() {
      Some(command) => command,
      None => {
        _warn!(inner, "Missing command following an MOVED redirect.");
        return Ok(());
      },
    };

    if let Err(e) = utils::sync_cluster_with_policy(inner, multiplexer).await {
      command.respond_to_caller(Err(e.clone()));
      return Err(e);
    }

    if let Err(e) = command.incr_check_attempted(inner.max_command_attempts()) {
      command.respond_to_caller(Err(e));
      break;
    }
    // TODO fix this for blocking commands
    if let Err((error, command)) = multiplexer.write_direct(command, &server).await {
      _warn!(inner, "Error retrying command after ASKING: {:?}", error);
      _command = Some(command);
      continue;
    } else {
      break;
    }
  }

  Ok(())
}

/// Reconnect to the server(s).
async fn process_reconnect(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  server: Option<ArcStr>,
  force: bool,
  tx: Option<ResponseSender>,
) -> Result<(), RedisError> {
  _debug!(inner, "Reconnecting to {:?} (force: {})", server, force);

  if let Some(server) = server {
    if multiplexer.connections.has_server_connection(&server) && !force {
      _debug!(inner, "Skip reconnecting to {}", server);
      if let Some(tx) = tx {
        let _ = tx.send(Ok(Resp3Frame::Null));
      }

      return Ok(());
    }
  }

  if let Err(e) = utils::reconnect_with_policy(inner, multiplexer).await {
    if let Some(tx) = tx {
      let _ = tx.send(Err(e.clone()));
    }

    Err(e)
  } else {
    if let Some(tx) = tx {
      let _ = tx.send(Ok(Resp3Frame::Null));
    }

    Ok(())
  }
}

/// Sync and update the cached cluster state.
async fn process_sync_cluster(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
) -> Result<(), RedisError> {
  utils::sync_cluster_with_policy(inner, multiplexer).await
}

/// Send a single command to the server(s).
async fn process_normal_command(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  command: RedisCommand,
) -> Result<(), RedisError> {
  write_with_backpressure_t(inner, multiplexer, command, false).await
}

/// Process any kind of multiplexer command.
async fn process_command(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  command: MultiplexerCommand,
) -> Result<(), RedisError> {
  match command {
    MultiplexerCommand::Ask { server, slot, command } => process_ask(inner, multiplexer, server, slot, command).await,
    MultiplexerCommand::Moved { server, slot, command } => {
      process_moved(inner, multiplexer, server, slot, command).await
    },
    MultiplexerCommand::Reconnect { server, force, tx } => {
      process_reconnect(inner, multiplexer, server, force, tx).await
    },
    MultiplexerCommand::SyncCluster => process_sync_cluster(inner, multiplexer).await,
    MultiplexerCommand::Transaction {
      commands,
      id,
      tx,
      abort_on_error,
    } => transactions::run(inner, multiplexer, commands, id, abort_on_error, tx).await,
    MultiplexerCommand::Pipeline { commands } => process_pipeline(inner, multiplexer, commands).await,
    MultiplexerCommand::Command(command) => process_normal_command(inner, multiplexer, command).await,
  }
}

/// Start processing commands from the client front end.
async fn process_commands(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  rx: &mut CommandReceiver,
) -> Result<(), RedisError> {
  _debug!(inner, "Starting command processing stream...");
  while let Some(command) = rx.recv().await {
    inner.counters.decr_cmd_buffer_len();

    _trace!(inner, "Recv command: {:?}", command);
    if let Err(e) = process_command(inner, multiplexer, command).await {
      // errors on this interface end the client connection task
      _error!(inner, "Disconnecting after error processing command: {:?}", e);
      let _ = multiplexer.disconnect_all().await;
      return Err(e);
    }
  }

  _debug!(inner, "Disconnecting after command stream closes.");
  let _ = multiplexer.disconnect_all().await;
  multiplexer.buffer.clear();
  Ok(())
}

/// Start the command processing stream, initiating new connections in the process.
pub async fn start(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  if !client_utils::check_and_set_client_state(&inner.state, ClientState::Disconnected, ClientState::Connecting) {
    return Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Connections are already initialized or connecting.",
    ));
  }
  inner.reset_reconnection_attempts();
  let mut multiplexer = Multiplexer::new(inner);

  _debug!(inner, "Initializing multiplexer...");
  if inner.config.fail_fast {
    if let Err(e) = multiplexer.connect().await {
      inner.notifications.broadcast_connect(Err(e.clone()));
      inner.notifications.broadcast_error(e.clone());
      return Err(e);
    } else {
      client_utils::set_client_state(&inner.state, ClientState::Connected);
      inner.notifications.broadcast_connect(Ok(()));
      inner.notifications.broadcast_reconnect();
    }
  } else {
    let _ = utils::reconnect_with_policy(inner, &mut multiplexer).await?;
  }

  let mut rx = match inner.take_command_rx() {
    Some(rx) => rx,
    None => {
      return Err(RedisError::new(
        RedisErrorKind::Config,
        "Redis client is already initialized.",
      ))
    },
  };
  let result = process_commands(inner, &mut multiplexer, &mut rx).await;
  inner.store_command_rx(rx);
  result
}
