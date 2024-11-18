use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::{CommandReceiver, RedisClientInner},
  protocol::command::{RedisCommand, RedisCommandKind, ResponseSender, RouterCommand},
  router::{utils, Backpressure, Router, WriteResult},
  runtime::{OneshotSender, RefCount},
  trace,
  types::{Blocking, ClientState, ClientUnblockFlag, ClusterHash, Server},
  utils as client_utils,
};
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;

#[cfg(feature = "transactions")]
use crate::router::transactions;
#[cfg(feature = "full-tracing")]
use tracing_futures::Instrument;

// In the past this logic was abstracted across several more composable async functions, but this was reworked in
// 9.5.0 to use this macro interface. On my machine the compiler was imposing a non-trivial amount of overhead (often
// in the form of __memmove_avx_unaligned_erms() calls) whenever crossing an await point, and each of these async
// functions added one or more memcpy calls. Refactoring the old logic into one giant macro improved writer
// throughput by almost 80%, but it's a lot harder to read.
macro_rules! write_one_command {
  ($inner:ident, $router:ident, $command:ident, $force_pipeline:ident) => {{
    _trace!($inner, "Writing command: {:?}", $command);

    let mut _command: Option<RedisCommand> = Some($command);
    let mut _backpressure: Option<Backpressure> = None;
    loop {
      let mut command = match _command.take() {
        Some(command) => command,
        None => return Err(RedisError::new(RedisErrorKind::Unknown, "Missing command.")),
      };
      if let Err(e) = command.decr_check_attempted() {
        command.respond_to_caller(Err(e));
        break;
      }

      // apply backpressure first if needed
      match _backpressure {
        Some(Backpressure::Wait(duration)) => {
          _debug!($inner, "Backpressure policy (wait): {:?}", duration);
          trace::backpressure_event(&command, Some(duration.as_millis()));
          $inner.wait_with_interrupt(duration).await?;
        },
        Some(Backpressure::Block) => {
          if let Err(error) = $router.drain().await {
            _debug!($inner, "Handle disconnect due to {:?}", error);
            // TODO improve this to only disconnect from the connection that failed to drain
            $router.disconnect_all().await;
            if command.should_finish_with_error($inner) {
              command.respond_to_caller(Err(error.clone()));
              return Ok(());
            } else {
              $router.buffer_command(command);
              return Err(error);
            }
          }
        },
        Some(Backpressure::Error(error)) => {
          command.respond_to_caller(Err(error));
          return Ok(());
        },
      };
      let closes_connection = command.kind.closes_connection();
      let is_blocking = command.blocks_connection();
      let use_replica = command.use_replica;

      let result = if use_replica {
        $router.write_replica(command, false).await
      } else {
        $router.write(command, false).await
      };

      match result {
        WriteResult::Backpressure((mut command, backpressure)) => {
          _debug!($inner, "Recv backpressure again for {}.", command.kind.to_str_debug());
          // backpressure doesn't count as a write attempt
          command.attempts_remaining += 1;
          _command = Some(command);
          _backpressure = Some(backpressure);

          continue;
        },
        WriteResult::Disconnected((server, command, error)) => {
          _debug!($inner, "Handle disconnect for {:?} due to {:?}", server, error);
          let commands = $router.connections.disconnect($inner, server.as_ref()).await;
          $router.buffer_commands(commands);
          if let Some(command) = command {
            if command.should_finish_with_error($inner) {
              command.respond_to_caller(Err(error));
            } else {
              $router.buffer_command(command);
            }
          }

          utils::reconnect_with_policy($inner, $router).await?;
          return Ok(());
        },
        WriteResult::NotFound(mut command) => {
          if let Err(err) = command.decr_check_redirections() {
            command.respond_to_caller(Err(err));
            utils::reconnect_with_policy($inner, $router).await?;
            break;
          } else {
            utils::reconnect_with_policy($inner, $router).await?;
            _command = Some(command);
            _backpressure = None;
            continue;
          }
        },
        WriteResult::Ignore => {
          _trace!($inner, "Ignore `Written` response.");
          break;
        },
        WriteResult::SentAll => {
          _trace!($inner, "Sent command to all servers.");
          let _ = $router.check_and_flush().await;

          if closes_connection {
            _trace!($inner, "Ending command loop after QUIT or SHUTDOWN.");
            return Err(RedisError::new_canceled());
          } else {
            break;
          }
        },
        WriteResult::Sent((server, flushed)) => {
          _trace!($inner, "Sent command to {}. Flushed: {}", server, flushed);
          if is_blocking {
            $inner.backchannel.write().await.set_blocked(&server);
          }
          if !flushed {
            let _ = $router.check_and_flush().await;
          }

          let should_interrupt =
            is_blocking && $inner.counters.read_cmd_buffer_len() > 0 && $inner.config.blocking == Blocking::Interrupt;
          if should_interrupt {
            // if there's other commands in the queue then interrupt the command that was just sent
            _debug!($inner, "Interrupt after write.");
            if let Err(e) = client_utils::interrupt_blocked_connection($inner, ClientUnblockFlag::Error).await {
              _warn!($inner, "Failed to unblock connection: {:?}", e);
            }
          }

          if closes_connection {
            _trace!($inner, "Ending command loop after QUIT or SHUTDOWN.");
            return Err(RedisError::new_canceled());
          } else {
            break;
          }
        },
        WriteResult::Error((error, command)) => {
          _debug!($inner, "Fatal error writing command: {:?}", error);
          if let Some(command) = command {
            command.respond_to_caller(Err(error.clone()));
          }
          $inner.notifications.broadcast_error(error.clone());

          return Err(error);
        },
        #[cfg(feature = "replicas")]
        WriteResult::Fallback(command) => {
          _error!(
            $inner,
            "Unexpected replica response to {} ({})",
            command.kind.to_str_debug(),
            command.debug_id()
          );
          command.respond_to_caller(Err(RedisError::new(
            RedisErrorKind::Replica,
            "Unexpected replica response.",
          )));
          break;
        },
      }
    }

    Ok::<_, RedisError>(())
  }};
}

#[cfg(feature = "full-tracing")]
macro_rules! write_command_t {
  ($inner:ident, $router:ident, $command:ident, $force_pipeline:ident) => {
    if $inner.should_trace() {
      $command.take_queued_span();
      let span = fspan!($command, $inner.full_tracing_span_level(), "fred.write");
      async move { write_one_command!($inner, $router, $command, $force_pipeline) }
        .instrument(span)
        .await
    } else {
      write_one_command!($inner, $router, $command, $force_pipeline)
    }
  };
}

#[cfg(not(feature = "full-tracing"))]
macro_rules! write_command_t {
  ($inner:ident, $router:ident, $command:ident, $force_pipeline:ident) => {
    write_one_command!($inner, $router, $command, $force_pipeline)
  };
}

/// Run a pipelined series of commands, queueing commands to run later if needed.
async fn process_pipeline(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  commands: Vec<RedisCommand>,
) -> Result<(), RedisError> {
  _debug!(inner, "Writing pipeline with {} commands", commands.len());

  for mut command in commands.into_iter() {
    // trying to pipeline `SSUBSCRIBE` is problematic since successful responses arrive out-of-order via pubsub push
    // frames, but error redirections are returned in-order and the client is expected to follow them. this makes it
    // very difficult to accurately associate redirections with `ssubscribe` calls within a pipeline. to avoid this we
    // never pipeline `ssubscribe`, even if the caller asks.
    let force_pipeline = if command.kind == RedisCommandKind::Ssubscribe {
      command.can_pipeline = false;
      false
    } else {
      command.can_pipeline = true;
      !command.is_all_cluster_nodes()
    };
    command.skip_backpressure = true;

    write_command_t!(inner, router, command, force_pipeline)?;
  }

  Ok(())
}

/// Send `ASKING` to the provided server, then retry the provided command.
async fn process_ask(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  server: Server,
  slot: u16,
  mut command: RedisCommand,
) -> Result<(), RedisError> {
  command.use_replica = false;
  command.hasher = ClusterHash::Custom(slot);

  if let Err(e) = command.decr_check_redirections() {
    command.respond_to_caller(Err(e));
    return Ok(());
  }
  if let Err(e) = utils::send_asking_with_policy(inner, router, &server, slot).await {
    command.respond_to_caller(Err(e.clone()));
    return Err(e);
  }

  if let Err(error) = write_command_t!(inner, router, command, false) {
    _debug!(inner, "Error sending command after ASKING: {:?}", error);
    Err(error)
  } else {
    Ok(())
  }
}

/// Sync the cluster state then retry the command.
async fn process_moved(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  server: Server,
  slot: u16,
  mut command: RedisCommand,
) -> Result<(), RedisError> {
  command.use_replica = false;
  command.hasher = ClusterHash::Custom(slot);

  utils::delay_cluster_sync(inner).await?;
  _debug!(inner, "Syncing cluster after MOVED {} {}", slot, server);
  if let Err(e) = utils::sync_cluster_with_policy(inner, router).await {
    command.respond_to_caller(Err(e.clone()));
    return Err(e);
  }
  if let Err(e) = command.decr_check_redirections() {
    command.respond_to_caller(Err(e));
    return Ok(());
  }

  if let Err(error) = write_command_t!(inner, router, command, false) {
    _debug!(inner, "Error sending command after MOVED: {:?}", error);
    Err(error)
  } else {
    Ok(())
  }
}

#[cfg(feature = "replicas")]
async fn process_replica_reconnect(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  server: Option<Server>,
  force: bool,
  tx: Option<ResponseSender>,
  replica: bool,
) -> Result<(), RedisError> {
  #[allow(unused_mut)]
  if replica {
    let result = utils::sync_replicas_with_policy(inner, router, false).await;
    if let Some(mut tx) = tx {
      let _ = tx.send(result.map(|_| Resp3Frame::Null));
    }

    Ok(())
  } else {
    process_reconnect(inner, router, server, force, tx).await
  }
}

/// Reconnect to the server(s).
#[allow(unused_mut)]
async fn process_reconnect(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  server: Option<Server>,
  force: bool,
  tx: Option<ResponseSender>,
) -> Result<(), RedisError> {
  _debug!(inner, "Maybe reconnecting to {:?} (force: {})", server, force);

  if let Some(server) = server {
    let has_connection = router.connections.has_server_connection(&server).await;
    _debug!(inner, "Has working connection: {}", has_connection);

    if has_connection && !force {
      _debug!(inner, "Skip reconnecting to {}", server);
      if let Some(mut tx) = tx {
        let _ = tx.send(Ok(Resp3Frame::Null));
      }

      return Ok(());
    }
  }

  if !force && router.has_healthy_centralized_connection() {
    _debug!(inner, "Skip reconnecting to centralized host");
    if let Some(mut tx) = tx {
      let _ = tx.send(Ok(Resp3Frame::Null));
    }
    return Ok(());
  }

  _debug!(inner, "Starting reconnection loop...");
  if let Err(e) = utils::reconnect_with_policy(inner, router).await {
    if let Some(mut tx) = tx {
      let _ = tx.send(Err(e.clone()));
    }

    Err(e)
  } else {
    if let Some(mut tx) = tx {
      let _ = tx.send(Ok(Resp3Frame::Null));
    }

    Ok(())
  }
}

#[cfg(feature = "replicas")]
#[allow(unused_mut)]
async fn process_sync_replicas(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  mut tx: OneshotSender<Result<(), RedisError>>,
  reset: bool,
) -> Result<(), RedisError> {
  let result = utils::sync_replicas_with_policy(inner, router, reset).await;
  let _ = tx.send(result);
  Ok(())
}

/// Sync and update the cached cluster state.
#[allow(unused_mut)]
async fn process_sync_cluster(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  mut tx: OneshotSender<Result<(), RedisError>>,
) -> Result<(), RedisError> {
  let result = utils::sync_cluster_with_policy(inner, router).await;
  let _ = tx.send(result.clone());
  result
}

/// Start processing commands from the client front end.
async fn process_commands(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  rx: &mut CommandReceiver,
) -> Result<(), RedisError> {
  _debug!(inner, "Starting command processing stream...");
  while let Some(command) = rx.recv().await {
    inner.counters.decr_cmd_buffer_len();

    _trace!(inner, "Recv command: {:?}", command);
    let result = match command {
      RouterCommand::SyncCluster { tx } => process_sync_cluster(inner, router, tx).await,
      #[cfg(feature = "transactions")]
      RouterCommand::Transaction {
        commands,
        id,
        tx,
        abort_on_error,
      } => transactions::exec::pipelined(inner, router, commands, id, tx).await,
      RouterCommand::Pipeline { commands } => process_pipeline(inner, router, commands).await,
      #[allow(unused_mut)]
      RouterCommand::Command(mut command) => write_command_t!(inner, router, command, false),
      #[cfg(feature = "replicas")]
      RouterCommand::SyncReplicas { tx, reset } => process_sync_replicas(inner, router, tx, reset).await,
      #[cfg(not(feature = "replicas"))]
      RouterCommand::Reconnect { server, force, tx } => process_reconnect(inner, router, server, force, tx).await,
      #[cfg(feature = "replicas")]
      RouterCommand::Reconnect {
        server,
        force,
        tx,
        replica,
      } => process_replica_reconnect(inner, router, server, force, tx, replica).await,
    };

    if let Err(e) = result {
      // errors on this interface end the client connection task
      if e.is_canceled() {
        break;
      } else {
        _error!(inner, "Disconnecting after error processing command: {:?}", e);
        let _ = router.disconnect_all().await;
        router.clear_retry_buffer();
        return Err(e);
      }
    }
  }

  _debug!(inner, "Disconnecting after command stream closes.");
  let _ = router.disconnect_all().await;
  router.clear_retry_buffer();
  Ok(())
}

/// Start the command processing stream, initiating new connections in the process.
pub async fn start(inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
  #[cfg(feature = "mocks")]
  if let Some(ref mocks) = inner.config.mocks {
    return mocking::start(inner, mocks).await;
  }

  let mut rx = match inner.take_command_rx() {
    Some(rx) => rx,
    None => {
      // the `_lock` field on inner synchronizes the getters/setters on the command channel halves, so if this field
      // is None then another task must have set and removed the receiver concurrently.
      return Err(RedisError::new(
        RedisErrorKind::Config,
        "Another connection task is already running.",
      ));
    },
  };

  inner.reset_reconnection_attempts();
  let mut router = Router::new(inner);
  _debug!(inner, "Initializing router with policy: {:?}", inner.reconnect_policy());
  let result = if inner.config.fail_fast {
    if let Err(e) = Box::pin(router.connect()).await {
      inner.notifications.broadcast_connect(Err(e.clone()));
      inner.notifications.broadcast_error(e.clone());
      Err(e)
    } else {
      client_utils::set_client_state(&inner.state, ClientState::Connected);
      inner.notifications.broadcast_connect(Ok(()));
      Ok(())
    }
  } else {
    utils::reconnect_with_policy(inner, &mut router).await
  };

  if let Err(error) = result {
    inner.store_command_rx(rx, false);
    Err(error)
  } else {
    #[cfg(feature = "credential-provider")]
    inner.reset_credential_refresh_task();

    let result = Box::pin(process_commands(inner, &mut router, &mut rx)).await;
    inner.store_command_rx(rx, false);
    #[cfg(feature = "credential-provider")]
    inner.abort_credential_refresh_task();
    result
  }
}

#[cfg(feature = "mocks")]
#[allow(unused_mut)]
mod mocking {
  use super::*;
  use crate::{
    modules::mocks::Mocks,
    protocol::{responders::ResponseKind, utils as protocol_utils},
  };
  use redis_protocol::resp3::types::BytesFrame;
  use std::sync::Arc;

  /// Process any kind of router command.
  pub fn process_command(mocks: &Arc<dyn Mocks>, command: RouterCommand) -> Result<(), RedisError> {
    match command {
      #[cfg(feature = "transactions")]
      RouterCommand::Transaction { commands, mut tx, .. } => {
        let mocked = commands.into_iter().skip(1).map(|c| c.to_mocked()).collect();

        match mocks.process_transaction(mocked) {
          Ok(result) => {
            let _ = tx.send(Ok(protocol_utils::mocked_value_to_frame(result)));
            Ok(())
          },
          Err(err) => {
            let _ = tx.send(Err(err));
            Ok(())
          },
        }
      },
      RouterCommand::Pipeline { mut commands } => {
        let mut results = Vec::with_capacity(commands.len());
        let response = commands.last_mut().map(|c| c.take_response());
        let uses_all_results = matches!(response, Some(ResponseKind::Buffer { .. }));
        let tx = response.and_then(|mut k| k.take_response_tx());

        for mut command in commands.into_iter() {
          let result = mocks
            .process_command(command.to_mocked())
            .map(protocol_utils::mocked_value_to_frame);

          results.push(result);
        }
        if let Some(mut tx) = tx {
          let mut frames = Vec::with_capacity(results.len());

          for frame in results.into_iter() {
            match frame {
              Ok(frame) => frames.push(frame),
              Err(err) => {
                frames.push(Resp3Frame::SimpleError {
                  data:       err.details().into(),
                  attributes: None,
                });
              },
            }
          }

          if uses_all_results {
            let _ = tx.send(Ok(BytesFrame::Array {
              data:       frames,
              attributes: None,
            }));
          } else {
            let _ = tx.send(Ok(frames.pop().unwrap_or(BytesFrame::Null)));
          }
        }

        Ok(())
      },
      RouterCommand::Command(mut command) => {
        let result = mocks
          .process_command(command.to_mocked())
          .map(protocol_utils::mocked_value_to_frame);
        command.respond_to_caller(result);

        Ok(())
      },
      _ => Err(RedisError::new(RedisErrorKind::Unknown, "Unimplemented.")),
    }
  }

  pub async fn process_commands(
    inner: &RefCount<RedisClientInner>,
    mocks: &Arc<dyn Mocks>,
    rx: &mut CommandReceiver,
  ) -> Result<(), RedisError> {
    while let Some(command) = rx.recv().await {
      inner.counters.decr_cmd_buffer_len();

      _trace!(inner, "Recv mock command: {:?}", command);
      if let Err(e) = process_command(mocks, command) {
        // errors on this interface end the client connection task
        _error!(inner, "Ending early after error processing mock command: {:?}", e);
        if e.is_canceled() {
          break;
        } else {
          return Err(e);
        }
      }
    }

    Ok(())
  }

  pub async fn start(inner: &RefCount<RedisClientInner>, mocks: &Arc<dyn Mocks>) -> Result<(), RedisError> {
    _debug!(inner, "Starting mocking layer");

    #[cfg(feature = "glommio")]
    glommio::yield_if_needed().await;
    #[cfg(not(feature = "glommio"))]
    tokio::task::yield_now().await;

    let mut rx = match inner.take_command_rx() {
      Some(rx) => rx,
      None => {
        return Err(RedisError::new(
          RedisErrorKind::Config,
          "Redis client is already initialized.",
        ))
      },
    };

    inner.notifications.broadcast_connect(Ok(()));
    let result = process_commands(inner, mocks, &mut rx).await;
    inner.store_command_rx(rx, false);
    result
  }
}
