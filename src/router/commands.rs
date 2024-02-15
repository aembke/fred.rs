use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::{CommandReceiver, RedisClientInner},
  protocol::command::{RedisCommand, ResponseSender, RouterCommand, RouterReceiver, RouterResponse},
  router::{utils, Backpressure, Router, Written},
  types::{ClientState, ClientUnblockFlag, ClusterHash, Server},
  utils as client_utils,
};
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::sync::Arc;
use tokio::sync::oneshot::Sender as OneshotSender;

#[cfg(feature = "transactions")]
use crate::router::transactions;
use crate::{protocol::command::RedisCommandKind, types::Blocking};
#[cfg(feature = "full-tracing")]
use tracing_futures::Instrument;

/// Wait for the response from the reader task, handling cluster redirections if needed.
///
/// The command is returned if it failed to write but could be immediately retried.
///
/// Errors from this function should end the connection task.
async fn handle_router_response(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  rx: Option<RouterReceiver>,
) -> Result<Option<RedisCommand>, RedisError> {
  if let Some(rx) = rx {
    _debug!(inner, "Waiting on router channel.");
    let response = match rx.await {
      Ok(response) => response,
      Err(e) => {
        _warn!(inner, "Dropped router response channel with error: {:?}", e);
        return Ok(None);
      },
    };

    _debug!(inner, "Recv router response.");
    match response {
      RouterResponse::Continue => Ok(None),
      RouterResponse::Ask((slot, server, mut command)) => {
        if let Err(e) = command.decr_check_redirections() {
          command.respond_to_caller(Err(e));
          Ok(None)
        } else {
          utils::send_asking_with_policy(inner, router, &server, slot).await?;
          command.hasher = ClusterHash::Custom(slot);
          command.use_replica = false;
          command.attempts_remaining += 1;
          Ok(Some(command))
        }
      },
      RouterResponse::Moved((slot, server, mut command)) => {
        // check if slot belongs to server, if not then run sync cluster
        if !router.cluster_node_owns_slot(slot, &server) {
          utils::sync_cluster_with_policy(inner, router).await?;
        }

        if let Err(e) = command.decr_check_redirections() {
          command.finish(inner, Err(e));
          Ok(None)
        } else {
          command.hasher = ClusterHash::Custom(slot);
          command.use_replica = false;
          command.attempts_remaining += 1;
          Ok(Some(command))
        }
      },
      RouterResponse::ConnectionClosed((error, command)) => {
        let command = if command.should_finish_with_error(inner) {
          command.finish(inner, Err(error.clone()));
          None
        } else {
          Some(command)
        };

        utils::reconnect_with_policy(inner, router).await?;
        Ok(command)
      },
      RouterResponse::TransactionError(_) | RouterResponse::TransactionResult(_) => {
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

/// Continuously write the command until it is sent, queued to try later, or fails with a fatal error.
async fn write_with_backpressure(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  command: RedisCommand,
  force_pipeline: bool,
) -> Result<(), RedisError> {
  Box::pin(async {
    _trace!(inner, "Writing command: {:?}", command);

    let mut _command: Option<RedisCommand> = Some(command);
    let mut _backpressure: Option<Backpressure> = None;
    loop {
      let mut command = match _command.take() {
        Some(command) => command,
        None => return Err(RedisError::new(RedisErrorKind::Unknown, "Missing command.")),
      };
      if let Err(e) = command.decr_check_attempted() {
        command.finish(inner, Err(e));
        break;
      }

      // apply backpressure first if needed. as a part of that check we may decide to block on the next command.
      let router_rx = match _backpressure {
        Some(backpressure) => match backpressure.wait(inner, &mut command).await {
          Ok(Some(rx)) => Some(rx),
          Ok(None) => {
            if command.should_auto_pipeline(inner, force_pipeline) {
              None
            } else {
              Some(command.create_router_channel())
            }
          },
          Err(e) => {
            command.respond_to_caller(Err(e));
            return Ok(());
          },
        },
        None => {
          if command.should_auto_pipeline(inner, force_pipeline) {
            None
          } else {
            Some(command.create_router_channel())
          }
        },
      };
      let closes_connection = command.kind.closes_connection();
      let is_blocking = command.blocks_connection();
      let use_replica = command.use_replica;

      let result = if use_replica {
        router.write_replica(command, false).await
      } else {
        router.write(command, false).await
      };

      match result {
        Written::Backpressure((mut command, backpressure)) => {
          _debug!(inner, "Recv backpressure again for {}.", command.kind.to_str_debug());
          // backpressure doesn't count as a write attempt
          command.attempts_remaining += 1;
          _command = Some(command);
          _backpressure = Some(backpressure);

          continue;
        },
        Written::Disconnected((server, command, error)) => {
          _debug!(inner, "Handle disconnect for {:?} due to {:?}", server, error);
          let commands = router.connections.disconnect(inner, server.as_ref()).await;
          router.buffer_commands(commands);
          if let Some(command) = command {
            if command.should_finish_with_error(inner) {
              command.finish(inner, Err(error));
            } else {
              router.buffer_command(command);
            }
          }

          utils::defer_reconnect(inner);
          break;
        },
        Written::NotFound(mut command) => {
          if let Err(e) = command.decr_check_redirections() {
            command.finish(inner, Err(e));
            utils::defer_reconnect(inner);
            break;
          }

          _debug!(inner, "Perform cluster sync after missing hash slot lookup.");
          if let Err(error) = router.sync_cluster().await {
            // try to sync the cluster once, and failing that buffer the command.
            _warn!(inner, "Failed to sync cluster after NotFound: {:?}", error);
            utils::defer_reconnect(inner);
            router.buffer_command(command);
            utils::delay_cluster_sync(inner).await?;
            break;
          } else {
            _command = Some(command);
            _backpressure = None;
            continue;
          }
        },
        Written::Ignore => {
          _trace!(inner, "Ignore `Written` response.");
          break;
        },
        Written::SentAll => {
          _trace!(inner, "Sent command to all servers.");
          let _ = router.check_and_flush().await;
          if let Some(command) = handle_router_response(inner, router, router_rx).await? {
            // commands that are sent to all nodes are not retried after a connection closing
            _warn!(inner, "Responding with canceled error after all nodes command failure.");
            command.finish(inner, Err(RedisError::new_canceled()));
            break;
          } else {
            if closes_connection {
              _trace!(inner, "Ending command loop after QUIT or SHUTDOWN.");
              return Err(RedisError::new_canceled());
            }

            break;
          }
        },
        Written::Sent((server, flushed)) => {
          _trace!(inner, "Sent command to {}. Flushed: {}", server, flushed);
          if is_blocking {
            inner.backchannel.write().await.set_blocked(&server);
          }
          if !flushed {
            let _ = router.check_and_flush().await;
          }

          let should_interrupt =
            is_blocking && inner.counters.read_cmd_buffer_len() > 0 && inner.config.blocking == Blocking::Interrupt;
          if should_interrupt {
            // if there's other commands in the queue then interrupt the command that was just sent
            _debug!(inner, "Interrupt after write.");
            if let Err(e) = client_utils::interrupt_blocked_connection(inner, ClientUnblockFlag::Error).await {
              _warn!(inner, "Failed to unblock connection: {:?}", e);
            }
          }

          if let Some(command) = handle_router_response(inner, router, router_rx).await? {
            _command = Some(command);
            _backpressure = None;
            continue;
          } else {
            if closes_connection {
              _trace!(inner, "Ending command loop after QUIT or SHUTDOWN.");
              return Err(RedisError::new_canceled());
            }

            break;
          }
        },
        Written::Error((error, command)) => {
          _debug!(inner, "Fatal error writing command: {:?}", error);
          if let Some(command) = command {
            command.finish(inner, Err(error.clone()));
          }
          inner.notifications.broadcast_error(error.clone());

          utils::defer_reconnect(inner);
          return Err(error);
        },
        #[cfg(feature = "replicas")]
        Written::Fallback(command) => {
          _error!(
            inner,
            "Unexpected replica response to {} ({})",
            command.kind.to_str_debug(),
            command.debug_id()
          );
          command.finish(
            inner,
            Err(RedisError::new(RedisErrorKind::Replica, "Unexpected replica response.")),
          );
          break;
        },
      }
    }

    Ok(())
  })
  .await
}

#[cfg(feature = "full-tracing")]
async fn write_with_backpressure_t(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  mut command: RedisCommand,
  force_pipeline: bool,
) -> Result<(), RedisError> {
  if inner.should_trace() {
    command.take_queued_span();
    let span = fspan!(command, inner.full_tracing_span_level(), "write_command");
    write_with_backpressure(inner, router, command, force_pipeline)
      .instrument(span)
      .await
  } else {
    write_with_backpressure(inner, router, command, force_pipeline).await
  }
}

#[cfg(not(feature = "full-tracing"))]
async fn write_with_backpressure_t(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  command: RedisCommand,
  force_pipeline: bool,
) -> Result<(), RedisError> {
  write_with_backpressure(inner, router, command, force_pipeline).await
}

/// Run a pipelined series of commands, queueing commands to run later if needed.
async fn process_pipeline(
  inner: &Arc<RedisClientInner>,
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

    if let Err(e) = write_with_backpressure_t(inner, router, command, force_pipeline).await {
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
  if let Err(error) = write_with_backpressure_t(inner, router, command, false).await {
    _debug!(inner, "Error sending command after ASKING: {:?}", error);
    Err(error)
  } else {
    Ok(())
  }
}

/// Sync the cluster state then retry the command.
async fn process_moved(
  inner: &Arc<RedisClientInner>,
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
  if let Err(error) = write_with_backpressure_t(inner, router, command, false).await {
    _debug!(inner, "Error sending command after MOVED: {:?}", error);
    Err(error)
  } else {
    Ok(())
  }
}

#[cfg(feature = "replicas")]
async fn process_replica_reconnect(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  server: Option<Server>,
  force: bool,
  tx: Option<ResponseSender>,
  replica: bool,
) -> Result<(), RedisError> {
  if replica {
    let result = utils::sync_replicas_with_policy(inner, router).await;
    if let Some(tx) = tx {
      let _ = tx.send(result.map(|_| Resp3Frame::Null));
    }

    Ok(())
  } else {
    process_reconnect(inner, router, server, force, tx).await
  }
}

/// Reconnect to the server(s).
async fn process_reconnect(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  server: Option<Server>,
  force: bool,
  tx: Option<ResponseSender>,
) -> Result<(), RedisError> {
  _debug!(inner, "Maybe reconnecting to {:?} (force: {})", server, force);

  if let Some(server) = server {
    let has_connection = router.connections.has_server_connection(&server);
    _debug!(inner, "Has working connection: {}", has_connection);

    if has_connection && !force {
      _debug!(inner, "Skip reconnecting to {}", server);
      if let Some(tx) = tx {
        let _ = tx.send(Ok(Resp3Frame::Null));
      }

      return Ok(());
    }
  }

  if !force && router.has_healthy_centralized_connection() {
    _debug!(inner, "Skip reconnecting to centralized host");
    if let Some(tx) = tx {
      let _ = tx.send(Ok(Resp3Frame::Null));
    }
    return Ok(());
  }

  _debug!(inner, "Starting reconnection loop...");
  if let Err(e) = utils::reconnect_with_policy(inner, router).await {
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

#[cfg(feature = "replicas")]
async fn process_sync_replicas(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  tx: OneshotSender<Result<(), RedisError>>,
) -> Result<(), RedisError> {
  let result = utils::sync_replicas_with_policy(inner, router).await;
  let _ = tx.send(result);
  Ok(())
}

/// Sync and update the cached cluster state.
async fn process_sync_cluster(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  tx: OneshotSender<Result<(), RedisError>>,
) -> Result<(), RedisError> {
  let result = utils::sync_cluster_with_policy(inner, router).await;
  let _ = tx.send(result.clone());
  result
}

/// Send a single command to the server(s).
async fn process_normal_command(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  command: RedisCommand,
) -> Result<(), RedisError> {
  write_with_backpressure_t(inner, router, command, false).await
}

/// Read the set of active connections managed by the client.
fn process_connections(
  inner: &Arc<RedisClientInner>,
  router: &Router,
  tx: OneshotSender<Vec<Server>>,
) -> Result<(), RedisError> {
  let connections = router.connections.active_connections();
  _debug!(inner, "Active connections: {:?}", connections);
  let _ = tx.send(connections);
  Ok(())
}

/// Process any kind of router command.
async fn process_command(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  command: RouterCommand,
) -> Result<(), RedisError> {
  match command {
    RouterCommand::Ask { server, slot, command } => process_ask(inner, router, server, slot, command).await,
    RouterCommand::Moved { server, slot, command } => process_moved(inner, router, server, slot, command).await,
    RouterCommand::SyncCluster { tx } => process_sync_cluster(inner, router, tx).await,
    #[cfg(feature = "transactions")]
    RouterCommand::Transaction {
      commands,
      watched,
      id,
      tx,
      abort_on_error,
    } => transactions::run(inner, router, commands, watched, id, abort_on_error, tx).await,
    RouterCommand::Pipeline { commands } => process_pipeline(inner, router, commands).await,
    RouterCommand::Command(command) => process_normal_command(inner, router, command).await,
    RouterCommand::Connections { tx } => process_connections(inner, router, tx),
    #[cfg(feature = "replicas")]
    RouterCommand::SyncReplicas { tx } => process_sync_replicas(inner, router, tx).await,
    #[cfg(not(feature = "replicas"))]
    RouterCommand::Reconnect { server, force, tx } => process_reconnect(inner, router, server, force, tx).await,
    #[cfg(feature = "replicas")]
    RouterCommand::Reconnect {
      server,
      force,
      tx,
      replica,
    } => process_replica_reconnect(inner, router, server, force, tx, replica).await,
  }
}

/// Start processing commands from the client front end.
async fn process_commands(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  rx: &mut CommandReceiver,
) -> Result<(), RedisError> {
  _debug!(inner, "Starting command processing stream...");
  while let Some(command) = rx.recv().await {
    inner.counters.decr_cmd_buffer_len();

    _trace!(inner, "Recv command: {:?}", command);
    if let Err(e) = process_command(inner, router, command).await {
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
pub async fn start(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
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
    let result = Box::pin(process_commands(inner, &mut router, &mut rx)).await;
    inner.store_command_rx(rx, false);
    result
  }
}

#[cfg(feature = "mocks")]
mod mocking {
  use super::*;
  use crate::{modules::mocks::Mocks, protocol::utils as protocol_utils};

  /// Process any kind of router command.
  pub fn process_command(mocks: &Arc<dyn Mocks>, command: RouterCommand) -> Result<(), RedisError> {
    match command {
      #[cfg(feature = "transactions")]
      RouterCommand::Transaction { commands, tx, .. } => {
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
      RouterCommand::Pipeline { commands } => {
        for mut command in commands.into_iter() {
          let mocked = command.to_mocked();
          let result = mocks.process_command(mocked).map(protocol_utils::mocked_value_to_frame);

          command.respond_to_caller(result);
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
    inner: &Arc<RedisClientInner>,
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

  pub async fn start(inner: &Arc<RedisClientInner>, mocks: &Arc<dyn Mocks>) -> Result<(), RedisError> {
    _debug!(inner, "Starting mocking layer");
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
