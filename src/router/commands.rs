use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::{CommandReceiver, RedisClientInner},
  protocol::command::{RedisCommand, RedisCommandKind, ResponseSender, RouterCommand},
  router::{clustered, utils, Router},
  runtime::{OneshotSender, RefCount},
  types::{Blocking, ClientState, ClientUnblockFlag, ClusterHash, Server},
  utils as client_utils,
};
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;

#[cfg(feature = "replicas")]
use crate::interfaces;
#[cfg(feature = "transactions")]
use crate::router::transactions;
#[cfg(feature = "full-tracing")]
use tracing_futures::Instrument;

#[cfg(feature = "replicas")]
async fn create_replica_connection(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  mut command: RedisCommand,
) -> Result<(), RedisError> {
  if command.use_replica && inner.connection.replica.lazy_connections {
    let (primary, replica) = match utils::route_replica(router, &command) {
      Ok((primary, replica)) => (primary, replica),
      Err(err) => {
        if inner.connection.replica.primary_fallback {
          command.attempts_remaining += 1;
          command.use_replica = false;
          interfaces::send_to_router(inner, command.into())?;
        } else {
          command.respond_to_caller(Err(err));
        }

        return Ok(());
      },
    };

    if let Err(err) = utils::add_replica_with_policy(inner, router, &primary, &replica).await {
      if inner.connection.replica.ignore_reconnection_errors {
        _warn!(
          inner,
          "Failed to connect to replica, ignoring and trying with primary node: {}",
          err
        );
        command.attempts_remaining += 1;
        command.use_replica = false;
        interfaces::send_to_router(inner, command.into())
      } else {
        command.respond_to_caller(Err(err.clone()));
        Err(err)
      }
    } else {
      // connected successfully
      command.attempts_remaining += 1;
      interfaces::send_to_router(inner, command.into())
    }
  } else if command.use_replica && !inner.connection.replica.lazy_connections {
    // connection does not exist and the client is not configured to create more
    if inner.connection.replica.primary_fallback {
      command.attempts_remaining += 1;
      command.use_replica = false;
      interfaces::send_to_router(inner, command.into())?;
    } else {
      command.respond_to_caller(Err(RedisError::new(
        RedisErrorKind::Routing,
        "Failed to route command to replica.",
      )));
    }
    Ok(())
  } else {
    // connection does not exist
    _debug!(inner, "Failed to route command to replica. Deferring reconnection...");
    let err = RedisError::new(RedisErrorKind::Routing, "Failed to route command.");
    utils::defer_reconnection(inner, router, None, err, false)?;
    command.attempts_remaining += 1;
    router.retry_command(command);
    Ok(())
  }
}

/// Write the command to a connection.
async fn write_command(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  mut command: RedisCommand,
) -> Result<(), RedisError> {
  _trace!(inner, "Writing command: {:?} ({})", command, command.debug_id());
  if let Err(err) = command.decr_check_attempted() {
    command.respond_to_caller(Err(err));
    return Ok(());
  }
  let closes_connection = command.kind.closes_connection();
  let is_blocking = command.blocks_connection();
  #[cfg(feature = "replicas")]
  let use_replica = command.use_replica;
  #[cfg(not(feature = "replicas"))]
  let use_replica = false;

  if closes_connection {
    router.drain_all(inner).await?;
  }
  // TODO refactor this
  let (flush, disconnect_from) = {
    if command.is_all_cluster_nodes() {
      if let Err(err) = router.drain_all(inner).await {
        router.disconnect_all(inner).await;
        router.retry_command(command);
        utils::defer_reconnection(inner, router, None, err, use_replica)?;
        (false, None)
      } else {
        if let Err(err) = clustered::send_all_cluster_command(inner, router, command).await {
          router.disconnect_all(inner).await;
          utils::defer_reconnection(inner, router, None, err, use_replica)?;
        }

        (false, None)
      }
    } else {
      let conn = match router.route(&command) {
        Some(conn) => conn,
        None => {
          #[cfg(feature = "replicas")]
          return Box::pin(create_replica_connection(inner, router, command)).await;

          #[cfg(not(feature = "replicas"))]
          {
            let err = RedisError::new(RedisErrorKind::Unknown, "Failed to route command.");
            utils::defer_reconnection(inner, router, None, err, use_replica)?;
            router.retry_command(command);
            return Ok(());
          }
        },
      };

      match utils::write_command(inner, conn, command, false).await {
        Ok(flushed) => {
          _trace!(inner, "Sent command to {}. Flushed: {}", conn.server, flushed);
          if is_blocking {
            inner.backchannel.set_blocked(&conn.server);
          }
          if !flushed && inner.counters.read_cmd_buffer_len() == 0 {
            let _ = conn.flush().await;
          }

          // interrupt the command that was just sent if another command is queued after sending this one
          let should_interrupt =
            is_blocking && inner.counters.read_cmd_buffer_len() > 0 && inner.config.blocking == Blocking::Interrupt;
          if should_interrupt {
            let _ = conn.flush().await;
            _debug!(inner, "Interrupt after write.");
            if let Err(e) = client_utils::interrupt_blocked_connection(inner, ClientUnblockFlag::Error).await {
              _warn!(inner, "Failed to unblock connection: {:?}", e);
            }
          }

          if closes_connection {
            _trace!(inner, "Ending command loop after QUIT or SHUTDOWN.");
            return Err(RedisError::new_canceled());
          } else {
            (flushed, None)
          }
        },
        Err((err, command)) => (false, Some((conn.server.clone(), err, command))),
      }
    }
  };

  if flush {
    if let Err(err) = router.flush().await {
      _debug!(inner, "Failed to flush connections: {:?}", err);
    }
  }
  if let Some((server, err, command)) = disconnect_from {
    if let Some(command) = command {
      router.retry_command(command);
    }
    utils::drop_connection(inner, router, &server, &err).await;
    utils::defer_reconnection(inner, router, None, err, use_replica)
  } else {
    Ok(())
  }
}

#[cfg(feature = "full-tracing")]
macro_rules! write_command_t {
  ($inner:ident, $router:ident, $command:ident) => {
    if $inner.should_trace() {
      $command.take_queued_span();
      let span = fspan!($command, $inner.full_tracing_span_level(), "fred.write");
      Box::pin(write_command($inner, $router, $command))
        .instrument(span)
        .await
    } else {
      Box::pin(write_command($inner, $router, $command)).await
    }
  };
}

#[cfg(not(feature = "full-tracing"))]
macro_rules! write_command_t {
  ($inner:ident, $router:ident, $command:ident) => {
    Box::pin(write_command($inner, $router, $command)).await
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
    // difficult to accurately associate redirections with `ssubscribe` calls within a pipeline. to avoid this we
    // never pipeline `ssubscribe`, even if the caller asks.
    command.can_pipeline = command.kind != RedisCommandKind::Ssubscribe;
    command.skip_backpressure = true;

    write_command_t!(inner, router, command)?;
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
  let attempts_remaining = command.attempts_remaining;
  let asking_result = Box::pin(utils::send_asking_with_policy(
    inner,
    router,
    &server,
    slot,
    attempts_remaining,
  ))
  .await;
  if let Err(e) = asking_result {
    command.respond_to_caller(Err(e.clone()));
    return Err(e);
  }

  if let Err(error) = write_command_t!(inner, router, command) {
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

  utils::delay_cluster_sync(inner, router).await?;
  _debug!(inner, "Syncing cluster after MOVED {} {}", slot, server);
  if let Err(e) = utils::sync_cluster_with_policy(inner, router).await {
    command.respond_to_caller(Err(e.clone()));
    return Err(e);
  }
  if let Err(e) = command.decr_check_redirections() {
    command.respond_to_caller(Err(e));
    return Ok(());
  }

  if let Err(error) = write_command_t!(inner, router, command) {
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
  router.reset_pending_reconnection(server.as_ref());

  #[allow(unused_mut)]
  if replica {
    let result = utils::sync_replicas_with_policy(inner, router, false).await;
    if let Some(mut tx) = tx {
      let _ = tx.send(result.map(|_| Resp3Frame::Null));
    }

    Ok(())
  } else {
    Box::pin(process_reconnect(inner, router, server, force, tx)).await
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
  router.reset_pending_reconnection(server.as_ref());

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

  if !force && router.has_healthy_centralized_connection().await {
    _debug!(inner, "Skip reconnecting to centralized host");
    if let Some(mut tx) = tx {
      let _ = tx.send(Ok(Resp3Frame::Null));
    }
    return Ok(());
  }

  _debug!(inner, "Starting reconnection loop...");
  if let Err(e) = Box::pin(utils::reconnect_with_policy(inner, router)).await {
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
async fn process_command(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  command: RouterCommand,
) -> Result<(), RedisError> {
  inner.counters.decr_cmd_buffer_len();

  _trace!(inner, "Recv command: {:?}", command);
  match command {
    RouterCommand::SyncCluster { tx } => process_sync_cluster(inner, router, tx).await,
    #[cfg(feature = "transactions")]
    RouterCommand::Transaction {
      commands,
      id,
      tx,
      abort_on_error,
    } => Box::pin(transactions::send(inner, router, commands, id, abort_on_error, tx)).await,
    RouterCommand::Pipeline { commands } => process_pipeline(inner, router, commands).await,
    #[allow(unused_mut)]
    RouterCommand::Command(mut command) => write_command_t!(inner, router, command),
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
    RouterCommand::Ask { server, slot, command } => process_ask(inner, router, server, slot, command).await,
    RouterCommand::Moved { command, server, slot } => process_moved(inner, router, server, slot, command).await,
  }
}

/// Try to read frames from any socket, otherwise try to write the next command.
async fn read_or_write(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  rx: &mut CommandReceiver,
) -> Result<(), RedisError> {
  // The most complicated part of the main client command loop is implemented in this function.
  //
  // In the past `fred` worked by spawning a separate task for each connection such that the Tokio scheduler could
  // read from all sockets concurrently. Unfortunately this introduced significant overhead in the scheduling
  // layer (via a huge number of calls to `next_expiration` within Tokio) and indirectly via the added message passing
  // communication mechanisms required between reader tasks and the writer task.
  //
  // In 9.5.0 the routing layer was reworked to operate on both readers and writers within a single task. This
  // increased throughput by 2-3x on the happy path, but requires some `select` shenanigans so that the client can
  // appear to operate on readers and writers concurrently.
  //
  // This function is called in a loop and drives futures that concurrently read and write to sockets.
  tokio::select! {
    biased;
    results = router.select_read(inner) => {
      for (server, result) in results.into_iter() {
        utils::process_response(inner, router, &server, result).await?;
      }
    },
    Some(command) = rx.recv() => {
      process_command(inner, router, command).await?;
    },
  };

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
    if let Err(e) = Box::pin(router.connect(inner)).await {
      inner.notifications.broadcast_connect(Err(e.clone()));
      inner.notifications.broadcast_error(e.clone());
      Err(e)
    } else {
      client_utils::set_client_state(&inner.state, ClientState::Connected);
      inner.notifications.broadcast_connect(Ok(()));
      Ok(())
    }
  } else {
    Box::pin(utils::reconnect_with_policy(inner, &mut router)).await
  };

  if let Err(error) = result {
    inner.store_command_rx(rx, false);
    Err(error)
  } else {
    #[cfg(feature = "credential-provider")]
    inner.reset_credential_refresh_task();

    let mut result = Ok(());
    loop {
      if let Err(err) = read_or_write(inner, &mut router, &mut rx).await {
        _debug!(inner, "Error processing command: {:?}", err);
        router.clear_retry_buffer();
        let _ = router.disconnect_all(inner).await;

        if !err.is_canceled() {
          result = Err(err);
        }
        break;
      }
    }
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
