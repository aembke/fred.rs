use crate::{
  error::{Error, ErrorKind},
  modules::inner::{ClientInner, CommandReceiver},
  protocol::command::{Command, CommandKind, ResponseSender, RouterCommand},
  router::{clustered, utils, ReconnectServer, Router},
  runtime::{sleep, OneshotSender, RefCount},
  types::{
    config::{Blocking, Server},
    ClientState,
    ClientUnblockFlag,
    ClusterHash,
  },
  utils as client_utils,
};
use redis_protocol::resp3::types::BytesFrame as Resp3Frame;
use std::{
  mem,
  ops::{Deref, DerefMut},
};
use tokio::pin;

#[cfg(feature = "replicas")]
use crate::interfaces;
#[cfg(feature = "transactions")]
use crate::router::transactions;
#[cfg(feature = "full-tracing")]
use tracing_futures::Instrument;

#[cfg(feature = "replicas")]
async fn create_replica_connection(
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  mut command: Command,
) -> Result<(), Error> {
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
      command.respond_to_caller(Err(Error::new(
        ErrorKind::Routing,
        "Failed to route command to replica.",
      )));
    }
    Ok(())
  } else {
    // connection does not exist
    _debug!(inner, "Failed to route command to replica. Deferring reconnection...");
    let err = Error::new(ErrorKind::Routing, "Failed to route command.");
    command.attempts_remaining += 1;
    finish_or_retry_command(router, command, &err);
    utils::defer_reconnection(inner, router, None, err, false)?;
    Ok(())
  }
}

fn finish_or_retry_command(router: &mut Router, mut command: Command, error: &Error) {
  if command.attempts_remaining == 0 {
    command.respond_to_caller(Err(error.clone()));
  } else {
    router.retry_command(command);
  }
}

/// Write the command to a connection.
pub async fn write_command(
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  mut command: Command,
) -> Result<(), Error> {
  _trace!(
    inner,
    "Writing command: {:?} ({})",
    command.kind.to_str_debug(),
    command.debug_id()
  );
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
        finish_or_retry_command(router, command, &err);
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
            let err = Error::new(ErrorKind::Unknown, "Failed to route command.");
            finish_or_retry_command(router, command, &err);
            utils::defer_reconnection(inner, router, None, err, use_replica)?;
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
            return Err(Error::new_canceled());
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
      finish_or_retry_command(router, command, &err);
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
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  commands: Vec<Command>,
) -> Result<(), Error> {
  _debug!(inner, "Writing pipeline with {} commands", commands.len());

  for mut command in commands.into_iter() {
    // trying to pipeline `SSUBSCRIBE` is problematic since successful responses arrive out-of-order via pubsub push
    // frames, but error redirections are returned in-order and the client is expected to follow them. this makes it
    // difficult to accurately associate redirections with `ssubscribe` calls within a pipeline. to avoid this we
    // never pipeline `ssubscribe`, even if the caller asks.
    command.can_pipeline = command.kind != CommandKind::Ssubscribe;

    write_command_t!(inner, router, command)?;
  }

  Ok(())
}

/// Send `ASKING` to the provided server, then retry the provided command.
async fn process_ask(
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  server: Server,
  slot: u16,
  mut command: Command,
) -> Result<(), Error> {
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
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  server: Server,
  slot: u16,
  mut command: Command,
) -> Result<(), Error> {
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
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  server: Option<Server>,
  force: bool,
  tx: Option<ResponseSender>,
  replica: bool,
) -> Result<(), Error> {
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
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  server: Option<Server>,
  force: bool,
  tx: Option<ResponseSender>,
) -> Result<(), Error> {
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
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  mut tx: OneshotSender<Result<(), Error>>,
  reset: bool,
) -> Result<(), Error> {
  let result = utils::sync_replicas_with_policy(inner, router, reset).await;
  let _ = tx.send(result);
  Ok(())
}

/// Sync and update the cached cluster state.
#[allow(unused_mut)]
async fn process_sync_cluster(
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  mut tx: OneshotSender<Result<(), Error>>,
) -> Result<(), Error> {
  let result = utils::sync_cluster_with_policy(inner, router).await;
  let _ = tx.send(result.clone());
  result
}

/// Start processing commands from the client front end.
async fn process_command(
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  command: RouterCommand,
) -> Result<(), Error> {
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

#[cfg(feature = "replicas")]
async fn process_deferred_reconnect(
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  server: Option<Server>,
  replica: bool,
) -> Result<(), Error> {
  Box::pin(process_replica_reconnect(inner, router, server, false, None, replica)).await
}

#[cfg(not(feature = "replicas"))]
async fn process_deferred_reconnect(
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  server: Option<Server>,
  _replica: bool,
) -> Result<(), Error> {
  Box::pin(process_reconnect(inner, router, server, false, None)).await
}

/// Reconnect to the servers whose connections failed while processing the previous command or response, or while
/// replaying commands on a new connection.
async fn process_pending_reconnections(inner: &RefCount<ClientInner>, router: &mut Router) -> Result<(), Error> {
  // reconnecting can defer more reconnections, which would otherwise wait for the next command or frame
  while !router.pending_reconnection.is_empty() {
    // reconnect to all servers first so that the remaining per-server reconnections can be skipped
    let mut pending: Vec<_> = mem::take(&mut router.pending_reconnection).into_iter().collect();
    pending.sort_by_key(|(server, _)| matches!(server, ReconnectServer::One(_)));

    for (server, replica) in pending {
      let server = match server {
        ReconnectServer::All => None,
        ReconnectServer::One(server) => Some(server),
      };
      _debug!(inner, "Processing deferred reconnection to {:?}", server);
      process_deferred_reconnect(inner, router, server, replica).await?;
    }
  }

  Ok(())
}

/// Try to read frames from any socket, otherwise try to write the next command.
async fn read_or_write(
  inner: &RefCount<ClientInner>,
  router: &mut Router,
  rx: &mut CommandReceiver,
) -> Result<(), Error> {
  process_pending_reconnections(inner, router).await?;

  if inner.connection.unresponsive.max_timeout.is_some() {
    let sleep_ft = sleep(inner.connection.unresponsive.interval);
    pin!(sleep_ft);

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
      _ = sleep_ft => {
        // break out and return early, starting another call to poll_next on all the sockets,
        // which also performs unresponsive checks on each socket
        return Ok(());
      }
    };
  } else {
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
  }

  Ok(())
}

#[cfg(feature = "glommio")]
async fn drain_command_rx(inner: &RefCount<ClientInner>, rx: &mut CommandReceiver) {
  while let Some(command) = rx.try_recv().await {
    _warn!(inner, "Skip command with canceled error after calling quit.");
    inner.counters.decr_cmd_buffer_len();
    command.cancel();
  }
}

#[cfg(not(feature = "glommio"))]
fn drain_command_rx(inner: &RefCount<ClientInner>, rx: &mut CommandReceiver) {
  while let Ok(command) = rx.try_recv() {
    _warn!(inner, "Skip command with canceled error after calling quit.");
    inner.counters.decr_cmd_buffer_len();
    command.cancel();
  }
}

/// Drains the command receiver if the routing task is dropped before it exits normally, for example when the caller
/// aborts the connection task. Queued commands would otherwise be dropped without updating the command queue length,
/// which prevents later commands from being flushed.
struct CommandReceiverGuard<'a> {
  inner: &'a RefCount<ClientInner>,
  rx:    Option<CommandReceiver>,
}

impl<'a> CommandReceiverGuard<'a> {
  fn new(inner: &'a RefCount<ClientInner>, rx: CommandReceiver) -> Self {
    CommandReceiverGuard { inner, rx: Some(rx) }
  }

  /// Store the receiver back on the client without draining it.
  fn store(mut self) {
    if let Some(rx) = self.rx.take() {
      self.inner.store_command_rx(rx, false);
    }
  }
}

impl Deref for CommandReceiverGuard<'_> {
  type Target = CommandReceiver;

  fn deref(&self) -> &CommandReceiver {
    self.rx.as_ref().expect("Command receiver was already stored.")
  }
}

impl DerefMut for CommandReceiverGuard<'_> {
  fn deref_mut(&mut self) -> &mut CommandReceiver {
    self.rx.as_mut().expect("Command receiver was already stored.")
  }
}

#[cfg(not(feature = "glommio"))]
impl Drop for CommandReceiverGuard<'_> {
  fn drop(&mut self) {
    if let Some(mut rx) = self.rx.take() {
      drain_command_rx(self.inner, &mut rx);
    }
  }
}

/// Initialize connections and start the routing task.
pub async fn start(inner: &RefCount<ClientInner>) -> Result<(), Error> {
  #[cfg(feature = "mocks")]
  if let Some(ref mocks) = inner.config.mocks {
    return mocking::start(inner, mocks).await;
  }

  let mut rx = match inner.take_command_rx() {
    Some(rx) => CommandReceiverGuard::new(inner, rx),
    None => {
      // the `_lock` field on inner synchronizes the getters/setters on the command channel halves, so if this field
      // is None then another task must have set and removed the receiver concurrently.
      return Err(Error::new(
        ErrorKind::Config,
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
      inner.notifications.broadcast_error(e.clone(), None);
      Err(e)
    } else {
      inner.set_client_state(ClientState::Connected);
      inner.notifications.broadcast_connect(Ok(()));
      Ok(())
    }
  } else {
    Box::pin(utils::reconnect_with_policy(inner, &mut router)).await
  };

  if let Err(error) = result {
    rx.store();
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
          inner.cas_client_state(ClientState::Connected, ClientState::Disconnected);
          result = Err(err);
        }
        break;
      }
    }
    #[cfg(feature = "glommio")]
    drain_command_rx(inner, &mut rx).await;
    #[cfg(not(feature = "glommio"))]
    drain_command_rx(inner, &mut rx);
    #[cfg(feature = "credential-provider")]
    inner.abort_credential_refresh_task();

    rx.store();
    result
  }
}

#[cfg(feature = "mocks")]
#[allow(unused_mut)]
mod mocking {
  use super::*;
  use crate::{
    modules::mocks::Mocks,
    protocol::{
      responders,
      responders::ResponseKind,
      types::{KeyScanBufferedInner, KeyScanInner, ValueScanInner},
      utils as protocol_utils,
    },
  };
  use redis_protocol::resp3::types::BytesFrame;
  use std::sync::Arc;

  fn mock_host(inner: &RefCount<ClientInner>) -> Server {
    inner
      .config
      .server
      .hosts()
      .first()
      .cloned()
      .unwrap_or(Server::from(("localhost", 6379)))
  }

  fn key_scanner(command: &mut Command) -> KeyScanInner {
    KeyScanInner {
      args:       command.args().to_vec(),
      hash_slot:  None,
      server:     None,
      cursor_idx: 0,
      tx:         command.take_key_scan_tx().unwrap(),
    }
  }

  fn buffered_key_scanner(command: &mut Command) -> KeyScanBufferedInner {
    KeyScanBufferedInner {
      args:       command.args().to_vec(),
      hash_slot:  None,
      server:     None,
      cursor_idx: 0,
      tx:         command.take_key_scan_buffered_tx().unwrap(),
    }
  }

  fn value_scanner(command: &mut Command) -> ValueScanInner {
    ValueScanInner {
      args:       command.args().to_vec(),
      cursor_idx: 0,
      tx:         command.take_value_scan_tx().unwrap(),
    }
  }

  /// Process any kind of router command.
  pub fn process_command(
    inner: &RefCount<ClientInner>,
    mocks: &Arc<dyn Mocks>,
    command: RouterCommand,
  ) -> Result<(), Error> {
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

        match result {
          Ok(frame) => match command.kind {
            CommandKind::Scan => {
              let is_buffered = matches!(command.response, ResponseKind::KeyScanBuffered(_));
              let server = mock_host(inner);

              if is_buffered {
                let scanner = buffered_key_scanner(&mut command);
                responders::respond_key_scan_buffered(inner, &server, command, scanner, frame)
                  .expect("Failed to respond key scan buffered");
              } else {
                let scanner = key_scanner(&mut command);
                responders::respond_key_scan(inner, &server, command, scanner, frame)
                  .expect("Failed to respond key scan");
              }
            },
            CommandKind::Sscan | CommandKind::Hscan | CommandKind::Zscan => {
              let server = mock_host(inner);
              let scanner = value_scanner(&mut command);

              responders::respond_value_scan(inner, &server, command, scanner, frame)
                .expect("Failed to respond value scan");
            },
            _ => command.respond_to_caller(Ok(frame)),
          },
          Err(err) => command.respond_to_caller(Err(err)),
        };

        Ok(())
      },
      _ => Err(Error::new(ErrorKind::Unknown, "Unimplemented.")),
    }
  }

  pub async fn process_commands(
    inner: &RefCount<ClientInner>,
    mocks: &Arc<dyn Mocks>,
    rx: &mut CommandReceiver,
  ) -> Result<(), Error> {
    while let Some(command) = rx.recv().await {
      inner.counters.decr_cmd_buffer_len();

      _trace!(inner, "Recv mock command: {:?}", command);
      if let Err(e) = process_command(inner, mocks, command) {
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

  pub async fn start(inner: &RefCount<ClientInner>, mocks: &Arc<dyn Mocks>) -> Result<(), Error> {
    _debug!(inner, "Starting mocking layer");

    #[cfg(feature = "glommio")]
    glommio::yield_if_needed().await;
    #[cfg(not(feature = "glommio"))]
    tokio::task::yield_now().await;

    let mut rx = match inner.take_command_rx() {
      Some(rx) => rx,
      None => return Err(Error::new(ErrorKind::Config, "Redis client is already initialized.")),
    };

    inner.notifications.broadcast_connect(Ok(()));
    let result = process_commands(inner, mocks, &mut rx).await;

    inner.store_command_rx(rx, false);
    result
  }
}

#[cfg(all(
  test,
  feature = "i-keys",
  not(any(feature = "glommio", feature = "smol", feature = "monoio"))
))]
mod tests {
  use crate::{
    clients::Client,
    error::{Error, ErrorKind},
    interfaces::{ClientLike, KeysInterface, MetricsInterface},
    types::{
      config::{Config, ConnectionConfig, PerformanceConfig, ReconnectPolicy, ServerConfig},
      ClusterHash,
      CustomCommand,
    },
  };
  use std::{
    io,
    sync::{
      atomic::{AtomicBool, AtomicUsize, Ordering},
      Arc,
      Mutex,
    },
    time::Duration,
  };
  use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
  };

  struct Peer {
    host:              String,
    port:              u16,
    accepted:          Arc<AtomicUsize>,
    connections:       Arc<Mutex<Vec<JoinHandle<io::Result<()>>>>>,
    close_on_next_set: Arc<AtomicBool>,
    listener:          JoinHandle<()>,
  }

  impl Peer {
    async fn start() -> Self {
      let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Failed to bind mock server");
      let addr = listener.local_addr().expect("Failed to read mock server address");
      let accepted = Arc::new(AtomicUsize::new(0));
      let connections = Arc::new(Mutex::new(Vec::new()));
      let close_on_next_set = Arc::new(AtomicBool::new(false));
      let listener = tokio::spawn({
        let (accepted, connections, close_on_next_set) =
          (accepted.clone(), connections.clone(), close_on_next_set.clone());
        async move {
          while let Ok((socket, _)) = listener.accept().await {
            accepted.fetch_add(1, Ordering::SeqCst);
            let connection = tokio::spawn(serve(socket, close_on_next_set.clone()));
            connections.lock().unwrap().push(connection);
          }
        }
      });

      Peer {
        host: addr.ip().to_string(),
        port: addr.port(),
        accepted,
        connections,
        close_on_next_set,
        listener,
      }
    }

    fn accepted(&self) -> usize {
      self.accepted.load(Ordering::SeqCst)
    }

    fn close_on_next_set(&self) {
      self.close_on_next_set.store(true, Ordering::SeqCst);
    }

    /// Abort every connection task, which closes its socket.
    fn disconnect(&self) {
      for connection in self.connections.lock().unwrap().drain(..) {
        connection.abort();
      }
    }
  }

  impl Drop for Peer {
    fn drop(&mut self) {
      self.disconnect();
      self.listener.abort();
    }
  }

  async fn serve(socket: TcpStream, close_on_next_set: Arc<AtomicBool>) -> io::Result<()> {
    let mut socket = BufReader::new(socket);
    let mut line = String::new();
    loop {
      line.clear();
      if socket.read_line(&mut line).await? == 0 {
        return Ok(());
      }
      let mut command = String::new();
      for idx in 0 .. resp_len(&line, '*')? {
        line.clear();
        socket.read_line(&mut line).await?;
        let mut arg = vec![0; resp_len(&line, '$')? + 2];
        socket.read_exact(&mut arg).await?;
        if idx == 0 {
          arg.truncate(arg.len() - 2);
          command = String::from_utf8_lossy(&arg).into_owned();
          if command == "SET" && close_on_next_set.swap(false, Ordering::SeqCst) {
            return Ok(());
          }
        }
      }

      let reply: &[u8] = match command.as_str() {
        "PING" => b"+PONG\r\n",
        "CLIENT" => b":1\r\n",
        "INFO" => b"$21\r\nredis_version:7.2.0\r\n\r\n",
        "GET" => b"$2\r\nok\r\n",
        _ => b"+OK\r\n",
      };
      socket.get_mut().write_all(reply).await?;
      if command == "STALL" {
        tokio::time::sleep(Duration::from_secs(3)).await;
      }
    }
  }

  fn resp_len(line: &str, prefix: char) -> io::Result<usize> {
    line
      .trim()
      .strip_prefix(prefix)
      .and_then(|len| len.parse().ok())
      .ok_or_else(|| io::Error::other("Unexpected RESP frame."))
  }

  fn new_client(peer: &Peer, max_command_buffer_len: usize) -> Client {
    let config = Config {
      server: ServerConfig::new_centralized(peer.host.clone(), peer.port),
      ..Default::default()
    };
    let perf = PerformanceConfig {
      default_command_timeout: Duration::from_secs(5),
      ..Default::default()
    };
    let connection = ConnectionConfig {
      max_command_buffer_len,
      ..Default::default()
    };

    Client::new(
      config,
      Some(perf),
      Some(connection),
      Some(ReconnectPolicy::new_constant(0, 10)),
    )
  }

  /// Run a regular `GET`, which is only flushed when no other commands are queued, unlike custom commands.
  async fn get(client: &Client) -> Result<Option<String>, Error> {
    client.get("key").await
  }

  fn spawn_get(client: &Client) -> JoinHandle<Result<Option<String>, Error>> {
    let client = client.clone();
    tokio::spawn(async move { get(&client).await })
  }

  async fn block_routing_task(client: &Client) -> JoinHandle<Result<String, Error>> {
    let _: String = client
      .custom(
        CustomCommand::new("STALL", ClusterHash::FirstKey, false),
        Vec::<String>::new(),
      )
      .await
      .expect("Failed to run STALL");
    let blocked = tokio::spawn({
      let client = client.clone();
      async move {
        client
          .custom::<String, _>(CustomCommand::new("SET", ClusterHash::FirstKey, false), vec![
            "key".to_owned(),
            "x".repeat(16 << 20),
          ])
          .await
      }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    blocked
  }

  /// Wait for the peer to accept `connections` sockets and for the client to report that it is connected.
  async fn wait_for_reconnect(peer: &Peer, client: &Client, connections: usize) {
    for _ in 0 .. 500 {
      if peer.accepted() >= connections && client.is_connected() {
        return;
      }
      tokio::time::sleep(Duration::from_millis(10)).await;
    }
  }

  #[tokio::test]
  async fn should_decrement_queue_len_after_backpressure() {
    let peer = Peer::start().await;
    let client = new_client(&peer, 2);
    // fill the queue before the routing task starts so that the third command is rejected
    let queued = [spawn_get(&client), spawn_get(&client)];
    tokio::task::yield_now().await;
    let rejected = get(&client).await;
    assert_eq!(rejected.map_err(|e| e.kind().clone()), Err(ErrorKind::Backpressure));

    let _task = client.connect();
    client.wait_for_connect().await.expect("Failed to connect");
    for handle in queued {
      let value = handle.await.expect("Failed to join").expect("Failed to run queued GET");
      assert_eq!(value.as_deref(), Some("ok"));
    }
    assert_eq!(client.command_queue_len(), 0);
    // a lone command must be flushed without another command queued behind it
    let value = get(&client).await.expect("Failed to run GET after backpressure");
    assert_eq!(value.as_deref(), Some("ok"));
  }

  #[tokio::test]
  async fn should_reconnect_with_full_command_queue() {
    let peer = Peer::start().await;
    let client = new_client(&peer, 2);
    let task = client.connect();
    client.wait_for_connect().await.expect("Failed to connect");

    // the write blocks the routing task while two more commands fill the queue
    let blocked = block_routing_task(&client).await;
    let queued = [spawn_get(&client), spawn_get(&client)];
    tokio::task::yield_now().await;

    peer.disconnect();
    wait_for_reconnect(&peer, &client, 2).await;

    assert!(!task.is_finished(), "The routing task should survive the disconnect.");
    assert!(peer.accepted() >= 2, "The client should reconnect.");
    let value = get(&client).await.expect("Failed to run GET after reconnecting");
    assert_eq!(value.as_deref(), Some("ok"));
    for handle in queued {
      assert!(
        handle.await.expect("Failed to join").is_ok(),
        "Queued commands should finish after reconnecting."
      );
    }
    assert!(
      blocked.await.expect("Failed to join").is_ok(),
      "The interrupted write should be retried after reconnecting."
    );
    assert_eq!(client.command_queue_len(), 0);
  }

  #[tokio::test]
  async fn should_reconnect_when_replayed_command_fails() {
    let peer = Peer::start().await;
    let client = new_client(&peer, 10);
    let task = client.connect();
    client.wait_for_connect().await.expect("Failed to connect");

    // the blocked SET is replayed on the next connection, which the peer closes as soon as the command arrives
    let _replayed = block_routing_task(&client).await;
    peer.close_on_next_set();
    peer.disconnect();
    wait_for_reconnect(&peer, &client, 3).await;

    assert!(
      !task.is_finished(),
      "The routing task should survive the failed replay."
    );
    assert!(
      peer.accepted() >= 3,
      "The client should reconnect again without waiting for another command."
    );
    let value = get(&client).await.expect("Failed to run GET after reconnecting");
    assert_eq!(value.as_deref(), Some("ok"));
  }

  #[tokio::test]
  async fn should_decrement_queue_len_when_quitting() {
    let peer = Peer::start().await;
    let client = new_client(&peer, 10);
    let task = client.connect();
    client.wait_for_connect().await.expect("Failed to connect");

    // the GET is queued behind QUIT, so the routing task cancels it instead of sending it
    let quit = tokio::spawn({
      let client = client.clone();
      async move { client.quit().await }
    });
    let canceled = spawn_get(&client);
    let _ = quit.await.expect("Failed to join");

    assert!(canceled.await.expect("Failed to join").is_err());
    assert!(task.await.expect("Failed to join").is_ok());
    assert_eq!(client.command_queue_len(), 0);

    let _task = client.connect();
    client.wait_for_connect().await.expect("Failed to reconnect");
    let value = get(&client).await.expect("Failed to run GET after quitting");
    assert_eq!(value.as_deref(), Some("ok"));
  }

  #[tokio::test]
  async fn should_decrement_queue_len_when_aborted() {
    let peer = Peer::start().await;
    let client = new_client(&peer, 10);
    let task = client.connect();
    client.wait_for_connect().await.expect("Failed to connect");

    // abort the routing task while it is blocked with commands queued behind the blocking write
    let blocked = block_routing_task(&client).await;
    let queued = [spawn_get(&client), spawn_get(&client)];
    tokio::task::yield_now().await;
    task.abort();
    assert!(task
      .await
      .expect_err("The routing task should be aborted.")
      .is_cancelled());

    assert!(blocked.await.expect("Failed to join").is_err());
    for handle in queued {
      assert!(handle.await.expect("Failed to join").is_err());
    }
    assert_eq!(client.command_queue_len(), 0);
    // a lone command must be flushed after reconnecting
    let _task = client.connect();
    client.wait_for_connect().await.expect("Failed to reconnect");
    let value = get(&client).await.expect("Failed to run GET after aborting");
    assert_eq!(value.as_deref(), Some("ok"));
  }
}
