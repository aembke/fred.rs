use crate::{
  clients::RedisClient,
  error::{RedisError, RedisErrorKind},
  modules::inner::{CommandReceiver, RedisClientInner},
  multiplexer::{utils, Backpressure, Multiplexer, SentCommand, Written},
  protocol::{
    command::{
      MultiplexerCommand, MultiplexerReceiver, MultiplexerResponse, RedisCommand, RedisCommandKind, ResponseSender,
    },
    connection::read_cluster_nodes,
    responders::ResponseKind,
    types::{RedisCommand, RedisCommandKind},
    utils as protocol_utils,
    utils::pretty_error,
  },
  trace,
  types::{ClientState, ClusterHash, ReconnectPolicy, ServerConfig},
  utils as client_utils,
};
use arcstr::ArcStr;
use futures::future::{select, Either};
use parking_lot::Mutex;
use redis_protocol::{redis_keyslot, resp3::types::Frame as Resp3Frame};
use std::{
  collections::VecDeque,
  ops::{DerefMut, Mul},
  sync::Arc,
  time::Duration,
};
use tokio::{
  self,
  sync::{
    mpsc::{unbounded_channel, UnboundedReceiver},
    oneshot::{channel as oneshot_channel, Receiver as OneshotReceiver},
  },
  time::sleep,
};
#[cfg(feature = "partial-tracing")]
use tracing_futures::Instrument;

async fn backpressure(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  mut duration: Duration,
  mut command: RedisCommand,
) -> Result<Backpressure, RedisError> {
  loop {
    _warn!(
      inner,
      "Sleeping for {} ms due to connection backpressure.",
      duration.as_millis()
    );
    if inner.should_trace() {
      trace::backpressure_event(&command, duration.as_millis());
    }
    sleep(duration).await;

    match multiplexer.write(command).await? {
      Backpressure::Wait((_duration, _command)) => {
        duration = _duration;
        command = _command;
      },
      Backpressure::Ok(s) => return Ok(Backpressure::Ok(s)),
      Backpressure::Skipped => return Ok(Backpressure::Skipped),
    }
  }
}

async fn cluster_backpressure(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  mut duration: Duration,
  mut command: RedisCommand,
) -> Result<Backpressure, RedisError> {
  loop {
    _warn!(
      inner,
      "Sleeping for {} ms due to cluster connection backpressure.",
      duration.as_millis()
    );
    if inner.should_trace() {
      trace::backpressure_event(&command, duration.as_millis());
    }
    sleep(duration).await;

    match multiplexer.write_all_cluster(command).await? {
      Backpressure::Wait((_duration, _command)) => {
        duration = _duration;
        command = _command;
      },
      Backpressure::Ok(s) => return Ok(Backpressure::Ok(s)),
      Backpressure::Skipped => return Ok(Backpressure::Skipped),
    }
  }
}

fn split_connection(inner: &Arc<RedisClientInner>, _multiplexer: &Multiplexer, command: RedisCommand) {
  let inner = inner.clone();
  _debug!(inner, "Splitting clustered connection...");

  let mut split = match command.kind {
    RedisCommandKind::_Split(split) => split,
    _ => {
      _error!(inner, "Skip slitting cluster due to invalid redis command.");
      utils::emit_error(
        &inner,
        &RedisError::new(
          RedisErrorKind::Unknown,
          "Invalid redis command provided to split cluster.",
        ),
      );
      return;
    },
  };
  let (tx, config) = (split.tx.write().take(), split.config.take());
  if tx.is_none() || config.is_none() {
    utils::emit_error(
      &inner,
      &RedisError::new(RedisErrorKind::Unknown, "Missing split response sender or config."),
    );
    return;
  }
  let (tx, config) = (tx.unwrap(), config.unwrap());

  let _ = tokio::spawn(async move {
    let cluster_state = match read_cluster_nodes(&inner).await {
      Ok(state) => state,
      Err(e) => {
        let _ = tx.send(Err(e));
        return;
      },
    };
    let main_nodes = cluster_state.unique_main_nodes();
    let mut clients = Vec::with_capacity(main_nodes.len());

    for main_node in main_nodes.into_iter() {
      let parts: Vec<&str> = main_node.split(":").collect();
      if parts.len() != 2 {
        let _ = tx.send(Err(RedisError::new(
          RedisErrorKind::ProtocolError,
          format!("Invalid host/port for {}", main_node),
        )));
        return;
      }

      let host = parts[0].to_owned();
      let port = match parts[1].parse::<u16>() {
        Ok(port) => port,
        Err(e) => {
          let _ = tx.send(Err(RedisError::from(e)));
          return;
        },
      };

      let mut new_config = config.clone();
      new_config.server = ServerConfig::Centralized { host, port };

      clients.push(RedisClient::new(new_config));
    }

    let _ = tx.send(Ok(clients));
  });
}

fn shutdown_client(inner: &Arc<RedisClientInner>, error: &RedisError) {
  utils::emit_connect_error(inner, &error);
  utils::emit_error(&inner, &error);
  client_utils::shutdown_listeners(&inner);
  client_utils::set_locked(&inner.multi_block, None);
  client_utils::set_client_state(&inner.state, ClientState::Disconnected);
  inner.update_cluster_state(None);
}

fn next_reconnect_delay(
  inner: &Arc<RedisClientInner>,
  policy: &mut ReconnectPolicy,
  error: &RedisError,
) -> Option<u64> {
  if error.is_cluster_error() {
    let amt = inner.perf_config.cluster_cache_update_delay_ms();
    _debug!(
      inner,
      "Waiting {} ms to rebuild cluster state due to cluster error",
      amt
    );
    Some(amt as u64)
  } else {
    policy.next_delay()
  }
}

async fn handle_backpressure(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  result: Backpressure,
  wait: bool,
  cluster: bool,
) -> Result<Option<Arc<String>>, RedisError> {
  match result {
    Backpressure::Wait((duration, command)) => {
      if wait {
        let backpressure_result = if cluster {
          cluster_backpressure(inner, multiplexer, duration, command).await?
        } else {
          backpressure(inner, multiplexer, duration, command).await?
        };

        match backpressure_result {
          Backpressure::Wait(_) => {
            _warn!(inner, "Failed waiting on backpressure.");
            Ok(None)
          },
          Backpressure::Ok(server) => Ok(Some(server)),
          Backpressure::Skipped => Ok(None),
        }
      } else {
        Ok(None)
      }
    },
    Backpressure::Ok(server) => Ok(Some(server)),
    Backpressure::Skipped => Ok(None),
  }
}

/// Check the command against the context of the connections to ensure it can be run, and if so return it, otherwise
/// respond with an error and move on.
async fn check_command_structure(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  has_policy: bool,
  mut command: RedisCommand,
) -> Result<Option<RedisCommand>, RedisError> {
  if command.kind.is_split() {
    split_connection(&inner, &multiplexer, command);
    return Ok(None);
  }
  if command.kind == RedisCommandKind::Mget {
    if let Err(error) = utils::check_mget_cluster_keys(&multiplexer, &command.args) {
      respond_with_error(&inner, command, error);
      return Ok(None);
    }
  }
  if command.kind.is_mset() {
    if let Err(error) = utils::check_mset_cluster_keys(&multiplexer, &command.args) {
      respond_with_error(&inner, command, error);
      return Ok(None);
    }
  }
  if is_exec_or_discard_without_multi_block(&inner, &command) {
    respond_with_canceled_error(&inner, command, "Cannot use EXEC or DISCARD outside MULTI block.");
    return Ok(None);
  }
  if let Err(error) = check_transaction_hash_slot(&inner, &command) {
    respond_with_error(&inner, command, error);
    return Ok(None);
  }
  if check_deferred_multi_command(&inner, &multiplexer, &mut command, has_policy).await? {
    _debug!(inner, "Skip command due to error with deferred MULTI request.");
    return Ok(None);
  }

  Ok(Some(command))
}

#[cfg(feature = "full-tracing")]
async fn check_command_structure_t(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  has_policy: bool,
  command: RedisCommand,
) -> Result<Option<RedisCommand>, RedisError> {
  if inner.should_trace() {
    let span = fspan!(command, "check_command_structure");
    check_command_structure(inner, multiplexer, has_policy, command)
      .instrument(span)
      .await
  } else {
    check_command_structure(inner, multiplexer, has_policy, command).await
  }
}

#[cfg(not(feature = "full-tracing"))]
async fn check_command_structure_t(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  has_policy: bool,
  command: RedisCommand,
) -> Result<Option<RedisCommand>, RedisError> {
  check_command_structure(inner, multiplexer, has_policy, command).await
}

#[cfg(feature = "full-tracing")]
async fn write_command_t(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  command: RedisCommand,
) -> Result<Option<Arc<String>>, RedisError> {
  if inner.should_trace() {
    let span = fspan!(
      command,
      "write_to_socket",
      pipelined = &command.resp_tx.read().is_none()
    );
    write_command(inner, multiplexer, command).instrument(span).await
  } else {
    write_command(inner, multiplexer, command).await
  }
}

#[cfg(not(feature = "full-tracing"))]
async fn write_command_t(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  command: RedisCommand,
) -> Result<Option<Arc<String>>, RedisError> {
  write_command(inner, multiplexer, command).await
}

async fn handle_command(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  command: RedisCommand,
  has_policy: bool,
  disable_pipeline: bool,
) -> Result<(), RedisError> {
  let cmd_buffer_len = client_utils::decr_atomic(&inner.cmd_buffer_len);
  _trace!(
    inner,
    "Recv command on multiplexer {}. Buffer len: {}",
    command.kind.to_str_debug(),
    cmd_buffer_len
  );

  let command = match check_command_structure_t(&inner, &multiplexer, has_policy, command).await? {
    Some(cmd) => cmd,
    None => return Ok(()),
  };
  let is_blocking = command.kind.is_blocking();
  let is_quit = command.kind.closes_connection();

  let rx = if should_disable_pipeline(&inner, &command, disable_pipeline) {
    _debug!(
      inner,
      "Will block multiplexer loop waiting on {} to finish.",
      command.kind.to_str_debug()
    );
    let (tx, rx) = oneshot_channel();
    command.add_resp_tx(tx);
    Some(rx)
  } else {
    _debug!(
      inner,
      "Skip blocking multiplexer after sending {}",
      command.kind.to_str_debug()
    );
    None
  };

  let mut command_wrapper = Some(command);
  // try to write the command until it works, pausing to reconnect if needed
  loop {
    let command = match command_wrapper.take() {
      Some(cmd) => cmd,
      None => {
        _warn!(inner, "Expected command, found none.");
        return Err(RedisError::new(RedisErrorKind::Unknown, "Invalid empty command."));
      },
    };

    let result = write_command_t(&inner, &multiplexer, command).await;
    if is_quit {
      _debug!(inner, "Closing command stream after Quit command.");
      // the server will close the connection when it gets the message, so we can just wait a second and return an
      // error to break the stream
      sleep(Duration::from_millis(100)).await;
      return Err(RedisError::new_canceled());
    }
    if is_config_error(&result) {
      _debug!(inner, "Closing command stream after fatal configuration error.");
      return result.map(|_| ());
    }

    match result {
      Ok(server) => {
        if let Some(rx) = rx {
          if is_blocking {
            if let Some(server) = server {
              _debug!(inner, "Setting blocked flag on backchannel: {}", server);
              inner.backchannel.write().await.set_blocked(server);
            }
          }

          _debug!(inner, "Waiting on last request to finish without pipelining.");
          // if pipelining is disabled then wait for the last request to finish
          let _ = rx.await;
          _debug!(inner, "Recv message to continue non-pipelined multiplexer loop.");
        }

        return Ok(());
      },
      Err(mut error) => {
        let command = error.take_context();
        _warn!(
          inner,
          "Error writing command {:?}: {:?}",
          command.as_ref().map(|c| c.kind.to_str_debug()),
          error
        );

        // TODO maybe send reconnect/sync-cluster message here to be safe
        if handle_write_error(&inner, &multiplexer, has_policy, &error).await? {
          return Err(error);
        } else {
          if let Some(command) = command {
            _debug!(
              inner,
              "Retrying command after write error: {}",
              command.kind.to_str_debug()
            );
            command_wrapper = Some(command);
            continue;
          }

          return Ok(());
        }
      },
    }
  }
}

#[cfg(feature = "full-tracing")]
async fn handle_command_t(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  mut command: RedisCommand,
  has_policy: bool,
  disable_pipeline: bool,
) -> Result<(), RedisError> {
  if inner.should_trace() {
    command.take_queued_span();
    let span = fspan!(command, "handle_command");
    handle_command(inner, multiplexer, command, has_policy, disable_pipeline)
      .instrument(span)
      .await
  } else {
    handle_command(inner, multiplexer, command, has_policy, disable_pipeline).await
  }
}

#[cfg(not(feature = "full-tracing"))]
async fn handle_command_t(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  command: RedisCommand,
  has_policy: bool,
  disable_pipeline: bool,
) -> Result<(), RedisError> {
  handle_command(inner, multiplexer, command, has_policy, disable_pipeline).await
}

async fn connect_with_policy(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  policy: &mut Option<ReconnectPolicy>,
) -> Result<(), RedisError> {
  if let Some(ref mut policy) = policy {
    loop {
      if let Err(err) = multiplexer.connect_and_flush().await {
        _warn!(inner, "Failed to connect with error {:?}", err);

        let delay = match policy.next_delay() {
          Some(delay) => delay,
          None => {
            _warn!(
              inner,
              "Max reconnect attempts reached. Stopping initial connection logic."
            );
            let error = RedisError::new(RedisErrorKind::Unknown, "Max reconnection attempts reached.");
            utils::emit_connect_error(inner, &error);
            utils::emit_error(inner, &error);
            return Err(error);
          },
        };
        _info!(inner, "Sleeping for {} ms before reconnecting", delay);
        sleep(Duration::from_millis(delay)).await;
      } else {
        break;
      }
    }
  } else {
    if let Err(err) = multiplexer.connect_and_flush().await {
      utils::emit_connect_error(inner, &err);
      utils::emit_error(inner, &err);
      return Err(err);
    }
  }

  Ok(())
}

/// Initialize the multiplexer and network interface to accept commands.
///
/// This function runs until the connection closes or all retry attempts have failed.
/// If a retry policy with infinite attempts is provided then this runs forever.
pub async fn init(inner: &Arc<RedisClientInner>, mut policy: Option<ReconnectPolicy>) -> Result<(), RedisError> {
  if !client_utils::check_and_set_client_state(&inner.state, ClientState::Disconnected, ClientState::Connecting) {
    return Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Connections are already initialized or connecting.",
    ));
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
  if let Some(ref mut policy) = policy {
    policy.reset_attempts();
  }
  client_utils::set_locked(&inner.policy, policy.clone());
  let multiplexer = Multiplexer::new(inner);

  _debug!(inner, "Initializing connections...");
  if inner.config.read().fail_fast {
    if let Err(err) = multiplexer.connect_and_flush().await {
      utils::emit_connect_error(inner, &err);
      utils::emit_error(inner, &err);
      return Err(err);
    }
  } else {
    connect_with_policy(inner, &multiplexer, &mut policy).await?;
  }

  client_utils::set_client_state(&inner.state, ClientState::Connected);
  utils::emit_connect(inner);
  utils::emit_reconnect(inner);

  let has_policy = policy.is_some();
  handle_connection_closed(inner, &multiplexer, policy);

  _debug!(inner, "Starting command stream...");
  while let Some(command) = rx.recv().await {
    let disable_pipeline = !inner.is_pipelined();
    if let Err(e) = handle_command_t(inner, &multiplexer, command, has_policy, disable_pipeline).await {
      if e.is_canceled() {
        break;
      } else {
        inner.store_command_rx(rx);
        return Err(e);
      }
    }
  }

  inner.store_command_rx(rx);
  Ok(())
}

// ------------------------------------------------------------------------------------

fn next_reconnection_delay(inner: &Arc<RedisClientInner>) -> Result<Duration, RedisError> {
  inner
    .policy
    .write()
    .as_mut()
    .and_then(|policy| policy.next_delay())
    .map(|amt| Duration::from_millis(amt))
    .ok_or(RedisError::new(
      RedisErrorKind::Canceled,
      "Max reconnection attempts reached.",
    ))
}

/// Reconnect to the server(s) until the max reconnect policy attempts are reached.
async fn reconnect_with_policy(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
) -> Result<(), RedisError> {
  loop {
    let delay = next_reconnection_delay(inner)?;
    _debug!(inner, "Sleeping for {} ms before reconnecting.", delay.as_millis());
    let _ = inner.wait_with_interrupt(delay).await?;

    client_utils::set_client_state(&inner.state, ClientState::Connecting);
    if let Err(e) = multiplexer.connect().await {
      _debug!(inner, "Failed reconnecting with error: {:?}", e);
      client_utils::set_client_state(&inner.state, ClientState::Disconnected);
      inner.notifications.broadcast_error(e);
      continue;
    } else {
      // try to flush any previously in-flight commands
      if let Err(e) = multiplexer.retry_buffer().await {
        _debug!(inner, "Failed retrying command buffer: {:?}", e);
        client_utils::set_client_state(&inner.state, ClientState::Disconnected);
        inner.notifications.broadcast_error(e);
        continue;
      }

      client_utils::set_client_state(&inner.state, ClientState::Connected);
      inner.notifications.broadcast_connect(Ok(()));
      inner.notifications.broadcast_reconnect();
      inner.reset_reconnection_attempts();
      return Ok(());
    }
  }
}

/// Repeatedly try to sync the cluster state, applying any provided reconnection policy as needed.
async fn sync_cluster_with_reconnect(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
) -> Result<(), RedisError> {
  let dur = inner.with_perf_config(|config| Duration::from_millis(config.cluster_cache_update_delay_ms as u64));
  if !dur.is_zero() {
    _trace!(inner, "Sleep for {:?}ms after cluster redirection.", dur);
    let _ = sleep(dur).await;
  }

  loop {
    if let Err(error) = multiplexer.sync_cluster().await {
      _warn!(inner, "Error syncing cluster after redirect: {:?}", error);
      if error.should_not_reconnect() {
        return Err(error);
      }

      match next_reconnection_delay(inner) {
        Ok(dur) => {
          let _ = sleep(dur).await;
          continue;
        },
        Err(e) => {
          return Err(e);
        },
      }
    } else {
      break;
    }
  }

  Ok(())
}

/// Repeatedly try to send `ASKING` to the provided server, reconnecting as needed.
async fn send_asking_with_reconnect(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  server: &str,
  slot: u16,
) -> Result<(), RedisError> {
  loop {
    // if the connection does not exist, then sync connections
    if !multiplexer.connections.has_server_connection(server) {
      let _ = sync_cluster_with_reconnect(inner, multiplexer).await?;
    }

    let mut command = RedisCommand::new_asking(slot);
    let (tx, rx) = oneshot_channel();
    command.skip_backpressure = true;
    command.response = ResponseKind::Respond(Some(tx));

    if let Err(error) = multiplexer.write_once(command, server).await {
      // sleep and retry from connection errors
      if error.should_not_reconnect() {
        return Err(error);
      } else {
        let dur = next_reconnection_delay(inner)?;
        _debug!(
          inner,
          "Sleeping for {}ms after failing to write ASKING.",
          dur.as_millis()
        );
        let _ = sleep(dur).await;
      }
    } else {
      inner.reset_reconnection_attempts();
      // wait on `rx`, returning response errors to the caller
      return match rx.await? {
        Ok(frame) => protocol_utils::frame_to_single_result(frame).map(|_| ()),
        Err(error) => Err(error),
      };
    }
  }
}

/// Wait for the response from the reader task, handling cluster redirections if needed.
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
        let _ = send_asking_with_reconnect(inner, multiplexer, &server, slot).await?;
        command.hasher = ClusterHash::Custom(slot);
        Ok(Some(command))
      },
      MultiplexerResponse::Moved((slot, server, command)) => {
        // check if slot belongs to server, if not then run sync cluster
        if !multiplexer.cluster_node_owns_slot(slot, &server) {
          let _ = sync_cluster_with_reconnect(inner, multiplexer).await?;
          inner.reset_reconnection_attempts();
        }

        Ok(Some(command))
      },
      MultiplexerResponse::TransactionError((error, command)) => {
        _error!(
          inner,
          "Recv transaction error outside a transaction block for {}",
          command.kind.to_str_debug()
        );
        Err(error)
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
// this is more complicated to avoid async recursion
async fn write_with_backpressure(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  command: RedisCommand,
) -> Result<(), RedisError> {
  let (mut _command, mut _backpressure) = (Some(command), None);
  loop {
    let mut command = match _command.take() {
      Some(command) => command,
      None => return Err(RedisError::new(RedisErrorKind::Unknown, "Missing command.")),
    };
    let rx = if let Some(backpressure) = _backpressure {
      match backpressure.run(inner, &mut command) {
        Some(rx) => Some(rx),
        None => {
          // TODO make sure `force` flag is correct here
          if command.should_auto_pipeline(inner, false) {
            Some(command.create_multiplexer_channel())
          } else {
            None
          }
        },
      }
    } else {
      if command.should_auto_pipeline(inner, false) {
        Some(command.create_multiplexer_channel())
      } else {
        None
      }
    };

    match multiplexer.write_command(command) {
      Ok(Written::Backpressure((command, backpressure))) => {
        _debug!(inner, "Recv backpressure again for {}.", command.kind.to_str_debug());
        _command = Some(command);
        _backpressure = Some(backpressure);

        continue;
      },
      Ok(Written::Disconnect((server, command, error))) => {
        _debug!(inner, "Handle disconnect backpressure for {} from {:?}", server, error);
        let commands = multiplexer.connections.disconnect(inner, Some(server)).await;
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
        if flushed {
          let _ = multiplexer.check_and_flush().await;
        }
        _trace!(inner, "Finished sending to {}", server);

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

/// Send an early error to the caller in the context of a pipeline.
fn write_pipeline_error(
  inner: &Arc<RedisClientInner>,
  tx: Option<Arc<Mutex<Option<ResponseSender>>>>,
  error: RedisError,
) {
  unimplemented!()
}

/// Write a command in the context of a transaction.
///
/// Returns the command and non-fatal error or a fatal error that should end the transaction.
async fn write_transaction_command(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  command: RedisCommand,
) -> Result<Option<(RedisCommand, RedisError)>, RedisError> {
  unimplemented!()
}

/// Process the commands in a transaction
async fn process_transaction(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  mut commands: Vec<RedisCommand>,
  id: u64,
  abort_on_error: bool,
) -> Result<(), RedisError> {
  let mut custom_hash_slot = None;

  'outer: loop {
    _debug!(inner, "Writing transaction with ID: {}", id);
    let mut idx = 0;
    'inner: while idx < commands.len() {
      let mut command = commands[idx].duplicate(ResponseKind::Skip);
      if let Some(hash_slot) = custom_hash_slot {
        command.hasher = ClusterHash::Custom(hash_slot);
      }

      // write the command
      // check the write response, sleep and continue 'outer if needed
      // wait on the multiplexer response
      //   handle trx errors
      //     if abort_on_error then take tx off commands.last_mut() and send error
      //     else continue, incrementing idx
      //   handle moved or ask by syncing cluster and continuing 'outer, optionally sending ASKING
      //     also override the custom_hash_slot
      //   if no error then increment idx
    }
  }

  Ok(())
}

async fn process_pipeline(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  commands: Vec<RedisCommand>,
) -> Result<(), RedisError> {
  _debug!(inner, "Writing pipeline with {} commands", commands.len());
  let tx = commands
    .iter()
    .find_map(|command| command.response.clone_shared_response_tx());

  for mut command in commands.into_iter() {
    command.can_pipeline = true;
    command.skip_backpressure = true;

    if let Err(e) = write_with_backpressure(inner, multiplexer, command).await {
      let dur = match next_reconnection_delay(inner) {
        Ok(dur) => dur,
        Err(e) => {

        }
      }
      sleep()
    }

    // send the command, running the reconnection policy on write errors
  }

  inner.reset_reconnection_attempts();
  Ok(())
}

async fn process_ask(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  server: String,
  slot: u16,
  command: RedisCommand,
) -> Result<(), RedisError> {
  unimplemented!()
}

async fn process_moved(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  server: String,
  slot: u16,
  command: RedisCommand,
) -> Result<(), RedisError> {
  unimplemented!()
}

async fn process_reconnect(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  server: Option<ArcStr>,
  force: bool,
  tx: Option<ResponseSender>,
) -> Result<(), RedisError> {
  unimplemented!()
}

async fn process_check_sentinels(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
) -> Result<(), RedisError> {
  unimplemented!()
}

async fn process_sync_cluster(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
) -> Result<(), RedisError> {
  unimplemented!()
}

async fn process_split(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  tx: OneshotSender<Result<Vec<RedisClient>, RedisError>>,
) -> Result<(), RedisError> {
  unimplemented!()
}

async fn process_normal_command(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  command: RedisCommand,
) -> Result<(), RedisError> {
  unimplemented!()
}

async fn process_command(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  command: MultiplexerCommand,
) -> Result<(), RedisError> {
  match command {
    MultiplexerCommand::Ask { server, slot, command } => process_ask(inner, multiplexer, server, slot, command).await,
    MultiplexerCommand::Moved { server, slot, command } => process_moved(inner, multiplexer, server, slot, command),
    MultiplexerCommand::Reconnect { server, force, tx } => {
      process_reconnect(inner, multiplexer, server, force, tx).await
    },
    MultiplexerCommand::Split { tx } => process_split(inner, multiplexer, tx).await,
    MultiplexerCommand::CheckSentinels => process_check_sentinels(inner, multiplexer).await,
    MultiplexerCommand::SyncCluster => process_sync_cluster(inner, multiplexer).await,
    MultiplexerCommand::Transaction {
      commands,
      id,
      abort_on_error,
    } => process_transaction(inner, multiplexer, commands, id, abort_on_error).await,
    MultiplexerCommand::Pipeline { commands } => process_pipeline(inner, multiplexer, commands).await,
    MultiplexerCommand::Command(command) => process_normal_command(inner, multiplexer, command).await,
  }
}

async fn process_commands(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  rx: &mut CommandReceiver,
) -> Result<(), RedisError> {
  _debug!(inner, "Starting command processing stream...");
  while let Some(command) = rx.recv().await {
    _trace!(inner, "Recv command: {:?}", command);
    if let Err(e) = process_command(inner, multiplexer, command).await {
      // TODO maybe don't do this, just log it and wait for the reconnection command
      _debug!(inner, "Disconnecting after error processing command: {:?}", e);
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
    let _ = reconnect_with_policy(inner, &mut multiplexer).await?;
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
