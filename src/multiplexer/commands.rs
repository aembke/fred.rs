use crate::client::RedisClient;
use crate::error::{RedisError, RedisErrorKind};
use crate::globals::globals;
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::{utils, SentCommand};
use crate::multiplexer::{Backpressure, Multiplexer};
use crate::protocol::connection::read_cluster_nodes;
use crate::protocol::types::{RedisCommand, RedisCommandKind};
use crate::protocol::utils::pretty_error;
use crate::trace;
use crate::types::{ClientState, ReconnectPolicy, ServerConfig};
use crate::utils as client_utils;
use redis_protocol::redis_keyslot;
use redis_protocol::resp2::types::Frame as ProtocolFrame;
use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use tokio;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot::channel as oneshot_channel;
use tokio::sync::oneshot::Receiver as OneshotReceiver;
use tokio::time::sleep;

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
      }
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
      }
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
    }
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
      }
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
        }
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
  client_utils::set_locked(&inner.command_tx, None);
  client_utils::set_locked(&inner.multi_block, None);
  client_utils::set_client_state(&inner.state, ClientState::Disconnected);
  inner.update_cluster_state(None);
}

fn respond_with_error(inner: &Arc<RedisClientInner>, command: RedisCommand, error: RedisError) {
  if let Some(tx) = command.tx {
    if let Err(_) = tx.send(Err(error)) {
      _warn!(inner, "Error responding early to command.");
    }
  }
}

fn respond_with_canceled_error(inner: &Arc<RedisClientInner>, command: RedisCommand, error: &'static str) {
  let error = RedisError::new(RedisErrorKind::Canceled, error);
  respond_with_error(inner, command, error);
}

fn clear_multi_block_commands(inner: &Arc<RedisClientInner>, commands: &mut VecDeque<SentCommand>) {
  let error = RedisError::new(
    RedisErrorKind::Canceled,
    "Transaction aborted due to connection closing.",
  );

  for command in commands.drain(..) {
    if let Some(tx) = command.command.tx {
      if let Err(_) = tx.send(Err(error.clone())) {
        _warn!(inner, "Error responding to caller with transaction aborted error.");
      }
    }
    if let Some(tx) = client_utils::take_locked(&command.command.resp_tx) {
      if let Err(e) = tx.send(()) {
        _warn!(inner, "Error sending message to multiplexer loop {:?}", e);
      }
    }
  }
}

fn next_reconnect_delay(
  inner: &Arc<RedisClientInner>,
  policy: &mut ReconnectPolicy,
  error: &RedisError,
) -> Option<u64> {
  if error.is_cluster_error() {
    let amt = globals().cluster_error_cache_delay();
    _debug!(inner, "Waiting {} ms to reconnect due to cluster error", amt);
    Some(amt as u64)
  } else {
    policy.next_delay()
  }
}

/// Handle connection closed events by trying to reconnect and then flushing all pending commands again.
fn handle_connection_closed(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  policy: Option<ReconnectPolicy>,
) {
  let (inner, multiplexer) = (inner.clone(), multiplexer.clone());
  let has_reconnect_policy = policy.is_some();
  // the extra provided policy will kick in when we get a cluster redirection error but no policy is otherwise specified
  let mut policy = policy.unwrap_or(ReconnectPolicy::Constant {
    attempts: 0,
    max_attempts: 0,
    delay: globals().cluster_error_cache_delay() as u32,
  });

  let reconnect_inner = inner.clone();
  let jh = tokio::spawn(async move {
    let (tx, mut rx) = unbounded_channel();
    _debug!(inner, "Set inner connection closed sender.");
    client_utils::set_locked(&inner.connection_closed_tx, Some(tx));

    'recv: while let Some(state) = rx.recv().await {
      let (mut commands, error) = (state.commands, state.error);

      let client_state = client_utils::read_client_state(&inner.state);
      _debug!(
        inner,
        "Recv reconnect message with {} commands. State: {:?}",
        commands.len(),
        client_state
      );

      if !has_reconnect_policy && !error.is_cluster_error() {
        _debug!(
          inner,
          "Exit early in reconnect loop due to no reconnect policy and non-cluster error."
        );
        break;
      }
      client_utils::set_client_state(&inner.state, ClientState::Connecting);

      'reconnect: loop {
        let next_delay = match next_reconnect_delay(&inner, &mut policy, &error) {
          Some(delay) => delay,
          None => {
            _warn!(inner, "Max reconnect attempts reached. Stopping redis client.");
            let error = RedisError::new(RedisErrorKind::Unknown, "Max reconnection attempts reached.");
            write_final_error_to_callers(&inner, commands, &error);
            shutdown_client(&inner, &error);
            break 'recv;
          }
        };

        _info!(inner, "Sleeping for {} ms before reconnecting", next_delay);
        sleep(Duration::from_millis(next_delay)).await;

        let result = if client_utils::is_clustered(&inner.config) {
          multiplexer.sync_cluster().await
        } else {
          multiplexer.connect_and_flush().await
        };
        _debug!(
          inner,
          "Reconnect task finished reconnecting or syncing with: {:?}",
          result
        );

        if let Err(error) = result {
          _warn!(inner, "Failed to reconnect with error {:?}", error);

          if *error.kind() == RedisErrorKind::Auth {
            // stop trying to connect if auth is failing
            _warn!(inner, "Stop trying to reconnect due to auth failure.");
            write_final_error_to_callers(&inner, commands, &error);
            shutdown_client(&inner, &error);
            break 'recv;
          } else {
            utils::emit_error(&inner, &error);
          }

          continue 'reconnect;
        }

        if client_utils::take_locked(&inner.multi_block).is_some() {
          // don't retry commands from a transaction, just tell the caller they failed. it's
          // problematic to pair automatic retry with transactions since the earlier futures
          // could have already resolved. instead here we just emit an error to the callers of
          // any commands inside a multi block. the final `EXEC` command always blocks the
          // multiplexer loop so we can be sure any queued commands here are a part of a multi
          // block if a policy is set on the inner client struct.
          clear_multi_block_commands(&inner, &mut commands);
        }

        _debug!(inner, "Sending {} commands after reconnecting.", commands.len());
        'retry: for _ in 0..commands.len() {
          let command = match commands.pop_front() {
            Some(cmd) => cmd,
            None => break 'retry,
          };

          utils::unblock_multiplexer(&inner, &command.command);
          if let Err(e) = client_utils::send_command(&inner, command.command) {
            _debug!(inner, "Failed to retry command: {:?}", e);
            continue 'reconnect;
          };
        }

        // break when the connection is established
        break 'reconnect;
      }

      policy.reset_attempts();
      utils::emit_connect(&inner);
      utils::emit_reconnect(&inner);
    }

    _debug!(inner, "Exit reconnection task.");
    Ok::<(), ()>(())
  });

  reconnect_inner.reconnect_sleep_jh.write().replace(jh);
}

/// Whether or not the error represents a fatal CONFIG error. CONFIG errors are fatal errors that represent problems with the provided config such that no amount of reconnect or retry will help.
fn is_config_error<T>(result: &Result<T, RedisError>) -> bool {
  if let Err(ref e) = result {
    *e.kind() == RedisErrorKind::Config
  } else {
    false
  }
}

/// Send the final error to all pending callers waiting on a response before shutting down the client.
fn write_final_error_to_callers(inner: &Arc<RedisClientInner>, commands: VecDeque<SentCommand>, error: &RedisError) {
  for mut command in commands.into_iter() {
    if let Some(tx) = command.command.tx.take() {
      if let Err(e) = tx.send(Err(error.clone())) {
        _warn!(inner, "Error sending final error to caller: {:?}", e);
      }
    }
  }
}

/// Whether or not the caller tried to use EXEC or DISCARD outside of a transaction.
fn is_exec_or_discard_without_multi_block(inner: &Arc<RedisClientInner>, command: &RedisCommand) -> bool {
  command.kind.ends_transaction() && !client_utils::is_locked_some(&inner.multi_block)
}

/// Whether or not to disable pipelining for a request. If this returns true the multiplexer will block subsequent commands until the current command receives a response.
fn should_disable_pipeline(inner: &Arc<RedisClientInner>, command: &RedisCommand, disable_pipeline: bool) -> bool {
  let in_multi_block = command.kind != RedisCommandKind::Multi && client_utils::is_locked_some(&inner.multi_block);

  // https://redis.io/commands/eval#evalsha-in-the-context-of-pipelining
  let force_no_pipeline = command.kind.is_eval()
    // we disable pipelining at the start and end of a transaction to clear the socket's command buffer so that
    // when the final response to EXEC or DISCARD arrives the command buffer will only contain commands that were
    // a part of the transaction. this makes reconnection logic much easier to reason about in the context of transactions
    || command.kind == RedisCommandKind::Multi
    || command.kind.ends_transaction();

  // prefer pipelining for all commands not in a multi block (unless specified above), unless the command is blocking.
  // but, in the context of a transaction blocking commands can be pipelined since the server responds immediately.
  // otherwise defer to the `disable_pipeline` flag from the config.
  force_no_pipeline || (!in_multi_block && (disable_pipeline || command.kind.is_blocking()))
}

/// Check that all commands within a transaction against a clustered deployment only modify the same hash slot.
fn check_transaction_hash_slot(inner: &Arc<RedisClientInner>, command: &RedisCommand) -> Result<(), RedisError> {
  if client_utils::is_clustered(&inner.config) && client_utils::is_locked_some(&inner.multi_block) {
    if let Some(key) = command.extract_key() {
      if let Some(policy) = inner.multi_block.write().deref_mut() {
        let _ = policy.check_and_set_hash_slot(redis_keyslot(&key))?;
      }
    }
  }

  Ok(())
}

/// Handle an error writing to the socket, returning whether the error should close the command stream.
async fn handle_write_error(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  has_policy: bool,
  error: &RedisError,
) -> Result<bool, RedisError> {
  _debug!(inner, "Reconnecting or stopping due to error: {:?}", error);
  utils::emit_closed_message(&inner, &multiplexer.close_tx, error);

  if has_policy {
    _debug!(inner, "Waiting for client to reconnect...");
    let _ = client_utils::wait_for_connect(&inner).await?;

    Ok(false)
  } else {
    _debug!(inner, "Closing command stream from error without reconnect policy.");
    let in_flight_commands = utils::take_sent_commands(&multiplexer.connections).await;
    write_final_error_to_callers(&inner, in_flight_commands, &error);

    Ok(true)
  }
}

/// Handle the response to the MULTI command, forwarding any errors onto the caller of the next command and returning whether the multiplexers should skip the next command.
async fn handle_deferred_multi_response(
  inner: &Arc<RedisClientInner>,
  rx: OneshotReceiver<Result<ProtocolFrame, RedisError>>,
  command: &mut RedisCommand,
) -> bool {
  match rx.await {
    Ok(Ok(frame)) => {
      if let ProtocolFrame::Error(s) = frame {
        if let Some(tx) = command.tx.take() {
          let _ = tx.send(Err(pretty_error(&s)));
        }
        true
      } else {
        false
      }
    }
    Ok(Err(e)) => {
      if let Some(tx) = command.tx.take() {
        let _ = tx.send(Err(e));
      }
      true
    }
    Err(_e) => {
      _warn!(inner, "Recv error on deferred MULTI command.");
      false
    }
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
          }
          Backpressure::Ok(server) => Ok(Some(server)),
          Backpressure::Skipped => Ok(None),
        }
      } else {
        Ok(None)
      }
    }
    Backpressure::Ok(server) => Ok(Some(server)),
    Backpressure::Skipped => Ok(None),
  }
}

async fn write_command(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  command: RedisCommand,
) -> Result<Option<Arc<String>>, RedisError> {
  if command.kind.is_exec() {
    if let Some(hash_slot) = client_utils::read_transaction_hash_slot(&inner) {
      let result = multiplexer.write_with_hash_slot(command, hash_slot).await?;
      handle_backpressure(inner, multiplexer, result, false, false).await
    } else {
      if client_utils::is_clustered(&inner.config) {
        _warn!(
          inner,
          "Detected EXEC without a known hash slot. This will probably result in an error."
        );
      }

      let result = multiplexer.write(command).await?;
      handle_backpressure(inner, multiplexer, result, true, false).await
    }
  } else if command.kind.is_all_cluster_nodes() && client_utils::is_clustered(&inner.config) {
    let result = multiplexer.write_all_cluster(command).await?;
    handle_backpressure(inner, multiplexer, result, true, true).await
  } else {
    let result = multiplexer.write(command).await?;
    handle_backpressure(inner, multiplexer, result, true, false).await
  }
}

/// Check and send a deferred MULTI command if needed, returning whether or not the multiplexer loop should skip to the next command.
async fn check_deferred_multi_command(
  inner: &Arc<RedisClientInner>,
  multiplexer: &Multiplexer,
  command: &mut RedisCommand,
  has_policy: bool,
) -> Result<bool, RedisError> {
  if let Some(hash_slot) = client_utils::should_send_multi_command(inner) {
    _debug!(inner, "Sending deferred MULTI command against hash slot {}.", hash_slot);
    let (tx, rx) = oneshot_channel();
    let multi_cmd = RedisCommand::new(RedisCommandKind::Multi, vec![], Some(tx));
    if let Err(error) = multiplexer.write_with_hash_slot(multi_cmd, hash_slot).await {
      _error!(inner, "Error sending deferred multi command: {:?}", error);
      if handle_write_error(inner, &multiplexer, has_policy, &error).await? {
        return Err(error);
      } else {
        // at this point the client is reconnected, but the multi command was not sent because we
        // dont send commands that are a part of a transaction when the connection dies. because
        // of this we need to tell the caller their actual command failed by forwarding the error
        // from the MULTI command onto the next command's response channel.

        if let Some(tx) = command.tx.take() {
          let _ = tx.send(Err(error));
        }
        return Ok(true);
      }
    } else {
      if handle_deferred_multi_response(inner, rx, command).await {
        _debug!(
          inner,
          "Skip first command in transaction after error with MULTI command."
        );
        return Ok(true);
      }
      client_utils::update_multi_sent_flag(inner, true);
    }
  }

  Ok(false)
}

/// Check the command against the context of the connections to ensure it can be run, and if so return it, otherwise respond with an error and move on.
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
      }
    };

    let result = write_command_t(&inner, &multiplexer, command).await;
    if is_quit {
      _debug!(inner, "Closing command stream after Quit command.");
      // the server will close the connection when it gets the message, so we can just wait a second and return an error to break the stream
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
      }
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
      }
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
          }
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

  let (tx, mut rx) = unbounded_channel();
  client_utils::set_locked(&inner.command_tx, Some(tx));
  client_utils::set_client_state(&inner.state, ClientState::Connected);
  utils::emit_connect(inner);
  utils::emit_reconnect(inner);

  let has_policy = policy.is_some();
  let disable_pipeline = !inner.is_pipelined();
  handle_connection_closed(inner, &multiplexer, policy);

  _debug!(inner, "Starting command stream...");
  while let Some(command) = rx.recv().await {
    if let Err(e) = handle_command_t(inner, &multiplexer, command, has_policy, disable_pipeline).await {
      if e.is_canceled() {
        return Ok(());
      } else {
        return Err(e);
      }
    }
  }

  Ok(())
}
