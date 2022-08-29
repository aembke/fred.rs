use crate::clients::RedisClient;
use crate::error::{RedisError, RedisErrorKind};
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
use redis_protocol::resp3::types::Frame as Resp3Frame;
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
