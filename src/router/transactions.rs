use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces::Resp3Frame,
  modules::inner::RedisClientInner,
  protocol::{
    command::{ClusterErrorKind, RedisCommand, RedisCommandKind, ResponseSender, RouterReceiver, RouterResponse},
    responders::ResponseKind,
    utils::pretty_error,
  },
  router::{clustered::parse_cluster_error_frame, utils, Router, Written},
  runtime::{oneshot_channel, AtomicUsize, Mutex, RefCount},
  types::{ClusterHash, Server},
  utils as client_utils,
};
use redis_protocol::resp3::types::{FrameKind, Resp3Frame as _Resp3Frame};
use std::iter::repeat;

/// An internal enum describing the result of an attempt to send a transaction command.
#[derive(Debug)]
enum TransactionResponse {
  /// Retry the entire transaction again after reconnecting or resetting connections.
  ///
  /// Returned in response to a write error or a connection closing.
  Retry(RedisError),
  /// Send `DISCARD` and retry the entire transaction against the provided server/hash slot.
  Redirection((ClusterErrorKind, u16, Server)),
  /// Finish the transaction with the associated response.
  Finished(Resp3Frame),
  /// Continue the transaction.
  ///
  /// Note: if `abort_on_error` is true the transaction will continue even after errors from the server. If `false`
  /// the error will instead be returned as a `Result::Err` that ends the transaction.
  Continue,
}

/// Write a command in the context of a transaction and process the router response.
///
/// Returns the command result policy or a fatal error that should end the transaction.
async fn write_command(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  server: &Server,
  command: RedisCommand,
  abort_on_error: bool,
  rx: Option<RouterReceiver>,
) -> Result<TransactionResponse, RedisError> {
  _trace!(
    inner,
    "Sending trx command {} ({}) to {}",
    command.kind.to_str_debug(),
    command.debug_id(),
    server
  );

  let timeout_dur = command.timeout_dur.unwrap_or_else(|| inner.default_command_timeout());
  let result = match router.write_direct(command, server).await {
    Written::Error((error, _)) => Err(error),
    Written::Disconnected((_, _, error)) => Err(error),
    Written::NotFound(_) => Err(RedisError::new(RedisErrorKind::Cluster, "Connection not found.")),
    _ => Ok(()),
  };
  if let Err(e) = result {
    // TODO check fail fast and exit early w/o retry here?
    _debug!(inner, "Error writing trx command: {:?}", e);
    return Ok(TransactionResponse::Retry(e));
  }

  if let Some(rx) = rx {
    match client_utils::timeout(rx, timeout_dur).await? {
      RouterResponse::Continue => Ok(TransactionResponse::Continue),
      RouterResponse::Ask((slot, server, _)) => {
        Ok(TransactionResponse::Redirection((ClusterErrorKind::Ask, slot, server)))
      },
      RouterResponse::Moved((slot, server, _)) => Ok(TransactionResponse::Redirection((
        ClusterErrorKind::Moved,
        slot,
        server,
      ))),
      RouterResponse::ConnectionClosed((err, _)) => Ok(TransactionResponse::Retry(err)),
      RouterResponse::TransactionError((err, _)) => {
        if abort_on_error {
          Err(err)
        } else {
          Ok(TransactionResponse::Continue)
        }
      },
      RouterResponse::TransactionResult(frame) => Ok(TransactionResponse::Finished(frame)),
    }
  } else {
    Ok(TransactionResponse::Continue)
  }
}

/// Send EXEC to the provided server.
async fn send_non_pipelined_exec(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  server: &Server,
  id: u64,
) -> Result<TransactionResponse, RedisError> {
  let mut command = RedisCommand::new(RedisCommandKind::Exec, vec![]);
  command.can_pipeline = false;
  command.skip_backpressure = true;
  command.transaction_id = Some(id);
  let rx = command.create_router_channel();

  write_command(inner, router, server, command, true, Some(rx)).await
}

/// Send DISCARD to the provided server.
async fn send_non_pipelined_discard(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  server: &Server,
  id: u64,
) -> Result<TransactionResponse, RedisError> {
  let mut command = RedisCommand::new(RedisCommandKind::Discard, vec![]);
  command.can_pipeline = false;
  command.skip_backpressure = true;
  command.transaction_id = Some(id);
  let rx = command.create_router_channel();

  write_command(inner, router, server, command, true, Some(rx)).await
}

fn update_hash_slot(commands: &mut [RedisCommand], slot: u16) {
  for command in commands.iter_mut() {
    command.hasher = ClusterHash::Custom(slot);
  }
}

/// Find the server that should receive the transaction, creating connections if needed.
async fn find_or_create_connection(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  command: &RedisCommand,
) -> Result<Option<Server>, RedisError> {
  if let Some(server) = command.cluster_node.as_ref() {
    Ok(Some(server.clone()))
  } else {
    match router.find_connection(command) {
      Some(server) => Ok(Some(server.clone())),
      None => {
        if inner.config.server.is_clustered() {
          // optimistically sync the cluster, then fall back to a full reconnect
          if router.sync_cluster().await.is_err() {
            utils::delay_cluster_sync(inner).await?;
            utils::reconnect_with_policy(inner, router).await?
          }
        } else {
          utils::reconnect_with_policy(inner, router).await?
        };

        Ok(None)
      },
    }
  }
}

fn build_pipeline(
  commands: &[RedisCommand],
  response: ResponseKind,
  id: u64,
) -> Result<Vec<RedisCommand>, RedisError> {
  let mut pipeline = Vec::with_capacity(commands.len() + 1);
  let mut exec = RedisCommand::new(RedisCommandKind::Exec, vec![]);
  exec.can_pipeline = true;
  exec.skip_backpressure = true;
  exec.fail_fast = true;
  exec.transaction_id = Some(id);
  exec.response = response
    .duplicate()
    .ok_or_else(|| RedisError::new(RedisErrorKind::Unknown, "Invalid pipelined transaction response."))?;
  exec.response.set_expected_index(commands.len());

  for (idx, command) in commands.iter().enumerate() {
    let mut response = response
      .duplicate()
      .ok_or_else(|| RedisError::new(RedisErrorKind::Unknown, "Invalid pipelined transaction response."))?;
    response.set_expected_index(idx);
    let mut command = command.duplicate(response);
    command.fail_fast = true;
    command.skip_backpressure = true;
    command.can_pipeline = true;

    pipeline.push(command);
  }
  pipeline.push(exec);
  Ok(pipeline)
}

pub mod exec {
  use super::*;
  // TODO find a better way to combine these functions

  /// Run the transaction, following cluster redirects and reconnecting as needed.
  #[allow(unused_mut)]
  pub async fn non_pipelined(
    inner: &RefCount<RedisClientInner>,
    router: &mut Router,
    mut commands: Vec<RedisCommand>,
    id: u64,
    abort_on_error: bool,
    mut tx: ResponseSender,
  ) -> Result<(), RedisError> {
    if commands.is_empty() {
      let _ = tx.send(Ok(Resp3Frame::Null));
      return Ok(());
    }
    // each of the commands should have the same options
    let max_attempts = if commands[0].attempts_remaining == 0 {
      inner.max_command_attempts()
    } else {
      commands[0].attempts_remaining
    };
    let max_redirections = if commands[0].redirections_remaining == 0 {
      inner.connection.max_redirections
    } else {
      commands[0].redirections_remaining
    };

    let mut attempted = 0;
    let mut redirections = 0;
    'outer: loop {
      _debug!(inner, "Starting transaction {} (attempted: {})", id, attempted);
      let server = match find_or_create_connection(inner, router, &commands[0]).await? {
        Some(server) => server,
        None => continue,
      };

      let mut idx = 0;
      if attempted > 0 {
        inner.counters.incr_redelivery_count();
      }
      // send each of the commands. the first one is always MULTI
      'inner: while idx < commands.len() {
        let command = commands[idx].duplicate(ResponseKind::Skip);
        let rx = command.create_router_channel();

        // wait on each response before sending the next command in order to handle errors or follow cluster
        // redirections as quickly as possible.
        match write_command(inner, router, &server, command, abort_on_error, Some(rx)).await {
          Ok(TransactionResponse::Continue) => {
            idx += 1;
            continue 'inner;
          },
          Ok(TransactionResponse::Retry(error)) => {
            _debug!(inner, "Retrying trx {} after error: {:?}", id, error);
            if let Err(e) = send_non_pipelined_discard(inner, router, &server, id).await {
              _warn!(inner, "Error sending DISCARD in trx {}: {:?}", id, e);
            }

            attempted += 1;
            if attempted >= max_attempts {
              let _ = tx.send(Err(error));
              return Ok(());
            } else {
              utils::reconnect_with_policy(inner, router).await?;
            }

            continue 'outer;
          },
          Ok(TransactionResponse::Redirection((kind, slot, server))) => {
            redirections += 1;
            if redirections > max_redirections {
              let _ = tx.send(Err(RedisError::new(
                RedisErrorKind::Cluster,
                "Too many cluster redirections.",
              )));
              return Ok(());
            }

            update_hash_slot(&mut commands, slot);
            if let Err(e) = send_non_pipelined_discard(inner, router, &server, id).await {
              _warn!(inner, "Error sending DISCARD in trx {}: {:?}", id, e);
            }
            utils::cluster_redirect_with_policy(inner, router, kind, slot, &server).await?;

            continue 'outer;
          },
          Ok(TransactionResponse::Finished(frame)) => {
            let _ = tx.send(Ok(frame));
            return Ok(());
          },
          Err(error) => {
            // fatal errors that end the transaction
            let _ = send_non_pipelined_discard(inner, router, &server, id).await;
            let _ = tx.send(Err(error));
            return Ok(());
          },
        }
      }

      match send_non_pipelined_exec(inner, router, &server, id).await {
        Ok(TransactionResponse::Finished(frame)) => {
          let _ = tx.send(Ok(frame));
          return Ok(());
        },
        Ok(TransactionResponse::Retry(error)) => {
          _debug!(inner, "Retrying trx {} after error: {:?}", id, error);
          if let Err(e) = send_non_pipelined_discard(inner, router, &server, id).await {
            _warn!(inner, "Error sending DISCARD in trx {}: {:?}", id, e);
          }

          attempted += 1;
          if attempted >= max_attempts {
            let _ = tx.send(Err(error));
            return Ok(());
          } else {
            utils::reconnect_with_policy(inner, router).await?;
          }

          continue 'outer;
        },
        Ok(TransactionResponse::Redirection((kind, slot, dest))) => {
          // doesn't make sense on EXEC, but return it as an error so it isn't lost
          let _ = send_non_pipelined_discard(inner, router, &server, id).await;
          let _ = tx.send(Err(RedisError::new(
            RedisErrorKind::Cluster,
            format!("{} {} {}", kind, slot, dest),
          )));
          return Ok(());
        },
        Ok(TransactionResponse::Continue) => {
          _warn!(inner, "Invalid final response to transaction {}", id);
          let _ = send_non_pipelined_discard(inner, router, &server, id).await;
          let _ = tx.send(Err(RedisError::new_canceled()));
          return Ok(());
        },
        Err(error) => {
          let _ = send_non_pipelined_discard(inner, router, &server, id).await;
          let _ = tx.send(Err(error));
          return Ok(());
        },
      };
    }
  }

  #[allow(unused_mut)]
  pub async fn pipelined(
    inner: &RefCount<RedisClientInner>,
    router: &mut Router,
    mut commands: Vec<RedisCommand>,
    id: u64,
    mut tx: ResponseSender,
  ) -> Result<(), RedisError> {
    if commands.is_empty() {
      let _ = tx.send(Ok(Resp3Frame::Null));
      return Ok(());
    }
    // each of the commands should have the same options
    let max_attempts = if commands[0].attempts_remaining == 0 {
      inner.max_command_attempts()
    } else {
      commands[0].attempts_remaining
    };
    let max_redirections = if commands[0].redirections_remaining == 0 {
      inner.connection.max_redirections
    } else {
      commands[0].redirections_remaining
    };

    let mut attempted = 0;
    let mut redirections = 0;
    'outer: loop {
      _debug!(inner, "Starting transaction {} (attempted: {})", id, attempted);
      let server = match find_or_create_connection(inner, router, &commands[0]).await? {
        Some(server) => server,
        None => continue,
      };

      if attempted > 0 {
        inner.counters.incr_redelivery_count();
      }
      let (exec_tx, exec_rx) = oneshot_channel();
      let buf: Vec<_> = repeat(Resp3Frame::Null).take(commands.len() + 1).collect();
      // pipelined transactions buffer their results until a response to EXEC is received
      let response = ResponseKind::Buffer {
        error_early: false,
        expected:    commands.len() + 1,
        received:    RefCount::new(AtomicUsize::new(0)),
        tx:          RefCount::new(Mutex::new(Some(exec_tx))),
        frames:      RefCount::new(Mutex::new(buf)),
        index:       0,
      };

      // write each command in the pipeline
      let pipeline = build_pipeline(&commands, response, id)?;
      for command in pipeline.into_iter() {
        match write_command(inner, router, &server, command, false, None).await? {
          TransactionResponse::Continue => continue,
          TransactionResponse::Retry(error) => {
            _debug!(inner, "Retrying pipelined trx {} after error: {:?}", id, error);
            if let Err(e) = send_non_pipelined_discard(inner, router, &server, id).await {
              _warn!(inner, "Error sending pipelined discard: {:?}", e);
            }

            attempted += 1;
            if attempted >= max_attempts {
              let _ = tx.send(Err(error));
              return Ok(());
            } else {
              utils::reconnect_with_policy(inner, router).await?;
            }
            continue 'outer;
          },
          _ => {
            _error!(inner, "Unexpected pipelined write response.");
            let _ = tx.send(Err(RedisError::new(
              RedisErrorKind::Protocol,
              "Unexpected pipeline write response.",
            )));
            return Ok(());
          },
        }
      }

      // wait on the response and deconstruct the output frames
      let mut response = match exec_rx.await.map_err(RedisError::from) {
        Ok(Ok(frame)) => match frame {
          Resp3Frame::Array { data, .. } => data,
          _ => {
            _error!(inner, "Unexpected pipelined exec response.");
            let _ = tx.send(Err(RedisError::new(
              RedisErrorKind::Protocol,
              "Unexpected pipeline exec response.",
            )));
            return Ok(());
          },
        },
        Ok(Err(err)) | Err(err) => {
          _debug!(
            inner,
            "Reconnecting and retrying pipelined transaction after error: {:?}",
            err
          );
          attempted += 1;
          if attempted >= max_attempts {
            let _ = tx.send(Err(err));
            return Ok(());
          } else {
            utils::reconnect_with_policy(inner, router).await?;
          }

          continue 'outer;
        },
      };
      if response.is_empty() {
        let _ = tx.send(Err(RedisError::new(
          RedisErrorKind::Protocol,
          "Unexpected empty pipeline exec response.",
        )));
        return Ok(());
      }

      // check the last result for EXECABORT
      let execabort = response
        .last()
        .and_then(|f| f.as_str())
        .map(|s| s.starts_with("EXECABORT"))
        .unwrap_or(false);

      if execabort {
        // find the first error, if it's a redirection then follow it and retry, otherwise return to the caller
        let first_error = response.iter().enumerate().find_map(|(idx, frame)| {
          if matches!(frame.kind(), FrameKind::SimpleError | FrameKind::BlobError) {
            Some(idx)
          } else {
            None
          }
        });

        if let Some(idx) = first_error {
          let first_error_frame = response[idx].take();
          // check if error is a cluster redirection, otherwise return the error to the caller
          if first_error_frame.is_redirection() {
            redirections += 1;
            if redirections > max_redirections {
              let _ = tx.send(Err(RedisError::new(
                RedisErrorKind::Cluster,
                "Too many cluster redirections.",
              )));
              return Ok(());
            }

            let (kind, slot, dest) = parse_cluster_error_frame(inner, &first_error_frame, &server)?;
            update_hash_slot(&mut commands, slot);
            utils::cluster_redirect_with_policy(inner, router, kind, slot, &dest).await?;
            continue 'outer;
          } else {
            // these errors are typically from the server, not from the connection layer
            let error = first_error_frame.as_str().map(pretty_error).unwrap_or_else(|| {
              RedisError::new(
                RedisErrorKind::Protocol,
                "Unexpected response to pipelined transaction.",
              )
            });

            let _ = tx.send(Err(error));
            return Ok(());
          }
        } else {
          // return the EXECABORT error to the caller if there's no other error
          let error = response
            .pop()
            .and_then(|f| f.as_str().map(pretty_error))
            .unwrap_or_else(|| RedisError::new(RedisErrorKind::Protocol, "Invalid pipelined transaction response."));
          let _ = tx.send(Err(error));
          return Ok(());
        }
      } else {
        // return the last frame to the caller
        let last = response.pop().unwrap_or(Resp3Frame::Null);
        let _ = tx.send(Ok(last));
        return Ok(());
      }
    }
  }
}
