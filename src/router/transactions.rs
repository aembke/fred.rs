use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces::Resp3Frame,
  modules::inner::RedisClientInner,
  protocol::{
    command::{ClusterErrorKind, RedisCommand, RedisCommandKind, ResponseSender, RouterReceiver, RouterResponse},
    responders::ResponseKind,
  },
  router::{utils, Router, Written},
  types::{ClusterHash, Server},
  utils as client_utils,
};
use std::sync::Arc;

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
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  server: &Server,
  command: RedisCommand,
  abort_on_error: bool,
  rx: RouterReceiver,
) -> Result<TransactionResponse, RedisError> {
  _trace!(
    inner,
    "Sending trx command {} to {}",
    command.kind.to_str_debug(),
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
    _debug!(inner, "Error writing trx command: {:?}", e);
    return Ok(TransactionResponse::Retry(e));
  }

  match client_utils::apply_timeout(rx, timeout_dur).await? {
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
}

/// Send EXEC to the provided server.
async fn send_exec(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  server: &Server,
  id: u64,
) -> Result<TransactionResponse, RedisError> {
  let mut command = RedisCommand::new(RedisCommandKind::Exec, vec![]);
  command.can_pipeline = false;
  command.skip_backpressure = true;
  command.transaction_id = Some(id);
  let rx = command.create_router_channel();

  write_command(inner, router, server, command, true, rx).await
}

/// Send DISCARD to the provided server.
async fn send_discard(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  server: &Server,
  id: u64,
) -> Result<TransactionResponse, RedisError> {
  let mut command = RedisCommand::new(RedisCommandKind::Discard, vec![]);
  command.can_pipeline = false;
  command.skip_backpressure = true;
  command.transaction_id = Some(id);
  let rx = command.create_router_channel();

  write_command(inner, router, server, command, true, rx).await
}

fn update_hash_slot(commands: &mut [RedisCommand], slot: u16) {
  for command in commands.iter_mut() {
    command.hasher = ClusterHash::Custom(slot);
  }
}

/// Run the transaction, following cluster redirects and reconnecting as needed.
// this would be a lot cleaner with GATs if we could abstract the inner loops with async closures
pub async fn run(
  inner: &Arc<RedisClientInner>,
  router: &mut Router,
  mut commands: Vec<RedisCommand>,
  watched: Option<RedisCommand>,
  id: u64,
  abort_on_error: bool,
  tx: ResponseSender,
) -> Result<(), RedisError> {
  if commands.is_empty() {
    let _ = tx.send(Ok(Resp3Frame::Null));
    return Ok(());
  }

  let mut attempted = 0;
  let mut redirections = 0;
  'outer: loop {
    _debug!(inner, "Starting transaction {} (attempted: {})", id, attempted);

    let server = if let Some(server) = commands[0].cluster_node.as_ref() {
      server.clone()
    } else {
      match router.find_connection(&commands[0]) {
        Some(server) => server.clone(),
        None => {
          if inner.config.server.is_clustered() {
            // optimistically sync the cluster, then fall back to a full reconnect
            if router.sync_cluster().await.is_err() {
              utils::reconnect_with_policy(inner, router).await?
            }
          } else {
            utils::reconnect_with_policy(inner, router).await?
          };

          attempted += 1;
          continue;
        },
      }
    };
    let mut idx = 0;

    // send the WATCH command before any of the trx commands
    if let Some(watch) = watched.as_ref() {
      let watch = watch.duplicate(ResponseKind::Skip);
      let rx = watch.create_router_channel();

      _debug!(
        inner,
        "Sending WATCH for {} keys in trx {} to {}",
        watch.args().len(),
        id,
        server
      );
      match write_command(inner, router, &server, watch, false, rx).await {
        Ok(TransactionResponse::Continue) => {
          _debug!(inner, "Successfully sent WATCH command before transaction {}.", id);
        },
        Ok(TransactionResponse::Retry(error)) => {
          _debug!(inner, "Retrying trx {} after WATCH error: {:?}.", id, error);

          if attempted >= inner.max_command_attempts() {
            let _ = tx.send(Err(error));
            return Ok(());
          } else {
            utils::reconnect_with_policy(inner, router).await?;
          }

          attempted += 1;
          continue 'outer;
        },
        Ok(TransactionResponse::Redirection((kind, slot, server))) => {
          redirections += 1;
          if redirections > inner.connection.max_redirections {
            let _ = tx.send(Err(RedisError::new(
              RedisErrorKind::Cluster,
              "Too many cluster redirections.",
            )));
            return Ok(());
          }

          _debug!(inner, "Recv {} redirection to {} for WATCH in trx {}", kind, server, id);
          update_hash_slot(&mut commands, slot);
          utils::cluster_redirect_with_policy(inner, router, kind, slot, &server).await?;
          attempted += 1;
          continue 'outer;
        },
        Ok(TransactionResponse::Finished(frame)) => {
          _warn!(inner, "Unexpected trx finished frame after WATCH.");
          let _ = tx.send(Ok(frame));
          return Ok(());
        },
        Err(error) => {
          let _ = tx.send(Err(error));
          return Ok(());
        },
      };
    }

    // start sending the trx commands
    'inner: while idx < commands.len() {
      let command = commands[idx].duplicate(ResponseKind::Skip);
      let rx = command.create_router_channel();

      match write_command(inner, router, &server, command, abort_on_error, rx).await {
        Ok(TransactionResponse::Continue) => {
          idx += 1;
          continue 'inner;
        },
        Ok(TransactionResponse::Retry(error)) => {
          _debug!(inner, "Retrying trx {} after error: {:?}", id, error);
          if let Err(e) = send_discard(inner, router, &server, id).await {
            _warn!(inner, "Error sending DISCARD in trx {}: {:?}", id, e);
          }

          if attempted >= inner.max_command_attempts() {
            let _ = tx.send(Err(error));
            return Ok(());
          } else {
            utils::reconnect_with_policy(inner, router).await?;
          }

          attempted += 1;
          continue 'outer;
        },
        Ok(TransactionResponse::Redirection((kind, slot, server))) => {
          redirections += 1;
          if redirections > inner.connection.max_redirections {
            let _ = tx.send(Err(RedisError::new(
              RedisErrorKind::Cluster,
              "Too many cluster redirections.",
            )));
            return Ok(());
          }

          update_hash_slot(&mut commands, slot);
          if let Err(e) = send_discard(inner, router, &server, id).await {
            _warn!(inner, "Error sending DISCARD in trx {}: {:?}", id, e);
          }
          utils::cluster_redirect_with_policy(inner, router, kind, slot, &server).await?;

          attempted += 1;
          continue 'outer;
        },
        Ok(TransactionResponse::Finished(frame)) => {
          let _ = tx.send(Ok(frame));
          return Ok(());
        },
        Err(error) => {
          // fatal errors that end the transaction
          let _ = send_discard(inner, router, &server, id).await;
          let _ = tx.send(Err(error));
          return Ok(());
        },
      }
    }

    match send_exec(inner, router, &server, id).await {
      Ok(TransactionResponse::Finished(frame)) => {
        let _ = tx.send(Ok(frame));
        return Ok(());
      },
      Ok(TransactionResponse::Retry(error)) => {
        _debug!(inner, "Retrying trx {} after error: {:?}", id, error);
        if let Err(e) = send_discard(inner, router, &server, id).await {
          _warn!(inner, "Error sending DISCARD in trx {}: {:?}", id, e);
        }

        if attempted >= inner.max_command_attempts() {
          let _ = tx.send(Err(error));
          return Ok(());
        } else {
          utils::reconnect_with_policy(inner, router).await?;
        }

        attempted += 1;
        continue 'outer;
      },
      Ok(TransactionResponse::Redirection((kind, slot, dest))) => {
        // doesn't make sense on EXEC, but return it as an error so it isn't lost
        let _ = send_discard(inner, router, &server, id).await;
        let _ = tx.send(Err(RedisError::new(
          RedisErrorKind::Cluster,
          format!("{} {} {}", kind, slot, dest),
        )));
        return Ok(());
      },
      Ok(TransactionResponse::Continue) => {
        _warn!(inner, "Invalid final response to transaction {}", id);
        let _ = send_discard(inner, router, &server, id).await;
        let _ = tx.send(Err(RedisError::new_canceled()));
        return Ok(());
      },
      Err(error) => {
        let _ = send_discard(inner, router, &server, id).await;
        let _ = tx.send(Err(error));
        return Ok(());
      },
    };
  }
}
