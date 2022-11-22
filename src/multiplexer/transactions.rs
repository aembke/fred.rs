use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces::Resp3Frame,
  modules::inner::RedisClientInner,
  multiplexer::{utils, Multiplexer},
  protocol::{
    command::{
      ClusterErrorKind,
      MultiplexerReceiver,
      MultiplexerResponse,
      RedisCommand,
      RedisCommandKind,
      ResponseSender,
    },
    responders::ResponseKind,
  },
  types::{ClusterHash, Server},
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

/// Write a command in the context of a transaction and process the multiplexer response.
///
/// Returns the command result policy or a fatal error that should end the transaction.
async fn write_command(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  server: &Server,
  command: RedisCommand,
  abort_on_error: bool,
  rx: MultiplexerReceiver,
) -> Result<TransactionResponse, RedisError> {
  _trace!(
    inner,
    "Sending trx command {} to {}",
    command.kind.to_str_debug(),
    server
  );

  if let Err(e) = multiplexer.write_once(command, server).await {
    _debug!(inner, "Error writing trx command: {:?}", e);
    return Ok(TransactionResponse::Retry(e));
  }

  match rx.await? {
    MultiplexerResponse::Continue => Ok(TransactionResponse::Continue),
    MultiplexerResponse::Ask((slot, server, _)) => {
      Ok(TransactionResponse::Redirection((ClusterErrorKind::Ask, slot, server)))
    },
    MultiplexerResponse::Moved((slot, server, _)) => Ok(TransactionResponse::Redirection((
      ClusterErrorKind::Moved,
      slot,
      server,
    ))),
    MultiplexerResponse::ConnectionClosed((err, _)) => Ok(TransactionResponse::Retry(err)),
    MultiplexerResponse::TransactionError((err, _)) => {
      if abort_on_error {
        Err(err)
      } else {
        Ok(TransactionResponse::Continue)
      }
    },
    MultiplexerResponse::TransactionResult(frame) => Ok(TransactionResponse::Finished(frame)),
  }
}

/// Send EXEC to the provided server.
async fn send_exec(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  server: &Server,
  id: u64,
) -> Result<TransactionResponse, RedisError> {
  let mut command = RedisCommand::new(RedisCommandKind::Exec, vec![]);
  command.can_pipeline = false;
  command.skip_backpressure = true;
  command.transaction_id = Some(id);
  let rx = command.create_multiplexer_channel();

  write_command(inner, multiplexer, server, command, true, rx).await
}

/// Send DISCARD to the provided server.
async fn send_discard(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  server: &Server,
  id: u64,
) -> Result<TransactionResponse, RedisError> {
  let mut command = RedisCommand::new(RedisCommandKind::Discard, vec![]);
  command.can_pipeline = false;
  command.skip_backpressure = true;
  command.transaction_id = Some(id);
  let rx = command.create_multiplexer_channel();

  write_command(inner, multiplexer, server, command, true, rx).await
}

fn update_hash_slot(commands: &mut Vec<RedisCommand>, slot: u16) {
  for command in commands.iter_mut() {
    command.hasher = ClusterHash::Custom(slot);
  }
}

/// Run the transaction, following cluster redirects and reconnecting as needed.
pub async fn run(
  inner: &Arc<RedisClientInner>,
  multiplexer: &mut Multiplexer,
  mut commands: Vec<RedisCommand>,
  id: u64,
  abort_on_error: bool,
  tx: ResponseSender,
) -> Result<(), RedisError> {
  if commands.is_empty() {
    let _ = tx.send(Ok(Resp3Frame::Null));
    return Ok(());
  }

  let mut attempted = 0;
  'outer: loop {
    _debug!(inner, "Starting transaction {} (attempted: {})", id, attempted);

    let server = match multiplexer.find_connection(&commands[0]) {
      Some(server) => server.clone(),
      None => {
        let _ = if inner.config.server.is_clustered() {
          // optimistically sync the cluster, then fall back to a full reconnect
          if multiplexer.sync_cluster().await.is_err() {
            utils::reconnect_with_policy(inner, multiplexer).await?
          }
        } else {
          utils::reconnect_with_policy(inner, multiplexer).await?
        };

        attempted += 1;
        continue;
      },
    };
    let mut idx = 0;

    'inner: while idx < commands.len() {
      let mut command = commands[idx].duplicate(ResponseKind::Skip);
      let rx = command.create_multiplexer_channel();

      match write_command(inner, multiplexer, &server, command, abort_on_error, rx).await {
        Ok(TransactionResponse::Continue) => {
          idx += 1;
          continue 'inner;
        },
        Ok(TransactionResponse::Retry(error)) => {
          _debug!(inner, "Retrying trx {} after error: {:?}", id, error);
          if let Err(e) = send_discard(inner, multiplexer, &server, id).await {
            _warn!(inner, "Error sending DISCARD in trx {}: {:?}", id, e);
          }

          if attempted >= inner.max_command_attempts() {
            let _ = tx.send(Err(error));
            return Ok(());
          } else {
            let _ = utils::reconnect_with_policy(inner, multiplexer).await?;
          }

          attempted += 1;
          continue 'outer;
        },
        Ok(TransactionResponse::Redirection((kind, slot, server))) => {
          update_hash_slot(&mut commands, slot);
          if let Err(e) = send_discard(inner, multiplexer, &server, id).await {
            _warn!(inner, "Error sending DISCARD in trx {}: {:?}", id, e);
          }
          let _ = utils::cluster_redirect_with_policy(inner, multiplexer, kind, slot, &server).await?;

          attempted += 1;
          continue 'outer;
        },
        Ok(TransactionResponse::Finished(frame)) => {
          let _ = tx.send(Ok(frame));
          return Ok(());
        },
        Err(error) => {
          // fatal errors that end the transaction
          let _ = send_discard(inner, multiplexer, &server, id).await;
          let _ = tx.send(Err(error));
          return Ok(());
        },
      }
    }

    match send_exec(inner, multiplexer, &server, id).await {
      Ok(TransactionResponse::Finished(frame)) => {
        let _ = tx.send(Ok(frame));
        return Ok(());
      },
      Ok(TransactionResponse::Retry(error)) => {
        _debug!(inner, "Retrying trx {} after error: {:?}", id, error);
        if let Err(e) = send_discard(inner, multiplexer, &server, id).await {
          _warn!(inner, "Error sending DISCARD in trx {}: {:?}", id, e);
        }

        if attempted >= inner.max_command_attempts() {
          let _ = tx.send(Err(error));
          return Ok(());
        } else {
          let _ = utils::reconnect_with_policy(inner, multiplexer).await?;
        }

        attempted += 1;
        continue 'outer;
      },
      Ok(TransactionResponse::Redirection((kind, slot, dest))) => {
        // doesn't make sense on EXEC, but return it as an error so it isn't lost
        let _ = send_discard(inner, multiplexer, &server, id).await;
        let _ = tx.send(Err(RedisError::new(
          RedisErrorKind::Cluster,
          format!("{} {} {}", kind, slot, dest),
        )));
        return Ok(());
      },
      Ok(TransactionResponse::Continue) => {
        _warn!(inner, "Invalid final response to transaction {}", id);
        let _ = send_discard(inner, multiplexer, &server, id).await;
        let _ = tx.send(Err(RedisError::new_canceled()));
        return Ok(());
      },
      Err(error) => {
        let _ = send_discard(inner, multiplexer, &server, id).await;
        let _ = tx.send(Err(error));
        return Ok(());
      },
    };
  }
}
