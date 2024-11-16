use crate::{
  error::RedisError,
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RedisCommandKind, ResponseSender},
    connection::SplitConnection,
  },
  router::{utils, Router},
  runtime::RefCount,
  types::{ClusterHash, Server},
};

/// Send DISCARD to the provided server.
async fn discard(inner: &RefCount<RedisClientInner>, conn: &mut SplitConnection) -> Result<(), RedisError> {
  let command = RedisCommand::new(RedisCommandKind::Discard, vec![]);
  // write it, read non pubsub, assert ok
  unimplemented!()
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

/// Send the transaction to the server.
pub async fn send(
  inner: &RefCount<RedisClientInner>,
  router: &mut Router,
  commands: Vec<RedisCommand>,
  id: u64,
  abort_on_error: bool,
  tx: ResponseSender,
) {
  // enter a loop to retry if an EXECABORT or redirection is received
  // find or create the connection
  // prepare the commands, adding exec at the end
  // try to write all the commands
  // read responses until receiving the expected amount or an error
  //   depending on the error retry the outer loop (sync + updating cluster hash slot on redirect)
  //   if not EXECABORT or redirect and abort_on_error then send discard and forward error
  // take the last response and forward to tx
  // incr attempted and redirections when continuing loop
  unimplemented!()
}
