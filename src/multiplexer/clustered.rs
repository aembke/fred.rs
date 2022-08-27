use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::utils;
use crate::multiplexer::Written;
use crate::protocol::command::RedisCommand;
use crate::protocol::connection::{Counters, RedisWriter};
use crate::protocol::types::ClusterRouting;
use crate::utils as client_utils;
use arcstr::ArcStr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub fn find_cluster_node<'a>(
  inner: &Arc<RedisClientInner>,
  state: &'a ClusterRouting,
  command: &RedisCommand,
) -> Option<&'a ArcStr> {
  command
    .cluster_hash()
    .and_then(|slot| state.get_server(slot))
    .or_else(|| {
      let node = state.random_node();
      _trace!(
        inner,
        "Using random cluster node `{:?}` for {}",
        node,
        command.kind.to_str_debug()
      );
      node
    })
}

pub async fn send_command<'a, T>(
  inner: &Arc<RedisClientInner>,
  writers: &mut HashMap<ArcStr, RedisWriter<T>>,
  state: &'a ClusterRouting,
  mut command: RedisCommand,
) -> Result<Written<'a>, (RedisError, RedisCommand)>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  let server = match find_cluster_node(inner, state, &command) {
    Some(server) => server,
    None => {
      // these errors almost always mean the cluster is misconfigured
      _warn!(
        inner,
        "Possible cluster misconfiguration. Missing hash slot owner for {:?}",
        command.cluster_hash()
      );
      command.respond_to_caller(Err(RedisError::new(
        RedisErrorKind::Cluster,
        "Missing cluster hash slot owner.",
      )));
      return Ok(Written::Ignore);
    },
  };

  if let Some(writer) = writers.get_mut(server) {
    _debug!(inner, "Writing command `{}` to {}", command.kind.to_str_debug(), server);
    Ok(utils::write_command(inner, writer, command).await)
  } else {
    // a reconnect message should already be queued from the reader task
    _debug!(
      inner,
      "Failed to read connection {} for {}",
      server,
      command.kind.to_str_debug()
    );
    Err((RedisError::new(RedisErrorKind::IO, "Missing connection."), command))
  }
}

pub async fn create_cluster_change(
  cluster_state: &ClusterKeyCache,
  writers: &Arc<AsyncRwLock<BTreeMap<Arc<String>, RedisSink>>>,
) -> ClusterChange {
  let mut old_servers = BTreeSet::new();
  let mut new_servers = BTreeSet::new();
  for server in cluster_state.unique_main_nodes().into_iter() {
    new_servers.insert(server);
  }
  {
    for server in writers.write().await.keys() {
      old_servers.insert(server.clone());
    }
  }

  ClusterChange {
    add: new_servers.difference(&old_servers).map(|s| s.clone()).collect(),
    remove: old_servers.difference(&new_servers).map(|s| s.clone()).collect(),
  }
}
