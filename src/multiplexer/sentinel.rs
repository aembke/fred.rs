use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::multiplexer::{utils, CloseTx, Connections, SentCommand};
use crate::protocol::codec::RedisCodec;
use crate::protocol::connection::RedisTransport;
use crate::utils as client_utils;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;

/// Use the sentinel API to find the correct primary/main node that should act as the centralized server to the multiplexer.
///
/// See [the documentation](https://redis.io/topics/sentinel-clients) for more information.
pub async fn connect_sentinel(
  inner: &Arc<RedisClientInner>,
  connections: &Connections,
  close_tx: &Arc<RwLock<Option<CloseTx>>>,
) -> Result<VecDeque<SentCommand>, RedisError> {
  let pending_commands = utils::take_sent_commands(connections).await;

  // try each node in the sentinel
  // when one is found that we can connect to, if it's not hte first one in the list then swap it in the list with the first one so its first next time
  // run SENTINEL get-master-addr-by-name master-name
  // run SENTINEL sentinels <master-name> to get the list of all the sentinels, update the hosts list on the config with this
  // get the host:port of the master from the last command
  // run the ROLE command on the master, if it's not a master then break with a special error
  // update the connection state with the connection to the master

  // TODO need to do a lot of work from connect_centralized here (getting connection IDs, etc)
  // TODO write an install script for redis sentinel

  Ok(pending_commands)
}
