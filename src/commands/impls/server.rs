use super::*;
use crate::{
  clients::RedisClient,
  error::*,
  interfaces,
  modules::inner::RedisClientInner,
  prelude::Resp3Frame,
  protocol::{
    command::{MultiplexerCommand, RedisCommand, RedisCommandKind},
    responders::ResponseKind,
    utils as protocol_utils,
  },
  types::*,
  utils,
};
use bytes_utils::Str;
use std::sync::Arc;
use tokio::sync::oneshot::channel as oneshot_channel;

pub async fn quit<C: ClientLike>(client: &C) -> Result<(), RedisError> {
  let inner = client.inner().clone();
  _debug!(inner, "Closing Redis connection with Quit command.");

  utils::set_client_state(&inner.state, ClientState::Disconnecting);
  inner.notifications.broadcast_close();
  let _ = utils::request_response(client, || Ok((RedisCommandKind::Quit, vec![]))).await;
  utils::set_client_state(&inner.state, ClientState::Disconnected);
  Ok(())
}

pub async fn shutdown<C: ClientLike>(client: &C, flags: Option<ShutdownFlags>) -> Result<(), RedisError> {
  let inner = client.inner().clone();
  _debug!(inner, "Shutting down server.");
  inner.notifications.broadcast_close();

  utils::set_client_state(&inner.state, ClientState::Disconnecting);
  let _ = utils::request_response(client, move || {
    let args = if let Some(flags) = flags {
      vec![flags.to_str().into()]
    } else {
      Vec::new()
    };

    Ok((RedisCommandKind::Shutdown, args))
  })
  .await?;

  utils::set_client_state(&inner.state, ClientState::Disconnected);
  Ok(())
}

/// Create a new client struct for each unique primary cluster node based on the cached cluster state.
pub fn split(inner: &Arc<RedisClientInner>) -> Result<Vec<RedisClient>, RedisError> {
  if !inner.config.server.is_clustered() {
    return Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected clustered redis deployment.",
    ));
  }
  let servers = inner.with_cluster_state(|state| Ok(state.unique_primary_nodes()))?;

  Ok(
    servers
      .into_iter()
      .filter_map(|server| {
        let (host, port) = match protocol_utils::server_to_parts(&server) {
          Ok((host, port)) => (host.to_owned(), port),
          Err(_) => {
            _debug!(inner, "Failed to parse server {}", server);
            return None;
          },
        };

        let mut config = inner.config.as_ref().clone();
        config.server = ServerConfig::Centralized { host, port };
        let perf = inner.performance_config();
        let policy = inner.reconnect_policy();

        Some(RedisClient::new(config, Some(perf), policy))
      })
      .collect(),
  )
}

pub async fn force_reconnection(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  let (tx, rx) = oneshot_channel();
  let command = MultiplexerCommand::Reconnect {
    server: None,
    force:  true,
    tx:     Some(tx),
  };
  let _ = interfaces::send_to_multiplexer(inner, command)?;

  rx.await?.map(|_| ())
}

pub async fn flushall<C: ClientLike>(client: &C, r#async: bool) -> Result<RedisValue, RedisError> {
  let args = if r#async { vec![static_val!(ASYNC)] } else { Vec::new() };
  let frame = utils::request_response(client, move || Ok((RedisCommandKind::FlushAll, args))).await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn flushall_cluster<C: ClientLike>(client: &C) -> Result<(), RedisError> {
  if !client.inner().config.server.is_clustered() {
    return flushall(client, false).await.map(|_| ());
  }

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::new_buffer(client.inner().num_cluster_nodes(), tx);
  let command: RedisCommand = (RedisCommandKind::_FlushAllCluster, vec![], response).into();
  let _ = client.send_command(command)?;

  let _ = rx.await??;
  Ok(())
}

pub async fn ping<C: ClientLike>(client: &C) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || Ok((RedisCommandKind::Ping, vec![]))).await?;
  protocol_utils::frame_to_single_result(frame)
}

pub async fn select<C: ClientLike>(client: &C, db: u8) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || Ok((RedisCommandKind::Select, vec![db.into()]))).await?;
  protocol_utils::frame_to_single_result(frame)
}

pub async fn info<C: ClientLike>(client: &C, section: Option<InfoKind>) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(1);
    if let Some(section) = section {
      args.push(section.to_str().into());
    }

    Ok((RedisCommandKind::Info, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn hello<C: ClientLike>(
  client: &C,
  version: RespVersion,
  auth: Option<(String, String)>,
) -> Result<(), RedisError> {
  let args = if let Some((username, password)) = auth {
    vec![username.into(), password.into()]
  } else {
    vec![]
  };

  if client.inner().config.server.is_clustered() {
    let (tx, rx) = oneshot_channel();
    let mut command: RedisCommand = RedisCommandKind::_HelloAllCluster(version).into();
    command.response = ResponseKind::new_buffer(client.inner().num_cluster_nodes(), tx);

    let _ = client.send_command(command)?;
    let _ = rx.await??;
    Ok(())
  } else {
    let frame = utils::request_response(client, move || Ok((RedisCommandKind::_Hello(version), args))).await?;
    let _ = protocol_utils::frame_to_results(frame)?;
    Ok(())
  }
}

pub async fn auth<C: ClientLike>(client: &C, username: Option<String>, password: Str) -> Result<(), RedisError> {
  let mut args = Vec::with_capacity(2);
  if let Some(username) = username {
    args.push(username.into());
  }
  args.push(password.into());

  if client.inner().config.server.is_clustered() {
    let (tx, rx) = oneshot_channel();
    let response = ResponseKind::new_buffer(client.inner().num_cluster_nodes(), tx);
    let command: RedisCommand = (RedisCommandKind::_AuthAllCluster, args, response).into();
    let _ = client.send_command(command)?;

    let _ = rx.await??;
    Ok(())
  } else {
    let frame = utils::request_response(client, move || Ok((RedisCommandKind::Auth, args))).await?;

    let response = protocol_utils::frame_to_single_result(frame)?;
    protocol_utils::expect_ok(&response)
  }
}

pub async fn custom<C: ClientLike>(
  client: &C,
  cmd: CustomCommand,
  args: Vec<RedisValue>,
) -> Result<RedisValue, RedisError> {
  args_values_cmd(client, RedisCommandKind::_Custom(cmd), args).await
}

pub async fn custom_raw<C: ClientLike>(
  client: &C,
  cmd: CustomCommand,
  args: Vec<RedisValue>,
) -> Result<Resp3Frame, RedisError> {
  utils::request_response(client, move || Ok((RedisCommandKind::_Custom(cmd), args))).await
}

value_cmd!(dbsize, DBSize);
value_cmd!(bgrewriteaof, BgreWriteAof);
value_cmd!(bgsave, BgSave);

pub async fn failover<C: ClientLike>(
  client: &C,
  to: Option<(String, u16)>,
  force: bool,
  abort: bool,
  timeout: Option<u32>,
) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(7);
    if let Some((host, port)) = to {
      args.push(static_val!(TO));
      args.push(host.into());
      args.push(port.into());
    }
    if force {
      args.push(static_val!(FORCE));
    }
    if abort {
      args.push(static_val!(ABORT));
    }
    if let Some(timeout) = timeout {
      args.push(static_val!(TIMEOUT));
      args.push(timeout.into());
    }

    Ok((RedisCommandKind::Failover, args))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

value_cmd!(lastsave, LastSave);
