use super::*;
use crate::{
  clients::RedisClient,
  error::*,
  interfaces,
  modules::inner::RedisClientInner,
  prelude::Resp3Frame,
  protocol::{
    command::{RedisCommand, RedisCommandKind, RouterCommand},
    responders::ResponseKind,
    utils as protocol_utils,
  },
  runtime::{oneshot_channel, RefCount},
  types::*,
  utils,
};
use bytes_utils::Str;

pub async fn quit<C: ClientLike>(client: &C) -> Result<(), RedisError> {
  let inner = client.inner().clone();
  _debug!(inner, "Closing Redis connection with Quit command.");

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::Respond(Some(tx));
  let mut command: RedisCommand = (RedisCommandKind::Quit, vec![], response).into();

  utils::set_client_state(&inner.state, ClientState::Disconnecting);
  inner.notifications.broadcast_close();
  let timeout_dur = utils::prepare_command(client, &mut command);
  client.send_command(command)?;
  let _ = utils::timeout(rx, timeout_dur).await??;
  inner
    .notifications
    .close_public_receivers(inner.with_perf_config(|c| c.broadcast_channel_capacity));
  inner.backchannel.check_and_disconnect(&inner, None).await;

  Ok(())
}

pub async fn shutdown<C: ClientLike>(client: &C, flags: Option<ShutdownFlags>) -> Result<(), RedisError> {
  let inner = client.inner().clone();
  _debug!(inner, "Shutting down server.");

  let args = if let Some(flags) = flags {
    vec![flags.to_str().into()]
  } else {
    Vec::new()
  };
  let (tx, rx) = oneshot_channel();
  let mut command: RedisCommand = if inner.config.server.is_clustered() {
    let response = ResponseKind::new_buffer(tx);
    (RedisCommandKind::Shutdown, args, response).into()
  } else {
    let response = ResponseKind::Respond(Some(tx));
    (RedisCommandKind::Shutdown, args, response).into()
  };
  utils::set_client_state(&inner.state, ClientState::Disconnecting);
  inner.notifications.broadcast_close();

  let timeout_dur = utils::prepare_command(client, &mut command);
  client.send_command(command)?;
  let _ = utils::timeout(rx, timeout_dur).await??;
  inner
    .notifications
    .close_public_receivers(inner.with_perf_config(|c| c.broadcast_channel_capacity));
  inner.backchannel.check_and_disconnect(&inner, None).await;

  Ok(())
}

/// Create a new client struct for each unique primary cluster node based on the cached cluster state.
pub fn split(inner: &RefCount<RedisClientInner>) -> Result<Vec<RedisClient>, RedisError> {
  if !inner.config.server.is_clustered() {
    return Err(RedisError::new(
      RedisErrorKind::Config,
      "Expected clustered redis deployment.",
    ));
  }
  let servers = inner.with_cluster_state(|state| Ok(state.unique_primary_nodes()))?;
  _debug!(inner, "Unique primary nodes in split: {:?}", servers);

  Ok(
    servers
      .into_iter()
      .map(|server| {
        let mut config = inner.config.as_ref().clone();
        config.server = ServerConfig::Centralized { server };
        let perf = inner.performance_config();
        let policy = inner.reconnect_policy();
        let connection = inner.connection_config();

        RedisClient::new(config, Some(perf), Some(connection), policy)
      })
      .collect(),
  )
}

pub async fn force_reconnection(inner: &RefCount<RedisClientInner>) -> Result<(), RedisError> {
  let (tx, rx) = oneshot_channel();
  let command = RouterCommand::Reconnect {
    server:                               None,
    force:                                true,
    tx:                                   Some(tx),
    #[cfg(feature = "replicas")]
    replica:                              false,
  };
  interfaces::send_to_router(inner, command)?;

  rx.await?.map(|_| ())
}

pub async fn flushall<C: ClientLike>(client: &C, r#async: bool) -> Result<RedisValue, RedisError> {
  let args = if r#async { vec![static_val!(ASYNC)] } else { Vec::new() };
  let frame = utils::request_response(client, move || Ok((RedisCommandKind::FlushAll, args))).await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn flushall_cluster<C: ClientLike>(client: &C) -> Result<(), RedisError> {
  if !client.inner().config.server.is_clustered() {
    return flushall(client, false).await.map(|_| ());
  }

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::Respond(Some(tx));
  let mut command: RedisCommand = (RedisCommandKind::_FlushAllCluster, vec![], response).into();
  let timeout_dur = utils::prepare_command(client, &mut command);
  client.send_command(command)?;

  let _ = utils::timeout(rx, timeout_dur).await??;
  Ok(())
}

pub async fn ping<C: ClientLike>(client: &C) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || Ok((RedisCommandKind::Ping, vec![]))).await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn select<C: ClientLike>(client: &C, db: u8) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || Ok((RedisCommandKind::Select, vec![(db as i64).into()]))).await?;
  protocol_utils::frame_to_results(frame)
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

  protocol_utils::frame_to_results(frame)
}

pub async fn hello<C: ClientLike>(
  client: &C,
  version: RespVersion,
  auth: Option<(Str, Str)>,
  setname: Option<Str>,
) -> Result<(), RedisError> {
  let mut args = if let Some((username, password)) = auth {
    vec![username.into(), password.into()]
  } else {
    vec![]
  };
  if let Some(name) = setname {
    args.push(name.into());
  }

  if client.inner().config.server.is_clustered() {
    let (tx, rx) = oneshot_channel();
    let mut command: RedisCommand = RedisCommandKind::_HelloAllCluster(version).into();
    command.response = ResponseKind::Respond(Some(tx));

    let timeout_dur = utils::prepare_command(client, &mut command);
    client.send_command(command)?;
    let _ = utils::timeout(rx, timeout_dur).await??;
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
    let response = ResponseKind::Respond(Some(tx));
    let mut command: RedisCommand = (RedisCommandKind::_AuthAllCluster, args, response).into();

    let timeout_dur = utils::prepare_command(client, &mut command);
    client.send_command(command)?;
    let _ = utils::timeout(rx, timeout_dur).await??;
    Ok(())
  } else {
    let frame = utils::request_response(client, move || Ok((RedisCommandKind::Auth, args))).await?;

    let response = protocol_utils::frame_to_results(frame)?;
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

#[cfg(feature = "i-server")]
value_cmd!(dbsize, DBSize);
#[cfg(feature = "i-server")]
value_cmd!(bgrewriteaof, BgreWriteAof);
#[cfg(feature = "i-server")]
value_cmd!(bgsave, BgSave);

#[cfg(feature = "i-server")]
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

  let response = protocol_utils::frame_to_results(frame)?;
  protocol_utils::expect_ok(&response)
}

#[cfg(feature = "i-server")]
value_cmd!(lastsave, LastSave);

#[cfg(feature = "i-server")]
pub async fn wait<C: ClientLike>(client: &C, numreplicas: i64, timeout: i64) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Wait, vec![numreplicas.into(), timeout.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
