use super::*;
use crate::{
  clients::RedisClient,
  error::*,
  modules::inner::RedisClientInner,
  prelude::Resp3Frame,
  protocol::{types::*, utils as protocol_utils},
  types::*,
  utils,
};
use bytes_utils::Str;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::oneshot::channel as oneshot_channel;

pub async fn quit(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  _debug!(inner, "Closing Redis connection with Quit command.");
  utils::interrupt_reconnect_sleep(inner);

  utils::set_client_state(&inner.state, ClientState::Disconnecting);
  let _ = utils::request_response(inner, || Ok((RedisCommandKind::Quit, vec![]))).await;

  // close anything left over from previous connections or reconnection attempts
  utils::shutdown_listeners(inner);
  utils::set_client_state(&inner.state, ClientState::Disconnected);

  Ok(())
}

pub async fn shutdown(inner: &Arc<RedisClientInner>, flags: Option<ShutdownFlags>) -> Result<(), RedisError> {
  _debug!(inner, "Shutting down server.");
  utils::interrupt_reconnect_sleep(inner);

  utils::set_client_state(&inner.state, ClientState::Disconnecting);
  let _ = utils::request_response(inner, move || {
    let args = if let Some(flags) = flags {
      vec![flags.to_str().into()]
    } else {
      Vec::new()
    };

    Ok((RedisCommandKind::Shutdown, args))
  })
  .await?;

  utils::shutdown_listeners(inner);
  utils::set_client_state(&inner.state, ClientState::Disconnected);

  Ok(())
}

pub async fn split(inner: &Arc<RedisClientInner>) -> Result<Vec<RedisClient>, RedisError> {
  let (tx, rx) = oneshot_channel();
  let config = utils::read_locked(&inner.config);
  if !config.server.is_clustered() {
    return Err(RedisError::new(
      RedisErrorKind::Unknown,
      "Expected clustered redis deployment.",
    ));
  }
  let tx = Arc::new(RwLock::new(Some(tx)));

  let split_cmd = SplitCommand {
    tx,
    config: Some(config),
  };
  let cmd = RedisCommand::new(RedisCommandKind::_Split(split_cmd), vec![], None);
  utils::send_command(inner, cmd)?;

  rx.await?
}

pub async fn flushall(inner: &Arc<RedisClientInner>, r#async: bool) -> Result<RedisValue, RedisError> {
  let args = if r#async { vec![static_val!(ASYNC)] } else { Vec::new() };
  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::FlushAll, args))).await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn flushall_cluster(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  utils::check_clustered(inner)?;

  let (tx, rx) = oneshot_channel();
  let kind = RedisCommandKind::_FlushAllCluster(AllNodesResponse::new(tx));
  let command = RedisCommand::new(kind, vec![], None);
  utils::send_command(inner, command)?;
  rx.await??;

  Ok(())
}

pub async fn ping(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  _debug!(inner, "Pinging redis server...");
  let frame = utils::request_response(inner, || Ok((RedisCommandKind::Ping, vec![]))).await?;
  _debug!(inner, "Recv ping response.");
  protocol_utils::frame_to_single_result(frame)
}

pub async fn select(inner: &Arc<RedisClientInner>, db: u8) -> Result<RedisValue, RedisError> {
  _debug!(inner, "Selecting database {}", db);
  let frame = utils::request_response(inner, || Ok((RedisCommandKind::Select, vec![db.into()]))).await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn info(inner: &Arc<RedisClientInner>, section: Option<InfoKind>) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1);
    if let Some(section) = section {
      args.push(section.to_str().into());
    }

    Ok((RedisCommandKind::Info, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn multi(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::Multi, vec![]))).await?;
  let _ = protocol_utils::frame_to_single_result(frame)?;
  Ok(())
}

pub async fn exec(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::Exec, vec![]))).await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn discard(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::Discard, vec![]))).await?;
  let _ = protocol_utils::frame_to_single_result(frame)?;
  Ok(())
}

pub async fn hello(
  inner: &Arc<RedisClientInner>,
  version: RespVersion,
  auth: Option<(String, String)>,
) -> Result<(), RedisError> {
  let args = if let Some((username, password)) = auth {
    vec![username.into(), password.into()]
  } else {
    vec![]
  };

  if utils::is_clustered(&inner.config) {
    let (tx, rx) = oneshot_channel();
    let kind = RedisCommandKind::_HelloAllCluster((AllNodesResponse::new(tx), version));
    let command = RedisCommand::new(kind, args, None);
    utils::send_command(inner, command)?;
    rx.await??;

    Ok(())
  } else {
    let frame = utils::request_response(inner, move || Ok((RedisCommandKind::Hello(version), args))).await?;
    let _ = protocol_utils::frame_to_results(frame)?;
    Ok(())
  }
}

pub async fn auth(inner: &Arc<RedisClientInner>, username: Option<String>, password: Str) -> Result<(), RedisError> {
  if utils::is_clustered(&inner.config) {
    let mut args = Vec::with_capacity(2);
    if let Some(username) = username {
      args.push(username.into());
    }
    args.push(password.into());

    let (tx, rx) = oneshot_channel();
    let kind = RedisCommandKind::_AuthAllCluster(AllNodesResponse::new(tx));
    let command = RedisCommand::new(kind, args, None);
    utils::send_command(inner, command)?;
    rx.await??;

    Ok(())
  } else {
    let frame = utils::request_response(inner, move || {
      let mut args = Vec::with_capacity(2);
      if let Some(username) = username {
        args.push(username.into());
      }
      args.push(password.into());

      Ok((RedisCommandKind::Auth, args))
    })
    .await?;

    let response = protocol_utils::frame_to_single_result(frame)?;
    protocol_utils::expect_ok(&response)
  }
}

pub async fn custom(
  inner: &Arc<RedisClientInner>,
  cmd: CustomCommand,
  args: Vec<RedisValue>,
) -> Result<RedisValue, RedisError> {
  args_values_cmd(inner, RedisCommandKind::_Custom(cmd), args).await
}

pub async fn custom_raw(
  inner: &Arc<RedisClientInner>,
  cmd: CustomCommand,
  args: Vec<RedisValue>,
) -> Result<Resp3Frame, RedisError> {
  utils::request_response(inner, move || Ok((RedisCommandKind::_Custom(cmd), args))).await
}

value_cmd!(dbsize, DBSize);
value_cmd!(bgrewriteaof, BgreWriteAof);
value_cmd!(bgsave, BgSave);

pub async fn failover(
  inner: &Arc<RedisClientInner>,
  to: Option<(String, u16)>,
  force: bool,
  abort: bool,
  timeout: Option<u32>,
) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || {
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
