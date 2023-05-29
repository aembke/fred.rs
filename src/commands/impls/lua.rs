use super::*;
use crate::{
  error::*,
  modules::inner::RedisClientInner,
  protocol::{
    command::{RedisCommand, RedisCommandKind},
    hashers::ClusterHash,
    responders::ResponseKind,
    types::*,
    utils as protocol_utils,
  },
  types::*,
  util::sha1_hash,
  utils,
};
use bytes::Bytes;
use bytes_utils::Str;
use std::{convert::TryInto, str, sync::Arc};
use tokio::sync::oneshot::channel as oneshot_channel;

/// Check that all the keys in an EVAL* command belong to the same server, returning a key slot that maps to that
/// server.
pub fn check_key_slot(inner: &Arc<RedisClientInner>, keys: &Vec<RedisKey>) -> Result<Option<u16>, RedisError> {
  if inner.config.server.is_clustered() {
    inner.with_cluster_state(|state| {
      let (mut cmd_server, mut cmd_slot) = (None, None);
      for key in keys.iter() {
        let key_slot = redis_keyslot(key.as_bytes());

        if let Some(server) = state.get_server(key_slot) {
          if let Some(ref cmd_server) = cmd_server {
            if cmd_server != server {
              return Err(RedisError::new(
                RedisErrorKind::Cluster,
                "All keys must belong to the same cluster node.",
              ));
            }
          } else {
            cmd_server = Some(server.clone());
            cmd_slot = Some(key_slot);
          }
        } else {
          return Err(RedisError::new(
            RedisErrorKind::Cluster,
            format!("Missing server for hash slot {}", key_slot),
          ));
        }
      }

      Ok(cmd_slot)
    })
  } else {
    Ok(None)
  }
}

pub async fn script_load<C: ClientLike>(client: &C, script: Str) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::ScriptLoad, script.into()).await
}

pub async fn script_load_cluster<C: ClientLike>(client: &C, script: Str) -> Result<RedisValue, RedisError> {
  if !client.inner().config.server.is_clustered() {
    return script_load(client, script).await;
  }
  let hash = sha1_hash(&script);

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::new_buffer(tx);
  let command: RedisCommand = (RedisCommandKind::_ScriptLoadCluster, vec![script.into()], response).into();
  client.send_command(command)?;

  let _ = rx.await??;
  Ok(hash.into())
}

ok_cmd!(script_kill, ScriptKill);

pub async fn script_kill_cluster<C: ClientLike>(client: &C) -> Result<(), RedisError> {
  if !client.inner().config.server.is_clustered() {
    return script_kill(client).await;
  }

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::new_buffer(tx);
  let command: RedisCommand = (RedisCommandKind::_ScriptKillCluster, vec![], response).into();
  client.send_command(command)?;

  let _ = rx.await??;
  Ok(())
}

pub async fn script_flush<C: ClientLike>(client: &C, r#async: bool) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || {
    let arg = static_val!(if r#async { ASYNC } else { SYNC });
    Ok((RedisCommandKind::ScriptFlush, vec![arg]))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn script_flush_cluster<C: ClientLike>(client: &C, r#async: bool) -> Result<(), RedisError> {
  if !client.inner().config.server.is_clustered() {
    return script_flush(client, r#async).await;
  }

  let (tx, rx) = oneshot_channel();
  let arg = static_val!(if r#async { ASYNC } else { SYNC });

  let response = ResponseKind::new_buffer(tx);
  let command: RedisCommand = (RedisCommandKind::_ScriptFlushCluster, vec![arg], response).into();
  client.send_command(command)?;

  let _ = rx.await??;
  Ok(())
}

pub async fn script_exists<C: ClientLike>(client: &C, hashes: MultipleStrings) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let args = hashes.inner().into_iter().map(|s| s.into()).collect();
    Ok((RedisCommandKind::ScriptExists, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn script_debug<C: ClientLike>(client: &C, flag: ScriptDebugFlag) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::ScriptDebug, vec![flag.to_str().into()]))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn evalsha<C: ClientLike>(
  client: &C,
  hash: Str,
  keys: MultipleKeys,
  cmd_args: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let keys = keys.inner();
  let custom_key_slot = check_key_slot(client.inner(), &keys)?;

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2 + keys.len() + cmd_args.len());
    args.push(hash.into());
    args.push(keys.len().try_into()?);

    for key in keys.into_iter() {
      args.push(key.into());
    }
    for arg in cmd_args.inner().into_iter() {
      args.push(arg);
    }

    let mut command: RedisCommand = (RedisCommandKind::EvalSha, args).into();
    command.hasher = custom_key_slot
      .map(ClusterHash::Custom)
      .unwrap_or(ClusterHash::Random);
    command.can_pipeline = false;
    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn eval<C: ClientLike>(
  client: &C,
  script: Str,
  keys: MultipleKeys,
  cmd_args: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let keys = keys.inner();
  let custom_key_slot = check_key_slot(client.inner(), &keys)?;

  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2 + keys.len() + cmd_args.len());
    args.push(script.into());
    args.push(keys.len().try_into()?);

    for key in keys.into_iter() {
      args.push(key.into());
    }
    for arg in cmd_args.inner().into_iter() {
      args.push(arg);
    }

    let mut command: RedisCommand = (RedisCommandKind::Eval, args).into();
    command.hasher = custom_key_slot
      .map(ClusterHash::Custom)
      .unwrap_or(ClusterHash::Random);
    command.can_pipeline = false;
    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn fcall<C: ClientLike>(
  client: &C,
  func: Str,
  keys: MultipleKeys,
  args: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut arguments = Vec::with_capacity(keys.len() + args.len() + 2);
    let mut custom_key_slot = None;

    arguments.push(func.into());
    arguments.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      custom_key_slot = Some(key.cluster_hash());
      arguments.push(key.into());
    }
    for arg in args.inner().into_iter() {
      arguments.push(arg);
    }

    let mut command: RedisCommand = (RedisCommandKind::Fcall, arguments).into();
    command.hasher = custom_key_slot
      .map(ClusterHash::Custom)
      .unwrap_or(ClusterHash::Random);
    command.can_pipeline = false;
    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn fcall_ro<C: ClientLike>(
  client: &C,
  func: Str,
  keys: MultipleKeys,
  args: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut arguments = Vec::with_capacity(keys.len() + args.len() + 2);
    let mut custom_key_slot = None;

    arguments.push(func.into());
    arguments.push(keys.len().try_into()?);

    for key in keys.inner().into_iter() {
      custom_key_slot = Some(key.cluster_hash());
      arguments.push(key.into());
    }
    for arg in args.inner().into_iter() {
      arguments.push(arg);
    }

    let mut command: RedisCommand = (RedisCommandKind::FcallRO, arguments).into();
    command.hasher = custom_key_slot
      .map(ClusterHash::Custom)
      .unwrap_or(ClusterHash::Random);
    command.can_pipeline = false;
    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn function_delete<C: ClientLike>(client: &C, library_name: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::FunctionDelete, vec![library_name.into()]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn function_delete_cluster<C: ClientLike>(client: &C, library_name: Str) -> Result<(), RedisError> {
  if !client.inner().config.server.is_clustered() {
    return function_delete(client, library_name).await.map(|_| ());
  }

  let (tx, rx) = oneshot_channel();
  let args: Vec<RedisValue> = vec![library_name.into()];

  let response = ResponseKind::new_buffer(tx);
  let command: RedisCommand = (RedisCommandKind::_FunctionDeleteCluster, args, response).into();
  client.send_command(command)?;

  let _ = rx.await??;
  Ok(())
}

pub async fn function_flush<C: ClientLike>(client: &C, r#async: bool) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let args = if r#async {
      vec![static_val!(ASYNC)]
    } else {
      vec![static_val!(SYNC)]
    };

    Ok((RedisCommandKind::FunctionFlush, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn function_flush_cluster<C: ClientLike>(client: &C, r#async: bool) -> Result<(), RedisError> {
  if !client.inner().config.server.is_clustered() {
    return function_flush(client, r#async).await.map(|_| ());
  }

  let (tx, rx) = oneshot_channel();
  let args = if r#async {
    vec![static_val!(ASYNC)]
  } else {
    vec![static_val!(SYNC)]
  };

  let response = ResponseKind::new_buffer(tx);
  let command: RedisCommand = (RedisCommandKind::_FunctionFlushCluster, args, response).into();
  client.send_command(command)?;

  let _ = rx.await??;
  Ok(())
}

pub async fn function_kill<C: ClientLike>(client: &C) -> Result<RedisValue, RedisError> {
  let inner = client.inner();
  let command = RedisCommand::new(RedisCommandKind::FunctionKill, vec![]);

  let frame = utils::backchannel_request_response(inner, command, true).await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn function_list<C: ClientLike>(
  client: &C,
  library_name: Option<Str>,
  withcode: bool,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(3);

    if let Some(library_name) = library_name {
      args.push(static_val!(LIBRARYNAME));
      args.push(library_name.into());
    }
    if withcode {
      args.push(static_val!(WITHCODE));
    }

    Ok((RedisCommandKind::FunctionList, args))
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}

pub async fn function_load<C: ClientLike>(client: &C, replace: bool, code: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2);
    if replace {
      args.push(static_val!(REPLACE));
    }
    args.push(code.into());

    Ok((RedisCommandKind::FunctionLoad, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn function_load_cluster<C: ClientLike>(
  client: &C,
  replace: bool,
  code: Str,
) -> Result<RedisValue, RedisError> {
  if !client.inner().config.server.is_clustered() {
    return function_load(client, replace, code).await;
  }

  let (tx, rx) = oneshot_channel();
  let mut args: Vec<RedisValue> = Vec::with_capacity(2);
  if replace {
    args.push(static_val!(REPLACE));
  }
  args.push(code.into());

  let response = ResponseKind::new_buffer(tx);
  let command: RedisCommand = (RedisCommandKind::_FunctionLoadCluster, args, response).into();
  client.send_command(command)?;

  // each value in the response array is the response from a different primary node
  match rx.await?? {
    Frame::Array { mut data, .. } => {
      if let Some(frame) = data.pop() {
        protocol_utils::frame_to_single_result(frame)
      } else {
        Err(RedisError::new(
          RedisErrorKind::Protocol,
          "Missing library name response frame.",
        ))
      }
    },
    Frame::SimpleError { data, .. } => Err(protocol_utils::pretty_error(&data)),
    Frame::BlobError { data, .. } => {
      let parsed = str::from_utf8(&data)?;
      Err(protocol_utils::pretty_error(parsed))
    },
    _ => Err(RedisError::new(RedisErrorKind::Protocol, "Invalid response type.")),
  }
}

pub async fn function_restore<C: ClientLike>(
  client: &C,
  serialized: Bytes,
  policy: FnPolicy,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2);
    args.push(serialized.into());
    args.push(policy.to_str().into());

    Ok((RedisCommandKind::FunctionRestore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn function_restore_cluster<C: ClientLike>(
  client: &C,
  serialized: Bytes,
  policy: FnPolicy,
) -> Result<(), RedisError> {
  if !client.inner().config.server.is_clustered() {
    return function_restore(client, serialized, policy).await.map(|_| ());
  }

  let (tx, rx) = oneshot_channel();
  let args: Vec<RedisValue> = vec![serialized.into(), policy.to_str().into()];

  let response = ResponseKind::new_buffer(tx);
  let command: RedisCommand = (RedisCommandKind::_FunctionRestoreCluster, args, response).into();
  client.send_command(command)?;

  let _ = rx.await??;
  Ok(())
}

pub async fn function_stats<C: ClientLike>(client: &C) -> Result<RedisValue, RedisError> {
  let inner = client.inner();
  let command = RedisCommand::new(RedisCommandKind::FunctionStats, vec![]);

  let frame = utils::backchannel_request_response(inner, command, true).await?;
  protocol_utils::frame_to_results_raw(frame)
}

value_cmd!(function_dump, FunctionDump);
