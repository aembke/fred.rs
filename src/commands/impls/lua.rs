use super::*;
use crate::error::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::command::{RedisCommand, RedisCommandKind};
use crate::protocol::hashers::ClusterHash;
use crate::protocol::responders::ResponseKind;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::util::sha1_hash;
use crate::utils;
use bytes_utils::Str;
use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::oneshot::channel as oneshot_channel;

/// Check that all the keys in an EVAL* command belong to the same server, returning a key slot that maps to that server.
pub fn check_key_slot(inner: &Arc<RedisClientInner>, keys: &Vec<RedisKey>) -> Result<Option<u16>, RedisError> {
  if inner.config.server.is_clustered() {
    inner.with_cluster_state(|state| {
      let (mut cmd_server, mut cmd_slot) = (None, None);
      for key in keys.iter() {
        let key_slot = redis_keyslot(key.as_bytes());

        if let Some(slot) = state.get_server(key_slot) {
          if let Some(ref cmd_server) = cmd_server {
            if *cmd_server != slot.id {
              return Err(RedisError::new(
                RedisErrorKind::Cluster,
                "All keys must belong to the same cluster node.",
              ));
            }
          } else {
            cmd_server = Some(slot.id.clone());
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

pub async fn script_load<C: ClientLike>(client: C, script: Str) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::ScriptLoad, script.into()).await
}

pub async fn script_load_cluster<C: ClientLike>(client: C, script: Str) -> Result<RedisValue, RedisError> {
  if !client.inner().config.server.is_clustered() {
    return script_load(client, script).await;
  }
  let hash = sha1_hash(&script);

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::new_multiple(client.inner().num_cluster_nodes(), tx);
  let command: RedisCommand = (RedisCommandKind::_ScriptLoadCluster, vec![script.into()], response).into();
  let _ = client.send_command(command)?;

  let _ = rx.await??;
  Ok(hash.into())
}

ok_cmd!(script_kill, ScriptKill);

pub async fn script_kill_cluster<C: ClientLike>(client: C) -> Result<(), RedisError> {
  if !client.inner().config.server.is_clustered() {
    return script_kill(client).await;
  }

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::new_multiple(client.inner().num_cluster_nodes(), tx);
  let command: RedisCommand = (RedisCommandKind::_ScriptKillCluster, vec![], response).into();
  let _ = client.send_command(command)?;

  let _ = rx.await??;
  Ok(())
}

pub async fn script_flush<C: ClientLike>(client: C, r#async: bool) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || {
    let arg = static_val!(if r#async { ASYNC } else { SYNC });
    Ok((RedisCommandKind::ScriptFlush, vec![arg]))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn script_flush_cluster<C: ClientLike>(client: C, r#async: bool) -> Result<(), RedisError> {
  if !client.inner().config.server.is_clustered() {
    return script_flush(client, r#async).await;
  }

  let (tx, rx) = oneshot_channel();
  let arg = static_val!(if r#async { ASYNC } else { SYNC });

  let response = ResponseKind::new_multiple(client.inner().num_cluster_nodes(), tx);
  let command: RedisCommand = (RedisCommandKind::_ScriptFlushCluster, vec![arg], response).into();
  let _ = client.send_command(command)?;

  let _ = rx.await??;
  Ok(())
}

pub async fn script_exists<C: ClientLike>(client: C, hashes: MultipleStrings) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let args = hashes.inner().into_iter().map(|s| s.into()).collect();
    Ok((RedisCommandKind::ScriptExists, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn script_debug<C: ClientLike>(client: C, flag: ScriptDebugFlag) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::ScriptDebug, vec![flag.to_str().into()]))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn evalsha<C: ClientLike>(
  client: C,
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
    command.hasher = ClusterHash::Custom(custom_key_slot);
    command.can_pipeline = false;
    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn eval<C: ClientLike>(
  client: C,
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
    command.hasher = ClusterHash::Custom(custom_key_slot);
    command.can_pipeline = false;
    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
