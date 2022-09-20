use super::*;
use crate::{
  error::*,
  modules::inner::RedisClientInner,
  protocol::{types::*, utils as protocol_utils},
  types::*,
  util::sha1_hash,
  utils,
};
use bytes_utils::Str;
use std::{convert::TryInto, sync::Arc};
use tokio::sync::oneshot::channel as oneshot_channel;

/// Check that all the keys in an EVAL* command belong to the same server, returning a key slot that maps to that
/// server.
pub fn check_key_slot(inner: &Arc<RedisClientInner>, keys: &Vec<RedisKey>) -> Result<CustomKeySlot, RedisError> {
  if utils::is_clustered(&inner.config) {
    let cluster_state = match &*inner.cluster_state.read() {
      Some(state) => state.clone(),
      None => return Err(RedisError::new(RedisErrorKind::Cluster, "Invalid cluster state.")),
    };

    let (mut cmd_server, mut cmd_slot) = (None, None);
    for key in keys.iter() {
      let key_slot = redis_keyslot(key.as_bytes());

      if let Some(slot) = cluster_state.get_server(key_slot) {
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

    Ok(CustomKeySlot { key_slot: cmd_slot })
  } else {
    Ok(CustomKeySlot { key_slot: None })
  }
}

pub async fn script_load(inner: &Arc<RedisClientInner>, script: Str) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(inner, RedisCommandKind::ScriptLoad, script.into()).await
}

pub async fn script_load_cluster(inner: &Arc<RedisClientInner>, script: Str) -> Result<RedisValue, RedisError> {
  utils::check_clustered(inner)?;
  let hash = sha1_hash(&script);

  let (tx, rx) = oneshot_channel();
  let kind = RedisCommandKind::_ScriptLoadCluster(AllNodesResponse::new(tx));
  let command = RedisCommand::new(kind, vec![script.into()], None);
  utils::send_command(inner, command)?;
  rx.await??;

  Ok(hash.into())
}

ok_cmd!(script_kill, ScriptKill);

pub async fn script_kill_cluster(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  utils::check_clustered(inner)?;

  let (tx, rx) = oneshot_channel();
  let kind = RedisCommandKind::_ScriptKillCluster(AllNodesResponse::new(tx));
  let command = RedisCommand::new(kind, vec![], None);
  utils::send_command(inner, command)?;
  rx.await??;

  Ok(())
}

pub async fn script_flush(inner: &Arc<RedisClientInner>, r#async: bool) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || {
    let arg = static_val!(if r#async { ASYNC } else { SYNC });
    Ok((RedisCommandKind::ScriptFlush, vec![arg]))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn script_flush_cluster(inner: &Arc<RedisClientInner>, r#async: bool) -> Result<(), RedisError> {
  utils::check_clustered(inner)?;

  let (tx, rx) = oneshot_channel();
  let kind = RedisCommandKind::_ScriptFlushCluster(AllNodesResponse::new(tx));
  let arg = static_val!(if r#async { ASYNC } else { SYNC });
  let command = RedisCommand::new(kind, vec![arg], None);
  utils::send_command(inner, command)?;
  rx.await??;

  Ok(())
}

pub async fn script_exists<H>(inner: &Arc<RedisClientInner>, hashes: H) -> Result<Vec<bool>, RedisError>
where
  H: Into<MultipleStrings>, {
  let hashes = hashes.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(hashes.len());
    for hash in hashes.inner().into_iter() {
      args.push(hash.into());
    }

    Ok((RedisCommandKind::ScriptExists, args))
  })
  .await?;

  let response = protocol_utils::frame_to_results(frame)?;
  if let RedisValue::Array(values) = response {
    values
      .into_iter()
      .map(|v| {
        v.as_bool().ok_or(RedisError::new(
          RedisErrorKind::ProtocolError,
          "Expected boolean response.",
        ))
      })
      .collect()
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Expected array response.",
    ))
  }
}

pub async fn script_debug(inner: &Arc<RedisClientInner>, flag: ScriptDebugFlag) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::ScriptDebug, vec![flag.to_str().into()]))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn evalsha(
  inner: &Arc<RedisClientInner>,
  hash: Str,
  keys: MultipleKeys,
  cmd_args: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let keys = keys.inner();
  let custom_key_slot = check_key_slot(inner, &keys)?;

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2 + keys.len() + cmd_args.len());
    args.push(hash.into());
    args.push(keys.len().try_into()?);

    for key in keys.into_iter() {
      args.push(key.into());
    }
    for arg in cmd_args.inner().into_iter() {
      args.push(arg);
    }

    Ok((RedisCommandKind::EvalSha(custom_key_slot), args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn eval(
  inner: &Arc<RedisClientInner>,
  script: Str,
  keys: MultipleKeys,
  cmd_args: MultipleValues,
) -> Result<RedisValue, RedisError> {
  let keys = keys.inner();
  let custom_key_slot = check_key_slot(inner, &keys)?;

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2 + keys.len() + cmd_args.len());
    args.push(script.into());
    args.push(keys.len().try_into()?);

    for key in keys.into_iter() {
      args.push(key.into());
    }
    for arg in cmd_args.inner().into_iter() {
      args.push(arg);
    }

    Ok((RedisCommandKind::Eval(custom_key_slot), args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
