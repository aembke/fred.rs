use super::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::sync::Arc;

value_cmd!(client_id, ClientID);
value_cmd!(client_info, ClientInfo);

pub async fn client_kill(
  inner: &Arc<RedisClientInner>,
  filters: Vec<ClientKillFilter>,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(filters.len() * 2);

    for filter in filters.into_iter() {
      let (field, value) = filter.to_str();
      args.push(field.into());
      args.push(value.into());
    }

    Ok((RedisCommandKind::ClientKill, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn client_list<K>(
  inner: &Arc<RedisClientInner>,
  r#type: Option<ClientKillType>,
  ids: Option<Vec<K>>,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let ids: Option<Vec<RedisKey>> = ids.map(|ids| ids.into_iter().map(|id| id.into()).collect());
  let frame = utils::request_response(inner, move || {
    let max_args = 2 + ids.as_ref().map(|i| i.len()).unwrap_or(0);
    let mut args = Vec::with_capacity(max_args);

    if let Some(kind) = r#type {
      args.push(TYPE.into());
      args.push(kind.to_str().into());
    }
    if let Some(ids) = ids {
      if !ids.is_empty() {
        args.push(ID.into());

        for id in ids.into_iter() {
          args.push(id.into());
        }
      }
    }

    Ok((RedisCommandKind::ClientList, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn client_pause(
  inner: &Arc<RedisClientInner>,
  timeout: i64,
  mode: Option<ClientPauseKind>,
) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2);
    args.push(timeout.into());

    if let Some(mode) = mode {
      args.push(mode.to_str().into());
    }

    Ok((RedisCommandKind::ClientPause, args))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

value_cmd!(client_getname, ClientGetName);

pub async fn client_setname<S>(inner: &Arc<RedisClientInner>, name: S) -> Result<(), RedisError>
where
  S: Into<String>,
{
  let name = name.into();
  _warn!(inner, "Changing client name from {} to {}", inner.id.as_str(), name);

  let frame =
    utils::request_response(inner, move || Ok((RedisCommandKind::ClientSetname, vec![name.into()]))).await?;
  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

ok_cmd!(client_unpause, ClientUnpause);

pub async fn client_reply(inner: &Arc<RedisClientInner>, flag: ClientReplyFlag) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::ClientReply, vec![flag.to_str().into()]))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn client_unblock<S>(
  inner: &Arc<RedisClientInner>,
  id: S,
  flag: Option<ClientUnblockFlag>,
) -> Result<RedisValue, RedisError>
where
  S: Into<RedisValue>,
{
  let id = id.into();
  let frame = utils::backchannel_request_response(inner, move || {
    let mut args = Vec::with_capacity(2);
    args.push(id);

    if let Some(flag) = flag {
      args.push(flag.to_str().into());
    }

    Ok((RedisCommandKind::ClientUnblock, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn unblock_self(inner: &Arc<RedisClientInner>, flag: Option<ClientUnblockFlag>) -> Result<(), RedisError> {
  let flag = flag.unwrap_or(ClientUnblockFlag::Error);
  utils::interrupt_blocked_connection(inner, flag).await
}
