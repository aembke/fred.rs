use super::*;
use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::protocol::command::{RedisCommand, RedisCommandKind};
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use bytes_utils::Str;
use std::net::IpAddr;
use std::sync::Arc;

static CONFIG: &'static str = "CONFIG";
static SET: &'static str = "SET";
static CKQUORUM: &'static str = "CKQUORUM";
static FLUSHCONFIG: &'static str = "FLUSHCONFIG";
static FAILOVER: &'static str = "FAILOVER";
static GET_MASTER_ADDR_BY_NAME: &'static str = "GET-MASTER-ADDR-BY-NAME";
static INFO_CACHE: &'static str = "INFO-CACHE";
static MASTERS: &'static str = "MASTERS";
static MASTER: &'static str = "MASTER";
static MONITOR: &'static str = "MONITOR";
static MYID: &'static str = "MYID";
static PENDING_SCRIPTS: &'static str = "PENDING-SCRIPTS";
static REMOVE: &'static str = "REMOVE";
static REPLICAS: &'static str = "REPLICAS";
static SENTINELS: &'static str = "SENTINELS";
static SIMULATE_FAILURE: &'static str = "SIMULATE-FAILURE";

pub async fn config_get<C: ClientLike>(client: C, name: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let args = vec![static_val!(CONFIG), static_val!(GET), name.into()];
    Ok((RedisCommandKind::Sentinel, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn config_set<C: ClientLike>(client: C, name: Str, value: RedisValue) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((
      RedisCommandKind::Sentinel,
      vec![static_val!(CONFIG), static_val!(SET), name.into(), value],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ckquorum<C: ClientLike>(client: C, name: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Sentinel, vec![static_val!(CKQUORUM), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn flushconfig<C: ClientLike>(client: C) -> Result<RedisValue, RedisError> {
  args_values_cmd(client, RedisCommandKind::Sentinel, vec![static_val!(FLUSHCONFIG)]).await
}

pub async fn failover<C: ClientLike>(client: C, name: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Sentinel, vec![static_val!(FAILOVER), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn get_master_addr_by_name<C: ClientLike>(client: C, name: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((
      RedisCommandKind::Sentinel,
      vec![static_val!(GET_MASTER_ADDR_BY_NAME), name.into()],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn info_cache<C: ClientLike>(client: C) -> Result<RedisValue, RedisError> {
  args_values_cmd(client, RedisCommandKind::Sentinel, vec![static_val!(INFO_CACHE)]).await
}

pub async fn masters<C: ClientLike>(client: C) -> Result<RedisValue, RedisError> {
  args_values_cmd(client, RedisCommandKind::Sentinel, vec![static_val!(MASTERS)]).await
}

pub async fn master<C: ClientLike>(client: C, name: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Sentinel, vec![static_val!(MASTER), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn monitor<C: ClientLike>(
  client: C,
  name: Str,
  ip: IpAddr,
  port: u16,
  quorum: u32,
) -> Result<RedisValue, RedisError> {
  let ip = ip.to_string();
  let frame = utils::request_response(client, move || {
    Ok((
      RedisCommandKind::Sentinel,
      vec![static_val!(MONITOR), name.into(), ip.into(), port.into(), quorum.into()],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn myid<C: ClientLike>(client: C) -> Result<RedisValue, RedisError> {
  args_values_cmd(client, RedisCommandKind::Sentinel, vec![static_val!(MYID)]).await
}

pub async fn pending_scripts<C: ClientLike>(client: C) -> Result<RedisValue, RedisError> {
  args_values_cmd(client, RedisCommandKind::Sentinel, vec![static_val!(PENDING_SCRIPTS)]).await
}

pub async fn remove<C: ClientLike>(client: C, name: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Sentinel, vec![static_val!(REMOVE), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn replicas<C: ClientLike>(client: C, name: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Sentinel, vec![static_val!(REPLICAS), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn sentinels<C: ClientLike>(client: C, name: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Sentinel, vec![static_val!(SENTINELS), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn set<C: ClientLike>(client: C, name: Str, options: RedisMap) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut args = Vec::with_capacity(2 + options.len());
    args.push(static_val!(SET));
    args.push(name.into());

    for (key, value) in options.inner().into_iter() {
      args.push(key.into());
      args.push(value);
    }
    Ok((RedisCommandKind::Sentinel, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn simulate_failure<C: ClientLike>(client: C, kind: SentinelFailureKind) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((
      RedisCommandKind::Sentinel,
      vec![static_val!(SIMULATE_FAILURE), kind.to_str().into()],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn reset<C: ClientLike>(client: C, pattern: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Sentinel, vec![static_val!(RESET), pattern.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
