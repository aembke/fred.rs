use super::*;
use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::net::IpAddr;
use std::sync::Arc;

pub async fn config_get<K>(inner: &Arc<RedisClientInner>, name: K) -> Result<RedisValue, RedisError>
where
  K: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((
      RedisCommandKind::Sentinel,
      vec!["CONFIG".into(), "GET".into(), name.into()],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn config_set<K>(
  inner: &Arc<RedisClientInner>,
  name: K,
  value: RedisValue,
) -> Result<RedisValue, RedisError>
where
  K: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((
      RedisCommandKind::Sentinel,
      vec!["CONFIG".into(), "SET".into(), name.into(), value],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn ckquorum<N>(inner: &Arc<RedisClientInner>, name: N) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Sentinel, vec!["CKQUORUM".into(), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn flushconfig(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  args_values_cmd(inner, RedisCommandKind::Sentinel, vec!["FLUSHCONFIG".into()]).await
}

pub async fn failover<N>(inner: &Arc<RedisClientInner>, name: N) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Sentinel, vec!["FAILOVER".into(), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn get_master_addr_by_name<N>(inner: &Arc<RedisClientInner>, name: N) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((
      RedisCommandKind::Sentinel,
      vec!["GET-MASTER-ADDR-BY-NAME".into(), name.into()],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn info_cache(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  args_values_cmd(inner, RedisCommandKind::Sentinel, vec!["INFO-CACHE".into()]).await
}

pub async fn masters(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  args_values_cmd(inner, RedisCommandKind::Sentinel, vec!["MASTERS".into()]).await
}

pub async fn master<N>(inner: &Arc<RedisClientInner>, name: N) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Sentinel, vec!["MASTER".into(), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn monitor<N>(
  inner: &Arc<RedisClientInner>,
  name: N,
  ip: IpAddr,
  port: u16,
  quorum: u32,
) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let (name, ip) = (name.into(), ip.to_string());
  let frame = utils::request_response(inner, move || {
    Ok((
      RedisCommandKind::Sentinel,
      vec!["MONITOR".into(), name.into(), ip.into(), port.into(), quorum.into()],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn myid(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  args_values_cmd(inner, RedisCommandKind::Sentinel, vec!["MYID".into()]).await
}

pub async fn pending_scripts(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  args_values_cmd(inner, RedisCommandKind::Sentinel, vec!["PENDING-SCRIPTS".into()]).await
}

pub async fn remove<N>(inner: &Arc<RedisClientInner>, name: N) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Sentinel, vec!["REMOVE".into(), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn replicas<N>(inner: &Arc<RedisClientInner>, name: N) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Sentinel, vec!["REPLICAS".into(), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn sentinels<N>(inner: &Arc<RedisClientInner>, name: N) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Sentinel, vec!["SENTINELS".into(), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn set<N>(inner: &Arc<RedisClientInner>, name: N, options: RedisMap) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(2 + options.len());
    args.push("SET".into());
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

pub async fn simulate_failure(
  inner: &Arc<RedisClientInner>,
  kind: SentinelFailureKind,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    Ok((
      RedisCommandKind::Sentinel,
      vec!["SIMULATE-FAILURE".into(), kind.to_str().into()],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn reset<P>(inner: &Arc<RedisClientInner>, pattern: P) -> Result<RedisValue, RedisError>
where
  P: Into<String>,
{
  let pattern = pattern.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Sentinel, vec!["RESET".into(), pattern.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
