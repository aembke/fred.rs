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
    let args = vec![static_val!("CONFIG"), static_val!(GET), name.into()];
    Ok((RedisCommandKind::Sentinel, args))
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
      vec![static_val!("CONFIG"), static_val!("SET"), name.into(), value],
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
    Ok((RedisCommandKind::Sentinel, vec![static_val!("CKQUORUM"), name.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn flushconfig(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  args_values_cmd(inner, RedisCommandKind::Sentinel, vec![static_val!("FLUSHCONFIG")]).await
}

pub async fn failover<N>(inner: &Arc<RedisClientInner>, name: N) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Sentinel, vec![static_val!("FAILOVER"), name.into()]))
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
      vec![static_val!("GET-MASTER-ADDR-BY-NAME"), name.into()],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn info_cache(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  args_values_cmd(inner, RedisCommandKind::Sentinel, vec![static_val!("INFO-CACHE")]).await
}

pub async fn masters(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  args_values_cmd(inner, RedisCommandKind::Sentinel, vec![static_val!("MASTERS")]).await
}

pub async fn master<N>(inner: &Arc<RedisClientInner>, name: N) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Sentinel, vec![static_val!("MASTER"), name.into()]))
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
      vec![
        static_val!("MONITOR"),
        name.into(),
        ip.into(),
        port.into(),
        quorum.into(),
      ],
    ))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn myid(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  args_values_cmd(inner, RedisCommandKind::Sentinel, vec![static_val!("MYID")]).await
}

pub async fn pending_scripts(inner: &Arc<RedisClientInner>) -> Result<RedisValue, RedisError> {
  args_values_cmd(inner, RedisCommandKind::Sentinel, vec![static_val!("PENDING-SCRIPTS")]).await
}

pub async fn remove<N>(inner: &Arc<RedisClientInner>, name: N) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let name = name.into();
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::Sentinel, vec![static_val!("REMOVE"), name.into()]))
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
    Ok((RedisCommandKind::Sentinel, vec![static_val!("REPLICAS"), name.into()]))
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
    Ok((RedisCommandKind::Sentinel, vec![static_val!("SENTINELS"), name.into()]))
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
    args.push(static_val!("SET"));
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
      vec![static_val!("SIMULATE-FAILURE"), kind.to_str().into()],
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
    Ok((RedisCommandKind::Sentinel, vec![static_val!("RESET"), pattern.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
