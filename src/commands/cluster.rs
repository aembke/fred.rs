use super::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::convert::TryInto;
use std::sync::Arc;

value_cmd!(cluster_bumpepoch, ClusterBumpEpoch);
ok_cmd!(cluster_flushslots, ClusterFlushSlots);
value_cmd!(cluster_myid, ClusterMyID);
value_cmd!(cluster_nodes, ClusterNodes);
ok_cmd!(cluster_saveconfig, ClusterSaveConfig);
values_cmd!(cluster_slots, ClusterSlots);

pub async fn cluster_info(inner: &Arc<RedisClientInner>) -> Result<ClusterInfo, RedisError> {
  let frame = utils::request_response(inner, || Ok((RedisCommandKind::ClusterInfo, vec![]))).await?;
  protocol_utils::parse_cluster_info(frame)
}

pub async fn cluster_add_slots<S>(inner: &Arc<RedisClientInner>, slots: S) -> Result<(), RedisError>
where
  S: Into<MultipleHashSlots>,
{
  let slots = slots.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(slots.len());

    for slot in slots.inner().into_iter() {
      args.push(slot.into());
    }

    Ok((RedisCommandKind::ClusterAddSlots, args))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn cluster_count_failure_reports<N>(
  inner: &Arc<RedisClientInner>,
  node_id: N,
) -> Result<RedisValue, RedisError>
where
  N: Into<String>,
{
  let node_id = node_id.into();

  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::ClusterCountFailureReports, vec![node_id.into()]))
  })
  .await?;
  protocol_utils::frame_to_single_result(frame)
}

pub async fn cluster_count_keys_in_slot(inner: &Arc<RedisClientInner>, slot: u16) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(inner, move || {
    Ok((RedisCommandKind::ClusterCountKeysInSlot, vec![slot.into()]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn cluster_del_slots<S>(inner: &Arc<RedisClientInner>, slots: S) -> Result<(), RedisError>
where
  S: Into<MultipleHashSlots>,
{
  let slots = slots.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(slots.len());

    for slot in slots.inner().into_iter() {
      args.push(slot.into());
    }

    Ok((RedisCommandKind::ClusterDelSlots, args))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn cluster_failover(
  inner: &Arc<RedisClientInner>,
  flag: Option<ClusterFailoverFlag>,
) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || {
    let args = if let Some(flag) = flag {
      vec![flag.to_str().into()]
    } else {
      Vec::new()
    };

    Ok((RedisCommandKind::ClusterFailOver, args))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn cluster_forget<S>(inner: &Arc<RedisClientInner>, node_id: S) -> Result<(), RedisError>
where
  S: Into<String>,
{
  one_arg_ok_cmd(inner, RedisCommandKind::ClusterForget, node_id.into().into()).await
}

pub async fn cluster_get_keys_in_slot(
  inner: &Arc<RedisClientInner>,
  slot: u16,
  count: u64,
) -> Result<RedisValue, RedisError> {
  let count: RedisValue = count.try_into()?;

  let frame = utils::request_response(inner, move || {
    let args = vec![slot.into(), count];
    Ok((RedisCommandKind::ClusterGetKeysInSlot, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn cluster_keyslot<K>(inner: &Arc<RedisClientInner>, key: K) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  one_arg_value_cmd(inner, RedisCommandKind::ClusterKeySlot, key.into().into()).await
}

pub async fn cluster_meet<S>(inner: &Arc<RedisClientInner>, ip: S, port: u16) -> Result<(), RedisError>
where
  S: Into<String>,
{
  args_ok_cmd(
    inner,
    RedisCommandKind::ClusterMeet,
    vec![ip.into().into(), port.into()],
  )
  .await
}

pub async fn cluster_replicate<S>(inner: &Arc<RedisClientInner>, node_id: S) -> Result<(), RedisError>
where
  S: Into<String>,
{
  one_arg_ok_cmd(inner, RedisCommandKind::ClusterReplicate, node_id.into().into()).await
}

pub async fn cluster_replicas<S>(inner: &Arc<RedisClientInner>, node_id: S) -> Result<RedisValue, RedisError>
where
  S: Into<String>,
{
  one_arg_value_cmd(inner, RedisCommandKind::ClusterReplicas, node_id.into().into()).await
}

pub async fn cluster_reset(inner: &Arc<RedisClientInner>, mode: Option<ClusterResetFlag>) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1);
    if let Some(flag) = mode {
      args.push(flag.to_str().into());
    }

    Ok((RedisCommandKind::ClusterReset, args))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn cluster_set_config_epoch(inner: &Arc<RedisClientInner>, epoch: u64) -> Result<(), RedisError> {
  let epoch: RedisValue = epoch.try_into()?;
  one_arg_ok_cmd(inner, RedisCommandKind::ClusterSetConfigEpoch, epoch).await
}

pub async fn cluster_setslot(
  inner: &Arc<RedisClientInner>,
  slot: u16,
  state: ClusterSetSlotState,
) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(3);
    args.push(slot.into());

    let (state, arg) = state.to_str();
    args.push(state.into());
    if let Some(arg) = arg {
      args.push(arg.into());
    }

    Ok((RedisCommandKind::ClusterSetSlot, args))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}
