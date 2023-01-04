use super::*;
use crate::{
  interfaces,
  protocol::{
    command::{RouterCommand, RedisCommandKind},
    utils as protocol_utils,
  },
  types::*,
  utils,
};
use bytes_utils::Str;
use std::convert::TryInto;
use tokio::sync::oneshot::channel as oneshot_channel;

value_cmd!(cluster_bumpepoch, ClusterBumpEpoch);
ok_cmd!(cluster_flushslots, ClusterFlushSlots);
value_cmd!(cluster_myid, ClusterMyID);
value_cmd!(cluster_nodes, ClusterNodes);
ok_cmd!(cluster_saveconfig, ClusterSaveConfig);
values_cmd!(cluster_slots, ClusterSlots);

pub async fn cluster_info<C: ClientLike>(client: &C) -> Result<ClusterInfo, RedisError> {
  let frame = utils::request_response(client, || Ok((RedisCommandKind::ClusterInfo, vec![]))).await?;
  protocol_utils::parse_cluster_info(frame)
}

pub async fn cluster_add_slots<C: ClientLike>(client: &C, slots: MultipleHashSlots) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn cluster_count_failure_reports<C: ClientLike>(
  client: &C,
  node_id: Str,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::ClusterCountFailureReports, vec![node_id.into()]))
  })
  .await?;
  protocol_utils::frame_to_single_result(frame)
}

pub async fn cluster_count_keys_in_slot<C: ClientLike>(client: &C, slot: u16) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::ClusterCountKeysInSlot, vec![slot.into()]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn cluster_del_slots<C: ClientLike>(client: &C, slots: MultipleHashSlots) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn cluster_failover<C: ClientLike>(
  client: &C,
  flag: Option<ClusterFailoverFlag>,
) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn cluster_forget<C: ClientLike>(client: &C, node_id: Str) -> Result<(), RedisError> {
  one_arg_ok_cmd(client, RedisCommandKind::ClusterForget, node_id.into()).await
}

pub async fn cluster_get_keys_in_slot<C: ClientLike>(
  client: &C,
  slot: u16,
  count: u64,
) -> Result<RedisValue, RedisError> {
  let count: RedisValue = count.try_into()?;

  let frame = utils::request_response(client, move || {
    let args = vec![slot.into(), count];
    Ok((RedisCommandKind::ClusterGetKeysInSlot, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn cluster_keyslot<C: ClientLike>(client: &C, key: RedisKey) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::ClusterKeySlot, key.into()).await
}

pub async fn cluster_meet<C: ClientLike>(client: &C, ip: Str, port: u16) -> Result<(), RedisError> {
  args_ok_cmd(client, RedisCommandKind::ClusterMeet, vec![ip.into(), port.into()]).await
}

pub async fn cluster_replicate<C: ClientLike>(client: &C, node_id: Str) -> Result<(), RedisError> {
  one_arg_ok_cmd(client, RedisCommandKind::ClusterReplicate, node_id.into()).await
}

pub async fn cluster_replicas<C: ClientLike>(client: &C, node_id: Str) -> Result<RedisValue, RedisError> {
  one_arg_value_cmd(client, RedisCommandKind::ClusterReplicas, node_id.into()).await
}

pub async fn cluster_reset<C: ClientLike>(client: &C, mode: Option<ClusterResetFlag>) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn cluster_set_config_epoch<C: ClientLike>(client: &C, epoch: u64) -> Result<(), RedisError> {
  let epoch: RedisValue = epoch.try_into()?;
  one_arg_ok_cmd(client, RedisCommandKind::ClusterSetConfigEpoch, epoch).await
}

pub async fn cluster_setslot<C: ClientLike>(
  client: &C,
  slot: u16,
  state: ClusterSetSlotState,
) -> Result<(), RedisError> {
  let frame = utils::request_response(client, move || {
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

pub async fn sync_cluster<C: ClientLike>(client: &C) -> Result<(), RedisError> {
  let (tx, rx) = oneshot_channel();
  let command = RouterCommand::SyncCluster { tx };
  let _ = interfaces::send_to_router(client.inner(), command)?;

  rx.await?
}
