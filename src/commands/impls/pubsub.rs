use super::*;
use crate::{
  protocol::{
    command::{RedisCommand, RedisCommandKind},
    responders::ResponseKind,
    utils as protocol_utils,
  },
  types::*,
  utils,
};
use bytes_utils::Str;
use redis_protocol::redis_keyslot;
use tokio::sync::oneshot::channel as oneshot_channel;

fn cluster_hash_legacy_command<C: ClientLike>(client: &C, command: &mut RedisCommand) {
  if client.is_clustered() {
    // send legacy (non-sharded) pubsub commands to the same node in a cluster so that `UNSUBSCRIBE` (without args)
    // works correctly. otherwise we'd have to send `UNSUBSCRIBE` to every node.
    let hash_slot = redis_keyslot(client.id().as_bytes());
    command.hasher = ClusterHash::Custom(hash_slot);
  }
}

pub async fn subscribe<C: ClientLike>(client: &C, channels: MultipleStrings) -> Result<RedisValue, RedisError> {
  if channels.len() == 0 {
    return Ok(RedisValue::Array(Vec::new()));
  }

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::new_multiple(channels.len(), tx);
  let args = channels.inner().into_iter().map(|c| c.into()).collect();
  let mut command: RedisCommand = (RedisCommandKind::Subscribe, args, response).into();
  cluster_hash_legacy_command(client, &mut command);

  let timeout_dur = utils::prepare_command(client, &mut command);
  let _ = client.send_command(command)?;

  let frame = utils::apply_timeout(rx, timeout_dur).await??;
  protocol_utils::frame_to_results(frame)
}

pub async fn unsubscribe<C: ClientLike>(client: &C, channels: MultipleStrings) -> Result<RedisValue, RedisError> {
  let (tx, rx) = oneshot_channel();
  let response = if channels.len() == 0 {
    ResponseKind::Respond(Some(tx))
  } else {
    ResponseKind::new_multiple(channels.len(), tx)
  };
  let args = channels.inner().into_iter().map(|c| c.into()).collect();
  let mut command: RedisCommand = (RedisCommandKind::Unsubscribe, args, response).into();
  cluster_hash_legacy_command(client, &mut command);

  let timeout_dur = utils::prepare_command(client, &mut command);
  let _ = client.send_command(command)?;

  let _ = utils::apply_timeout(rx, timeout_dur).await??;
  Ok(RedisValue::Null)
}

pub async fn publish<C: ClientLike>(client: &C, channel: Str, message: RedisValue) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Publish, vec![channel.into(), message]))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn psubscribe<C: ClientLike>(client: &C, patterns: MultipleStrings) -> Result<RedisValue, RedisError> {
  if patterns.len() == 0 {
    return Ok(RedisValue::Array(Vec::new()));
  }

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::new_multiple(patterns.len(), tx);
  let args = patterns.inner().into_iter().map(|p| p.into()).collect();
  let mut command: RedisCommand = (RedisCommandKind::Psubscribe, args, response).into();
  cluster_hash_legacy_command(client, &mut command);

  let timeout_dur = utils::prepare_command(client, &mut command);
  let _ = client.send_command(command)?;

  let frame = utils::apply_timeout(rx, timeout_dur).await??;
  protocol_utils::frame_to_results(frame)
}

pub async fn punsubscribe<C: ClientLike>(client: &C, patterns: MultipleStrings) -> Result<RedisValue, RedisError> {
  let (tx, rx) = oneshot_channel();
  let response = if patterns.len() == 0 {
    ResponseKind::Respond(Some(tx))
  } else {
    ResponseKind::new_multiple(patterns.len(), tx)
  };
  let args = patterns.inner().into_iter().map(|p| p.into()).collect();
  let mut command: RedisCommand = (RedisCommandKind::Punsubscribe, args, response).into();
  cluster_hash_legacy_command(client, &mut command);

  let timeout_dur = utils::prepare_command(client, &mut command);
  let _ = client.send_command(command)?;
  let _ = utils::apply_timeout(rx, timeout_dur).await??;
  Ok(RedisValue::Null)
}

pub async fn spublish<C: ClientLike>(
  client: &C,
  channel: Str,
  message: RedisValue,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    let mut command: RedisCommand = (RedisCommandKind::Spublish, vec![channel.into(), message]).into();
    command.hasher = ClusterHash::FirstKey;

    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn ssubscribe<C: ClientLike>(client: &C, channels: MultipleStrings) -> Result<RedisValue, RedisError> {
  if channels.len() == 0 {
    return Ok(RedisValue::Array(Vec::new()));
  }

  let (tx, rx) = oneshot_channel();
  let response = ResponseKind::new_multiple(channels.len(), tx);
  let args = channels.inner().into_iter().map(|p| p.into()).collect();
  let mut command: RedisCommand = (RedisCommandKind::Ssubscribe, args, response).into();
  command.hasher = ClusterHash::FirstKey;

  let timeout_dur = utils::prepare_command(client, &mut command);
  let _ = client.send_command(command)?;

  let frame = utils::apply_timeout(rx, timeout_dur).await??;
  protocol_utils::frame_to_results(frame)
}

pub async fn sunsubscribe<C: ClientLike>(client: &C, channels: MultipleStrings) -> Result<RedisValue, RedisError> {
  let (tx, rx) = oneshot_channel();
  let (response, hasher) = if channels.len() == 0 {
    // does this need to go to all cluster nodes?
    (ResponseKind::Respond(Some(tx)), ClusterHash::Random)
  } else {
    (ResponseKind::new_multiple(channels.len(), tx), ClusterHash::FirstKey)
  };
  let args = channels.inner().into_iter().map(|p| p.into()).collect();
  let mut command: RedisCommand = (RedisCommandKind::Sunsubscribe, args, response).into();
  command.hasher = hasher;
  let timeout_dur = utils::prepare_command(client, &mut command);
  let _ = client.send_command(command)?;

  let _ = utils::apply_timeout(rx, timeout_dur).await??;
  Ok(RedisValue::Null)
}

pub async fn pubsub_channels<C: ClientLike>(client: &C, pattern: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || {
    let args = if pattern.is_empty() {
      vec![]
    } else {
      vec![pattern.into()]
    };

    let mut command: RedisCommand = RedisCommand::new(RedisCommandKind::PubsubChannels, args);
    cluster_hash_legacy_command(client, &mut command);

    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}

pub async fn pubsub_numpat<C: ClientLike>(client: &C) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || {
    let mut command: RedisCommand = RedisCommand::new(RedisCommandKind::PubsubNumpat, vec![]);
    cluster_hash_legacy_command(client, &mut command);

    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn pubsub_numsub<C: ClientLike>(client: &C, channels: MultipleStrings) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || {
    let args: Vec<RedisValue> = channels.inner().into_iter().map(|s| s.into()).collect();
    let mut command: RedisCommand = RedisCommand::new(RedisCommandKind::PubsubNumsub, args);
    cluster_hash_legacy_command(client, &mut command);

    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}

pub async fn pubsub_shardchannels<C: ClientLike>(client: &C, pattern: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::PubsubShardchannels, vec![pattern.into()]))
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}

pub async fn pubsub_shardnumsub<C: ClientLike>(
  client: &C,
  channels: MultipleStrings,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || {
    let args: Vec<RedisValue> = channels.inner().into_iter().map(|s| s.into()).collect();
    let has_args = args.len() > 0;
    let mut command: RedisCommand = RedisCommand::new(RedisCommandKind::PubsubShardnumsub, args);
    if !has_args {
      cluster_hash_legacy_command(client, &mut command);
    }

    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results_raw(frame)
}
