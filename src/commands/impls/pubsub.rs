use super::*;
use crate::{
  protocol::{
    command::{RedisCommand, RedisCommandKind},
    utils as protocol_utils,
  },
  types::*,
  utils,
};
use bytes_utils::Str;
use redis_protocol::redis_keyslot;

fn cluster_hash_legacy_command<C: ClientLike>(client: &C, command: &mut RedisCommand) {
  if client.is_clustered() {
    // send legacy (non-sharded) pubsub commands to the same node in a cluster so that `UNSUBSCRIBE` (without args)
    // works correctly. otherwise we'd have to send `UNSUBSCRIBE` to every node.
    let hash_slot = redis_keyslot(client.id().as_bytes());
    command.hasher = ClusterHash::Custom(hash_slot);
  }
}

pub async fn subscribe<C: ClientLike>(client: &C, channels: MultipleStrings) -> Result<(), RedisError> {
  let args = channels.inner().into_iter().map(|c| c.into()).collect();
  let mut command = RedisCommand::new(RedisCommandKind::Subscribe, args);
  cluster_hash_legacy_command(client, &mut command);

  let frame = utils::request_response(client, move || Ok(command)).await?;
  protocol_utils::frame_to_results(frame).map(|_| ())
}

pub async fn unsubscribe<C: ClientLike>(client: &C, channels: MultipleStrings) -> Result<(), RedisError> {
  let args = channels.inner().into_iter().map(|c| c.into()).collect();
  let mut command = RedisCommand::new(RedisCommandKind::Unsubscribe, args);
  cluster_hash_legacy_command(client, &mut command);

  let frame = utils::request_response(client, move || Ok(command)).await?;
  protocol_utils::frame_to_results(frame).map(|_| ())
}

pub async fn publish<C: ClientLike>(client: &C, channel: Str, message: RedisValue) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, move || {
    Ok((RedisCommandKind::Publish, vec![channel.into(), message]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn psubscribe<C: ClientLike>(client: &C, patterns: MultipleStrings) -> Result<(), RedisError> {
  let args = patterns.inner().into_iter().map(|c| c.into()).collect();
  let mut command = RedisCommand::new(RedisCommandKind::Psubscribe, args);
  cluster_hash_legacy_command(client, &mut command);

  let frame = utils::request_response(client, move || Ok(command)).await?;
  protocol_utils::frame_to_results(frame).map(|_| ())
}

pub async fn punsubscribe<C: ClientLike>(client: &C, patterns: MultipleStrings) -> Result<(), RedisError> {
  let args = patterns.inner().into_iter().map(|c| c.into()).collect();
  let mut command = RedisCommand::new(RedisCommandKind::Punsubscribe, args);
  cluster_hash_legacy_command(client, &mut command);

  let frame = utils::request_response(client, move || Ok(command)).await?;
  protocol_utils::frame_to_results(frame).map(|_| ())
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

  protocol_utils::frame_to_results(frame)
}

pub async fn ssubscribe<C: ClientLike>(client: &C, channels: MultipleStrings) -> Result<(), RedisError> {
  let args = channels.inner().into_iter().map(|c| c.into()).collect();
  let mut command = RedisCommand::new(RedisCommandKind::Ssubscribe, args);
  command.hasher = ClusterHash::FirstKey;

  let frame = utils::request_response(client, move || Ok(command)).await?;
  protocol_utils::frame_to_results(frame).map(|_| ())
}

pub async fn sunsubscribe<C: ClientLike>(client: &C, channels: MultipleStrings) -> Result<(), RedisError> {
  // TODO
  // if clustered and channels is empty then send to all cluster nodes
  // else send once based on first key

  let args = channels.inner().into_iter().map(|c| c.into()).collect();
  let mut command = RedisCommand::new(RedisCommandKind::Sunsubscribe, args);
  command.hasher = ClusterHash::FirstKey;

  let frame = utils::request_response(client, move || Ok(command)).await?;
  protocol_utils::frame_to_results(frame).map(|_| ())
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

  protocol_utils::frame_to_results(frame)
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

  protocol_utils::frame_to_results(frame)
}

pub async fn pubsub_shardchannels<C: ClientLike>(client: &C, pattern: Str) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::PubsubShardchannels, vec![pattern.into()]))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn pubsub_shardnumsub<C: ClientLike>(
  client: &C,
  channels: MultipleStrings,
) -> Result<RedisValue, RedisError> {
  let frame = utils::request_response(client, || {
    let args: Vec<RedisValue> = channels.inner().into_iter().map(|s| s.into()).collect();
    let has_args = !args.is_empty();
    let mut command: RedisCommand = RedisCommand::new(RedisCommandKind::PubsubShardnumsub, args);
    if !has_args {
      cluster_hash_legacy_command(client, &mut command);
    }

    Ok(command)
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}
