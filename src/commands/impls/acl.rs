use super::*;
use crate::error::*;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use redis_protocol::resp3::types::Frame;
use std::sync::Arc;

ok_cmd!(acl_load, AclLoad);
ok_cmd!(acl_save, AclSave);
values_cmd!(acl_list, AclList);
values_cmd!(acl_users, AclUsers);
value_cmd!(acl_whoami, AclWhoAmI);

pub async fn acl_setuser(
  inner: &Arc<RedisClientInner>,
  username: String,
  rules: Vec<AclRule>,
) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(rules.len() + 1);
    args.push(username.into());

    for rule in rules.into_iter() {
      args.push(rule.to_value());
    }

    Ok((RedisCommandKind::AclSetUser, args))
  })
  .await?;

  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn acl_getuser(inner: &Arc<RedisClientInner>, username: String) -> Result<Option<AclUser>, RedisError> {
  let frame =
    utils::request_response(inner, move || Ok((RedisCommandKind::AclGetUser, vec![username.into()]))).await?;

  if protocol_utils::is_null(&frame) {
    return Ok(None);
  }
  let frame = protocol_utils::frame_map_or_set_to_nested_array(frame)?;

  if let Frame::Array { data, .. } = frame {
    protocol_utils::parse_acl_getuser_frames(data).map(|u| Some(u))
  } else {
    Err(RedisError::new(
      RedisErrorKind::ProtocolError,
      "Invalid response frame. Expected array or nil.",
    ))
  }
}

pub async fn acl_deluser(inner: &Arc<RedisClientInner>, usernames: MultipleKeys) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = usernames.inner().into_iter().map(|k| k.into()).collect();
  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::AclDelUser, args))).await?;
  protocol_utils::frame_to_single_result(frame)
}

pub async fn acl_cat(inner: &Arc<RedisClientInner>, category: Option<String>) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = if let Some(cat) = category {
    vec![cat.into()]
  } else {
    Vec::new()
  };

  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::AclCat, args))).await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn acl_genpass(inner: &Arc<RedisClientInner>, bits: Option<u16>) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = if let Some(bits) = bits {
    vec![bits.into()]
  } else {
    Vec::new()
  };

  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::AclGenPass, args))).await?;
  protocol_utils::frame_to_single_result(frame)
}

pub async fn acl_log_reset(inner: &Arc<RedisClientInner>) -> Result<(), RedisError> {
  let frame = utils::request_response(inner, || Ok((RedisCommandKind::AclLog, vec![static_val!(RESET)]))).await?;
  let response = protocol_utils::frame_to_single_result(frame)?;
  protocol_utils::expect_ok(&response)
}

pub async fn acl_log_count(inner: &Arc<RedisClientInner>, count: Option<u32>) -> Result<RedisValue, RedisError> {
  let args: Vec<RedisValue> = if let Some(count) = count {
    vec![count.into()]
  } else {
    Vec::new()
  };

  let frame = utils::request_response(inner, move || Ok((RedisCommandKind::AclLog, args))).await?;
  protocol_utils::frame_to_results(frame)
}
