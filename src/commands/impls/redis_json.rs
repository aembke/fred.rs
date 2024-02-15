use crate::{
  error::{RedisError, RedisErrorKind},
  interfaces::{ClientLike, RedisResult},
  protocol::{command::RedisCommandKind, utils as protocol_utils},
  types::{MultipleKeys, MultipleStrings, RedisKey, RedisValue, SetOptions},
  utils,
};
use bytes_utils::Str;
use serde_json::Value;

const INDENT: &str = "INDENT";
const NEWLINE: &str = "NEWLINE";
const SPACE: &str = "SPACE";

fn key_path_args(key: RedisKey, path: Option<Str>, extra: usize) -> Vec<RedisValue> {
  let mut out = Vec::with_capacity(2 + extra);
  out.push(key.into());
  if let Some(path) = path {
    out.push(path.into());
  }
  out
}

/// Convert the provided json value to a redis value by serializing into a json string.
fn value_to_bulk_str(value: &Value) -> Result<RedisValue, RedisError> {
  Ok(match value {
    Value::String(ref s) => RedisValue::String(Str::from(s)),
    _ => RedisValue::String(Str::from(serde_json::to_string(value)?)),
  })
}

/// Convert the provided json value to a redis value directly without serializing into a string. This only works with
/// scalar values.
fn json_to_redis(value: Value) -> Result<RedisValue, RedisError> {
  let out = match value {
    Value::String(s) => Some(RedisValue::String(Str::from(s))),
    Value::Null => Some(RedisValue::Null),
    Value::Number(n) => {
      if n.is_f64() {
        n.as_f64().map(RedisValue::Double)
      } else {
        n.as_i64().map(RedisValue::Integer)
      }
    },
    Value::Bool(b) => Some(RedisValue::Boolean(b)),
    _ => None,
  };

  out.ok_or(RedisError::new(
    RedisErrorKind::InvalidArgument,
    "Expected string or number.",
  ))
}

fn values_to_bulk(values: &[Value]) -> Result<Vec<RedisValue>, RedisError> {
  values.iter().map(value_to_bulk_str).collect()
}

pub async fn json_arrappend<C: ClientLike>(
  client: &C,
  key: RedisKey,
  path: Str,
  values: Vec<Value>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    let mut args = key_path_args(key, Some(path), values.len());
    args.extend(values_to_bulk(&values)?);

    Ok((RedisCommandKind::JsonArrAppend, args))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_arrindex<C: ClientLike>(
  client: &C,
  key: RedisKey,
  path: Str,
  value: Value,
  start: Option<i64>,
  stop: Option<i64>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    let mut args = Vec::with_capacity(5);
    args.extend([key.into(), path.into(), value_to_bulk_str(&value)?]);
    if let Some(start) = start {
      args.push(start.into());
    }
    if let Some(stop) = stop {
      args.push(stop.into());
    }

    Ok((RedisCommandKind::JsonArrIndex, args))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_arrinsert<C: ClientLike>(
  client: &C,
  key: RedisKey,
  path: Str,
  index: i64,
  values: Vec<Value>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    let mut args = Vec::with_capacity(3 + values.len());
    args.extend([key.into(), path.into(), index.into()]);
    args.extend(values_to_bulk(&values)?);

    Ok((RedisCommandKind::JsonArrInsert, args))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_arrlen<C: ClientLike>(client: &C, key: RedisKey, path: Option<Str>) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::JsonArrLen, key_path_args(key, path, 0)))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_arrpop<C: ClientLike>(
  client: &C,
  key: RedisKey,
  path: Option<Str>,
  index: Option<i64>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    let mut args = key_path_args(key, path, 1);
    if let Some(index) = index {
      args.push(index.into());
    }

    Ok((RedisCommandKind::JsonArrPop, args))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_arrtrim<C: ClientLike>(
  client: &C,
  key: RedisKey,
  path: Str,
  start: i64,
  stop: i64,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::JsonArrTrim, vec![
      key.into(),
      path.into(),
      start.into(),
      stop.into(),
    ]))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_clear<C: ClientLike>(client: &C, key: RedisKey, path: Option<Str>) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::JsonClear, key_path_args(key, path, 0)))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_debug_memory<C: ClientLike>(
  client: &C,
  key: RedisKey,
  path: Option<Str>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::JsonDebugMemory, key_path_args(key, path, 0)))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_del<C: ClientLike>(client: &C, key: RedisKey, path: Str) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::JsonDel, key_path_args(key, Some(path), 0)))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_get<C: ClientLike>(
  client: &C,
  key: RedisKey,
  indent: Option<Str>,
  newline: Option<Str>,
  space: Option<Str>,
  paths: MultipleStrings,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    let mut args = Vec::with_capacity(7 + paths.len());
    args.push(key.into());
    if let Some(indent) = indent {
      args.push(static_val!(INDENT));
      args.push(indent.into());
    }
    if let Some(newline) = newline {
      args.push(static_val!(NEWLINE));
      args.push(newline.into());
    }
    if let Some(space) = space {
      args.push(static_val!(SPACE));
      args.push(space.into());
    }
    args.extend(paths.into_values());

    Ok((RedisCommandKind::JsonGet, args))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_merge<C: ClientLike>(
  client: &C,
  key: RedisKey,
  path: Str,
  value: Value,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::JsonMerge, vec![
      key.into(),
      path.into(),
      value_to_bulk_str(&value)?,
    ]))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_mget<C: ClientLike>(client: &C, keys: MultipleKeys, path: Str) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    let mut args = Vec::with_capacity(keys.len() + 1);
    args.extend(keys.into_values());
    args.push(path.into());

    Ok((RedisCommandKind::JsonMGet, args))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_mset<C: ClientLike>(client: &C, values: Vec<(RedisKey, Str, Value)>) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    let mut args = Vec::with_capacity(values.len() * 3);
    for (key, path, value) in values.into_iter() {
      args.extend([key.into(), path.into(), value_to_bulk_str(&value)?]);
    }

    Ok((RedisCommandKind::JsonMSet, args))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_numincrby<C: ClientLike>(
  client: &C,
  key: RedisKey,
  path: Str,
  value: Value,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::JsonNumIncrBy, vec![
      key.into(),
      path.into(),
      json_to_redis(value)?,
    ]))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_objkeys<C: ClientLike>(client: &C, key: RedisKey, path: Option<Str>) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::JsonObjKeys, key_path_args(key, path, 0)))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_objlen<C: ClientLike>(client: &C, key: RedisKey, path: Option<Str>) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::JsonObjLen, key_path_args(key, path, 0)))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_resp<C: ClientLike>(client: &C, key: RedisKey, path: Option<Str>) -> RedisResult<RedisValue> {
  let frame =
    utils::request_response(client, || Ok((RedisCommandKind::JsonResp, key_path_args(key, path, 0)))).await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_set<C: ClientLike>(
  client: &C,
  key: RedisKey,
  path: Str,
  value: Value,
  options: Option<SetOptions>,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    let mut args = key_path_args(key, Some(path), 2);
    args.push(value_to_bulk_str(&value)?);
    if let Some(options) = options {
      args.push(options.to_str().into());
    }

    Ok((RedisCommandKind::JsonSet, args))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_strappend<C: ClientLike>(
  client: &C,
  key: RedisKey,
  path: Option<Str>,
  value: Value,
) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    let mut args = key_path_args(key, path, 1);
    args.push(value_to_bulk_str(&value)?);

    Ok((RedisCommandKind::JsonStrAppend, args))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_strlen<C: ClientLike>(client: &C, key: RedisKey, path: Option<Str>) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::JsonStrLen, key_path_args(key, path, 0)))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_toggle<C: ClientLike>(client: &C, key: RedisKey, path: Str) -> RedisResult<RedisValue> {
  let frame = utils::request_response(client, || {
    Ok((RedisCommandKind::JsonToggle, key_path_args(key, Some(path), 0)))
  })
  .await?;
  protocol_utils::frame_to_results(frame)
}

pub async fn json_type<C: ClientLike>(client: &C, key: RedisKey, path: Option<Str>) -> RedisResult<RedisValue> {
  let frame =
    utils::request_response(client, || Ok((RedisCommandKind::JsonType, key_path_args(key, path, 0)))).await?;
  protocol_utils::frame_to_results(frame)
}
