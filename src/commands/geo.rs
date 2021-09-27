use super::*;
use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::protocol::types::*;
use crate::protocol::utils as protocol_utils;
use crate::types::*;
use crate::utils;
use std::convert::TryInto;
use std::sync::Arc;

static WITH_COORD: &'static str = "WITHCOORD";
static WITH_DIST: &'static str = "WITHDIST";
static WITH_HASH: &'static str = "WITHHASH";
static STORE_DIST: &'static str = "STOREDIST";
static FROM_MEMBER: &'static str = "FROMMEMBER";
static FROM_LONLAT: &'static str = "FROMLONLAT";
static BY_RADIUS: &'static str = "BYRADIUS";
static BY_BOX: &'static str = "BYBOX";

pub async fn geoadd<K, V>(
  inner: &Arc<RedisClientInner>,
  key: K,
  options: Option<SetOptions>,
  changed: bool,
  values: V,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  V: Into<MultipleGeoValues>,
{
  let (key, values) = (key.into(), values.into());

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(3 + (values.len() * 3));
    args.push(key.into());

    if let Some(options) = options {
      args.push(options.to_str().into());
    }
    if changed {
      args.push(CHANGED.into());
    }

    for value in values.inner().into_iter() {
      args.push(value.coordinates.longitude.try_into()?);
      args.push(value.coordinates.latitude.try_into()?);
      args.push(value.member)
    }

    Ok((RedisCommandKind::GeoAdd, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn geohash<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  members: MultipleValues,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + members.len());
    args.push(key.into());

    for member in members.inner().into_iter() {
      args.push(member);
    }

    Ok((RedisCommandKind::GeoHash, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn geopos<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  members: MultipleValues,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(1 + members.len());
    args.push(key.into());

    for member in members.inner().into_iter() {
      args.push(member);
    }

    Ok((RedisCommandKind::GeoPos, args))
  })
  .await?;

  protocol_utils::frame_to_results(frame)
}

pub async fn geodist<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  src: RedisValue,
  dest: RedisValue,
  unit: Option<GeoUnit>,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(4);
    args.push(key.into());
    args.push(src);
    args.push(dest);

    if let Some(unit) = unit {
      args.push(unit.to_str().into());
    }

    Ok((RedisCommandKind::GeoDist, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}

pub async fn georadius<K, P>(
  inner: &Arc<RedisClientInner>,
  key: K,
  position: P,
  radius: f64,
  unit: GeoUnit,
  withcoord: bool,
  withdist: bool,
  withhash: bool,
  count: Option<(u64, Any)>,
  ord: Option<SortOrder>,
  store: Option<RedisKey>,
  storedist: Option<RedisKey>,
) -> Result<Vec<GeoRadiusInfo>, RedisError>
where
  K: Into<RedisKey>,
  P: Into<GeoPosition>,
{
  let (key, position) = (key.into(), position.into());

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(16);
    args.push(key.into());
    args.push(position.longitude.try_into()?);
    args.push(position.latitude.try_into()?);
    args.push(radius.try_into()?);
    args.push(unit.to_str().into());

    if withcoord {
      args.push(WITH_COORD.into());
    }
    if withdist {
      args.push(WITH_DIST.into());
    }
    if withhash {
      args.push(WITH_HASH.into());
    }
    if let Some((count, any)) = count {
      args.push(COUNT.into());
      args.push(count.try_into()?);
      if any {
        args.push(ANY.into());
      }
    }
    if let Some(ord) = ord {
      args.push(ord.to_str().into());
    }
    if let Some(store) = store {
      args.push(STORE.into());
      args.push(store.into());
    }
    if let Some(store_dist) = storedist {
      args.push(STORE_DIST.into());
      args.push(store_dist.into());
    }

    Ok((RedisCommandKind::GeoRadius, args))
  })
  .await?;

  protocol_utils::parse_georadius_result(frame, withcoord, withdist, withhash)
}

pub async fn georadiusbymember<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  member: RedisValue,
  radius: f64,
  unit: GeoUnit,
  withcoord: bool,
  withdist: bool,
  withhash: bool,
  count: Option<(u64, Any)>,
  ord: Option<SortOrder>,
  store: Option<RedisKey>,
  storedist: Option<RedisKey>,
) -> Result<Vec<GeoRadiusInfo>, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(15);
    args.push(key.into());
    args.push(member);
    args.push(radius.try_into()?);
    args.push(unit.to_str().into());

    if withcoord {
      args.push(WITH_COORD.into());
    }
    if withdist {
      args.push(WITH_DIST.into());
    }
    if withhash {
      args.push(WITH_HASH.into());
    }
    if let Some((count, any)) = count {
      args.push(COUNT.into());
      args.push(count.try_into()?);
      if any {
        args.push(ANY.into());
      }
    }
    if let Some(ord) = ord {
      args.push(ord.to_str().into());
    }
    if let Some(store) = store {
      args.push(STORE.into());
      args.push(store.into());
    }
    if let Some(store_dist) = storedist {
      args.push(STORE_DIST.into());
      args.push(store_dist.into());
    }

    Ok((RedisCommandKind::GeoRadiusByMember, args))
  })
  .await?;

  protocol_utils::parse_georadius_result(frame, withcoord, withdist, withhash)
}

pub async fn geosearch<K>(
  inner: &Arc<RedisClientInner>,
  key: K,
  from_member: Option<RedisValue>,
  from_lonlat: Option<GeoPosition>,
  by_radius: Option<(f64, GeoUnit)>,
  by_box: Option<(f64, f64, GeoUnit)>,
  ord: Option<SortOrder>,
  count: Option<(u64, Any)>,
  withcoord: bool,
  withdist: bool,
  withhash: bool,
) -> Result<Vec<GeoRadiusInfo>, RedisError>
where
  K: Into<RedisKey>,
{
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(15);
    args.push(key.into());

    if let Some(member) = from_member {
      args.push(FROM_MEMBER.into());
      args.push(member);
    }
    if let Some(position) = from_lonlat {
      args.push(FROM_LONLAT.into());
      args.push(position.longitude.try_into()?);
      args.push(position.latitude.try_into()?);
    }

    if let Some((radius, unit)) = by_radius {
      args.push(BY_RADIUS.into());
      args.push(radius.try_into()?);
      args.push(unit.to_str().into());
    }
    if let Some((width, height, unit)) = by_box {
      args.push(BY_BOX.into());
      args.push(width.try_into()?);
      args.push(height.try_into()?);
      args.push(unit.to_str().into());
    }
    if let Some(ord) = ord {
      args.push(ord.to_str().into());
    }
    if let Some((count, any)) = count {
      args.push(COUNT.into());
      args.push(count.try_into()?);
      if any {
        args.push(ANY.into());
      }
    }
    if withcoord {
      args.push(WITH_COORD.into());
    }
    if withdist {
      args.push(WITH_DIST.into());
    }
    if withhash {
      args.push(WITH_HASH.into());
    }

    Ok((RedisCommandKind::GeoSearch, args))
  })
  .await?;

  protocol_utils::parse_georadius_result(frame, withcoord, withdist, withhash)
}

pub async fn geosearchstore<D, S>(
  inner: &Arc<RedisClientInner>,
  dest: D,
  source: S,
  from_member: Option<RedisValue>,
  from_lonlat: Option<GeoPosition>,
  by_radius: Option<(f64, GeoUnit)>,
  by_box: Option<(f64, f64, GeoUnit)>,
  ord: Option<SortOrder>,
  count: Option<(u64, Any)>,
  storedist: bool,
) -> Result<RedisValue, RedisError>
where
  D: Into<RedisKey>,
  S: Into<RedisKey>,
{
  let (dest, source) = (dest.into(), source.into());
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(14);
    args.push(dest.into());
    args.push(source.into());

    if let Some(member) = from_member {
      args.push(FROM_MEMBER.into());
      args.push(member);
    }
    if let Some(position) = from_lonlat {
      args.push(FROM_LONLAT.into());
      args.push(position.longitude.try_into()?);
      args.push(position.latitude.try_into()?);
    }
    if let Some((radius, unit)) = by_radius {
      args.push(BY_RADIUS.into());
      args.push(radius.try_into()?);
      args.push(unit.to_str().into());
    }
    if let Some((width, height, unit)) = by_box {
      args.push(BY_BOX.into());
      args.push(width.try_into()?);
      args.push(height.try_into()?);
      args.push(unit.to_str().into());
    }
    if let Some(ord) = ord {
      args.push(ord.to_str().into());
    }
    if let Some((count, any)) = count {
      args.push(COUNT.into());
      args.push(count.try_into()?);
      if any {
        args.push(ANY.into());
      }
    }
    if storedist {
      args.push(STORE_DIST.into());
    }

    Ok((RedisCommandKind::GeoSearchStore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}
