use super::*;
use crate::{
  error::RedisError,
  modules::inner::RedisClientInner,
  protocol::{types::*, utils as protocol_utils},
  types::*,
  utils,
};
use std::{convert::TryInto, sync::Arc};

static WITH_COORD: &str = "WITHCOORD";
static WITH_DIST: &str = "WITHDIST";
static WITH_HASH: &str = "WITHHASH";
static STORE_DIST: &str = "STOREDIST";
static FROM_MEMBER: &str = "FROMMEMBER";
static FROM_LONLAT: &str = "FROMLONLAT";
static BY_RADIUS: &str = "BYRADIUS";
static BY_BOX: &str = "BYBOX";

pub async fn geoadd<K, V>(
  inner: &Arc<RedisClientInner>,
  key: K,
  options: Option<SetOptions>,
  changed: bool,
  values: V,
) -> Result<RedisValue, RedisError>
where
  K: Into<RedisKey>,
  V: Into<MultipleGeoValues>, {
  let (key, values) = (key.into(), values.into());

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(3 + (values.len() * 3));
    args.push(key.into());

    if let Some(options) = options {
      args.push(options.to_str().into());
    }
    if changed {
      args.push(static_val!(CHANGED));
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
  K: Into<RedisKey>, {
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
  K: Into<RedisKey>, {
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
  K: Into<RedisKey>, {
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
  P: Into<GeoPosition>, {
  let (key, position) = (key.into(), position.into());

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(16);
    args.push(key.into());
    args.push(position.longitude.try_into()?);
    args.push(position.latitude.try_into()?);
    args.push(radius.try_into()?);
    args.push(unit.to_str().into());

    if withcoord {
      args.push(static_val!(WITH_COORD));
    }
    if withdist {
      args.push(static_val!(WITH_DIST));
    }
    if withhash {
      args.push(static_val!(WITH_HASH));
    }
    if let Some((count, any)) = count {
      args.push(static_val!(COUNT));
      args.push(count.try_into()?);
      if any {
        args.push(static_val!(ANY));
      }
    }
    if let Some(ord) = ord {
      args.push(ord.to_str().into());
    }
    if let Some(store) = store {
      args.push(static_val!(STORE));
      args.push(store.into());
    }
    if let Some(store_dist) = storedist {
      args.push(static_val!(STORE_DIST));
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
  K: Into<RedisKey>, {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(15);
    args.push(key.into());
    args.push(member);
    args.push(radius.try_into()?);
    args.push(unit.to_str().into());

    if withcoord {
      args.push(static_val!(WITH_COORD));
    }
    if withdist {
      args.push(static_val!(WITH_DIST));
    }
    if withhash {
      args.push(static_val!(WITH_HASH));
    }
    if let Some((count, any)) = count {
      args.push(static_val!(COUNT));
      args.push(count.try_into()?);
      if any {
        args.push(static_val!(ANY));
      }
    }
    if let Some(ord) = ord {
      args.push(ord.to_str().into());
    }
    if let Some(store) = store {
      args.push(static_val!(STORE));
      args.push(store.into());
    }
    if let Some(store_dist) = storedist {
      args.push(static_val!(STORE_DIST));
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
  K: Into<RedisKey>, {
  let key = key.into();

  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(15);
    args.push(key.into());

    if let Some(member) = from_member {
      args.push(static_val!(FROM_MEMBER));
      args.push(member);
    }
    if let Some(position) = from_lonlat {
      args.push(static_val!(FROM_LONLAT));
      args.push(position.longitude.try_into()?);
      args.push(position.latitude.try_into()?);
    }

    if let Some((radius, unit)) = by_radius {
      args.push(static_val!(BY_RADIUS));
      args.push(radius.try_into()?);
      args.push(unit.to_str().into());
    }
    if let Some((width, height, unit)) = by_box {
      args.push(static_val!(BY_BOX));
      args.push(width.try_into()?);
      args.push(height.try_into()?);
      args.push(unit.to_str().into());
    }
    if let Some(ord) = ord {
      args.push(ord.to_str().into());
    }
    if let Some((count, any)) = count {
      args.push(static_val!(COUNT));
      args.push(count.try_into()?);
      if any {
        args.push(static_val!(ANY));
      }
    }
    if withcoord {
      args.push(static_val!(WITH_COORD));
    }
    if withdist {
      args.push(static_val!(WITH_DIST));
    }
    if withhash {
      args.push(static_val!(WITH_HASH));
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
  S: Into<RedisKey>, {
  let (dest, source) = (dest.into(), source.into());
  let frame = utils::request_response(inner, move || {
    let mut args = Vec::with_capacity(14);
    args.push(dest.into());
    args.push(source.into());

    if let Some(member) = from_member {
      args.push(static_val!(FROM_MEMBER));
      args.push(member);
    }
    if let Some(position) = from_lonlat {
      args.push(static_val!(FROM_LONLAT));
      args.push(position.longitude.try_into()?);
      args.push(position.latitude.try_into()?);
    }
    if let Some((radius, unit)) = by_radius {
      args.push(static_val!(BY_RADIUS));
      args.push(radius.try_into()?);
      args.push(unit.to_str().into());
    }
    if let Some((width, height, unit)) = by_box {
      args.push(static_val!(BY_BOX));
      args.push(width.try_into()?);
      args.push(height.try_into()?);
      args.push(unit.to_str().into());
    }
    if let Some(ord) = ord {
      args.push(ord.to_str().into());
    }
    if let Some((count, any)) = count {
      args.push(static_val!(COUNT));
      args.push(count.try_into()?);
      if any {
        args.push(static_val!(ANY));
      }
    }
    if storedist {
      args.push(static_val!(STORE_DIST));
    }

    Ok((RedisCommandKind::GeoSearchStore, args))
  })
  .await?;

  protocol_utils::frame_to_single_result(frame)
}
