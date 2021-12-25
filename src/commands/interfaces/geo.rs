use crate::commands;
use crate::error::RedisError;
use crate::interfaces::{async_spawn, AsyncResult, ClientLike};
use crate::types::{
  Any, FromRedis, GeoPosition, GeoRadiusInfo, GeoUnit, MultipleGeoValues, MultipleValues, RedisKey, RedisValue,
  SetOptions, SortOrder,
};
use std::convert::TryInto;

/// Functions that implement the [GEO](https://redis.io/commands#geo) interface.
pub trait GeoInterface: ClientLike + Sized {
  /// Adds the specified geospatial items (longitude, latitude, name) to the specified key.
  ///
  /// <https://redis.io/commands/geoadd>
  fn geoadd<R, K, V>(&self, key: K, options: Option<SetOptions>, changed: bool, values: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: Into<MultipleGeoValues>,
  {
    into!(key, values);
    async_spawn(self, move |inner| async move {
      commands::geo::geoadd(&inner, key, options, changed, values)
        .await?
        .convert()
    })
  }

  /// Return valid Geohash strings representing the position of one or more elements in a sorted set value representing a geospatial index (where elements were added using GEOADD).
  ///
  /// <https://redis.io/commands/geohash>
  fn geohash<R, K, V>(&self, key: K, members: V) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(members);
    async_spawn(self, |inner| async move {
      commands::geo::geohash(&inner, key, members).await?.convert()
    })
  }

  /// Return the positions (longitude,latitude) of all the specified members of the geospatial index represented by the sorted set at key.
  ///
  /// Callers can use [as_geo_position](crate::types::RedisValue::as_geo_position) to lazily parse results as needed.
  ///
  /// <https://redis.io/commands/geopos>
  fn geopos<K, V>(&self, key: K, members: V) -> AsyncResult<RedisValue>
  where
    K: Into<RedisKey>,
    V: TryInto<MultipleValues>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(members);
    async_spawn(self, |inner| async move {
      commands::geo::geopos(&inner, key, members).await
    })
  }

  /// Return the distance between two members in the geospatial index represented by the sorted set.
  ///
  /// <https://redis.io/commands/geodist>
  fn geodist<R, K, S, D>(&self, key: K, src: S, dest: D, unit: Option<GeoUnit>) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    K: Into<RedisKey>,
    S: TryInto<RedisValue>,
    S::Error: Into<RedisError>,
    D: TryInto<RedisValue>,
    D::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(src, dest);
    async_spawn(self, |inner| async move {
      commands::geo::geodist(&inner, key, src, dest, unit).await?.convert()
    })
  }

  /// Return the members of a sorted set populated with geospatial information using GEOADD, which are within the borders of the area specified with
  /// the center location and the maximum distance from the center (the radius).
  ///
  /// <https://redis.io/commands/georadius>
  fn georadius<K, P>(
    &self,
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
  ) -> AsyncResult<Vec<GeoRadiusInfo>>
  where
    K: Into<RedisKey>,
    P: Into<GeoPosition>,
  {
    into!(key, position);
    async_spawn(self, |inner| async move {
      commands::geo::georadius(
        &inner, key, position, radius, unit, withcoord, withdist, withhash, count, ord, store, storedist,
      )
      .await
    })
  }

  /// This command is exactly like GEORADIUS with the sole difference that instead of taking, as the center of the area to query, a longitude and
  /// latitude value, it takes the name of a member already existing inside the geospatial index represented by the sorted set.
  ///
  /// <https://redis.io/commands/georadiusbymember>
  fn georadiusbymember<K, V>(
    &self,
    key: K,
    member: V,
    radius: f64,
    unit: GeoUnit,
    withcoord: bool,
    withdist: bool,
    withhash: bool,
    count: Option<(u64, Any)>,
    ord: Option<SortOrder>,
    store: Option<RedisKey>,
    storedist: Option<RedisKey>,
  ) -> AsyncResult<Vec<GeoRadiusInfo>>
  where
    K: Into<RedisKey>,
    V: TryInto<RedisValue>,
    V::Error: Into<RedisError>,
  {
    into!(key);
    try_into!(member);
    async_spawn(self, |inner| async move {
      commands::geo::georadiusbymember(
        &inner,
        key,
        to!(member)?,
        radius,
        unit,
        withcoord,
        withdist,
        withhash,
        count,
        ord,
        store,
        storedist,
      )
      .await
    })
  }

  /// Return the members of a sorted set populated with geospatial information using GEOADD, which are within the borders of the area specified by a given shape.
  ///
  /// <https://redis.io/commands/geosearch>
  fn geosearch<K>(
    &self,
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
  ) -> AsyncResult<Vec<GeoRadiusInfo>>
  where
    K: Into<RedisKey>,
  {
    into!(key);
    async_spawn(self, |inner| async move {
      commands::geo::geosearch(
        &inner,
        key,
        from_member,
        from_lonlat,
        by_radius,
        by_box,
        ord,
        count,
        withcoord,
        withdist,
        withhash,
      )
      .await
    })
  }

  /// This command is like GEOSEARCH, but stores the result in destination key. Returns the number of members added to the destination key.
  ///
  /// <https://redis.io/commands/geosearchstore>
  fn geosearchstore<R, D, S>(
    &self,
    dest: D,
    source: S,
    from_member: Option<RedisValue>,
    from_lonlat: Option<GeoPosition>,
    by_radius: Option<(f64, GeoUnit)>,
    by_box: Option<(f64, f64, GeoUnit)>,
    ord: Option<SortOrder>,
    count: Option<(u64, Any)>,
    storedist: bool,
  ) -> AsyncResult<R>
  where
    R: FromRedis + Unpin + Send,
    D: Into<RedisKey>,
    S: Into<RedisKey>,
  {
    into!(dest, source);
    async_spawn(self, |inner| async move {
      commands::geo::geosearchstore(
        &inner,
        dest,
        source,
        from_member,
        from_lonlat,
        by_radius,
        by_box,
        ord,
        count,
        storedist,
      )
      .await?
      .convert()
    })
  }
}
