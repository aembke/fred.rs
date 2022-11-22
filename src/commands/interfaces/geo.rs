use crate::{
  commands,
  error::RedisError,
  interfaces::{ClientLike, RedisResult},
  types::{
    Any,
    FromRedis,
    GeoPosition,
    GeoRadiusInfo,
    GeoUnit,
    MultipleGeoValues,
    MultipleValues,
    RedisKey,
    RedisValue,
    SetOptions,
    SortOrder,
  },
};
use std::convert::TryInto;

/// Functions that implement the [GEO](https://redis.io/commands#geo) interface.
#[async_trait]
pub trait GeoInterface: ClientLike + Sized {
  /// Adds the specified geospatial items (longitude, latitude, name) to the specified key.
  ///
  /// <https://redis.io/commands/geoadd>
  async fn geoadd<R, K, V>(&self, key: K, options: Option<SetOptions>, changed: bool, values: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: Into<MultipleGeoValues> + Send,
  {
    into!(key, values);
    commands::geo::geoadd(self, key, options, changed, values)
      .await?
      .convert()
  }

  /// Return valid Geohash strings representing the position of one or more elements in a sorted set value
  /// representing a geospatial index (where elements were added using GEOADD).
  ///
  /// <https://redis.io/commands/geohash>
  async fn geohash<R, K, V>(&self, key: K, members: V) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(members);
    commands::geo::geohash(self, key, members).await?.convert()
  }

  /// Return the positions (longitude,latitude) of all the specified members of the geospatial index represented by
  /// the sorted set at key.
  ///
  /// Callers can use [as_geo_position](crate::types::RedisValue::as_geo_position) to lazily parse results as needed.
  ///
  /// <https://redis.io/commands/geopos>
  async fn geopos<K, V>(&self, key: K, members: V) -> RedisResult<RedisValue>
  where
    K: Into<RedisKey> + Send,
    V: TryInto<MultipleValues> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(members);
    commands::geo::geopos(self, key, members).await
  }

  /// Return the distance between two members in the geospatial index represented by the sorted set.
  ///
  /// <https://redis.io/commands/geodist>
  async fn geodist<R, K, S, D>(&self, key: K, src: S, dest: D, unit: Option<GeoUnit>) -> RedisResult<R>
  where
    R: FromRedis,
    K: Into<RedisKey> + Send,
    S: TryInto<RedisValue> + Send,
    S::Error: Into<RedisError> + Send,
    D: TryInto<RedisValue> + Send,
    D::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(src, dest);
    commands::geo::geodist(self, key, src, dest, unit).await?.convert()
  }

  /// Return the members of a sorted set populated with geospatial information using GEOADD, which are within the
  /// borders of the area specified with the center location and the maximum distance from the center (the radius).
  ///
  /// <https://redis.io/commands/georadius>
  async fn georadius<K, P>(
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
  ) -> RedisResult<Vec<GeoRadiusInfo>>
  where
    K: Into<RedisKey> + Send,
    P: Into<GeoPosition> + Send,
  {
    into!(key, position);
    commands::geo::georadius(
      self, key, position, radius, unit, withcoord, withdist, withhash, count, ord, store, storedist,
    )
    .await
  }

  /// This command is exactly like GEORADIUS with the sole difference that instead of taking, as the center of the
  /// area to query, a longitude and latitude value, it takes the name of a member already existing inside the
  /// geospatial index represented by the sorted set.
  ///
  /// <https://redis.io/commands/georadiusbymember>
  async fn georadiusbymember<K, V>(
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
  ) -> RedisResult<Vec<GeoRadiusInfo>>
  where
    K: Into<RedisKey> + Send,
    V: TryInto<RedisValue> + Send,
    V::Error: Into<RedisError> + Send,
  {
    into!(key);
    try_into!(member);
    commands::geo::georadiusbymember(
      self,
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
  }

  /// Return the members of a sorted set populated with geospatial information using GEOADD, which are within the
  /// borders of the area specified by a given shape.
  ///
  /// <https://redis.io/commands/geosearch>
  async fn geosearch<K>(
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
  ) -> RedisResult<Vec<GeoRadiusInfo>>
  where
    K: Into<RedisKey> + Send,
  {
    into!(key);
    commands::geo::geosearch(
      self,
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
  }

  /// This command is like GEOSEARCH, but stores the result in destination key. Returns the number of members added to
  /// the destination key.
  ///
  /// <https://redis.io/commands/geosearchstore>
  async fn geosearchstore<R, D, S>(
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
  ) -> RedisResult<R>
  where
    R: FromRedis,
    D: Into<RedisKey> + Send,
    S: Into<RedisKey> + Send,
  {
    into!(dest, source);
    commands::geo::geosearchstore(
      self,
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
  }
}
