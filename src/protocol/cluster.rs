use crate::error::{RedisError, RedisErrorKind};
use crate::modules::inner::RedisClientInner;
use crate::protocol::command::{RedisCommand, RedisCommandKind};
use crate::protocol::connection::RedisTransport;
use crate::protocol::tls::TlsConnector;
use crate::protocol::types::{ClusterRouting, ProtocolFrame, SlotRange};
use crate::protocol::utils as protocol_utils;
use crate::types::{ClusterRouting, RedisKey, RedisMap, RedisValue, Resolve};
use arcstr::ArcStr;
use bytes_utils::Str;
use nom::combinator::value;
use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

fn parse_as_u16(value: RedisValue) -> Result<u16, RedisError> {
  match value {
    RedisValue::Integer(i) => {
      if i < 0 || i > u16::MAX as i64 {
        Err(RedisError::new(RedisErrorKind::Parse, "Invalid cluster slot integer."))
      } else {
        Ok(i as u16)
      }
    },
    RedisValue::String(s) => s.parse::<u16>()?,
    _ => Err(RedisError::new(
      RedisErrorKind::Parse,
      "Could not parse value as cluster slot.",
    )),
  }
}

fn is_ip_address(value: &str) -> bool {
  IpAddr::from_str(value).is_ok()
}

fn check_metadata_hostname(data: &HashMap<String, String>) -> Option<&str> {
  data.get("hostname").map(|s| s.as_str())
}

/// Find the correct hostname for the server, preferring hostnames over IP addresses for TLS purposes.
///
/// The format of `server` is `[<preferred host>|null, <port>, <id>, <metadata>]`. However, in Redis <=6 the `metadata` value will not be present.
///
/// The implementation here does the following:
/// 1. If `server[0]` is a hostname then use that.
/// 2. If `server[0]` is an IP address, then check `server[3]` for a "hostname" metadata field and use that if found. Otherwise use the IP address in `server[0]`.
/// 3. If `server[0]` is null, but `server[3]` has a "hostname" metadata field, then use the metadata field. Otherwise use `default_host`.
///
/// <https://redis.io/commands/cluster-slots/#nested-result-array>
fn parse_cluster_slot_hostname(primary: &[RedisValue], default_host: &str) -> Result<String, RedisError> {
  if server.is_empty() {
    return Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Invalid CLUSTER SLOTS server block.",
    ));
  }
  let should_parse_metadata = server.len() >= 4 && !server[3].is_null() && server[3].array_len().unwrap_or(0) > 0;

  let metadata: HashMap<String, String> = if should_parse_metadata {
    // not ideal, but all the variants with data on the heap are ref counted (`Bytes`, `Str`, etc)
    server[3].clone().convert()?
  } else {
    HashMap::new()
  };
  if server[0].is_null() {
    // step 3
    Ok(check_metadata_hostname(&metadata).unwrap_or(default_host).to_owned())
  } else {
    let preferred_host = match server[0].clone().into_string() {
      Some(host) => host,
      None => {
        return Err(RedisError::new(
          RedisErrorKind::Protocol,
          "Invalid CLUSTER SLOTS server block hostname.",
        ))
      },
    };

    if is_ip_address(&preferred_host) {
      // step 2
      Ok(
        check_metadata_hostname(&metadata)
          .map(|s| s.to_owned())
          .unwrap_or(preferred_host),
      )
    } else {
      // step 1
      Ok(preferred_host)
    }
  }
}

/// Parse the cluster slot range and associated server blocks.
fn parse_cluster_slot_nodes(mut slot_range: Vec<RedisValue>, default_host: &str) -> Result<SlotRange, RedisError> {
  if slot_range.len() < 3 {
    return Err(RedisError::new(
      RedisErrorKind::Protocol,
      "Invalid CLUSTER SLOTS response.",
    ));
  }
  slot_range.reverse();
  // length checked above
  let start = parse_as_u16(slot_range.pop().unwrap())?;
  let end = parse_as_u16(slot_range.pop().unwrap())?;

  // the third value is the primary node, following values are optional replica nodes
  let (primary, id) = {
    // length checked above. format is `<hostname>|null, <port>, <id>, [metadata]`
    let server_block: Vec<RedisValue> = slot_range.pop().unwrap().convert()?;
    if server_block.len() < 3 {
      return Err(RedisError::new(
        RedisErrorKind::Protocol,
        "Invalid CLUSTER SLOTS server block.",
      ));
    }

    let hostname = parse_cluster_slot_hostname(&server_block, default_host)?;
    let port: u16 = parse_as_u16(server_block[1].clone())?;
    let primary = ArcStr::from(format!("{}:{}", hostname, port));
    let id = if let Some(s) = server_block[2].as_str() {
      ArcStr::from(s.as_ref().to_string())
    } else {
      return Err(RedisError::new(
        RedisErrorKind::Protocol,
        "Invalid CLUSTER SLOTS node ID.",
      ));
    };

    (primary, id)
  };
  // TODO parse replica nodes from remaining elements in `slot_range`

  Ok(SlotRange {
    start,
    end,
    primary,
    id,
  })
}

/// Parse the entire CLUSTER SLOTS response with the provided `default_host` of the connection used to send the command.
pub fn parse_cluster_slots(frame: RedisValue, default_host: &str) -> Result<Vec<SlotRange>, RedisError> {
  let slot_ranges: Vec<Vec<RedisValue>> = frame.convert()?;
  let mut out: Vec<SlotRange> = Vec::with_capacity(slot_ranges.len());

  for mut slot_range in slot_ranges.into_iter() {
    out.push(parse_cluster_slot_nodes(slot_range, default_host)?);
  }

  out.shrink_to_fit();
  Ok(out)
}

pub async fn read_cluster_slots<T>(
  transport: &mut RedisTransport<T>,
  inner: &Arc<RedisClientInner>,
) -> Result<ClusterRouting, RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  _debug!(inner, "Reading cluster slots from {}", transport.server);
  let cmd = RedisCommand::new(RedisCommandKind::ClusterSlots, vec![], None);
  let response = transport.request_response(cmd, inner.is_resp3()).await?;
  let response = protocol_utils::frame_to_results_raw(response.into_resp3())?;

  let mut cache = ClusterRouting::new();
  cache.rebuild(response, transport.default_host.as_str())?;
  Ok(cache)
}

pub async fn read_cluster_state<T>(
  transport: &mut RedisTransport<T>,
  inner: &Arc<RedisClientInner>,
) -> Result<ClusterRouting, RedisError>
where
  T: AsyncRead + AsyncWrite + Unpin + 'static,
{
  let major_version = protocol_utils::major_redis_version(&transport.version);

  if major_version < 3 {
    return Err(RedisError::new(
      RedisErrorKind::Config,
      "Invalid Redis version. Cluster support requires >=3.0.0.",
    ));
  } else if major_version <= 6 {
    read_cluster_slots(transport, inner).await
  } else {
    // TODO implement CLUSTER SHARDS now that CLUSTER SLOTS is deprecated in v7
    // read_cluster_shards(transport, inner).await
    read_cluster_slots(transport, inner).await
  }
}

/// Attempt to connect and read the cluster state from all known hosts in the provided `RedisConfig`.
pub async fn read_cluster_state_all_nodes(inner: &Arc<RedisClientInner>) -> Result<ClusterRouting, RedisError> {
  let known_nodes = protocol_utils::read_clustered_hosts(&inner.config)?;

  for (host, port) in known_nodes.into_iter() {
    _debug!(inner, "Attempting to read cluster state from {}:{}", host, port);

    if inner.config.uses_tls() {
      match inner.config.tls {
        #[cfg(feature = "enable-native-tls")]
        Some(TlsConnector::Native(_)) => {
          let mut transport = try_or_continue!(RedisTransport::new_native_tls(inner, host, port).await);

          if let Ok(cache) = read_cluster_state(&mut transport, inner).await {
            return Ok(cache);
          }
        },
        #[cfg(feature = "enable-rustls")]
        Some(TlsConnector::Rustls(_)) => {
          let mut transport = try_or_continue!(RedisTransport::new_rustls(inner, host, port).await);

          if let Ok(cache) = read_cluster_state(&mut transport, inner).await {
            return Ok(cache);
          }
        },
        _ => return Err(RedisError::new(RedisErrorKind::Tls, "Invalid TLS configuration.")),
      }
    } else {
      let mut transport = try_or_continue!(RedisTransport::new_tcp(inner, host, port).await);

      if let Ok(cache) = read_cluster_state(&mut transport, inner).await {
        return Ok(cache);
      }
    }
  }

  Err(RedisError::new(
    RedisErrorKind::Unknown,
    "Could not read cluster state from any known node in the cluster.",
  ))
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol::types::SlotRange;
  use std::collections::HashMap;

  #[test]
  fn should_parse_cluster_slots_example_metadata_hostnames() {
    let first_slot_range = RedisValue::Array(vec![
      0.into(),
      5460.into(),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30001.into(),
        "09dbe9720cda62f7865eabc5fd8857c5d2678366".into(),
        RedisValue::Array(vec!["hostname".into(), "host-1.redis.example.com".into()]),
      ]),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30004.into(),
        "821d8ca00d7ccf931ed3ffc7e3db0599d2271abf".into(),
        RedisValue::Array(vec!["hostname".into(), "host-2.redis.example.com".into()]),
      ]),
    ]);
    let second_slot_range = RedisValue::Array(vec![
      5461.into(),
      10922.into(),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30002.into(),
        "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
        RedisValue::Array(vec!["hostname".into(), "host-3.redis.example.com".into()]),
      ]),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30005.into(),
        "faadb3eb99009de4ab72ad6b6ed87634c7ee410f".into(),
        RedisValue::Array(vec!["hostname".into(), "host-4.redis.example.com".into()]),
      ]),
    ]);
    let third_slot_range = RedisValue::Array(vec![
      10923.into(),
      16383.into(),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30003.into(),
        "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
        RedisValue::Array(vec!["hostname".into(), "host-5.redis.example.com".into()]),
      ]),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30006.into(),
        "58e6e48d41228013e5d9c1c37c5060693925e97e".into(),
        RedisValue::Array(vec!["hostname".into(), "host-6.redis.example.com".into()]),
      ]),
    ]);
    let input = RedisValue::Array(vec![first_slot_range, second_slot_range, third_slot_range]);

    let actual = parse_cluster_slots(input, "bad-host").expect("Failed to parse input");
    let expected = vec![
      SlotRange {
        start: 0,
        end: 5460,
        primary: "host-1.redis.example.com:30001".into(),
        id: "09dbe9720cda62f7865eabc5fd8857c5d2678366".into(),
      },
      SlotRange {
        start: 5461,
        end: 10922,
        primary: "host-3.redis.example.com:30002".into(),
        id: "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
      },
      SlotRange {
        start: 10923,
        end: 16383,
        primary: "host-5.redis.example.com:30003".into(),
        id: "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
      },
    ];
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_cluster_slots_example_no_metadata() {
    let first_slot_range = RedisValue::Array(vec![
      0.into(),
      5460.into(),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30001.into(),
        "09dbe9720cda62f7865eabc5fd8857c5d2678366".into(),
      ]),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30004.into(),
        "821d8ca00d7ccf931ed3ffc7e3db0599d2271abf".into(),
      ]),
    ]);
    let second_slot_range = RedisValue::Array(vec![
      5461.into(),
      10922.into(),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30002.into(),
        "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
      ]),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30005.into(),
        "faadb3eb99009de4ab72ad6b6ed87634c7ee410f".into(),
      ]),
    ]);
    let third_slot_range = RedisValue::Array(vec![
      10923.into(),
      16383.into(),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30003.into(),
        "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
      ]),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30006.into(),
        "58e6e48d41228013e5d9c1c37c5060693925e97e".into(),
      ]),
    ]);
    let input = RedisValue::Array(vec![first_slot_range, second_slot_range, third_slot_range]);

    let actual = parse_cluster_slots(input, "bad-host").expect("Failed to parse input");
    let expected = vec![
      SlotRange {
        start: 0,
        end: 5460,
        primary: "127.0.0.1:30001".into(),
        id: "09dbe9720cda62f7865eabc5fd8857c5d2678366".into(),
      },
      SlotRange {
        start: 5461,
        end: 10922,
        primary: "127.0.0.1:30002".into(),
        id: "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
      },
      SlotRange {
        start: 10923,
        end: 16383,
        primary: "127.0.0.1:30003".into(),
        id: "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
      },
    ];
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_cluster_slots_example_empty_metadata() {
    let first_slot_range = RedisValue::Array(vec![
      0.into(),
      5460.into(),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30001.into(),
        "09dbe9720cda62f7865eabc5fd8857c5d2678366".into(),
        RedisValue::Array(vec![]),
      ]),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30004.into(),
        "821d8ca00d7ccf931ed3ffc7e3db0599d2271abf".into(),
        RedisValue::Array(vec![]),
      ]),
    ]);
    let second_slot_range = RedisValue::Array(vec![
      5461.into(),
      10922.into(),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30002.into(),
        "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
        RedisValue::Array(vec![]),
      ]),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30005.into(),
        "faadb3eb99009de4ab72ad6b6ed87634c7ee410f".into(),
        RedisValue::Array(vec![]),
      ]),
    ]);
    let third_slot_range = RedisValue::Array(vec![
      10923.into(),
      16383.into(),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30003.into(),
        "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
        RedisValue::Array(vec![]),
      ]),
      RedisValue::Array(vec![
        "127.0.0.1".into(),
        30006.into(),
        "58e6e48d41228013e5d9c1c37c5060693925e97e".into(),
        RedisValue::Array(vec![]),
      ]),
    ]);
    let input = RedisValue::Array(vec![first_slot_range, second_slot_range, third_slot_range]);

    let actual = parse_cluster_slots(input, "bad-host").expect("Failed to parse input");
    let expected = vec![
      SlotRange {
        start: 0,
        end: 5460,
        primary: "127.0.0.1:30001".into(),
        id: "09dbe9720cda62f7865eabc5fd8857c5d2678366".into(),
      },
      SlotRange {
        start: 5461,
        end: 10922,
        primary: "127.0.0.1:30002".into(),
        id: "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
      },
      SlotRange {
        start: 10923,
        end: 16383,
        primary: "127.0.0.1:30003".into(),
        id: "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
      },
    ];
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_cluster_slots_example_null_hostname() {
    let first_slot_range = RedisValue::Array(vec![
      0.into(),
      5460.into(),
      RedisValue::Array(vec![
        RedisValue::Null,
        30001.into(),
        "09dbe9720cda62f7865eabc5fd8857c5d2678366".into(),
        RedisValue::Array(vec![]),
      ]),
      RedisValue::Array(vec![
        RedisValue::Null,
        30004.into(),
        "821d8ca00d7ccf931ed3ffc7e3db0599d2271abf".into(),
        RedisValue::Array(vec![]),
      ]),
    ]);
    let second_slot_range = RedisValue::Array(vec![
      5461.into(),
      10922.into(),
      RedisValue::Array(vec![
        RedisValue::Null,
        30002.into(),
        "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
        RedisValue::Array(vec![]),
      ]),
      RedisValue::Array(vec![
        RedisValue::Null,
        30005.into(),
        "faadb3eb99009de4ab72ad6b6ed87634c7ee410f".into(),
        RedisValue::Array(vec![]),
      ]),
    ]);
    let third_slot_range = RedisValue::Array(vec![
      10923.into(),
      16383.into(),
      RedisValue::Array(vec![
        RedisValue::Null,
        30003.into(),
        "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
        RedisValue::Array(vec![]),
      ]),
      RedisValue::Array(vec![
        RedisValue::Null,
        30006.into(),
        "58e6e48d41228013e5d9c1c37c5060693925e97e".into(),
        RedisValue::Array(vec![]),
      ]),
    ]);
    let input = RedisValue::Array(vec![first_slot_range, second_slot_range, third_slot_range]);

    let actual = parse_cluster_slots(input, "fake-host").expect("Failed to parse input");
    let expected = vec![
      SlotRange {
        start: 0,
        end: 5460,
        primary: "fake-host:30001".into(),
        id: "09dbe9720cda62f7865eabc5fd8857c5d2678366".into(),
      },
      SlotRange {
        start: 5461,
        end: 10922,
        primary: "fake-host:30002".into(),
        id: "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
      },
      SlotRange {
        start: 10923,
        end: 16383,
        primary: "fake-host:30003".into(),
        id: "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
      },
    ];
    assert_eq!(actual, expected);
  }
}
