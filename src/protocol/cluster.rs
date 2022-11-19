use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::types::SlotRange,
  types::RedisValue,
};
use arcstr::ArcStr;
use std::{collections::HashMap, net::IpAddr, str::FromStr};

#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use crate::protocol::{
  tls::{HostMapping, TlsHostMapping},
  utils::server_to_parts,
};
#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use bytes_utils::Str;
#[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
use std::sync::Arc;

fn parse_as_u16(value: RedisValue) -> Result<u16, RedisError> {
  match value {
    RedisValue::Integer(i) => {
      if i < 0 || i > u16::MAX as i64 {
        Err(RedisError::new(RedisErrorKind::Parse, "Invalid cluster slot integer."))
      } else {
        Ok(i as u16)
      }
    },
    RedisValue::String(s) => s.parse::<u16>().map_err(|e| e.into()),
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
/// The format of `server` is `[<preferred host>|null, <port>, <id>, <metadata>]`. However, in Redis <=6 the
/// `metadata` value will not be present.
///
/// The implementation here does the following:
/// 1. If `server[0]` is a hostname then use that.
/// 2. If `server[0]` is an IP address, then check `server[3]` for a "hostname" metadata field and use that if found.
/// Otherwise use the IP address in `server[0]`. 3. If `server[0]` is null, but `server[3]` has a "hostname" metadata
/// field, then use the metadata field. Otherwise use `default_host`.
///
/// <https://redis.io/commands/cluster-slots/#nested-result-array>
fn parse_cluster_slot_hostname(server: &[RedisValue], default_host: &str) -> Result<String, RedisError> {
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

/// Read the node block with format `<hostname>|null, <port>, <id>, [metadata]`
fn parse_node_block(data: &Vec<RedisValue>, default_host: &str) -> Option<(String, u16, ArcStr, ArcStr)> {
  if data.len() < 3 {
    return None;
  }

  let hostname = match parse_cluster_slot_hostname(&data, default_host) {
    Ok(host) => host,
    Err(_) => return None,
  };
  let port: u16 = match parse_as_u16(data[1].clone()) {
    Ok(port) => port,
    Err(_) => return None,
  };
  let primary = ArcStr::from(format!("{}:{}", hostname, port));
  let id = if let Some(s) = data[2].as_str() {
    ArcStr::from(s.as_ref().to_string())
  } else {
    return None;
  };

  Some((hostname, port, primary, id))
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
  // length checked above. format is `<hostname>|null, <port>, <id>, [metadata]`
  let server_block: Vec<RedisValue> = slot_range.pop().unwrap().convert()?;
  let (primary, id) = match parse_node_block(&server_block, default_host) {
    Some((_, _, p, i)) => (p, i),
    None => {
      trace!("Failed to parse CLUSTER SLOTS response: {:?}", server_block);
      return Err(RedisError::new(
        RedisErrorKind::Cluster,
        "Invalid CLUSTER SLOTS response.",
      ));
    },
  };

  let mut replicas = Vec::with_capacity(slot_range.len());
  while let Some(server_block) = slot_range.pop() {
    let server_block: Vec<RedisValue> = match server_block.convert() {
      Ok(b) => b,
      Err(_) => continue,
    };
    let server = match parse_node_block(&server_block, default_host) {
      Some((_, _, s, _)) => s,
      None => continue,
    };

    replicas.push(server)
  }

  Ok(SlotRange {
    start,
    end,
    primary,
    id,
    #[cfg(feature = "replicas")]
    replicas,
    #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
    tls_server_name: None,
  })
}

/// Parse the entire CLUSTER SLOTS response with the provided `default_host` of the connection used to send the
/// command.
pub fn parse_cluster_slots(frame: RedisValue, default_host: &str) -> Result<Vec<SlotRange>, RedisError> {
  let slot_ranges: Vec<Vec<RedisValue>> = frame.convert()?;
  let mut out: Vec<SlotRange> = Vec::with_capacity(slot_ranges.len());

  for slot_range in slot_ranges.into_iter() {
    out.push(parse_cluster_slot_nodes(slot_range, default_host)?);
  }

  out.shrink_to_fit();
  Ok(out)
}

/// Modify the `CLUSTER SLOTS` command according to the hostname mapping policy in the `TlsHostMapping`.
#[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
pub fn modify_cluster_slot_hostnames(inner: &Arc<RedisClientInner>, ranges: &mut Vec<SlotRange>, default_host: &str) {
  let policy = match inner.config.tls {
    Some(ref config) => &config.hostnames,
    None => {
      _trace!(inner, "Skip modifying TLS hostnames.");
      return;
    },
  };
  if policy == TlsHostMapping::None {
    _trace!(inner, "Skip modifying TLS hostnames.");
    return;
  }

  for slot_range in ranges.iter_mut() {
    let ip = match server_to_parts(&slot_range.primary) {
      Ok((host, _)) => match IpAddr::from_str(host) {
        Ok(ip) => ip,
        Err(_) => continue,
      },
      Err(e) => {
        _debug!(inner, "Failed to parse CLUSTER SLOTS primary hostname: {}", e);
        continue;
      },
    };

    if let Some(tls_server_name) = policy.map(&ip, default_host) {
      _debug!(inner, "Mapping {} to {} for TLS handshake.", ip, tls_server_name);
      slot_range.tls_server_name = Some(ArcStr::from(tls_server_name));
    }
  }
}

#[cfg(not(any(feature = "enable-rustls", feature = "enable-native-tls")))]
pub fn modify_cluster_slot_hostnames(inner: &Arc<RedisClientInner>, _: &mut Vec<SlotRange>, _: &str) {
  _trace!(inner, "Skip modifying TLS hostnames.")
}

#[cfg(test)]
mod tests {
  use super::*;
  #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
  use crate::protocol::tls::{HostMapping, TlsHostMapping};
  use crate::protocol::types::SlotRange;

  fn fake_cluster_slots_without_metadata() -> RedisValue {
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

    RedisValue::Array(vec![first_slot_range, second_slot_range, third_slot_range])
  }

  fn fake_cluster_slots_with_metadata() -> RedisValue {
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

    RedisValue::Array(vec![first_slot_range, second_slot_range, third_slot_range])
  }

  #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
  #[derive(Debug)]
  struct FakeHostMapper;

  #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
  impl HostMapping for FakeHostMapper {
    fn map(&self, ip: &IpAddr, default_host: &str) -> Option<String> {
      Some("foobarbaz".into())
    }
  }

  #[test]
  #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
  fn should_modify_cluster_slot_hostnames_default_host_without_metadata() {
    unimplemented!()
  }

  #[test]
  #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
  fn should_modify_cluster_slot_hostnames_default_host_with_metadata() {
    unimplemented!()
  }

  #[test]
  #[cfg(any(feature = "enable-native-tls", feature = "enable-rustls"))]
  fn should_modify_cluster_slot_hostnames_custom() {
    unimplemented!()
  }

  #[test]
  fn should_parse_cluster_slots_example_metadata_hostnames() {
    let input = fake_cluster_slots_with_metadata();

    let actual = parse_cluster_slots(input, "bad-host").expect("Failed to parse input");
    let expected = vec![
      SlotRange {
        start: 0,
        end: 5460,
        primary: "host-1.redis.example.com:30001".into(),
        id: "09dbe9720cda62f7865eabc5fd8857c5d2678366".into(),
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
      },
      SlotRange {
        start: 5461,
        end: 10922,
        primary: "host-3.redis.example.com:30002".into(),
        id: "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
      },
      SlotRange {
        start: 10923,
        end: 16383,
        primary: "host-5.redis.example.com:30003".into(),
        id: "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
      },
    ];
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_cluster_slots_example_no_metadata() {
    let input = fake_cluster_slots_without_metadata();

    let actual = parse_cluster_slots(input, "bad-host").expect("Failed to parse input");
    let expected = vec![
      SlotRange {
        start: 0,
        end: 5460,
        primary: "127.0.0.1:30001".into(),
        id: "09dbe9720cda62f7865eabc5fd8857c5d2678366".into(),
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
      },
      SlotRange {
        start: 5461,
        end: 10922,
        primary: "127.0.0.1:30002".into(),
        id: "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
      },
      SlotRange {
        start: 10923,
        end: 16383,
        primary: "127.0.0.1:30003".into(),
        id: "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
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
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
      },
      SlotRange {
        start: 5461,
        end: 10922,
        primary: "127.0.0.1:30002".into(),
        id: "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
      },
      SlotRange {
        start: 10923,
        end: 16383,
        primary: "127.0.0.1:30003".into(),
        id: "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
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
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
      },
      SlotRange {
        start: 5461,
        end: 10922,
        primary: "fake-host:30002".into(),
        id: "c9d93d9f2c0c524ff34cc11838c2003d8c29e013".into(),
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
      },
      SlotRange {
        start: 10923,
        end: 16383,
        primary: "fake-host:30003".into(),
        id: "044ec91f325b7595e76dbcb18cc688b6a5b434a1".into(),
        #[cfg(any(feature = "enable-rustls", feature = "enable-native-tls"))]
        tls_server_name: None,
      },
    ];
    assert_eq!(actual, expected);
  }
}
