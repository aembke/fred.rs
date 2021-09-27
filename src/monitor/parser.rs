use crate::modules::inner::RedisClientInner;
use crate::monitor::Command;
use crate::types::RedisValue;
use nom::bytes::complete::{escaped as nom_escaped, tag as nom_tag, take as nom_take, take_until as nom_take_until};
use nom::character::complete::none_of as nom_none_of;
use nom::combinator::{map_res as nom_map_res, opt as nom_opt};
use nom::multi::many0 as nom_many0;
use nom::sequence::{delimited as nom_delimited, preceded as nom_preceded, terminated as nom_terminated};
use nom::IResult;
use redis_protocol::resp2::types::Frame as ProtocolFrame;
use redis_protocol::types::RedisParseError;
use std::str;
use std::sync::Arc;

const EMPTY_SPACE: &'static str = " ";
const RIGHT_BRACKET: &'static str = "]";
const QUOTE: &'static str = "\"";

fn to_f64(s: &str) -> Result<f64, RedisParseError<&[u8]>> {
  s.parse::<f64>()
    .map_err(|e| RedisParseError::new_custom("to_f64", format!("{:?}", e)))
}

fn to_u8(s: &str) -> Result<u8, RedisParseError<&[u8]>> {
  s.parse::<u8>()
    .map_err(|e| RedisParseError::new_custom("to_u8", format!("{:?}", e)))
}

fn to_redis_value(s: &[u8]) -> Result<RedisValue, RedisParseError<&[u8]>> {
  // TODO make this smarter in the future
  if let Ok(value) = str::from_utf8(s) {
    Ok(RedisValue::String(value.to_owned()))
  } else {
    Ok(RedisValue::Bytes(s.to_vec()))
  }
}

fn to_str(input: &[u8]) -> Result<&str, RedisParseError<&[u8]>> {
  str::from_utf8(input).map_err(|e| RedisParseError::new_custom("to_str", format!("{:?}", e)))
}

fn d_parse_timestamp(input: &[u8]) -> IResult<&[u8], f64, RedisParseError<&[u8]>> {
  nom_map_res(
    nom_map_res(nom_terminated(nom_take_until(EMPTY_SPACE), nom_take(1_usize)), to_str),
    to_f64,
  )(input)
}

fn d_parse_db(input: &[u8]) -> IResult<&[u8], u8, RedisParseError<&[u8]>> {
  nom_map_res(
    nom_map_res(
      nom_preceded(
        nom_take(1_usize),
        nom_terminated(nom_take_until(EMPTY_SPACE), nom_take(1_usize)),
      ),
      to_str,
    ),
    to_u8,
  )(input)
}

fn d_parse_client(input: &[u8]) -> IResult<&[u8], String, RedisParseError<&[u8]>> {
  let (input, client) = nom_map_res(nom_terminated(nom_take_until(RIGHT_BRACKET), nom_take(2_usize)), to_str)(input)?;
  Ok((input, client.to_owned()))
}

fn d_parse_command(input: &[u8]) -> IResult<&[u8], String, RedisParseError<&[u8]>> {
  let (input, command) = nom_map_res(
    nom_terminated(
      nom_delimited(nom_tag(QUOTE), nom_take_until(QUOTE), nom_tag(QUOTE)),
      // args are optional after the command string, including the empty space separating the command and args
      nom_opt(nom_take(1_usize)),
    ),
    to_str,
  )(input)?;

  Ok((input, command.to_owned()))
}

fn d_parse_arg(input: &[u8]) -> IResult<&[u8], RedisValue, RedisParseError<&[u8]>> {
  let escaped_parser = nom_escaped(nom_none_of("\\\""), '\\', nom_tag(QUOTE));
  nom_map_res(
    nom_terminated(
      nom_delimited(nom_tag(QUOTE), escaped_parser, nom_tag(QUOTE)),
      nom_opt(nom_take(1_usize)),
    ),
    to_redis_value,
  )(input)
}

fn d_parse_args(input: &[u8]) -> IResult<&[u8], Vec<RedisValue>, RedisParseError<&[u8]>> {
  nom_many0(d_parse_arg)(input)
}

fn d_parse_frame(input: &[u8]) -> Result<Command, RedisParseError<&[u8]>> {
  let (input, timestamp) = d_parse_timestamp(input)?;
  let (input, db) = d_parse_db(input)?;
  let (input, client) = d_parse_client(input)?;
  let (input, command) = d_parse_command(input)?;
  let (_, args) = d_parse_args(input)?;

  Ok(Command {
    timestamp,
    db,
    client,
    command,
    args,
  })
}

#[cfg(feature = "network-logs")]
fn log_frame(inner: &Arc<RedisClientInner>, frame: &[u8]) {
  if let Ok(s) = str::from_utf8(frame) {
    _trace!(inner, "Monitor frame: {}", s);
  } else {
    _trace!(inner, "Monitor frame: {:?}", frame);
  }
}

#[cfg(not(feature = "network-logs"))]
fn log_frame(_: &Arc<RedisClientInner>, _: &[u8]) {}

pub fn parse(inner: &Arc<RedisClientInner>, frame: ProtocolFrame) -> Option<Command> {
  let frame_bytes = match frame {
    ProtocolFrame::SimpleString(ref s) => s.as_bytes(),
    ProtocolFrame::BulkString(ref b) => b,
    _ => {
      _warn!(inner, "Unexpected frame type on monitor stream: {:?}", frame.kind());
      return None;
    }
  };

  log_frame(inner, frame_bytes);
  d_parse_frame(frame_bytes).ok()
}

#[cfg(test)]
mod tests {
  use crate::monitor::parser::d_parse_frame;
  use crate::monitor::Command;

  #[test]
  fn should_parse_frame_without_spaces_or_quotes() {
    let input = "1631469940.785623 [0 127.0.0.1:46998] \"SET\" \"foo\" \"2\"";
    let expected = Command {
      timestamp: 1631469940.785623,
      db: 0,
      client: "127.0.0.1:46998".into(),
      command: "SET".into(),
      args: vec!["foo".into(), "2".into()],
    };

    let actual = d_parse_frame(input.as_bytes()).unwrap();
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_frame_with_inner_spaces() {
    let input = "1631469940.785623 [0 127.0.0.1:46998] \"SET\" \"foo bar\" \"2\"";
    let expected = Command {
      timestamp: 1631469940.785623,
      db: 0,
      client: "127.0.0.1:46998".into(),
      command: "SET".into(),
      args: vec!["foo bar".into(), "2".into()],
    };

    let actual = d_parse_frame(input.as_bytes()).unwrap();
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_frame_with_inner_quotes() {
    let input = "1631475365.563304 [0 127.0.0.1:47438] \"SET\" \"foo\" \"0 - \\\"abc\\\"\" \"1 - \\\"def\\\"\" \"2 - \\\"ghi\\\" \\\"jkl\\\"\"";
    let expected = Command {
      timestamp: 1631475365.563304,
      db: 0,
      client: "127.0.0.1:47438".into(),
      command: "SET".into(),
      args: vec![
        "foo".into(),
        "0 - \\\"abc\\\"".into(),
        "1 - \\\"def\\\"".into(),
        "2 - \\\"ghi\\\" \\\"jkl\\\"".into(),
      ],
    };

    let actual = d_parse_frame(input.as_bytes()).unwrap();
    assert_eq!(actual, expected);
  }

  #[test]
  fn should_parse_frame_without_args() {
    let input = "1631469940.785623 [0 127.0.0.1:46998] \"KEYS\"";
    let expected = Command {
      timestamp: 1631469940.785623,
      db: 0,
      client: "127.0.0.1:46998".into(),
      command: "KEYS".into(),
      args: vec![],
    };

    let actual = d_parse_frame(input.as_bytes()).unwrap();
    assert_eq!(actual, expected);
  }
}
