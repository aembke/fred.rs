use redis_protocol::resp2::types::Frame as Resp2Frame;
use redis_protocol::resp3::types::Auth;
use redis_protocol::resp3::types::Frame as Resp3Frame;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::str;

#[derive(Debug)]
enum DebugFrame {
  String(String),
  Bytes(Vec<u8>),
  Integer(i64),
  Double(f64),
  Array(Vec<DebugFrame>),
  // TODO add support for maps in network logs
  #[allow(dead_code)]
  Map(HashMap<DebugFrame, DebugFrame>),
  #[allow(dead_code)]
  Set(HashSet<DebugFrame>),
}

impl Hash for DebugFrame {
  fn hash<H: Hasher>(&self, state: &mut H) {
    match self {
      DebugFrame::String(ref s) => {
        's'.hash(state);
        s.hash(state)
      }
      DebugFrame::Bytes(ref b) => {
        'b'.hash(state);
        b.hash(state)
      }
      DebugFrame::Integer(ref i) => {
        'i'.hash(state);
        i.hash(state)
      }
      DebugFrame::Double(ref f) => {
        'd'.hash(state);
        f.to_be_bytes().hash(state)
      }
      _ => panic!("Cannot hash network log debug frame {:?}", self),
    }
  }
}

fn bytes_or_string(b: &[u8]) -> DebugFrame {
  match str::from_utf8(b) {
    Ok(s) => DebugFrame::String(s.to_owned()),
    Err(_) => DebugFrame::Bytes(b.to_vec()),
  }
}

impl<'a> From<&'a Resp2Frame> for DebugFrame {
  fn from(f: &'a Resp2Frame) -> Self {
    match f {
      Resp2Frame::Error(s) => DebugFrame::String(s.to_string()),
      Resp2Frame::SimpleString(s) => bytes_or_string(s),
      Resp2Frame::Integer(i) => DebugFrame::Integer(*i),
      Resp2Frame::BulkString(b) => bytes_or_string(b),
      Resp2Frame::Null => DebugFrame::String("nil".into()),
      Resp2Frame::Array(frames) => DebugFrame::Array(frames.iter().map(|f| f.into()).collect()),
    }
  }
}

impl<'a> From<&'a Resp3Frame> for DebugFrame {
  fn from(frame: &'a Resp3Frame) -> Self {
    match frame {
      Resp3Frame::Map { ref data, .. } => DebugFrame::Array(data.iter().fold(vec![], |mut memo, (key, value)| {
        memo.push(key.into());
        memo.push(value.into());
        memo
      })),
      Resp3Frame::Set { ref data, .. } => DebugFrame::Array(data.iter().map(|d| d.into()).collect()),
      Resp3Frame::Array { ref data, .. } => DebugFrame::Array(data.iter().map(|d| d.into()).collect()),
      Resp3Frame::BlobError { ref data, .. } => bytes_or_string(data),
      Resp3Frame::BlobString { ref data, .. } => bytes_or_string(data),
      Resp3Frame::SimpleString { ref data, .. } => bytes_or_string(data),
      Resp3Frame::SimpleError { ref data, .. } => DebugFrame::String(data.to_string()),
      Resp3Frame::Double { ref data, .. } => DebugFrame::Double(*data),
      Resp3Frame::BigNumber { ref data, .. } => bytes_or_string(data),
      Resp3Frame::Number { ref data, .. } => DebugFrame::Integer(*data),
      Resp3Frame::Boolean { ref data, .. } => DebugFrame::String(data.to_string()),
      Resp3Frame::Null => DebugFrame::String("nil".into()),
      Resp3Frame::Push { ref data, .. } => DebugFrame::Array(data.iter().map(|d| d.into()).collect()),
      Resp3Frame::ChunkedString(ref data) => bytes_or_string(data),
      Resp3Frame::VerbatimString { ref data, .. } => bytes_or_string(data),
      Resp3Frame::Hello {
        ref version, ref auth, ..
      } => {
        let mut values = vec![DebugFrame::Integer(version.to_byte() as i64)];
        if let Some(Auth {
          ref username,
          ref password,
        }) = auth
        {
          values.push(DebugFrame::String(username.to_string()));
          values.push(DebugFrame::String(password.to_string()));
        }
        DebugFrame::Array(values)
      }
    }
  }
}

pub fn log_resp2_frame(name: &str, frame: &Resp2Frame, encode: bool) {
  let prefix = if encode { "Encoded" } else { "Decoded" };
  trace!("{}: {} {:?}", name, prefix, DebugFrame::from(frame))
}

pub fn log_resp3_frame(name: &str, frame: &Resp3Frame, encode: bool) {
  let prefix = if encode { "Encoded" } else { "Decoded" };
  trace!("{}: {} {:?}", name, prefix, DebugFrame::from(frame))
}
