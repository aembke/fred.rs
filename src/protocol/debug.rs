use crate::protocol::types::ProtocolFrame;
use redis_protocol::resp2::types::Frame as Resp2Frame;
use redis_protocol::resp3::types::Frame as Resp3Frame;
use redis_protocol::resp3::types::RespVersion;
use std::borrow::Cow;
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
  Map(HashMap<DebugFrame, DebugFrame>),
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
    Ok(s) => DebugFrame::String(s),
    Err(_) => DebugFrame::Bytes(b.to_vec()),
  }
}

impl<'a> From<&'a Resp2Frame> for DebugFrame {
  fn from(f: &'a Resp2Frame) -> Self {
    match f {
      Resp2Frame::Error(s) | Resp2Frame::SimpleString(s) => DebugFrame::String(s.to_owned()),
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
      Resp3Frame::Map { ref data, .. } => {}
      Resp3Frame::Set { ref data, .. } => {}
      Resp3Frame::Array { ref data, .. } => {}
      Resp3Frame::BlobError { ref data, .. } => {}
      Resp3Frame::BlobString { ref data, .. } => {}
      Resp3Frame::SimpleString { ref data, .. } => {}
      Resp3Frame::SimpleError { ref data, .. } => {}
      Resp3Frame::Double { ref data, .. } => {}
      Resp3Frame::BigNumber { ref data, .. } => {}
      Resp3Frame::Number { ref data, .. } => {}
      Resp3Frame::Boolean { ref data, .. } => {}
      Resp3Frame::Null => {}
      Resp3Frame::Push { ref data, .. } => {}
      Resp3Frame::ChunkedString(ref data) => {}
      Resp3Frame::Hello {
        ref version, ref auth, ..
      } => {}
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
