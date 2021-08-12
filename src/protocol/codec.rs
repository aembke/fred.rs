use crate::client::RedisClientInner;
use crate::error::RedisError;
use crate::metrics::SizeStats;
use crate::protocol::utils as protocol_utils;
use bytes::BytesMut;
use parking_lot::RwLock;
use redis_protocol::decode::decode_bytes;
use redis_protocol::encode::encode_bytes;
use redis_protocol::types::Frame as ProtocolFrame;
use std::sync::Arc;
use tokio_util::codec::{Decoder, Encoder};

#[cfg(feature = "network-logs")]
use std::fmt;
#[cfg(feature = "network-logs")]
use std::str;

#[cfg(feature = "blocking-encoding")]
use crate::globals::globals;

#[cfg(not(feature = "network-logs"))]
fn log_frame(_: &str, _: &ProtocolFrame, _: bool) {}

#[cfg(feature = "network-logs")]
#[derive(Debug)]
enum DebugFrame {
  String(String),
  Bytes(Vec<u8>),
  Integer(i64),
  Array(Vec<DebugFrame>),
}

#[cfg(feature = "network-logs")]
impl<'a> From<&'a ProtocolFrame> for DebugFrame {
  fn from(f: &'a ProtocolFrame) -> Self {
    match f {
      ProtocolFrame::Moved(s) | ProtocolFrame::Ask(s) | ProtocolFrame::Error(s) | ProtocolFrame::SimpleString(s) => {
        DebugFrame::String(s.to_owned())
      }
      ProtocolFrame::Integer(i) => DebugFrame::Integer(*i),
      ProtocolFrame::BulkString(b) => match str::from_utf8(b) {
        Ok(s) => DebugFrame::String(s.to_owned()),
        Err(_) => DebugFrame::Bytes(b.to_vec()),
      },
      ProtocolFrame::Null => DebugFrame::String("nil".into()),
      ProtocolFrame::Array(frames) => DebugFrame::Array(frames.iter().map(|f| f.into()).collect()),
    }
  }
}

#[cfg(feature = "network-logs")]
fn log_frame(name: &str, frame: &ProtocolFrame, encode: bool) {
  let prefix = if encode { "Encoded" } else { "Decoded" };
  trace!("{}: {} {:?}", name, prefix, DebugFrame::from(frame))
}

fn encode_frame(codec: &RedisCodec, item: ProtocolFrame, dst: &mut BytesMut) -> Result<(), RedisError> {
  let offset = dst.len();

  let res = encode_bytes(dst, &item)?;
  let len = res.saturating_sub(offset);

  trace!(
    "{}: Encoded {} bytes to {}. Buffer len: {}",
    codec.name,
    len,
    codec.server,
    res
  );
  log_frame(&codec.name, &item, true);
  codec.req_size_stats.write().sample(len as u64);

  Ok(())
}

fn decode_frame(codec: &RedisCodec, src: &mut BytesMut) -> Result<Option<ProtocolFrame>, RedisError> {
  trace!("{}: Recv {} bytes from {}.", codec.name, src.len(), codec.server);

  if src.is_empty() {
    return Ok(None);
  }
  let (frame, amt) = decode_bytes(src)?;

  if let Some(frame) = frame {
    trace!("{}: Parsed {} bytes from {}", codec.name, amt, codec.server);
    log_frame(&codec.name, &frame, false);
    codec.res_size_stats.write().sample(amt as u64);

    let _ = src.split_to(amt);
    Ok(Some(protocol_utils::check_auth_error(frame)))
  } else {
    Ok(None)
  }
}

pub struct RedisCodec {
  pub name: Arc<String>,
  pub server: String,
  pub req_size_stats: Arc<RwLock<SizeStats>>,
  pub res_size_stats: Arc<RwLock<SizeStats>>,
}

impl RedisCodec {
  pub fn new(inner: &Arc<RedisClientInner>, server: String) -> Self {
    RedisCodec {
      server,
      name: inner.id.clone(),
      req_size_stats: inner.req_size_stats.clone(),
      res_size_stats: inner.res_size_stats.clone(),
    }
  }
}

impl Encoder<ProtocolFrame> for RedisCodec {
  type Error = RedisError;

  #[cfg(not(feature = "blocking-encoding"))]
  fn encode(&mut self, item: ProtocolFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
    encode_frame(&self, item, dst)
  }

  #[cfg(feature = "blocking-encoding")]
  fn encode(&mut self, item: ProtocolFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
    let frame_size = protocol_utils::frame_size(&item);

    if frame_size >= globals().blocking_encode_threshold() {
      trace!("{}: Encoding in blocking task with size {}", self.name, frame_size);
      tokio::task::block_in_place(|| encode_frame(&self, item, dst))
    } else {
      encode_frame(&self, item, dst)
    }
  }
}

impl Decoder for RedisCodec {
  type Item = ProtocolFrame;
  type Error = RedisError;

  #[cfg(not(feature = "blocking-encoding"))]
  fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    decode_frame(&self, src)
  }

  #[cfg(feature = "blocking-encoding")]
  fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    if src.len() >= globals().blocking_encode_threshold() {
      trace!("{}: Decoding in blocking task with size {}", self.name, src.len());
      tokio::task::block_in_place(|| decode_frame(&self, src))
    } else {
      decode_frame(&self, src)
    }
  }
}
