use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use crate::protocol::utils as protocol_utils;
use bytes::BytesMut;
use parking_lot::RwLock;
use rand::Rng;
use redis_protocol::resp2::decode::decode as resp2_decode;
use redis_protocol::resp2::encode::encode_bytes as resp2_encode;
use redis_protocol::resp2::types::Frame as Resp2Frame;
use std::sync::Arc;
use tokio_util::codec::{Decoder, Encoder};

#[cfg(feature = "blocking-encoding")]
use crate::globals::globals;
#[cfg(feature = "metrics")]
use crate::modules::metrics::MovingStats;
#[cfg(feature = "network-logs")]
use std::str;

#[cfg(not(feature = "network-logs"))]
fn log_resp2_frame(_: &str, _: &Resp2Frame, _: bool) {}

#[cfg(feature = "network-logs")]
#[derive(Debug)]
enum DebugFrame {
  String(String),
  Bytes(Vec<u8>),
  Integer(i64),
  Array(Vec<DebugFrame>),
}

#[cfg(feature = "network-logs")]
impl<'a> From<&'a Resp2Frame> for DebugFrame {
  fn from(f: &'a Resp2Frame) -> Self {
    match f {
      Resp2Frame::Error(s) | Resp2Frame::SimpleString(s) => DebugFrame::String(s.to_owned()),
      Resp2Frame::Integer(i) => DebugFrame::Integer(*i),
      Resp2Frame::BulkString(b) => match str::from_utf8(b) {
        Ok(s) => DebugFrame::String(s.to_owned()),
        Err(_) => DebugFrame::Bytes(b.to_vec()),
      },
      Resp2Frame::Null => DebugFrame::String("nil".into()),
      Resp2Frame::Array(frames) => DebugFrame::Array(frames.iter().map(|f| f.into()).collect()),
    }
  }
}

#[cfg(feature = "network-logs")]
fn log_resp2_frame(name: &str, frame: &Resp2Frame, encode: bool) {
  let prefix = if encode { "Encoded" } else { "Decoded" };
  trace!("{}: {} {:?}", name, prefix, DebugFrame::from(frame))
}

#[cfg(feature = "metrics")]
fn sample_stats(codec: &RedisCodec, decode: bool, value: i64) {
  if decode {
    codec.res_size_stats.write().sample(value);
  } else {
    codec.req_size_stats.write().sample(value);
  }
}

#[cfg(not(feature = "metrics"))]
fn sample_stats(_: &RedisCodec, _: bool, _: i64) {}

fn resp2_encode_frame(codec: &RedisCodec, item: Resp2Frame, dst: &mut BytesMut) -> Result<(), RedisError> {
  let offset = dst.len();

  let res = resp2_encode(dst, &item)?;
  let len = res.saturating_sub(offset);

  trace!(
    "{}: Encoded {} bytes to {}. Buffer len: {}",
    codec.name,
    len,
    codec.server,
    res
  );
  log_resp2_frame(&codec.name, &item, true);
  sample_stats(&codec, false, len as i64);

  Ok(())
}

fn resp2_decode_frame(codec: &RedisCodec, src: &mut BytesMut) -> Result<Option<Resp2Frame>, RedisError> {
  trace!("{}: Recv {} bytes from {}.", codec.name, src.len(), codec.server);
  if src.is_empty() {
    return Ok(None);
  }

  if let Some((frame, amt)) = resp2_decode(src)? {
    trace!("{}: Parsed {} bytes from {}", codec.name, amt, codec.server);
    log_resp2_frame(&codec.name, &frame, false);
    sample_stats(&codec, true, amt as i64);

    let _ = src.split_to(amt);
    Ok(Some(protocol_utils::check_auth_error(frame)))
  } else {
    Ok(None)
  }
}

pub struct RedisCodec {
  pub name: Arc<String>,
  pub server: String,
  #[cfg(feature = "metrics")]
  pub req_size_stats: Arc<RwLock<MovingStats>>,
  #[cfg(feature = "metrics")]
  pub res_size_stats: Arc<RwLock<MovingStats>>,
}

impl RedisCodec {
  pub fn new(inner: &Arc<RedisClientInner>, server: String) -> Self {
    RedisCodec {
      server,
      name: inner.id.clone(),
      #[cfg(feature = "metrics")]
      req_size_stats: inner.req_size_stats.clone(),
      #[cfg(feature = "metrics")]
      res_size_stats: inner.res_size_stats.clone(),
    }
  }
}

impl Encoder<Resp2Frame> for RedisCodec {
  type Error = RedisError;

  #[cfg(not(feature = "blocking-encoding"))]
  fn encode(&mut self, item: Resp2Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
    resp2_encode_frame(&self, item, dst)
  }

  #[cfg(feature = "blocking-encoding")]
  fn encode(&mut self, item: Resp2Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
    let frame_size = protocol_utils::frame_size(&item);

    if frame_size >= globals().blocking_encode_threshold() {
      trace!("{}: Encoding in blocking task with size {}", self.name, frame_size);
      tokio::task::block_in_place(|| resp2_encode_frame(&self, item, dst))
    } else {
      resp2_encode_frame(&self, item, dst)
    }
  }
}

impl Decoder for RedisCodec {
  type Item = Resp2Frame;
  type Error = RedisError;

  #[cfg(not(feature = "blocking-encoding"))]
  fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    resp2_decode_frame(&self, src)
  }

  #[cfg(feature = "blocking-encoding")]
  fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    if src.len() >= globals().blocking_encode_threshold() {
      trace!("{}: Decoding in blocking task with size {}", self.name, src.len());
      tokio::task::block_in_place(|| resp2_decode_frame(&self, src))
    } else {
      resp2_decode_frame(&self, src)
    }
  }
}
