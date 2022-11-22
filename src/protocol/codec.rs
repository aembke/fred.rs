use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{types::ProtocolFrame, utils as protocol_utils},
  utils,
};
use arcstr::ArcStr;
use bytes::BytesMut;
use redis_protocol::{
  resp2::{decode::decode_mut as resp2_decode, encode::encode_bytes as resp2_encode, types::Frame as Resp2Frame},
  resp3::{
    decode::streaming::decode_mut as resp3_decode,
    encode::complete::encode_bytes as resp3_encode,
    types::{Frame as Resp3Frame, StreamedFrame},
  },
};
use std::sync::{atomic::AtomicBool, Arc};
use tokio_util::codec::{Decoder, Encoder};

#[cfg(feature = "blocking-encoding")]
use crate::globals::globals;
#[cfg(feature = "metrics")]
use crate::modules::metrics::MovingStats;
#[cfg(feature = "metrics")]
use parking_lot::RwLock;

#[cfg(not(feature = "network-logs"))]
fn log_resp2_frame(_: &str, _: &Resp2Frame, _: bool) {}
#[cfg(not(feature = "network-logs"))]
fn log_resp3_frame(_: &str, _: &Resp3Frame, _: bool) {}
#[cfg(feature = "network-logs")]
pub use crate::protocol::debug::log_resp2_frame;
#[cfg(feature = "network-logs")]
pub use crate::protocol::debug::log_resp3_frame;
use crate::protocol::types::Server;

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
    "{}: Encoded {} bytes to {}. Buffer len: {} (RESP2)",
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
  trace!(
    "{}: Recv {} bytes from {} (RESP2).",
    codec.name,
    src.len(),
    codec.server
  );
  if src.is_empty() {
    return Ok(None);
  }

  if let Some((frame, amt, _)) = resp2_decode(src)? {
    trace!("{}: Parsed {} bytes from {}", codec.name, amt, codec.server);
    log_resp2_frame(&codec.name, &frame, false);
    sample_stats(&codec, true, amt as i64);

    Ok(Some(protocol_utils::check_resp2_auth_error(frame)))
  } else {
    Ok(None)
  }
}

fn resp3_encode_frame(codec: &RedisCodec, item: Resp3Frame, dst: &mut BytesMut) -> Result<(), RedisError> {
  let offset = dst.len();

  let res = resp3_encode(dst, &item)?;
  let len = res.saturating_sub(offset);

  trace!(
    "{}: Encoded {} bytes to {}. Buffer len: {} (RESP3)",
    codec.name,
    len,
    codec.server,
    res
  );
  log_resp3_frame(&codec.name, &item, true);
  sample_stats(&codec, false, len as i64);

  Ok(())
}

fn resp3_decode_frame(codec: &mut RedisCodec, src: &mut BytesMut) -> Result<Option<Resp3Frame>, RedisError> {
  trace!(
    "{}: Recv {} bytes from {} (RESP3).",
    codec.name,
    src.len(),
    codec.server
  );
  if src.is_empty() {
    return Ok(None);
  }

  if let Some((frame, amt, _)) = resp3_decode(src)? {
    sample_stats(&codec, true, amt as i64);

    if codec.streaming_state.is_some() && frame.is_streaming() {
      return Err(RedisError::new(
        RedisErrorKind::Protocol,
        "Cannot start a stream while already inside a stream.",
      ));
    }

    let result = if let Some(ref mut streamed_frame) = codec.streaming_state {
      // we started receiving streamed data earlier
      let frame = frame.into_complete_frame()?;
      streamed_frame.add_frame(frame);

      if streamed_frame.is_finished() {
        let frame = streamed_frame.into_frame()?;
        trace!("{}: Ending {:?} stream", codec.name, frame.kind());
        log_resp3_frame(&codec.name, &frame, false);
        Some(frame)
      } else {
        trace!("{}: Continuing {:?} stream", codec.name, streamed_frame.kind);
        None
      }
    } else {
      // we're processing a complete frame or starting a new streamed frame
      if frame.is_streaming() {
        let frame = frame.into_streaming_frame()?;
        trace!("{}: Starting {:?} stream", codec.name, frame.kind);
        codec.streaming_state = Some(frame);
        None
      } else {
        // we're not in the middle of a stream and we found a complete frame
        let frame = frame.into_complete_frame()?;
        log_resp3_frame(&codec.name, &frame, false);
        Some(protocol_utils::check_resp3_auth_error(frame))
      }
    };

    if result.is_some() {
      let _ = codec.streaming_state.take();
    }
    Ok(result)
  } else {
    Ok(None)
  }
}

/// Attempt to decode with RESP2, and if that fails try once with RESP3.
///
/// This is useful when handling HELLO commands sent in the middle of a RESP2 command sequence.
fn resp2_decode_with_fallback(
  codec: &mut RedisCodec,
  src: &mut BytesMut,
) -> Result<Option<ProtocolFrame>, RedisError> {
  let resp2_result = resp2_decode_frame(codec, src).map(|f| f.map(|f| f.into()));
  if resp2_result.is_err() {
    let resp3_result = resp3_decode_frame(codec, src).map(|f| f.map(|f| f.into()));
    if resp3_result.is_ok() {
      resp3_result
    } else {
      resp2_result
    }
  } else {
    resp2_result
  }
}

pub struct RedisCodec {
  pub name:            ArcStr,
  pub server:          Server,
  pub resp3:           Arc<AtomicBool>,
  pub streaming_state: Option<StreamedFrame>,
  #[cfg(feature = "metrics")]
  pub req_size_stats:  Arc<RwLock<MovingStats>>,
  #[cfg(feature = "metrics")]
  pub res_size_stats:  Arc<RwLock<MovingStats>>,
}

impl RedisCodec {
  pub fn new(inner: &Arc<RedisClientInner>, server: &Server) -> Self {
    RedisCodec {
      server:                                     server.clone(),
      name:                                       inner.id.clone(),
      resp3:                                      inner.shared_resp3(),
      streaming_state:                            None,
      #[cfg(feature = "metrics")]
      req_size_stats:                             inner.req_size_stats.clone(),
      #[cfg(feature = "metrics")]
      res_size_stats:                             inner.res_size_stats.clone(),
    }
  }

  pub fn is_resp3(&self) -> bool {
    utils::read_bool_atomic(&self.resp3)
  }
}

impl Encoder<ProtocolFrame> for RedisCodec {
  type Error = RedisError;

  #[cfg(not(feature = "blocking-encoding"))]
  fn encode(&mut self, item: ProtocolFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
    match item {
      ProtocolFrame::Resp2(frame) => resp2_encode_frame(&self, frame, dst),
      ProtocolFrame::Resp3(frame) => resp3_encode_frame(&self, frame, dst),
    }
  }

  #[cfg(feature = "blocking-encoding")]
  fn encode(&mut self, item: ProtocolFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
    let frame_size = protocol_utils::frame_size(&item);

    if frame_size >= globals().blocking_encode_threshold() {
      trace!("{}: Encoding in blocking task with size {}", self.name, frame_size);

      tokio::task::block_in_place(|| match item {
        ProtocolFrame::Resp2(frame) => resp2_encode_frame(&self, frame, dst),
        ProtocolFrame::Resp3(frame) => resp3_encode_frame(&self, frame, dst),
      })
    } else {
      match item {
        ProtocolFrame::Resp2(frame) => resp2_encode_frame(&self, frame, dst),
        ProtocolFrame::Resp3(frame) => resp3_encode_frame(&self, frame, dst),
      }
    }
  }
}

impl Decoder for RedisCodec {
  type Error = RedisError;
  type Item = ProtocolFrame;

  #[cfg(not(feature = "blocking-encoding"))]
  fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    if self.is_resp3() {
      resp3_decode_frame(self, src).map(|f| f.map(|f| f.into()))
    } else {
      resp2_decode_with_fallback(self, src)
    }
  }

  #[cfg(feature = "blocking-encoding")]
  fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    if src.len() >= globals().blocking_encode_threshold() {
      trace!("{}: Decoding in blocking task with size {}", self.name, src.len());

      tokio::task::block_in_place(|| {
        if self.is_resp3() {
          resp3_decode_frame(self, src).map(|f| f.map(|f| f.into()))
        } else {
          resp2_decode_with_fallback(self, src)
        }
      })
    } else {
      if self.is_resp3() {
        resp3_decode_frame(self, src).map(|f| f.map(|f| f.into()))
      } else {
        resp2_decode_with_fallback(self, src)
      }
    }
  }
}
