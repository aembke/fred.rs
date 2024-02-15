use crate::error::{RedisError, RedisErrorKind};
use bytes::BytesMut;
use redis_protocol::{
  resp2::{decode::decode_mut as resp2_decode, encode::encode_bytes as resp2_encode},
  resp3::{
    decode::streaming::decode_mut as resp3_decode,
    encode::complete::encode_bytes as resp3_encode,
    types::StreamedFrame,
  },
};
use tokio_util::codec::{Decoder, Encoder};

pub use redis_protocol::{
  redis_keyslot,
  resp2::types::{Frame as Resp2Frame, FrameKind as Resp2FrameKind},
  resp2_frame_to_resp3,
  resp3::types::{Auth, Frame as Resp3Frame, FrameKind as Resp3FrameKind, RespVersion},
};

/// Encode a redis command string (`SET foo bar NX`, etc) into a RESP3 blob string array.
pub fn resp3_encode_command(cmd: &str) -> Resp3Frame {
  Resp3Frame::Array {
    data:       cmd
      .split(' ')
      .map(|s| Resp3Frame::BlobString {
        data:       s.as_bytes().to_vec().into(),
        attributes: None,
      })
      .collect(),
    attributes: None,
  }
}

/// Encode a redis command string (`SET foo bar NX`, etc) into a RESP2 bulk string array.
pub fn resp2_encode_command(cmd: &str) -> Resp2Frame {
  Resp2Frame::Array(
    cmd
      .split(' ')
      .map(|s| Resp2Frame::BulkString(s.as_bytes().to_vec().into()))
      .collect(),
  )
}

/// A framed RESP2 codec.
///
/// ```rust
/// use fred::{
///   codec::{resp2_encode_command, Resp2, Resp2Frame},
///   prelude::*,
/// };
/// use futures::{SinkExt, StreamExt};
/// use tokio::net::TcpStream;
/// use tokio_util::codec::Framed;
///
/// async fn example() -> Result<(), RedisError> {
///   let socket = TcpStream::connect("127.0.0.1:6379").await?;
///   let mut framed = Framed::new(socket, Resp2::default());
///
///   let auth = resp2_encode_command("AUTH foo bar");
///   let get_foo = resp2_encode_command("GET foo");
///
///   let _ = framed.send(auth).await?;
///   let response = framed.next().await.unwrap().unwrap();
///   assert_eq!(response.as_str().unwrap(), "OK");
///
///   let _ = framed.send(get_foo).await?;
///   let response = framed.next().await.unwrap().unwrap();
///   assert_eq!(response, Resp2Frame::Null);
///
///   Ok(())
/// }
/// ```
#[derive(Default)]
pub struct Resp2;

impl Encoder<Resp2Frame> for Resp2 {
  type Error = RedisError;

  fn encode(&mut self, item: Resp2Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
    #[cfg(feature = "network-logs")]
    trace!("RESP2 codec encode: {:?}", item);

    resp2_encode(dst, &item).map(|_| ()).map_err(RedisError::from)
  }
}

impl Decoder for Resp2 {
  type Error = RedisError;
  type Item = Resp2Frame;

  fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    if src.is_empty() {
      return Ok(None);
    }
    let parsed = match resp2_decode(src)? {
      Some((frame, _, _)) => frame,
      None => return Ok(None),
    };
    #[cfg(feature = "network-logs")]
    trace!("RESP2 codec decode: {:?}", parsed);

    Ok(Some(parsed))
  }
}

/// A framed codec for complete and streaming/chunked RESP3 frames with optional attributes.
///
/// ```rust
/// use fred::{
///   codec::{resp3_encode_command, Auth, Resp3, Resp3Frame, RespVersion},
///   prelude::*,
/// };
/// use futures::{SinkExt, StreamExt};
/// use tokio::net::TcpStream;
/// use tokio_util::codec::Framed;
///
/// // send `HELLO 3 AUTH foo bar` then `GET foo`
/// async fn example() -> Result<(), RedisError> {
///   let socket = TcpStream::connect("127.0.0.1:6379").await?;
///   let mut framed = Framed::new(socket, Resp3::default());
///
///   let hello = Resp3Frame::Hello {
///     version: RespVersion::RESP3,
///     auth:    Some(Auth {
///       username: "foo".into(),
///       password: "bar".into(),
///     }),
///   };
///   // or use the shorthand, but this likely only works for simple use cases
///   let get_foo = resp3_encode_command("GET foo");
///
///   // `Framed` implements both `Sink` and `Stream`
///   let _ = framed.send(hello).await?;
///   let response = framed.next().await;
///   println!("HELLO response: {:?}", response);
///
///   let _ = framed.send(get_foo).await?;
///   let response = framed.next().await;
///   println!("GET foo: {:?}", response);
///
///   Ok(())
/// }
/// ```
#[derive(Default)]
pub struct Resp3 {
  streaming: Option<StreamedFrame>,
}

impl Encoder<Resp3Frame> for Resp3 {
  type Error = RedisError;

  fn encode(&mut self, item: Resp3Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
    #[cfg(feature = "network-logs")]
    trace!("RESP3 codec encode: {:?}", item);

    resp3_encode(dst, &item).map(|_| ()).map_err(RedisError::from)
  }
}

impl Decoder for Resp3 {
  type Error = RedisError;
  type Item = Resp3Frame;

  // FIXME ideally this would refer to the corresponding fn in codec.rs, but that code is too tightly coupled to the
  // private inner interface to expose here
  fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    if src.is_empty() {
      return Ok(None);
    }
    let parsed = match resp3_decode(src)? {
      Some((f, _, _)) => f,
      None => return Ok(None),
    };

    if self.streaming.is_some() && parsed.is_streaming() {
      return Err(RedisError::new(
        RedisErrorKind::Protocol,
        "Cannot start a stream while already inside a stream.",
      ));
    }

    let result = if let Some(ref mut state) = self.streaming {
      // we started receiving streamed data earlier
      state.add_frame(parsed.into_complete_frame()?);

      if state.is_finished() {
        Some(state.into_frame()?)
      } else {
        None
      }
    } else {
      // we're processing a complete frame or starting a new streamed frame
      if parsed.is_streaming() {
        self.streaming = Some(parsed.into_streaming_frame()?);
        None
      } else {
        // we're not in the middle of a stream and we found a complete frame
        Some(parsed.into_complete_frame()?)
      }
    };

    if result.is_some() {
      let _ = self.streaming.take();
    }

    #[cfg(feature = "network-logs")]
    trace!("RESP3 codec decode: {:?}", result);
    Ok(result)
  }
}
