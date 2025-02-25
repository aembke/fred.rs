use crate::{
  commands::command::{Command, CommandKind},
  error::{Error, ErrorKind},
  types::{
    events::{Message, MessageKind},
    values::{Key, Map, Value},
    Server,
    QUEUED,
  },
};
use bytes::Bytes;
use bytes_utils::Str;
use redis_protocol::{
  resp2::types::BytesFrame as Resp2Frame,
  resp3::types::{BytesFrame as Resp3Frame, Resp3Frame as _Resp3Frame, RespVersion},
  types::PUBSUB_PUSH_PREFIX,
};
use std::{borrow::Cow, collections::HashMap, ops::Deref, str};

pub static OK: &str = "OK";

/// Any kind of owned RESP frame.
#[derive(Debug, Clone)]
pub enum ProtocolFrame {
  Resp2(Resp2Frame),
  Resp3(Resp3Frame),
}

impl ProtocolFrame {
  /// Convert the frame to RESP3.
  pub fn into_resp3(self) -> Resp3Frame {
    // the `Value::convert` logic already accounts for different encodings of maps and sets, so
    // we can just change everything to RESP3 above the protocol layer
    match self {
      ProtocolFrame::Resp2(frame) => frame.into_resp3(),
      ProtocolFrame::Resp3(frame) => frame,
    }
  }

  /// Whether the frame is encoded as a RESP3 frame.
  pub fn is_resp3(&self) -> bool {
    matches!(*self, ProtocolFrame::Resp3(_))
  }
}

impl From<Resp2Frame> for ProtocolFrame {
  fn from(frame: Resp2Frame) -> Self {
    ProtocolFrame::Resp2(frame)
  }
}

impl From<Resp3Frame> for ProtocolFrame {
  fn from(frame: Resp3Frame) -> Self {
    ProtocolFrame::Resp3(frame)
  }
}

/// Parse the protocol frame into a redis value, with support for arbitrarily nested arrays.
pub fn frame_to_results(frame: Resp3Frame) -> Result<Value, Error> {
  let value = match frame {
    Resp3Frame::Null => Value::Null,
    Resp3Frame::SimpleString { data, .. } => {
      let value = string_or_bytes(data);

      if value.as_str().map(|s| s == QUEUED).unwrap_or(false) {
        Value::Queued
      } else {
        value
      }
    },
    Resp3Frame::SimpleError { data, .. } => return Err(pretty_error(&data)),
    Resp3Frame::BlobString { data, .. } => string_or_bytes(data),
    Resp3Frame::BlobError { data, .. } => {
      let parsed = String::from_utf8_lossy(&data);
      return Err(pretty_error(parsed.as_ref()));
    },
    Resp3Frame::VerbatimString { data, .. } => string_or_bytes(data),
    Resp3Frame::Number { data, .. } => data.into(),
    Resp3Frame::Double { data, .. } => data.into(),
    Resp3Frame::BigNumber { data, .. } => string_or_bytes(data),
    Resp3Frame::Boolean { data, .. } => data.into(),
    Resp3Frame::Array { data, .. } | Resp3Frame::Push { data, .. } => Value::Array(
      data
        .into_iter()
        .map(frame_to_results)
        .collect::<Result<Vec<Value>, _>>()?,
    ),
    Resp3Frame::Set { data, .. } => Value::Array(
      data
        .into_iter()
        .map(frame_to_results)
        .collect::<Result<Vec<Value>, _>>()?,
    ),
    Resp3Frame::Map { data, .. } => {
      let mut out = HashMap::with_capacity(data.len());
      for (key, value) in data.into_iter() {
        let key: Key = frame_to_results(key)?.try_into()?;
        let value = frame_to_results(value)?;

        out.insert(key, value);
      }

      Value::Map(Map { inner: out })
    },
    _ => return Err(Error::new(ErrorKind::Protocol, "Invalid response frame type.")),
  };

  Ok(value)
}

/// Convert a frame to a `Error`.
pub fn frame_to_error(frame: &Resp3Frame) -> Option<Error> {
  match frame {
    Resp3Frame::SimpleError { ref data, .. } => Some(pretty_error(data)),
    Resp3Frame::BlobError { ref data, .. } => {
      let parsed = String::from_utf8_lossy(data);
      Some(pretty_error(parsed.as_ref()))
    },
    _ => None,
  }
}

/// Check for the "OK" response or return an error.
pub fn expect_ok(value: &Value) -> Result<(), Error> {
  match *value {
    Value::String(ref resp) => {
      if resp.deref() == OK || resp.deref() == QUEUED {
        Ok(())
      } else {
        Err(Error::new(ErrorKind::Unknown, format!("Expected OK, found {}", resp)))
      }
    },
    _ => Err(Error::new(
      ErrorKind::Unknown,
      format!("Expected OK, found {:?}.", value),
    )),
  }
}

/// Convert the command to a RESP3 frame.
// TODO find a way to optimize these functions to use borrowed frame types
pub fn command_to_resp3_frame(command: &Command) -> Result<ProtocolFrame, Error> {
  let args = command.args();

  match command.kind {
    CommandKind::_Custom(ref kind) => {
      let parts: Vec<&str> = kind.cmd.trim().split(' ').collect();
      let mut bulk_strings = Vec::with_capacity(parts.len() + args.len());
      for part in parts.into_iter() {
        bulk_strings.push(Resp3Frame::BlobString {
          data:       part.as_bytes().to_vec().into(),
          attributes: None,
        });
      }
      for value in args.iter() {
        bulk_strings.push(value_to_outgoing_resp3_frame(value)?);
      }

      Ok(ProtocolFrame::Resp3(Resp3Frame::Array {
        data:       bulk_strings,
        attributes: None,
      }))
    },
    CommandKind::_HelloAllCluster(ref version) | CommandKind::_Hello(ref version) => {
      serialize_hello(command, version)
    },
    _ => {
      let mut bulk_strings = Vec::with_capacity(args.len() + 2);

      bulk_strings.push(Resp3Frame::BlobString {
        data:       command.kind.cmd_str().into_inner(),
        attributes: None,
      });

      if let Some(subcommand) = command.kind.subcommand_str() {
        bulk_strings.push(Resp3Frame::BlobString {
          data:       subcommand.into_inner(),
          attributes: None,
        });
      }
      for value in args.iter() {
        bulk_strings.push(value_to_outgoing_resp3_frame(value)?);
      }

      Ok(ProtocolFrame::Resp3(Resp3Frame::Array {
        data:       bulk_strings,
        attributes: None,
      }))
    },
  }
}

/// Convert the command to a RESP2 frame.
pub fn command_to_resp2_frame(command: &Command) -> Result<ProtocolFrame, Error> {
  let args = command.args();

  match command.kind {
    CommandKind::_Custom(ref kind) => {
      let parts: Vec<&str> = kind.cmd.trim().split(' ').collect();
      let mut bulk_strings = Vec::with_capacity(parts.len() + args.len());

      for part in parts.into_iter() {
        bulk_strings.push(Resp2Frame::BulkString(part.as_bytes().to_vec().into()));
      }
      for value in args.iter() {
        bulk_strings.push(value_to_outgoing_resp2_frame(value)?);
      }

      Ok(Resp2Frame::Array(bulk_strings).into())
    },
    _ => {
      let mut bulk_strings = Vec::with_capacity(args.len() + 2);

      bulk_strings.push(Resp2Frame::BulkString(command.kind.cmd_str().into_inner()));
      if let Some(subcommand) = command.kind.subcommand_str() {
        bulk_strings.push(Resp2Frame::BulkString(subcommand.into_inner()));
      }
      for value in args.iter() {
        bulk_strings.push(value_to_outgoing_resp2_frame(value)?);
      }

      Ok(Resp2Frame::Array(bulk_strings).into())
    },
  }
}

/// Serialize the command as a protocol frame.
pub fn command_to_frame(command: &Command, resp3: bool) -> Result<ProtocolFrame, Error> {
  if resp3 || command.kind.is_hello() {
    command_to_resp3_frame(command)
  } else {
    command_to_resp2_frame(command)
  }
}

/// Convert the HELLO command to a RESP3 frame.
pub fn serialize_hello(command: &Command, version: &RespVersion) -> Result<ProtocolFrame, Error> {
  let args = command.args();

  let (auth, setname) = if args.len() == 3 {
    // has auth and setname
    let username = match args[0].as_bytes_str() {
      Some(username) => username,
      None => {
        return Err(Error::new(
          ErrorKind::InvalidArgument,
          "Invalid username. Expected string.",
        ));
      },
    };
    let password = match args[1].as_bytes_str() {
      Some(password) => password,
      None => {
        return Err(Error::new(
          ErrorKind::InvalidArgument,
          "Invalid password. Expected string.",
        ));
      },
    };
    let name = match args[2].as_bytes_str() {
      Some(val) => val,
      None => {
        return Err(Error::new(
          ErrorKind::InvalidArgument,
          "Invalid setname value. Expected string.",
        ));
      },
    };

    (Some((username, password)), Some(name))
  } else if args.len() == 2 {
    // has auth but no setname
    let username = match args[0].as_bytes_str() {
      Some(username) => username,
      None => {
        return Err(Error::new(
          ErrorKind::InvalidArgument,
          "Invalid username. Expected string.",
        ));
      },
    };
    let password = match args[1].as_bytes_str() {
      Some(password) => password,
      None => {
        return Err(Error::new(
          ErrorKind::InvalidArgument,
          "Invalid password. Expected string.",
        ));
      },
    };

    (Some((username, password)), None)
  } else if args.len() == 1 {
    // has setname but no auth
    let name = match args[0].as_bytes_str() {
      Some(val) => val,
      None => {
        return Err(Error::new(
          ErrorKind::InvalidArgument,
          "Invalid setname value. Expected string.",
        ));
      },
    };

    (None, Some(name))
  } else {
    (None, None)
  };

  Ok(ProtocolFrame::Resp3(Resp3Frame::Hello {
    version: version.clone(),
    auth,
    setname,
  }))
}

/// Convert the value to an outgoing RESP2 frame.
pub fn value_to_outgoing_resp2_frame(value: &Value) -> Result<Resp2Frame, Error> {
  let frame = match value {
    Value::Double(ref f) => Resp2Frame::BulkString(f.to_string().into()),
    Value::Boolean(ref b) => Resp2Frame::BulkString(b.to_string().into()),
    // the `int_as_bulkstring` flag in redis-protocol converts this to a bulk string
    Value::Integer(ref i) => Resp2Frame::Integer(*i),
    Value::String(ref s) => Resp2Frame::BulkString(s.inner().clone()),
    Value::Bytes(ref b) => Resp2Frame::BulkString(b.clone()),
    Value::Queued => Resp2Frame::BulkString(Bytes::from_static(QUEUED.as_bytes())),
    Value::Null => Resp2Frame::Null,
    _ => {
      return Err(Error::new(
        ErrorKind::InvalidArgument,
        format!("Invalid argument type: {}", value.kind()),
      ))
    },
  };

  Ok(frame)
}

/// Convert the value to an outgoing RESP3 frame.
pub fn value_to_outgoing_resp3_frame(value: &Value) -> Result<Resp3Frame, Error> {
  let frame = match value {
    Value::Double(ref f) => Resp3Frame::BlobString {
      data:       f.to_string().into(),
      attributes: None,
    },
    Value::Boolean(ref b) => Resp3Frame::BlobString {
      data:       b.to_string().into(),
      attributes: None,
    },
    // the `int_as_blobstring` flag in redis-protocol converts this to a blob string
    Value::Integer(ref i) => Resp3Frame::Number {
      data:       *i,
      attributes: None,
    },
    Value::String(ref s) => Resp3Frame::BlobString {
      data:       s.inner().clone(),
      attributes: None,
    },
    Value::Bytes(ref b) => Resp3Frame::BlobString {
      data:       b.clone(),
      attributes: None,
    },
    Value::Queued => Resp3Frame::BlobString {
      data:       Bytes::from_static(QUEUED.as_bytes()),
      attributes: None,
    },
    Value::Null => Resp3Frame::Null,
    _ => {
      return Err(Error::new(
        ErrorKind::InvalidArgument,
        format!("Invalid argument type: {}", value.kind()),
      ))
    },
  };

  Ok(frame)
}

/// Convert the value to mocked frame.
#[cfg(feature = "mocks")]
pub fn mocked_value_to_frame(value: Value) -> Resp3Frame {
  match value {
    Value::Array(values) => Resp3Frame::Array {
      data:       values.into_iter().map(mocked_value_to_frame).collect(),
      attributes: None,
    },
    Value::Map(values) => Resp3Frame::Map {
      data:       values
        .inner()
        .into_iter()
        .map(|(key, value)| (mocked_value_to_frame(key.into()), mocked_value_to_frame(value)))
        .collect(),
      attributes: None,
    },
    Value::Null => Resp3Frame::Null,
    Value::Queued => Resp3Frame::SimpleString {
      data:       Bytes::from_static(QUEUED.as_bytes()),
      attributes: None,
    },
    Value::Bytes(value) => Resp3Frame::BlobString {
      data:       value,
      attributes: None,
    },
    Value::Boolean(value) => Resp3Frame::Boolean {
      data:       value,
      attributes: None,
    },
    Value::Integer(value) => Resp3Frame::Number {
      data:       value,
      attributes: None,
    },
    Value::Double(value) => Resp3Frame::Double {
      data:       value,
      attributes: None,
    },
    Value::String(value) => Resp3Frame::BlobString {
      data:       value.into_inner(),
      attributes: None,
    },
  }
}

/// Extract the inner bytes in the frame.
pub fn frame_to_bytes(frame: Resp3Frame) -> Option<Bytes> {
  match frame {
    Resp3Frame::BigNumber { data, .. } => Some(data),
    Resp3Frame::VerbatimString { data, .. } => Some(data),
    Resp3Frame::BlobString { data, .. } => Some(data),
    Resp3Frame::SimpleString { data, .. } => Some(data),
    Resp3Frame::BlobError { data, .. } => Some(data),
    Resp3Frame::SimpleError { data, .. } => Some(data.into_inner()),
    _ => None,
  }
}

/// Extract the inner bytes and attempt to convert them to a string.
pub fn frame_to_str(frame: Resp3Frame) -> Option<Str> {
  frame_to_bytes(frame).and_then(|b| Str::from_inner(b).ok())
}

/// Try to parse the data as a string, and failing that return a byte slice.
pub fn string_or_bytes(data: Bytes) -> Value {
  // avoid cloning or copying the buffer
  if str::from_utf8(&data).is_ok() {
    Value::String(unsafe { Str::from_inner_unchecked(data) })
  } else {
    Value::Bytes(data)
  }
}

/// Parse the error contents into an `Error`.
pub fn pretty_error(resp: &str) -> Error {
  let kind = {
    let mut parts = resp.split_whitespace();

    match parts.next().unwrap_or("") {
      "" => ErrorKind::Unknown,
      "ERR" => {
        if resp.contains("instance has cluster support disabled") {
          // Cluster client connecting to non-cluster server.
          // Returning Config to signal no reconnect will help.
          ErrorKind::Config
        } else {
          ErrorKind::Unknown
        }
      },
      "WRONGTYPE" => ErrorKind::InvalidArgument,
      "NOAUTH" | "WRONGPASS" => ErrorKind::Auth,
      "MOVED" | "ASK" | "CLUSTERDOWN" => ErrorKind::Cluster,
      "Invalid" => match parts.next().unwrap_or("") {
        "argument(s)" | "Argument" => ErrorKind::InvalidArgument,
        "command" | "Command" => ErrorKind::InvalidCommand,
        _ => ErrorKind::Unknown,
      },
      _ => ErrorKind::Unknown,
    }
  };

  let details = if resp.is_empty() {
    Cow::Borrowed("No response!")
  } else {
    Cow::Owned(resp.to_owned())
  };
  Error::new(kind, details)
}

/// Parse the frame as a string, without support for error frames.
pub fn frame_into_string(frame: Resp3Frame) -> Result<String, Error> {
  match frame {
    Resp3Frame::SimpleString { data, .. } => Ok(String::from_utf8(data.to_vec())?),
    Resp3Frame::BlobString { data, .. } => Ok(String::from_utf8(data.to_vec())?),
    Resp3Frame::Double { data, .. } => Ok(data.to_string()),
    Resp3Frame::Number { data, .. } => Ok(data.to_string()),
    Resp3Frame::Boolean { data, .. } => Ok(data.to_string()),
    Resp3Frame::VerbatimString { data, .. } => Ok(String::from_utf8(data.to_vec())?),
    Resp3Frame::BigNumber { data, .. } => Ok(String::from_utf8(data.to_vec())?),
    Resp3Frame::SimpleError { data, .. } => Err(pretty_error(&data)),
    Resp3Frame::BlobError { data, .. } => Err(pretty_error(str::from_utf8(&data)?)),
    _ => Err(Error::new(ErrorKind::Protocol, "Expected string.")),
  }
}

/// Parse the kind of pubsub message (pattern, sharded, or regular).
pub fn parse_message_kind(frame: &Resp3Frame) -> Result<MessageKind, Error> {
  let frames = match frame {
    Resp3Frame::Array { ref data, .. } => data,
    Resp3Frame::Push { ref data, .. } => data,
    _ => return Err(Error::new(ErrorKind::Protocol, "Invalid pubsub frame type.")),
  };

  let parsed = if frames.len() == 3 {
    // resp2 format, normal message
    frames[0].as_str().and_then(MessageKind::from_str)
  } else if frames.len() == 4 {
    // resp3 normal message or resp2 pattern/shard message
    frames[1]
      .as_str()
      .and_then(MessageKind::from_str)
      .or(frames[0].as_str().and_then(MessageKind::from_str))
  } else if frames.len() == 5 {
    // resp3 pattern or shard message
    frames[1]
      .as_str()
      .and_then(MessageKind::from_str)
      .or(frames[2].as_str().and_then(MessageKind::from_str))
  } else {
    None
  };

  parsed.ok_or(Error::new(ErrorKind::Protocol, "Invalid pubsub message kind."))
}

/// Parse the channel and value fields from a pubsub frame.
pub fn parse_message_fields(frame: Resp3Frame) -> Result<(Str, Value), Error> {
  let mut frames = match frame {
    Resp3Frame::Array { data, .. } => data,
    Resp3Frame::Push { data, .. } => data,
    _ => return Err(Error::new(ErrorKind::Protocol, "Invalid pubsub frame type.")),
  };

  let value = frames
    .pop()
    .ok_or(Error::new(ErrorKind::Protocol, "Invalid pubsub message."))?;
  let channel = frames
    .pop()
    .ok_or(Error::new(ErrorKind::Protocol, "Invalid pubsub channel."))?;
  let channel = frame_to_str(channel).ok_or(Error::new(ErrorKind::Protocol, "Failed to parse channel."))?;
  let value = frame_to_results(value)?;

  Ok((channel, value))
}

/// Parse the frame from a shard pubsub channel.
// TODO clean this up with the v5 redis_protocol interface
pub fn parse_shard_pubsub_frame(server: &Server, frame: &Resp3Frame) -> Option<Message> {
  let value = match frame {
    Resp3Frame::Array { ref data, .. } | Resp3Frame::Push { ref data, .. } => {
      if data.len() >= 3 && data.len() <= 5 {
        // check both resp2 and resp3 formats
        let has_either_prefix = (data[0].as_str().map(|s| s == PUBSUB_PUSH_PREFIX).unwrap_or(false)
          && data[1].as_str().map(|s| s == "smessage").unwrap_or(false))
          || (data[0].as_str().map(|s| s == "smessage").unwrap_or(false));

        if has_either_prefix {
          let channel = match frame_to_str(data[data.len() - 2].clone()) {
            Some(channel) => channel,
            None => return None,
          };
          let message = match frame_to_results(data[data.len() - 1].clone()) {
            Ok(message) => message,
            Err(_) => return None,
          };

          Some((channel, message))
        } else {
          None
        }
      } else {
        None
      }
    },
    _ => None,
  };

  value.map(|(channel, value)| Message {
    channel,
    value,
    kind: MessageKind::SMessage,
    server: server.clone(),
  })
}

/// Parse the frame as a pubsub message.
pub fn frame_to_pubsub(server: &Server, frame: Resp3Frame) -> Result<Message, Error> {
  if let Some(message) = parse_shard_pubsub_frame(server, &frame) {
    return Ok(message);
  }

  let kind = parse_message_kind(&frame)?;
  let (channel, value) = parse_message_fields(frame)?;

  Ok(Message {
    kind,
    channel,
    value,
    server: server.clone(),
  })
}
