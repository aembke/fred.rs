use crate::types::Server;
use bytes_utils::Str;
use tracing::trace;

/// The kind of pubsub message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MessageKind {
  /// A message from a `subscribe` command.
  Message,
  /// A message from a pattern `psubscribe` command.
  PMessage,
  /// A message from a sharded `ssubscribe` command.
  SMessage,
}

impl MessageKind {
  pub(crate) fn from_str(s: &str) -> Option<MessageKind> {
    Some(match s {
      "message" => MessageKind::Message,
      "pmessage" => MessageKind::PMessage,
      "smessage" => MessageKind::SMessage,
      _ => return None,
    })
  }
}

/// A [publish-subscribe](https://redis.io/docs/manual/pubsub/) message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
  /// The channel on which the message was sent.
  pub channel: Str,
  /// The message contents.
  pub value:   Value,
  /// The type of message subscription.
  pub kind:    MessageKind,
  /// The server that sent the message.
  pub server:  Server,
}

/// An enum describing the possible ways in which a Redis cluster can change state.
///
/// See [on_cluster_change](crate::interfaces::EventInterface::on_cluster_change) for more information.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterStateChange {
  /// A node was added to the cluster.
  ///
  /// This implies that hash slots were also probably rebalanced.
  Add(Server),
  /// A node was removed from the cluster.
  ///
  /// This implies that hash slots were also probably rebalanced.
  Remove(Server),
  /// Hash slots were rebalanced across the cluster and/or local routing state was updated.
  Rebalance,
}

/// An event on the publish-subscribe interface describing a keyspace notification.
///
/// <https://redis.io/topics/notifications>
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct KeyspaceEvent {
  pub db:        u8,
  pub operation: String,
  pub key:       Key,
}

/// A [client tracking](https://redis.io/docs/manual/client-side-caching/) invalidation message from the provided server.
#[cfg(feature = "i-tracking")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-tracking")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Invalidation {
  pub keys:   Vec<Key>,
  pub server: Server,
}

#[cfg(feature = "i-tracking")]
#[cfg_attr(docsrs, doc(cfg(feature = "i-tracking")))]
impl Invalidation {
  pub(crate) fn from_message(message: Message, server: &Server) -> Option<Invalidation> {
    Some(Invalidation {
      keys:   match message.value {
        Value::Array(values) => values.into_iter().filter_map(|v| v.try_into().ok()).collect(),
        Value::String(s) => vec![s.into()],
        Value::Bytes(b) => vec![b.into()],
        Value::Double(f) => vec![f.into()],
        Value::Integer(i) => vec![i.into()],
        Value::Boolean(b) => vec![b.into()],
        Value::Null => vec![],
        _ => {
          trace!("Dropping invalid invalidation message.");
          return None;
        },
      },
      server: server.clone(),
    })
  }
}
