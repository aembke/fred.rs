use crate::{
  error::RedisError,
  protocol::{connection::RedisConnection, types::Server},
  types::Resp3Frame,
};
use futures::stream::{Stream, StreamExt};
use futures_lite::StreamExt;
use std::{
  collections::HashMap,
  future::Future,
  pin::Pin,
  task::{Context, Poll},
};

/// Options describing how to change connections in a cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterChange {
  pub add:    Vec<Server>,
  pub remove: Vec<Server>,
}

impl Default for ClusterChange {
  fn default() -> Self {
    ClusterChange {
      add:    Vec::new(),
      remove: Vec::new(),
    }
  }
}

pub struct SelectReadAll<'a> {
  transports: &'a mut HashMap<Server, RedisConnection>,
}

impl SelectReadAll {
  pub fn new(transports: &mut HashMap<Server, RedisConnection>) -> Self {
    Self { transports }
  }
}

impl<'a> Future for SelectReadAll<'a> {
  type Output = Vec<(Server, Result<Resp3Frame, RedisError>)>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    if self.transports.is_empty() {
      return Poll::Ready(Vec::new());
    }

    // TODO do unresponsive checks
    // check and buffer any frames that are ready already
    let mut out = Vec::new();
    for (server, conn) in self.transports.iter_mut() {
      if let Poll::Ready(Some(frame)) = conn.transport.poll_next(cx) {
        out.push((server.clone(), frame.map(|f| f.into_resp3())));
      };
    }

    if out.is_empty() {
      Poll::Pending
    } else {
      Poll::Ready(out)
    }
  }
}
