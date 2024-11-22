use crate::{
  error::{RedisError, RedisErrorKind},
  modules::inner::RedisClientInner,
  protocol::{connection::RedisConnection, types::Server},
  runtime::RefCount,
  types::Resp3Frame,
};
use futures::stream::Stream;
use std::{
  collections::HashMap,
  future::Future,
  pin::Pin,
  task::{Context, Poll},
  time::Instant,
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

/// A future that reads from all connections and performs unresponsive checks.
// `poll_next` on a Framed<TcpStream> is not cancel-safe
pub struct ReadAllFuture<'a, 'b> {
  transports: &'a mut HashMap<Server, RedisConnection>,
  inner:      &'b RefCount<RedisClientInner>,
}

impl ReadAllFuture {
  pub fn new(inner: &RefCount<RedisClientInner>, transports: &mut HashMap<Server, RedisConnection>) -> Self {
    Self { transports, inner }
  }
}

impl<'a, 'b> Future for ReadAllFuture<'a, 'b> {
  type Output = Vec<(Server, Option<Result<Resp3Frame, RedisError>>)>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    if self.transports.is_empty() {
      return Poll::Ready(Vec::new());
    }

    // check and buffer any frames that are ready already
    let now = Instant::now();
    let mut out = Vec::new();
    for (server, conn) in self.transports.iter_mut() {
      match conn.transport.poll_next(cx) {
        Poll::Ready(Some(frame)) => {
          out.push((server.clone(), Some(frame.map(|f| f.into_resp3()))));
        },
        Poll::Ready(None) => {
          out.push((server.clone(), None));
        },
        Poll::Pending => {
          if let Some(duration) = self.inner.connection.unresponsive.max_timeout {
            if let Some(last_write) = conn.last_write {
              if now.saturating_duration_since(last_write) > duration {
                out.push((
                  server.clone(),
                  Some(Err(RedisError::new(RedisErrorKind::IO, "Unresponsive connection."))),
                ));
              }
            }
          }
        },
      };
    }

    if out.is_empty() {
      Poll::Pending
    } else {
      Poll::Ready(out)
    }
  }
}

/// A future that reads from the connection and performs unresponsive checks.
pub struct ReadFuture<'a, 'b> {
  connection: &'a mut RedisConnection,
  inner:      &'b RefCount<RedisClientInner>,
}

impl ReadFuture {
  pub fn new(inner: &RefCount<RedisClientInner>, connection: &mut RedisConnection) -> Self {
    Self { connection, inner }
  }
}

impl<'a, 'b> Future for ReadFuture<'a, 'b> {
  type Output = Vec<(Server, Option<Result<Resp3Frame, RedisError>>)>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    match self.connection.transport.poll_next(cx) {
      Poll::Ready(Some(result)) => Poll::Ready(vec![(
        self.connection.server.clone(),
        Some(result.map(|f| f.into_resp3())),
      )]),
      Poll::Ready(None) => Poll::Ready(vec![(self.connection.server.clone(), None)]),
      Poll::Pending => {
        if let Some(duration) = self.inner.connection.unresponsive.max_timeout {
          if let Some(last_write) = self.connection.last_write {
            if Instant::now().duration_since(last_write) > duration {
              return Poll::Ready(vec![(
                self.connection.server.clone(),
                Err(RedisError::new(RedisErrorKind::IO, "Unresponsive connection.")),
              )]);
            }
          }
        }

        Poll::Pending
      },
    }
  }
}
