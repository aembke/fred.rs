use crate::error::{RedisError, RedisErrorKind};
use futures::Stream;
use glommio::{
  channels::local_channel::{LocalReceiver, LocalSender},
  GlommioError,
};
use std::{
  ops::Deref,
  pin::Pin,
  rc::Rc,
  task::{Context, Poll},
};

pub type UnboundedReceiver<T> = LocalReceiver<T>;

pub struct UnboundedReceiverStream<T> {
  rx: LocalReceiver<T>,
}

impl<T> From<LocalReceiver<T>> for UnboundedReceiverStream<T> {
  fn from(rx: LocalReceiver<T>) -> Self {
    UnboundedReceiverStream { rx }
  }
}

impl<T> UnboundedReceiverStream<T> {
  #[allow(dead_code)]
  pub async fn recv(&mut self) -> Option<T> {
    self.rx.recv().await
  }
}

impl<T> Stream for UnboundedReceiverStream<T> {
  type Item = T;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    use futures_lite::stream::StreamExt;

    // TODO make sure this is cancellation-safe. it's a bit unclear why the internal impl of ChannelStream does what
    // it does.
    self.rx.stream().poll_next(cx)
  }
}

pub struct UnboundedSender<T> {
  tx: Rc<LocalSender<T>>,
}

// https://github.com/rust-lang/rust/issues/26925
impl<T> Clone for UnboundedSender<T> {
  fn clone(&self) -> Self {
    UnboundedSender { tx: self.tx.clone() }
  }
}

impl<T> From<LocalSender<T>> for UnboundedSender<T> {
  fn from(tx: LocalSender<T>) -> Self {
    UnboundedSender { tx: Rc::new(tx) }
  }
}

impl<T> UnboundedSender<T> {
  pub fn try_send(&self, msg: T) -> Result<(), GlommioError<T>> {
    self.tx.try_send(msg)
  }

  pub fn send(&self, msg: T) -> Result<(), RedisError> {
    if let Err(_e) = self.tx.deref().try_send(msg) {
      // shouldn't happen since we use unbounded channels
      Err(RedisError::new(
        RedisErrorKind::Canceled,
        "Failed to send message on channel.",
      ))
    } else {
      Ok(())
    }
  }
}

pub fn rx_stream<T: 'static>(rx: LocalReceiver<T>) -> impl Stream<Item = T> + 'static {
  // what happens if we `join` the futures from `recv()` and `rx.stream().next()`?
  UnboundedReceiverStream::from(rx)
}
