// add a connectionConfig field for task queue, use for tcp connections
// spawn local for the connect() task

// glommio's network interfaces use AsyncRead and AsyncWrite from futures_io, but tokio re-implements these traits and
// uses their own impl in the tokio_util::codec interface. this means we have to reimplement the codec traits
// (Encoder+Decoder) here, but with the futures_io version of AsyncRead and AsyncWrite

use futures::{Future, FutureExt};

#[cfg(all(feature = "glommio", feature = "unix-sockets"))]
compile_error!("Cannot use glommio and unix-sockets features together.");

// TODO add `AtomicBool` replacement type that doesn't use atomics
pub(crate) mod broadcast;
pub(crate) mod interfaces;
pub(crate) mod io_compat;

pub(crate) mod compat {
  pub use super::broadcast::{BroadcastReceiver, BroadcastSender};
  use crate::{error::RedisError, prelude::RedisErrorKind};
  pub use async_oneshot::{oneshot as oneshot_channel, Receiver as OneshotReceiver, Sender as OneshotSender};
  use futures::{stream::StreamExt, Stream};
  pub use glommio::{
    channels::local_channel::new_unbounded as unbounded_channel,
    task::JoinHandle as GlommioJoinHandle,
    timer::sleep,
    GlommioError as BroadcastSendError,
  };
  use glommio::{
    channels::local_channel::{LocalReceiver, LocalSender},
    sync::{RwLock, RwLockWriteGuard},
    GlommioError,
    TaskQueueHandle,
  };
  use std::{
    cell::{Ref, RefCell},
    fmt::Debug,
    future::Future,
    mem,
    ops::Deref,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
  };

  pub type RefCount<T> = Rc<T>;
  pub type UnboundedReceiver<T> = LocalReceiver<T>;

  pub fn broadcast_send<T: Clone, F: Fn(&T)>(tx: &BroadcastSender<T>, msg: &T, func: F) {
    tx.send(msg, func);
  }

  pub fn broadcast_channel<T: Clone>(_: usize) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    let tx = BroadcastSender::new();
    let rx = tx.subscribe();
    (tx, rx)
  }

  pub struct AsyncRwLock<T> {
    inner: RwLock<T>,
  }

  impl<T> AsyncRwLock<T> {
    pub fn new(val: T) -> Self {
      AsyncRwLock {
        inner: RwLock::new(val),
      }
    }

    pub async fn write(&self) -> RwLockWriteGuard<T> {
      self.inner.write().await.unwrap()
    }
  }

  /// A wrapper type around [JoinHandle](glommio::task::JoinHandle) with an interface similar to Tokio's
  /// [JoinHandle](tokio::task::JoinHandle)
  pub struct JoinHandle<T> {
    pub(crate) inner:    GlommioJoinHandle<T>,
    pub(crate) finished: Rc<RefCell<bool>>,
  }

  pub fn spawn<T: 'static>(ft: impl Future<Output = T> + 'static) -> JoinHandle<T> {
    let finished = Rc::new(RefCell::new(false));
    let _finished = finished.clone();
    let inner = glommio::spawn_local(async move {
      let result = ft.await;
      _finished.replace(true);
      result
    })
    .detach();

    JoinHandle { inner, finished }
  }

  pub fn spawn_into<T: 'static>(
    ft: impl Future<Output = T> + 'static,
    tq: TaskQueueHandle,
  ) -> Result<JoinHandle<T>, RedisError> {
    let finished = Rc::new(RefCell::new(false));
    let _finished = finished.clone();
    let inner = glommio::spawn_local_into(
      async move {
        let result = ft.await;
        _finished.replace(true);
        result
      },
      tq,
    )?
    .detach();

    Ok(JoinHandle { inner, finished })
  }

  impl<T> Future for JoinHandle<T> {
    type Output = Result<T, RedisError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
      let result = self
        .inner
        .poll(cx)
        .map(|result| result.ok_or(RedisError::new_canceled()));

      if Poll::Ready(_) = result {
        self.set_finished();
      }
      result
    }
  }

  impl<T> JoinHandle<T> {
    pub(crate) fn set_finished(&self) {
      self.finished.replace(true);
    }

    pub fn is_finished(&self) -> bool {
      *self.finished.as_ref().borrow()
    }

    pub fn abort(&self) {
      self.inner.cancel();
      self.set_finished();
    }
  }

  pub fn rx_stream<T: 'static>(rx: LocalReceiver<T>) -> impl Stream<Item = T> + 'static {
    // what happens if we `join` the futures from `recv()` and `rx.stream().next()`?
    UnboundedReceiverStream::from(rx)
  }

  pub struct UnboundedReceiverStream<T> {
    rx: LocalReceiver<T>,
  }

  impl<T> From<LocalReceiver<T>> for UnboundedReceiverStream<T> {
    fn from(rx: LocalReceiver<T>) -> Self {
      UnboundedReceiverStream { rx }
    }
  }

  impl<T> UnboundedReceiverStream<T> {
    pub async fn recv(&mut self) -> Option<T> {
      self.rx.recv().await
    }
  }

  impl<T> Stream for UnboundedReceiverStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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

  pub struct RefSwap<T> {
    inner: RefCell<T>,
  }

  impl<T> RefSwap<T> {
    pub fn new(val: T) -> Self {
      RefSwap {
        inner: RefCell::new(val),
      }
    }

    pub fn swap(&self, other: T) -> T {
      mem::replace(&mut self.inner.borrow_mut(), other)
    }

    pub fn store(&self, other: T) {
      self.swap(other);
    }

    pub fn load(&self) -> Ref<'_, T> {
      self.inner.borrow()
    }
  }
}
