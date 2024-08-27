// add a connectionConfig field for task queue, use for tcp connections
// spawn local for the connect() task

// glommio's network interfaces use AsyncRead and AsyncWrite from futures_io, but tokio re-implements these traits and
// uses their own impl in the tokio_util::codec interface. this means we have to reimplement the codec traits
// (Encoder+Decoder) here, but with the futures_io version of AsyncRead and AsyncWrite

#[cfg(all(feature = "glommio", feature = "blocking-encoding"))]
compile_error!("Cannot use glommio and blocking-encoding features together.");
#[cfg(all(feature = "glommio", feature = "unix-sockets"))]
compile_error!("Cannot use glommio and unix-sockets features together.");

// TODO add `AtomicBool` replacement type that doesn't use atomics
pub(crate) mod broadcast;
pub(crate) mod interfaces;
pub(crate) mod io_compat;

pub(crate) mod compat {
  pub use super::broadcast::{BroadcastReceiver, BroadcastSender};
  use crate::error::RedisError;
  pub use async_oneshot::{oneshot as oneshot_channel, Receiver as OneshotReceiver, Sender as OneshotSender};
  use futures::Stream;
  pub use glommio::{
    channels::local_channel::new_unbounded as unbounded_channel,
    task::JoinHandle as GlommioJoinHandle,
    timer::sleep,
    GlommioError as BroadcastSendError,
  };
  use glommio::{
    channels::local_channel::{LocalReceiver, LocalSender},
    sync::{RwLock, RwLockWriteGuard},
    TaskQueueHandle,
  };
  use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
  };

  pub type RefCount<T> = Rc<T>;
  pub type UnboundedSender<T> = LocalSender<T>;
  pub type UnboundedReceiver<T> = LocalReceiver<T>;

  pub fn broadcast_send<T, F: Fn(&T)>(tx: &BroadcastSender<T>, msg: &T, func: F) {
    tx.send(msg, func);
  }

  pub fn broadcast_channel<T>(_: usize) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
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

  pub fn spawn_into<T: 'static>(ft: impl Future<Output = T> + 'static, tq: TaskQueueHandle) -> JoinHandle<T> {
    let finished = Rc::new(RefCell::new(false));
    let _finished = finished.clone();
    let inner = glommio::spawn_local_into(
      async move {
        let result = ft.await;
        _finished.replace(true);
        result
      },
      tq,
    )
    .detach();

    JoinHandle { inner, finished }
  }

  impl<T> Future for JoinHandle<T> {
    type Output = Result<T, RedisError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
      let result = self
        .inner
        .poll(cx)
        .map(|result| result.unwrap_or(RedisError::new_canceled()));

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

  pub fn rx_stream<T>(rx: UnboundedReceiver<T>) -> impl Stream<Item = T> {
    rx.stream()
  }
}
