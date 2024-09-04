#[cfg(all(feature = "glommio", feature = "unix-sockets"))]
compile_error!("Cannot use glommio and unix-sockets features together.");

pub(crate) mod broadcast;
pub(crate) mod interfaces;
pub(crate) mod io_compat;
pub(crate) mod mpsc;
pub(crate) mod sync;

pub(crate) mod compat {
  pub use super::{
    broadcast::{BroadcastReceiver, BroadcastSender},
    mpsc::{rx_stream, UnboundedReceiver, UnboundedSender},
    sync::*,
  };
  use crate::error::RedisError;
  use futures::Future;
  use glommio::TaskQueueHandle;
  pub use glommio::{
    channels::local_channel::new_unbounded as unbounded_channel,
    task::JoinHandle as GlommioJoinHandle,
    timer::sleep,
  };
  pub use oneshot::{channel as oneshot_channel, Receiver as OneshotReceiver, Sender as OneshotSender};
  use std::{
    cell::RefCell,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
  };

  /// The reference counting container type.
  ///
  /// This type may change based on the runtime feature flags used.
  pub type RefCount<T> = Rc<T>;

  pub fn broadcast_send<T: Clone, F: Fn(&T)>(tx: &BroadcastSender<T>, msg: &T, func: F) {
    tx.send(msg, func);
  }

  pub fn broadcast_channel<T: Clone>(_: usize) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    let tx = BroadcastSender::new();
    let rx = tx.subscribe();
    (tx, rx)
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
    .unwrap_or_else(|e| panic!("Failed to spawn task into task queue {tq:?}: {e:?}"))
    .detach();

    JoinHandle { inner, finished }
  }

  impl<T> Future for JoinHandle<T> {
    type Output = Result<T, RedisError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
      use futures_lite::FutureExt;

      let finished = self.finished.clone();
      let result = self
        .get_mut()
        .inner
        .poll(cx)
        .map(|result| result.ok_or(RedisError::new_canceled()));

      if let Poll::Ready(_) = result {
        finished.replace(true);
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
}
