// add a connectionConfig field for task queue, use for tcp connections
// spawn local for the connect() task

// glommio's network interfaces use AsyncRead and AsyncWrite from futures_io, but tokio re-implements these traits and
// uses their own impl in the tokio_util::codec interface. this means we have to reimplement the codec traits
// (Encoder+Decoder) here, but with the futures_io version of AsyncRead and AsyncWrite

#[cfg(all(feature = "glommio", feature = "blocking-encoding"))]
compile_error!("Cannot use glommio and blocking-encoding features together.");
#[cfg(all(feature = "glommio", feature = "unix-sockets"))]
compile_error!("Cannot use glommio and unix-sockets features together.");

use crate::{error::RedisError, modules::inner::RedisClientInner, types::ConnectHandle, utils};
use glommio::task::JoinHandle;
use std::{rc::Rc, sync::Arc};
// TODO add `AtomicBool` replacement type that doesn't use atomics

pub(crate) mod broadcast;
pub(crate) mod interfaces;
pub(crate) mod io_compat;

pub(crate) mod runtime_compat {
  pub use super::broadcast::{BroadcastReceiver, BroadcastSender};
  pub use async_oneshot::{oneshot as oneshot_channel, Receiver as OneshotReceiver, Sender as OneshotSender};
  pub use glommio::{
    channels::local_channel::new_unbounded as unbounded_channel,
    task::JoinHandle,
    timer::sleep,
    GlommioError as BroadcastSendError,
  };
  use glommio::{
    channels::local_channel::{LocalReceiver, LocalSender},
    sync::{RwLock, RwLockWriteGuard},
  };
  use std::rc::Rc;

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
}
