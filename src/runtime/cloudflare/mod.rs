pub(crate) mod interfaces;

pub(crate) mod compat {
    use crate::error::Error;
  use futures::Future;
  pub use oneshot::{channel as oneshot_channel, Receiver as OneshotReceiver, Sender as OneshotSender};
  use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::Duration,
  };
  use tokio::sync::mpsc::{
    channel as bounded_channel,
    error::{TryRecvError, TrySendError},
    unbounded_channel,
    Receiver as BoundedReceiver,
    Sender as BoundedSender,
    UnboundedReceiver,
    UnboundedSender,
  };
  use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
  use futures::{Stream, StreamExt};

  

  /// The reference counting container type.
  pub type RefCount<T> = Rc<T>;

  pub async fn sleep(duration: Duration) {
    worker::Delay::from(duration).await;
  }

  // --- Broadcast Channel ---
  struct BroadcastInner<T: Clone> {
    pub counter: u64,
    pub senders: BTreeMap<u64, UnboundedSender<T>>,
  }

  pub struct BroadcastReceiver<T: Clone> {
    id: u64,
    inner: Rc<RefCell<BroadcastInner<T>>>,
    rx: UnboundedReceiver<T>,
  }

  impl<T: Clone> BroadcastReceiver<T> {
    pub async fn recv(&mut self) -> Result<T, Error> {
      self.rx.recv().await.ok_or_else(Error::new_canceled)
    }
  }

  impl<T: Clone> Drop for BroadcastReceiver<T> {
    fn drop(&mut self) {
      self.inner.borrow_mut().senders.remove(&self.id);
    }
  }

  #[derive(Clone)]
  pub struct BroadcastSender<T: Clone> {
    inner: Rc<RefCell<BroadcastInner<T>>>,
  }

  impl<T: Clone> BroadcastSender<T> {
    pub fn new() -> Self {
      BroadcastSender {
        inner: Rc::new(RefCell::new(BroadcastInner {
          counter: 0,
          senders: BTreeMap::new(),
        })),
      }
    }

    pub fn subscribe(&self) -> BroadcastReceiver<T> {
      let (tx, rx) = unbounded_channel();
      let mut guard = self.inner.borrow_mut();
      let count = guard.counter.wrapping_add(1);
      guard.counter = count;
      guard.senders.insert(count, tx);
      BroadcastReceiver {
        id: count,
        inner: self.inner.clone(),
        rx,
      }
    }

    pub fn send<F: Fn(&T)>(&self, msg: &T, func: F) {
      let mut guard = self.inner.borrow_mut();
      let to_remove: Vec<u64> = guard
        .senders
        .iter()
        .filter_map(|(id, tx)| {
          if let Err(e) = tx.send(msg.clone()) {
            func(&e.0);
            Some(*id)
          } else {
            None
          }
        })
        .collect();
      for id in to_remove {
        guard.senders.remove(&id);
      }
    }
  }

  pub fn broadcast_send<T: Clone, F: Fn(&T)>(tx: &BroadcastSender<T>, msg: &T, func: F) {
    tx.send(msg, func);
  }

  pub fn broadcast_channel<T: Clone>(_: usize) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    let tx = BroadcastSender::new();
    let rx = tx.subscribe();
    (tx, rx)
  }

  // --- MPSC Channel ---
  enum SenderKind<T: 'static> {
    Bounded(BoundedSender<T>),
    Unbounded(UnboundedSender<T>),
  }

  impl<T: 'static> Clone for SenderKind<T> {
    fn clone(&self) -> Self {
      match self {
        SenderKind::Bounded(tx) => SenderKind::Bounded(tx.clone()),
        SenderKind::Unbounded(tx) => SenderKind::Unbounded(tx.clone()),
      }
    }
  }

  pub struct Sender<T: 'static> {
    tx: SenderKind<T>,
  }

  impl<T: 'static> Clone for Sender<T> {
    fn clone(&self) -> Self {
      Sender { tx: self.tx.clone() }
    }
  }

  impl<T: 'static> Sender<T> {
    pub async fn send(&self, val: T) -> Result<(), T> {
      match self.tx {
        SenderKind::Bounded(ref tx) => tx.send(val).await.map_err(|e| e.0),
        SenderKind::Unbounded(ref tx) => tx.send(val).map_err(|e| e.0),
      }
    }

    pub fn try_send(&self, val: T) -> Result<(), TrySendError<T>> {
      match self.tx {
        SenderKind::Bounded(ref tx) => tx.try_send(val),
        SenderKind::Unbounded(ref tx) => tx.send(val).map_err(|e| TrySendError::Closed(e.0)),
      }
    }
  }

  enum ReceiverKind<T: 'static> {
    Bounded(BoundedReceiver<T>),
    Unbounded(UnboundedReceiver<T>),
  }

  pub struct Receiver<T: 'static> {
    rx: ReceiverKind<T>,
  }

  impl<T: 'static> Receiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
      match self.rx {
        ReceiverKind::Bounded(ref mut tx) => tx.recv().await,
        ReceiverKind::Unbounded(ref mut tx) => tx.recv().await,
      }
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
      match self.rx {
        ReceiverKind::Bounded(ref mut tx) => tx.try_recv(),
        ReceiverKind::Unbounded(ref mut tx) => tx.try_recv(),
      }
    }

    pub fn into_stream(self) -> impl Stream<Item = T> + 'static {
      match self.rx {
        ReceiverKind::Bounded(rx) => ReceiverStream::new(rx).boxed_local(),
        ReceiverKind::Unbounded(rx) => UnboundedReceiverStream::new(rx).boxed_local(),
      }
    }
  }

  pub fn channel<T: 'static>(size: usize) -> (Sender<T>, Receiver<T>) {
    if size == 0 {
      let (tx, rx) = unbounded_channel();
      (
        Sender {
          tx: SenderKind::Unbounded(tx),
        },
        Receiver {
          rx: ReceiverKind::Unbounded(rx),
        },
      )
    } else {
      let (tx, rx) = bounded_channel(size);
      (
        Sender {
          tx: SenderKind::Bounded(tx),
        },
        Receiver {
          rx: ReceiverKind::Bounded(rx),
        },
      )
    }
  }

  // --- JoinHandle & Spawn ---
  pub struct JoinHandle<T> {
    rx: OneshotReceiver<T>,
    finished: Rc<Cell<bool>>,
  }

  pub fn spawn<T: 'static>(ft: impl Future<Output = T> + 'static) -> JoinHandle<T> {
    let (tx, rx) = oneshot_channel();
    let finished = Rc::new(Cell::new(false));
    let _finished = finished.clone();

    wasm_bindgen_futures::spawn_local(async move {
      let result = ft.await;
      _finished.replace(true);
      let _ = tx.send(result);
    });

    JoinHandle { rx, finished }
  }

  impl<T> Future for JoinHandle<T> {
    type Output = Result<T, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
      Pin::new(&mut self.rx)
        .poll(cx)
        .map_err(|_| Error::new_canceled())
    }
  }

  impl<T> JoinHandle<T> {
    pub fn is_finished(&self) -> bool {
      self.finished.get()
    }

    pub fn abort(&self) {
      self.finished.replace(true);
    }
  }

  // --- AsyncRwLock ---
  pub struct AsyncRwLock<T> {
    inner: Rc<RefCell<T>>,
  }

  impl<T> AsyncRwLock<T> {
    pub fn new(val: T) -> Self {
      AsyncRwLock {
        inner: Rc::new(RefCell::new(val)),
      }
    }

    pub async fn write(&self) -> std::cell::RefMut<'_, T> {
      self.inner.borrow_mut()
    }

    pub async fn read(&self) -> std::cell::Ref<'_, T> {
      self.inner.borrow()
    }
  }
}
