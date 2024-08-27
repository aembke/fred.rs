use futures::Stream;
use tokio::sync::broadcast::{Receiver, Sender};
pub use tokio::{
  spawn,
  sync::{
    broadcast::{self, error::SendError as BroadcastSendError},
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot::{channel as oneshot_channel, Receiver as OneshotReceiver, Sender as OneshotSender},
    RwLock as AsyncRwLock,
  },
  task::JoinHandle,
  time::sleep,
};
use tokio_stream::wrappers::UnboundedReceiverStream;

pub type BroadcastSender<T> = Sender<T>;
pub type BroadcastReceiver<T> = Receiver<T>;

pub fn broadcast_send<T: Clone, F: Fn(&T)>(tx: &BroadcastSender<T>, msg: &T, func: F) {
  if let Err(BroadcastSendError(val)) = tx.send(msg.clone()) {
    func(&val);
  }
}

pub fn broadcast_channel<T: Clone>(capacity: usize) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
  broadcast::channel(capacity)
}

pub fn rx_stream<T>(rx: UnboundedReceiver<T>) -> impl Stream<Item = T> {
  UnboundedReceiverStream::new(rx)
}
