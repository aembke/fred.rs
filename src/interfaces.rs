use crate::error::RedisError;
use crate::modules::inner::RedisClientInner;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::task::JoinHandle;

// This is a strange file.
//
// The motivation for these abstractions comes from the fact that the Redis interface is huge, and managing it all
// in one `RedisClient` struck is becoming unwieldy and unsustainable. Additionally, there are now use cases for
// supporting different client structs that implement subsets of the Redis interface where some of the underlying
// command implementations should be shared across multiple different client structs.
//
// For example, `SentinelClient` supports ACL commands and pubsub commands like a `RedisClient`, but also implements
// its own unique interface for sentinel nodes. However, it does *not* support zset commands, cluster commands, etc.
// In the future I would like the flexibility to add a `PubsubClient` for example, where it can share the same pubsub
// command interface as a `RedisClient`, but with some added features to manage subscriptions across reconnections
// automatically for callers.
//
// In an ideal world we could implement this with a trait for each section of the Redis interface. For example we
// could have an `AclInterface`, `ZsetInterface`, `HashInterface`, etc, where each interface implemented the command
// functions for that portion of the Redis interface. Since everything in this module revolves around a
// `RedisClientInner` this would be easy to do with a super trait that implemented a function to return this inner
// struct (`fn inner(&self) -> &Arc<RedisClientInner>`).
//
// However, async functions are not supported in traits, which makes this much more difficult, and if we don't find
// some way to use traits to reuse portions of the Redis interface then we'll end up duplicating a lot of code, or
// relying on `Deref`, which has downsides in that it doesn't let you remove certain sections of the underlying interface.
//
// The abstractions implemented here are necessary because of this lack of async functions in traits. The
// `async-trait` crate exists, but it reverts to using trait objects (a la futures 0.1 pre `impl Trait`) and I'm
// not a fan of how it messes with your function signatures and uses boxes all over the place.
//
// Additionally, this module already spawns a tokio task for commands, so we can get around the lack of async
// functions in traits simply by moving the current `tokio::spawn` call up the stack a bit, which can turn an async
// function into a "sync" function that returns a Future, which is really the same thing. This is pretty much
// what `async-trait` does under the hood (minus the spawn), but instead of using a trait object we just return
// a struct that we define here which implements the `Future` trait.
//
// We'll call this file "some cost abstractions" until async functions in traits are available since it does require
// one more `Arc` clone than it otherwise would with async functions in traits. Once that feature is stable I'll come
// back and remove this to avoid the added `Arc` clone used here (which is required to `move` the inner struct into the
// new tokio task).

/// An enum used to represent the return value from a function that does some fallible synchronous work,
/// followed by some more fallible async logic inside a new tokio task.
enum AsyncInner<T: Unpin + Send + 'static> {
  Result(Option<Result<T, RedisError>>),
  Task(JoinHandle<Result<T, RedisError>>),
}

/// A wrapper type for return values from async functions implemented in a trait.
pub struct AsyncResult<T: Unpin + Send + 'static> {
  inner: AsyncInner<T>,
}

impl<T, E> From<Result<T, E>> for AsyncResult<T>
where
  T: Unpin + Send + 'static,
  E: Into<RedisError>,
{
  fn from(value: Result<T, E>) -> Self {
    AsyncResult {
      inner: AsyncInner::Result(Some(value.map_err(|e| e.into()))),
    }
  }
}

impl<T> From<JoinHandle<Result<T, RedisError>>> for AsyncResult<T>
where
  T: Unpin + Send + 'static,
{
  fn from(value: JoinHandle<Result<T, RedisError>>) -> Self {
    AsyncResult {
      inner: AsyncInner::Task(value),
    }
  }
}

impl<T> Future for AsyncResult<T>
where
  T: Unpin + Send + 'static,
{
  type Output = Result<T, RedisError>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    match self.get_mut().inner {
      AsyncInner::Result(ref mut output) => {
        if let Some(value) = output.take() {
          Poll::Ready(value)
        } else {
          error!("Tried calling poll on an AsyncResult::Result more than once.");
          Poll::Ready(Err(RedisError::new_canceled()))
        }
      }
      AsyncInner::Task(ref mut handle) => match Pin::new(handle).poll(cx) {
        Poll::Ready(value) => match value {
          Ok(value) => Poll::Ready(value),
          Err(e) => Poll::Ready(Err(e.into())),
        },
        Poll::Pending => Poll::Pending,
      },
    }
  }
}

pub(crate) fn async_spawn<C, F, Fut, T>(client: &C, func: F) -> AsyncResult<T>
where
  C: ClientLike,
  Fut: Future<Output = Result<T, RedisError>> + Send + 'static,
  F: FnOnce(Arc<RedisClientInner>) -> Fut,
  T: Unpin + Send + 'static,
{
  // this is unfortunate but necessary without async functions in traits
  let inner = client.inner().clone();
  tokio::spawn(func(inner)).into()
}

#[doc(hidden)]
pub trait ClientLike {
  fn inner(&self) -> &Arc<RedisClientInner>;
}

pub use crate::commands::acl_interface::AclInterface;
