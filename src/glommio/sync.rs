use std::{
  cell::{Ref, RefCell, RefMut},
  fmt,
  mem,
  sync::atomic::Ordering,
};

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

pub struct AsyncRwLock<T> {
  inner: glommio::sync::RwLock<T>,
}

impl<T> AsyncRwLock<T> {
  pub fn new(val: T) -> Self {
    AsyncRwLock {
      inner: glommio::sync::RwLock::new(val),
    }
  }

  pub async fn write(&self) -> glommio::sync::RwLockWriteGuard<T> {
    self.inner.write().await.unwrap()
  }

  pub async fn read(&self) -> glommio::sync::RwLockReadGuard<T> {
    self.inner.read().await.unwrap()
  }
}

#[derive(Debug)]
pub struct AtomicUsize {
  inner: RefCell<usize>,
}

impl AtomicUsize {
  pub fn new(val: usize) -> Self {
    AtomicUsize {
      inner: RefCell::new(val),
    }
  }

  pub fn fetch_add(&self, val: usize, _: Ordering) -> usize {
    let mut guard = self.inner.borrow_mut();

    let new = guard.saturating_add(val);
    *guard = new;
    new
  }

  pub fn fetch_sub(&self, val: usize, _: Ordering) -> usize {
    let mut guard = self.inner.borrow_mut();

    let new = guard.saturating_sub(val);
    *guard = new;
    new
  }

  pub fn load(&self, _: Ordering) -> usize {
    *self.inner.borrow()
  }

  pub fn swap(&self, val: usize, _: Ordering) -> usize {
    let mut guard = self.inner.borrow_mut();
    let old = *guard;
    *guard = val;
    old
  }
}

#[derive(Debug)]
pub struct AtomicBool {
  inner: RefCell<bool>,
}

impl AtomicBool {
  pub fn new(val: bool) -> Self {
    AtomicBool {
      inner: RefCell::new(val),
    }
  }

  pub fn load(&self, _: Ordering) -> bool {
    *self.inner.borrow()
  }

  pub fn swap(&self, val: bool, _: Ordering) -> bool {
    let mut guard = self.inner.borrow_mut();
    let old = *guard;
    *guard = val;
    old
  }
}

pub type MutexGuard<'a, T> = RefMut<'a, T>;

pub struct Mutex<T> {
  inner: RefCell<T>,
}

impl<T: fmt::Debug> fmt::Debug for Mutex<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:?}", self.inner)
  }
}

impl<T> Mutex<T> {
  pub fn new(val: T) -> Self {
    Mutex {
      inner: RefCell::new(val),
    }
  }

  pub fn lock(&self) -> MutexGuard<T> {
    self.inner.borrow_mut()
  }
}

pub type RwLockReadGuard<'a, T> = Ref<'a, T>;
pub type RwLockWriteGuard<'a, T> = RefMut<'a, T>;

pub struct RwLock<T> {
  inner: RefCell<T>,
}

impl<T: fmt::Debug> fmt::Debug for RwLock<T> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:?}", self.inner)
  }
}

impl<T> RwLock<T> {
  pub fn new(val: T) -> Self {
    RwLock {
      inner: RefCell::new(val),
    }
  }

  pub fn read(&self) -> RwLockReadGuard<T> {
    self.inner.borrow()
  }

  pub fn write(&self) -> RwLockWriteGuard<T> {
    self.inner.borrow_mut()
  }
}
