use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Clone, Copy, PartialEq, Eq)]
enum WaitKind {
    Reader,
    Writer,
}

struct Waiter {
    kind: WaitKind,
    granted: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Waiter {
    fn new(kind: WaitKind) -> Arc<Waiter> {
        Arc::new(Waiter {
            kind,
            granted: AtomicBool::new(false),
            waker: Mutex::new(None),
        })
    }
}

struct State {
    readers: usize,
    writer: bool,
    waiting_writers: usize,
    queue: VecDeque<Arc<Waiter>>,
}

struct Inner<T: ?Sized> {
    state: Mutex<State>,
    data: UnsafeCell<T>,
}

// SAFETY: state serializes access to data and waiter ownership.
unsafe impl<T: Send + ?Sized> Send for Inner<T> {}
// SAFETY: shared access is synchronized by the state mutex.
unsafe impl<T: Send + Sync + ?Sized> Sync for Inner<T> {}

pub struct RwLock<T: ?Sized> {
    inner: Arc<Inner<T>>,
}

pub struct ReadGuard<'a, T: ?Sized> {
    inner: Arc<Inner<T>>,
    marker: std::marker::PhantomData<&'a T>,
}

pub struct WriteGuard<'a, T: ?Sized> {
    inner: Arc<Inner<T>>,
    marker: std::marker::PhantomData<&'a mut T>,
}

pub struct Read<'a, T: ?Sized> {
    inner: Arc<Inner<T>>,
    waiter: Arc<Waiter>,
    registered: bool,
    marker: std::marker::PhantomData<&'a RwLock<T>>,
}

pub struct Write<'a, T: ?Sized> {
    inner: Arc<Inner<T>>,
    waiter: Arc<Waiter>,
    registered: bool,
    marker: std::marker::PhantomData<&'a RwLock<T>>,
}

fn wake_waiter(waiter: &Arc<Waiter>) {
    if let Some(waker) = waiter.waker.lock().unwrap().take() {
        waker.wake();
    }
}

fn wake_readers_locked(state: &mut State) {
    while let Some(waiter) = state.queue.front() {
        if waiter.kind != WaitKind::Reader {
            break;
        }
        let waiter = state.queue.pop_front().unwrap();
        state.readers += 1;
        waiter.granted.store(true, Ordering::Release);
        wake_waiter(&waiter);
    }
}

fn wake_next_locked(state: &mut State) {
    if state.writer {
        return;
    }
    if state.readers != 0 {
        // A shared lock is held. Additional readers may join unless a writer
        // is next in line (write-preferring). This is also the downgrade path.
        if let Some(waiter) = state.queue.front() {
            if waiter.kind == WaitKind::Writer {
                return;
            }
        }
        wake_readers_locked(state);
        return;
    }
    if let Some(waiter) = state.queue.front() {
        if waiter.kind == WaitKind::Writer {
            let waiter = state.queue.pop_front().unwrap();
            state.waiting_writers -= 1;
            state.writer = true;
            waiter.granted.store(true, Ordering::Release);
            wake_waiter(&waiter);
            return;
        }
    }
    wake_readers_locked(state);
}

impl<T> RwLock<T> {
    pub fn new(value: T) -> RwLock<T> {
        RwLock {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    readers: 0,
                    writer: false,
                    waiting_writers: 0,
                    queue: VecDeque::new(),
                }),
                data: UnsafeCell::new(value),
            }),
        }
    }
}

impl<T: ?Sized> RwLock<T> {
    pub fn read(&self) -> Read<'_, T> {
        Read {
            inner: self.inner.clone(),
            waiter: Waiter::new(WaitKind::Reader),
            registered: false,
            marker: std::marker::PhantomData,
        }
    }

    pub fn write(&self) -> Write<'_, T> {
        Write {
            inner: self.inner.clone(),
            waiter: Waiter::new(WaitKind::Writer),
            registered: false,
            marker: std::marker::PhantomData,
        }
    }

    pub fn try_read(&self) -> Option<ReadGuard<'_, T>> {
        let mut state = self.inner.state.lock().unwrap();
        if state.writer || state.waiting_writers != 0 || !state.queue.is_empty() {
            return None;
        }
        state.readers += 1;
        Some(ReadGuard {
            inner: self.inner.clone(),
            marker: std::marker::PhantomData,
        })
    }

    pub fn try_write(&self) -> Option<WriteGuard<'_, T>> {
        let mut state = self.inner.state.lock().unwrap();
        if state.writer || state.readers != 0 || !state.queue.is_empty() {
            return None;
        }
        state.writer = true;
        Some(WriteGuard {
            inner: self.inner.clone(),
            marker: std::marker::PhantomData,
        })
    }

    pub fn get_mut(&mut self) -> &mut T {
        Arc::get_mut(&mut self.inner)
            .expect("eddy: RwLock has outstanding guards")
            .data
            .get_mut()
    }

    pub fn into_inner(self) -> T
    where
        T: Sized,
    {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner.data.into_inner(),
            Err(_) => unreachable!("eddy: RwLock still has guards"),
        }
    }
}

impl<'a, T: ?Sized> Future for Read<'a, T> {
    type Output = ReadGuard<'a, T>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: polling does not move the pinned future allocation.
        let this = unsafe { self.get_unchecked_mut() };
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        let mut state = this.inner.state.lock().unwrap();
        if this.waiter.granted.swap(false, Ordering::Acquire) {
            this.registered = false;
            return Poll::Ready(ReadGuard {
                inner: this.inner.clone(),
                marker: std::marker::PhantomData,
            });
        }
        if !this.registered && !state.writer && state.waiting_writers == 0 && state.queue.is_empty()
        {
            state.readers += 1;
            return Poll::Ready(ReadGuard {
                inner: this.inner.clone(),
                marker: std::marker::PhantomData,
            });
        }
        *this.waiter.waker.lock().unwrap() = Some(cx.waker().clone());
        if !this.registered {
            state.queue.push_back(this.waiter.clone());
            this.registered = true;
        }
        Poll::Pending
    }
}

impl<T: ?Sized> Drop for Read<'_, T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        if self.waiter.granted.swap(false, Ordering::Acquire) {
            state.readers -= 1;
            wake_next_locked(&mut state);
        } else if self.registered {
            state
                .queue
                .retain(|waiter| !Arc::ptr_eq(waiter, &self.waiter));
        }
    }
}

impl<'a, T: ?Sized> Future for Write<'a, T> {
    type Output = WriteGuard<'a, T>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: polling does not move the pinned future allocation.
        let this = unsafe { self.get_unchecked_mut() };
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        let mut state = this.inner.state.lock().unwrap();
        if this.waiter.granted.swap(false, Ordering::Acquire) {
            this.registered = false;
            return Poll::Ready(WriteGuard {
                inner: this.inner.clone(),
                marker: std::marker::PhantomData,
            });
        }
        if !this.registered && !state.writer && state.readers == 0 && state.queue.is_empty() {
            state.writer = true;
            return Poll::Ready(WriteGuard {
                inner: this.inner.clone(),
                marker: std::marker::PhantomData,
            });
        }
        *this.waiter.waker.lock().unwrap() = Some(cx.waker().clone());
        if !this.registered {
            state.waiting_writers += 1;
            state.queue.push_back(this.waiter.clone());
            this.registered = true;
        }
        Poll::Pending
    }
}

impl<T: ?Sized> Drop for Write<'_, T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        if self.waiter.granted.swap(false, Ordering::Acquire) {
            state.writer = false;
            wake_next_locked(&mut state);
        } else if self.registered {
            state
                .queue
                .retain(|waiter| !Arc::ptr_eq(waiter, &self.waiter));
            state.waiting_writers -= 1;
        }
    }
}

impl<T: ?Sized> Deref for ReadGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: readers may share immutable access.
        unsafe { &*self.inner.data.get() }
    }
}

impl<T: ?Sized> Deref for WriteGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // SAFETY: this guard owns the writer state.
        unsafe { &*self.inner.data.get() }
    }
}

impl<T: ?Sized> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        // SAFETY: this guard owns the writer state.
        unsafe { &mut *self.inner.data.get() }
    }
}

impl<T: ?Sized> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.readers -= 1;
        wake_next_locked(&mut state);
    }
}

impl<'a, T: ?Sized> WriteGuard<'a, T> {
    pub fn downgrade(self) -> ReadGuard<'a, T> {
        // SAFETY: `self` is consumed, so moving its Arc out before suppressing
        // Drop does not leave an aliased field behind.
        let inner = unsafe { std::ptr::read(&self.inner) };
        std::mem::forget(self);
        {
            let mut state = inner.state.lock().unwrap();
            state.writer = false;
            state.readers = 1;
            wake_next_locked(&mut state);
        }
        ReadGuard {
            inner,
            marker: std::marker::PhantomData,
        }
    }
}

impl<T: ?Sized> Drop for WriteGuard<'_, T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.writer = false;
        wake_next_locked(&mut state);
    }
}
