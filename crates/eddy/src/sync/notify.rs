use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Waiter {
    notified: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

struct State {
    permit: bool,
    waiters: VecDeque<Arc<Waiter>>,
}

struct Inner {
    state: Mutex<State>,
}

pub struct Notify {
    inner: Arc<Inner>,
}

pub struct Notified<'a> {
    inner: Arc<Inner>,
    waiter: Arc<Waiter>,
    registered: bool,
    marker: std::marker::PhantomData<&'a Notify>,
    _pin: std::marker::PhantomPinned,
}

impl Notify {
    pub fn new() -> Notify {
        Notify {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    permit: false,
                    waiters: VecDeque::new(),
                }),
            }),
        }
    }

    pub fn notified(&self) -> Notified<'_> {
        Notified {
            inner: self.inner.clone(),
            waiter: Arc::new(Waiter {
                notified: AtomicBool::new(false),
                waker: Mutex::new(None),
            }),
            registered: false,
            marker: std::marker::PhantomData,
            _pin: std::marker::PhantomPinned,
        }
    }

    pub fn notify_one(&self) {
        let waiter = {
            let mut state = self.inner.state.lock().unwrap();
            state
                .waiters
                .pop_front()
                .inspect(|waiter| waiter.notified.store(true, Ordering::Release))
                .or_else(|| {
                    state.permit = true;
                    None
                })
        };
        if let Some(waiter) = waiter {
            if let Some(waker) = waiter.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    pub fn notify_waiters(&self) {
        let waiters = {
            let mut state = self.inner.state.lock().unwrap();
            state
                .waiters
                .drain(..)
                .inspect(|waiter| waiter.notified.store(true, Ordering::Release))
                .collect::<Vec<_>>()
        };
        for waiter in waiters {
            if let Some(waker) = waiter.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

impl Notified<'_> {
    pub fn enable(self: Pin<&mut Self>) {
        // SAFETY: the future is pinned by the caller and only its fields are
        // mutated in place during registration.
        let this = unsafe { self.get_unchecked_mut() };
        let mut state = this.inner.state.lock().unwrap();
        if this.registered || this.waiter.notified.load(Ordering::Acquire) {
            return;
        }
        if state.permit {
            state.permit = false;
            this.waiter.notified.store(true, Ordering::Release);
            return;
        }
        state.waiters.push_back(this.waiter.clone());
        this.registered = true;
    }
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // SAFETY: polling does not move the pinned future allocation.
        let this = unsafe { self.get_unchecked_mut() };
        let mut state = this.inner.state.lock().unwrap();
        if this.waiter.notified.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        if !this.registered && state.permit {
            state.permit = false;
            this.waiter.notified.store(true, Ordering::Release);
            return Poll::Ready(());
        }
        *this.waiter.waker.lock().unwrap() = Some(cx.waker().clone());
        if !this.registered {
            state.waiters.push_back(this.waiter.clone());
            this.registered = true;
        }
        Poll::Pending
    }
}

impl Drop for Notified<'_> {
    fn drop(&mut self) {
        if !self.registered || self.waiter.notified.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.inner.state.lock().unwrap();
        state
            .waiters
            .retain(|waiter| !Arc::ptr_eq(waiter, &self.waiter));
    }
}
