use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::loom::sync::atomic::{AtomicBool, Ordering};
use crate::loom::sync::{Arc, Mutex};

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

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;

    #[test]
    fn notify_one_racing_with_registration_is_never_lost() {
        // The permit is the whole point of `Notify`: whether the notify lands
        // before the waiter registers (permit consumed later) or after (waiter
        // woken directly), the future must resolve.
        loom::model(|| {
            let notify: crate::loom::sync::Arc<Notify> = crate::loom::sync::Arc::new(Notify::new());
            let notifier = {
                let notify = notify.clone();
                loom::thread::spawn(move || {
                    notify.notify_one();
                })
            };
            loom::future::block_on(notify.notified());
            notifier.join().unwrap();

            // A second notify now must park as a permit, not vanish — a later
            // waiter must still be woken.
            notify.notify_one();
            loom::future::block_on(notify.notified());
        });
    }

    #[test]
    fn notify_one_wakes_a_registered_waiter() {
        // The inverse of the permit race: the waiter registers first (its
        // initial poll returns Pending) and the notify that lands afterwards
        // must wake it directly rather than being parked as a permit that the
        // already-registered future would never consume.
        loom::model(|| {
            let notify: crate::loom::sync::Arc<Notify> = crate::loom::sync::Arc::new(Notify::new());
            let mut notified = Box::pin(notify.notified());
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            // Register the waiter before any notify can exist; the first poll
            // must be Pending (the permit path is its own race, exercised by
            // the test above).
            assert!(notified.as_mut().poll(&mut cx).is_pending());
            // The notify that now lands must resolve the registered waiter.
            let notifier = {
                let notify = notify.clone();
                loom::thread::spawn(move || {
                    notify.notify_one();
                })
            };
            loom::future::block_on(notified);
            notifier.join().unwrap();
        });
    }
}
