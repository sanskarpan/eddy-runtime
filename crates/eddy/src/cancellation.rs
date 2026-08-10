use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

struct Inner {
    cancelled: AtomicBool,
    /// Registered `Cancelled` waiters as `(slot id, waker)` pairs. Each
    /// waiting future owns one slot and removes it on drop, so the list is
    /// bounded by the number of live waiters rather than the number of polls.
    waiters: Mutex<Vec<(usize, Waker)>>,
    /// Monotonically increasing slot ids; never reused, so a drained entry
    /// can never be confused with a later registration.
    next_waiter_id: AtomicUsize,
    children: Mutex<Vec<Weak<Inner>>>,
}

pub struct CancellationToken {
    inner: Arc<Inner>,
}

pub struct Cancelled<'a> {
    token: &'a CancellationToken,
    /// This future's slot id in `Inner::waiters`, if registered.
    waiter_id: Option<usize>,
}

impl CancellationToken {
    pub fn new() -> CancellationToken {
        CancellationToken {
            inner: Arc::new(Inner {
                cancelled: AtomicBool::new(false),
                waiters: Mutex::new(Vec::new()),
                next_waiter_id: AtomicUsize::new(0),
                children: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn cancel(&self) {
        if self.inner.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        let (waiters, children) = {
            let mut waiters = self.inner.waiters.lock().unwrap();
            let waiters = std::mem::take(&mut *waiters);
            let children = std::mem::take(&mut *self.inner.children.lock().unwrap());
            (waiters, children)
        };
        for (_, waker) in waiters {
            waker.wake();
        }
        for child in children {
            if let Some(child) = child.upgrade() {
                let token = CancellationToken { inner: child };
                token.cancel();
            }
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    pub fn cancelled(&self) -> Cancelled<'_> {
        Cancelled {
            token: self,
            waiter_id: None,
        }
    }

    pub fn child_token(&self) -> CancellationToken {
        let child = CancellationToken::new();
        if self.is_cancelled() {
            child.cancel();
        } else {
            self.inner
                .children
                .lock()
                .unwrap()
                .push(Arc::downgrade(&child.inner));
            if self.is_cancelled() {
                child.cancel();
            }
        }
        child
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl Future for Cancelled<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.token.is_cancelled() {
            return Poll::Ready(());
        }
        {
            let mut waiters = this.token.inner.waiters.lock().unwrap();
            match this.waiter_id {
                Some(waiter_id) => {
                    if let Some((_, waker)) = waiters.iter_mut().find(|(id, _)| *id == waiter_id) {
                        if !waker.will_wake(cx.waker()) {
                            *waker = cx.waker().clone();
                        }
                    } else {
                        // `cancel` drained the list while we were registered;
                        // re-register so the re-check below cannot lose us.
                        let id = this
                            .token
                            .inner
                            .next_waiter_id
                            .fetch_add(1, Ordering::SeqCst);
                        waiters.push((id, cx.waker().clone()));
                        this.waiter_id = Some(id);
                    }
                }
                None => {
                    let id = this
                        .token
                        .inner
                        .next_waiter_id
                        .fetch_add(1, Ordering::SeqCst);
                    waiters.push((id, cx.waker().clone()));
                    this.waiter_id = Some(id);
                }
            }
            if this.token.is_cancelled() {
                cx.waker().wake_by_ref();
            }
        }
        Poll::Pending
    }
}

impl Drop for Cancelled<'_> {
    fn drop(&mut self) {
        if let Some(waiter_id) = self.waiter_id.take() {
            let mut waiters = self.token.inner.waiters.lock().unwrap();
            if let Some(position) = waiters.iter().position(|(id, _)| *id == waiter_id) {
                waiters.swap_remove(position);
            }
        }
    }
}
