use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

struct Inner {
    cancelled: AtomicBool,
    waiters: Mutex<Vec<Waker>>,
    children: Mutex<Vec<Weak<Inner>>>,
}

pub struct CancellationToken {
    inner: Arc<Inner>,
}

pub struct Cancelled<'a> {
    token: &'a CancellationToken,
}

impl CancellationToken {
    pub fn new() -> CancellationToken {
        CancellationToken {
            inner: Arc::new(Inner {
                cancelled: AtomicBool::new(false),
                waiters: Mutex::new(Vec::new()),
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
        for waker in waiters {
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
        Cancelled { token: self }
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
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }
        self.token
            .inner
            .waiters
            .lock()
            .unwrap()
            .push(cx.waker().clone());
        if self.token.is_cancelled() {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}
