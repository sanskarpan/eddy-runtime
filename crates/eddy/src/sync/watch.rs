//! Watch channel: a single writer publishes the latest value, readers observe
//! every change. Unlike [`crate::sync::broadcast`] there is no buffering — a
//! reader always sees the newest value, and `changed().await` resolves once a
//! new value arrives after the reader's last seen version.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner<T> {
    state: Mutex<State<T>>,
}

struct State<T> {
    value: T,
    version: u64,
    wakers: Vec<Waker>,
}

#[derive(Clone)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> std::fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    /// The last version this receiver has observed (`changed()` compares
    /// against it, so a `notify` before any read still triggers).
    version: u64,
}

impl<T> std::fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("watch channel is closed")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

/// The error from [`Receiver::changed`]: the sender was dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("watch sender was dropped")
    }
}

impl std::error::Error for RecvError {}

/// Create a watch channel holding `init` as the initial value.
pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        state: Mutex::new(State {
            value: init,
            version: 0,
            wakers: Vec::new(),
        }),
    });
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner, version: 0 },
    )
}

impl<T> Sender<T> {
    /// Publish a new value and wake every waiting reader.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let wakers = {
            let mut state = self.inner.state.lock().unwrap();
            state.value = value;
            state.version = state.version.wrapping_add(1);
            std::mem::take(&mut state.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
        Ok(())
    }

    pub fn send_replace(&self, value: T) -> T {
        let mut state = self.inner.state.lock().unwrap();
        let previous = std::mem::replace(&mut state.value, value);
        state.version = state.version.wrapping_add(1);
        let wakers = std::mem::take(&mut state.wakers);
        drop(state);
        for waker in wakers {
            waker.wake();
        }
        previous
    }

    pub fn borrow(&self) -> T
    where
        T: Clone,
    {
        self.inner.state.lock().unwrap().value.clone()
    }

    pub fn has_changed(&self, receiver: &Receiver<T>) -> bool {
        let state = self.inner.state.lock().unwrap();
        state.version != receiver.version
    }

    /// The version currently published.
    pub fn version(&self) -> u64 {
        self.inner.state.lock().unwrap().version
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let wakers = {
            let mut state = self.inner.state.lock().unwrap();
            state.wakers.drain(..).collect::<Vec<_>>()
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

impl<T> Receiver<T> {
    /// The latest value, cloned, without advancing the observed version — a
    /// `changed()` after this still fires for the current value.
    pub fn borrow(&self) -> T
    where
        T: Clone,
    {
        self.inner.state.lock().unwrap().value.clone()
    }

    /// The latest value, cloned, and mark the current version as observed.
    pub fn borrow_and_update(&mut self) -> T
    where
        T: Clone,
    {
        let state = self.inner.state.lock().unwrap();
        let value = state.value.clone();
        self.version = state.version;
        value
    }

    pub fn has_changed(&self) -> bool {
        let state = self.inner.state.lock().unwrap();
        state.version != self.version
    }

    /// Wait until a new value is published after the last observed version.
    /// Resolves with [`RecvError`] when the sender is dropped.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        std::future::poll_fn(|cx| self.poll_changed(cx)).await
    }

    fn poll_changed(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), RecvError>> {
        let mut state = self.inner.state.lock().unwrap();
        if state.version != self.version {
            // A new value arrived since the last observed version.
            return Poll::Ready(Ok(()));
        }
        if Arc::strong_count(&self.inner) == 1 {
            // Only the receiver holds the inner Arc: the sender is gone.
            return Poll::Ready(Err(RecvError));
        }
        state.wakers.push(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<(), RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll_changed(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_value_is_immediately_available() {
        let (tx, rx) = channel(41);
        assert_eq!(rx.borrow(), 41);
        assert_eq!(tx.borrow(), 41);
    }

    #[test]
    fn send_updates_value_and_version() {
        let (tx, rx) = channel(1);
        tx.send(2).unwrap();
        assert_eq!(rx.borrow(), 2);
        assert!(rx.has_changed());
    }

    #[test]
    fn borrow_does_not_consume_the_change() {
        let (tx, mut rx) = channel(1);
        tx.send(2).unwrap();
        assert_eq!(rx.borrow(), 2);
        // `changed()` must still fire for version 2 (tokio semantics:
        // borrow_and_update is the only way to consume it).
        assert!(rx.has_changed());
        assert_eq!(rx.borrow_and_update(), 2);
        assert!(!rx.has_changed());
    }

    #[test]
    fn sender_drop_closes_the_channel() {
        let (tx, rx) = channel(1);
        drop(tx);
        assert_eq!(rx.borrow(), 1);
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut rx = std::pin::pin!(rx);
        assert!(matches!(
            rx.as_mut().poll(&mut cx),
            Poll::Ready(Err(RecvError))
        ));
    }

    #[cfg(not(loom))]
    #[test]
    fn changed_waits_for_next_value() {
        let rt = crate::runtime::Builder::new_current_thread().build();
        rt.block_on(async {
            let (tx, mut rx) = channel(0);
            let sender = crate::runtime::Handle::current().spawn(async move {
                crate::time::sleep(std::time::Duration::from_millis(5)).await;
                tx.send(7).unwrap();
            });
            rx.changed().await.expect("not closed");
            sender.await.unwrap();
            assert_eq!(rx.borrow_and_update(), 7);
        });
    }

    #[test]
    fn send_replace_returns_previous() {
        let (tx, rx) = channel(1);
        let previous = tx.send_replace(2);
        assert_eq!(previous, 1);
        assert_eq!(rx.borrow(), 2);
    }
}
