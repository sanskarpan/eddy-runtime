use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::loom::sync::{Arc, Mutex};

struct State<T> {
    value: Option<T>,
    sent: bool,
    receiver_alive: bool,
    sender_alive: bool,
    receiver_waker: Option<Waker>,
    closed_waker: Option<Waker>,
}

struct Inner<T> {
    state: Mutex<State<T>>,
}

pub struct Sender<T> {
    inner: Option<Arc<Inner<T>>>,
}

pub struct Receiver<T> {
    inner: Option<Arc<Inner<T>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

#[derive(Debug, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("oneshot sender was dropped")
    }
}

impl std::error::Error for RecvError {}

pub struct Closed<'a, T> {
    sender: &'a Sender<T>,
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        state: Mutex::new(State {
            value: None,
            sent: false,
            receiver_alive: true,
            sender_alive: true,
            receiver_waker: None,
            closed_waker: None,
        }),
    });
    (
        Sender {
            inner: Some(inner.clone()),
        },
        Receiver { inner: Some(inner) },
    )
}

impl<T> Sender<T> {
    pub fn send(mut self, value: T) -> Result<(), SendError<T>> {
        let inner = self.inner.take().expect("eddy: oneshot sender used twice");
        let waker = {
            let mut state = inner.state.lock().unwrap();
            if !state.receiver_alive || state.sent {
                return Err(SendError(value));
            }
            state.sent = true;
            state.value = Some(value);
            state.receiver_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.inner
            .as_ref()
            .map(|inner| !inner.state.lock().unwrap().receiver_alive)
            .unwrap_or(true)
    }

    pub fn closed(&self) -> Closed<'_, T> {
        Closed { sender: self }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let inner = this
            .inner
            .as_ref()
            .expect("eddy: receiver polled twice")
            .clone();
        let mut state = inner.state.lock().unwrap();
        if let Some(value) = state.value.take() {
            drop(state);
            this.inner.take();
            return Poll::Ready(Ok(value));
        }
        if state.sent || !state.sender_alive {
            drop(state);
            this.inner.take();
            return Poll::Ready(Err(RecvError));
        }
        state.receiver_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let waker = {
            let mut state = inner.state.lock().unwrap();
            state.receiver_alive = false;
            state.closed_waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Future for Closed<'_, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Some(inner) = &self.sender.inner else {
            return Poll::Ready(());
        };
        let mut state = inner.state.lock().unwrap();
        if !state.receiver_alive {
            Poll::Ready(())
        } else {
            state.closed_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let waker = {
            let mut state = inner.state.lock().unwrap();
            state.sender_alive = false;
            if !state.sent {
                state.receiver_waker.take()
            } else {
                None
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;

    #[test]
    fn send_racing_with_receiver_drop_is_safe_and_consistent() {
        // The classic oneshot tear-down race: a concurrent `send` must either
        // observe the receiver as alive (and win) or observe the drop (and
        // return the value back in `SendError`). It must never panic, never
        // lose the value, and never wake a receiver that was already dropped.
        loom::model(|| {
            let (sender, receiver) = channel::<usize>();
            let sender_thread = loom::thread::spawn(move || {
                let outcome = sender.send(7);
                match outcome {
                    Ok(()) => {}
                    Err(SendError(value)) => assert_eq!(value, 7),
                }
            });
            // Let the model explore both orders: drop before the send lands
            // and send landing before the drop.
            drop(receiver);
            sender_thread.join().unwrap();
        });
    }
}
