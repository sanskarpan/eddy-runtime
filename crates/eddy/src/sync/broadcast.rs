//! Broadcast channel: every send is delivered to every live receiver.
//!
//! Backed by a fixed-capacity ring buffer. A receiver that falls behind loses
//! values and observes [`RecvError::Lagged`] naming how many were skipped —
//! slow receivers never stall the sender. This is the model's core trade:
//! the buffer drops instead of backpressuring, so a receiver is only ever as
//! current as it polls. A cloned receiver (tokio-style) sees only values sent
//! after the clone was created.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// The sender was dropped; no further values.
    Closed,
    /// The receiver missed `n` values because its cursor fell behind the ring.
    Lagged(u64),
}

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvError::Closed => f.write_str("broadcast sender was dropped"),
            RecvError::Lagged(n) => {
                write!(f, "broadcast receiver lagged behind by {n} value(s)")
            }
        }
    }
}

impl std::error::Error for RecvError {}

/// One receiver's shared slot. `cursor` is the next sequence number this
/// receiver expects; `waker` is set only while it is waiting.
struct ReceiverSlot {
    id: u64,
    cursor: u64,
    waker: Option<Waker>,
}

struct State<T> {
    buffer: VecDeque<T>,
    next_seq: u64,
    receivers: Vec<ReceiverSlot>,
}

struct Inner<T> {
    capacity: usize,
    next_receiver_id: AtomicU64,
    state: Mutex<State<T>>,
}

#[derive(Debug)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("broadcast channel has no receivers")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    Closed,
    Lagged(u64),
}

impl std::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => f.write_str("broadcast channel is empty"),
            TryRecvError::Closed => f.write_str("broadcast channel is closed"),
            TryRecvError::Lagged(n) => write!(f, "receiver lagged behind by {n} value(s)"),
        }
    }
}

impl std::error::Error for TryRecvError {}

pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            inner: self.inner.clone(),
        }
    }
}

impl<T> std::fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    id: u64,
    cursor: u64,
}

impl<T> std::fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

/// Create a broadcast channel that buffers the most recent `capacity` values.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(
        capacity > 0,
        "eddy: broadcast channel capacity must be greater than zero"
    );
    let inner = Arc::new(Inner {
        capacity,
        next_receiver_id: AtomicU64::new(0),
        state: Mutex::new(State {
            buffer: VecDeque::new(),
            next_seq: 0,
            receivers: Vec::new(),
        }),
    });
    let mut receiver = Receiver {
        inner: inner.clone(),
        id: inner.next_receiver_id.fetch_add(1, Ordering::Relaxed),
        cursor: 0,
    };
    // A receiver is registered eagerly so a send before the first poll is not
    // mistaken for "no receivers".
    receiver.register_slot();
    (Sender { inner }, receiver)
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        let mut clone = Receiver {
            inner: self.inner.clone(),
            // The clone is independent and (tokio-style) sees only values
            // sent after the clone was created; a new id gets its own slot.
            id: self.inner.next_receiver_id.fetch_add(1, Ordering::Relaxed),
            cursor: self.inner.state.lock().unwrap().next_seq,
        };
        clone.register_slot();
        clone
    }
}

/// The oldest sequence number still buffered.
fn first_seq<T>(state: &State<T>, capacity: usize) -> u64 {
    state
        .next_seq
        .saturating_sub(state.buffer.len().min(capacity) as u64)
}

impl<T> Receiver<T> {
    fn register_slot(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        assert!(
            !state.receivers.iter().any(|slot| slot.id == self.id),
            "eddy: broadcast receiver registered twice"
        );
        state.receivers.push(ReceiverSlot {
            id: self.id,
            cursor: self.cursor,
            waker: None,
        });
    }

    /// The index of this receiver's slot, registering it lazily if needed.
    /// Takes `id`/`cursor` by value so the caller is not double-borrowed
    /// while the state guard is alive.
    fn slot_index(state: &mut State<T>, id: u64, cursor: u64) -> usize {
        if let Some(index) = state.receivers.iter().position(|slot| slot.id == id) {
            index
        } else {
            state.receivers.push(ReceiverSlot {
                id,
                cursor,
                waker: None,
            });
            state.receivers.len() - 1
        }
    }
}

impl<T: Clone> Sender<T> {
    /// Broadcast a value to every live receiver. Returns the value back with
    /// a [`SendError`] if there are no receivers at all.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let wakers: Vec<Waker> = {
            let mut state = self.inner.state.lock().unwrap();
            if state.receivers.is_empty() {
                return Err(SendError(value));
            }
            if state.buffer.len() == self.inner.capacity {
                state.buffer.pop_front();
            }
            state.buffer.push_back(value);
            state.next_seq += 1;
            // Wake every waiting receiver. A lagging one recomputes its gap
            // from its own cursor on the next poll.
            let mut wakers = Vec::new();
            for receiver in &mut state.receivers {
                if let Some(waker) = receiver.waker.take() {
                    wakers.push(waker);
                }
            }
            wakers
        };
        for waker in wakers {
            waker.wake();
        }
        Ok(())
    }

    pub fn receiver_count(&self) -> usize {
        self.inner.state.lock().unwrap().receivers.len()
    }
}

impl<T: Clone> Receiver<T> {
    /// Read one buffered value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut state = self.inner.state.lock().unwrap();
        if Arc::strong_count(&self.inner) == state.receivers.len() {
            return Err(TryRecvError::Closed);
        }
        let first = first_seq(&state, self.inner.capacity);
        if self.cursor < first {
            let lagged = first - self.cursor;
            self.cursor = first;
            let index = Self::slot_index(&mut state, self.id, self.cursor);
            state.receivers[index].cursor = self.cursor;
            return Err(TryRecvError::Lagged(lagged));
        }
        if self.cursor < state.next_seq {
            let index = (self.cursor - first) as usize;
            let value = state.buffer[index].clone();
            self.cursor += 1;
            let slot = Self::slot_index(&mut state, self.id, self.cursor);
            state.receivers[slot].cursor = self.cursor;
            return Ok(value);
        }
        Err(TryRecvError::Empty)
    }
}

impl<T: Clone> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        let this = self.get_mut();
        let mut state = this.inner.state.lock().unwrap();
        let first = first_seq(&state, this.inner.capacity);
        if this.cursor < first {
            let lagged = first - this.cursor;
            this.cursor = first;
            let index = Self::slot_index(&mut state, this.id, this.cursor);
            state.receivers[index].cursor = this.cursor;
            return Poll::Ready(Err(RecvError::Lagged(lagged)));
        }
        if this.cursor < state.next_seq {
            let index = (this.cursor - first) as usize;
            let value = state.buffer[index].clone();
            this.cursor += 1;
            let slot = Self::slot_index(&mut state, this.id, this.cursor);
            state.receivers[slot].cursor = this.cursor;
            return Poll::Ready(Ok(value));
        }
        // Nothing buffered: pending unless the senders are all gone. The
        // receivers hold one `Arc` each and the senders share another (or
        // zero), so a count of receiver-slots means no senders remain.
        if Arc::strong_count(&this.inner) == state.receivers.len() {
            return Poll::Ready(Err(RecvError::Closed));
        }
        let slot = Self::slot_index(&mut state, this.id, this.cursor);
        state.receivers[slot].waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.receivers.retain(|slot| slot.id != self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivers_every_value_to_each_receiver() {
        let (tx, mut rx) = channel(16);
        let mut rx2 = rx.clone();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx2.try_recv(), Ok(1));
        assert_eq!(rx2.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn clone_sees_only_post_clone_values() {
        let (tx, mut rx) = channel(16);
        tx.send(1).unwrap();
        let mut rx2 = rx.clone();
        tx.send(2).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        // tokio-style: the clone starts at the send-time cursor.
        assert_eq!(rx2.try_recv(), Ok(2));
        assert_eq!(rx2.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn lagging_receiver_reports_lagged() {
        let (tx, mut rx) = channel(2);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        tx.send(4).unwrap();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Lagged(2)));
        assert_eq!(rx.try_recv(), Ok(3));
        assert_eq!(rx.try_recv(), Ok(4));
    }

    #[test]
    fn sender_dropped_is_closed() {
        let (tx, mut rx) = channel::<i32>(4);
        drop(tx);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Closed));
    }

    #[test]
    fn send_with_no_receivers_returns_error() {
        let (tx, rx) = channel(4);
        drop(rx);
        assert!(tx.send(1).is_err());
    }

    #[test]
    fn lag_with_two_receivers_is_independent() {
        let (tx, mut fast) = channel::<i32>(2);
        let mut slow = fast.clone();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        fast.try_recv().unwrap();
        fast.try_recv().unwrap();
        tx.send(3).unwrap();
        tx.send(4).unwrap();
        assert_eq!(slow.try_recv(), Err(TryRecvError::Lagged(2)));
        assert_eq!(slow.try_recv(), Ok(3));
        assert_eq!(slow.try_recv(), Ok(4));
    }

    #[cfg(not(loom))]
    #[test]
    fn async_recv_waits_until_send() {
        let rt = crate::runtime::Builder::new_current_thread().build();
        rt.block_on(async {
            let (tx, mut rx) = channel::<i32>(4);
            let sender = {
                let tx = tx.clone();
                crate::runtime::Handle::current().spawn(async move {
                    crate::time::sleep(std::time::Duration::from_millis(5)).await;
                    tx.send(42).unwrap();
                })
            };
            // First poll registers the wait; the eager slot from channel()
            // must not break the pending path.
            let value = (&mut rx).await;
            sender.await.unwrap();
            assert_eq!(value.unwrap(), 42);
        });
    }
}
