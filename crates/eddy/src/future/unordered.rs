//! `FuturesUnordered`: a dynamically-sized set of futures driven by a ready
//! queue.
//!
//! Members are stored in a map keyed by an id; their wakers push the member's
//! id into a shared ready queue, so a poll only touches members that were
//! woken (plus each new member on its first poll). A member that has not been
//! woken is never polled again, which keeps the set's cost proportional to
//! the number of *ready* members rather than the total size.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// Shared state between the set and its members' wakers. Members and the
/// polling task may live on different threads, so access is synchronized.
struct Shared {
    ready: Mutex<VecDeque<u64>>,
    outer: Mutex<Option<Waker>>,
}

/// Waker payload for one member: waking it enqueues its id and wakes the
/// task currently polling the set.
struct EntryWake {
    shared: Arc<Shared>,
    id: u64,
}

impl EntryWake {
    fn wake(&self) {
        self.shared.ready.lock().unwrap().push_back(self.id);
        if let Some(waker) = self.shared.outer.lock().unwrap().take() {
            waker.wake();
        }
    }
}

unsafe fn entry_wake_clone(ptr: *const ()) -> RawWaker {
    // SAFETY: the caller passed the data pointer of a `RawWaker` that was
    // built from `Arc::into_raw` (or its clone), so `ptr` is a valid
    // `Arc<EntryWake>`. Reconstituting it temporarily lets us clone and
    // forget it, keeping the reference count balanced.
    let entry = unsafe { Arc::from_raw(ptr as *const EntryWake) };
    let cloned = Arc::clone(&entry);
    std::mem::forget(entry);
    RawWaker::new(Arc::into_raw(cloned) as *const (), &WAKER_VTABLE)
}

unsafe fn entry_wake_wake(ptr: *const ()) {
    // SAFETY: same as `entry_wake_clone`; the arc is consumed by `from_raw`
    // and forgotten after waking, matching the by-value waker token.
    let entry = unsafe { Arc::from_raw(ptr as *const EntryWake) };
    entry.wake();
    std::mem::forget(entry);
}

unsafe fn entry_wake_wake_ref(ptr: *const ()) {
    // SAFETY: same provenance as `entry_wake_clone`; we only borrow for the
    // duration of the call and never drop, so the reference count is
    // unaffected.
    let entry = unsafe { &*(ptr as *const EntryWake) };
    entry.wake();
}

unsafe fn entry_wake_drop(ptr: *const ()) {
    // SAFETY: same provenance as `entry_wake_clone`; dropping the
    // reconstituted arc balances the clone performed when the waker was
    // created.
    unsafe { Arc::from_raw(ptr as *const EntryWake) };
}

static WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    entry_wake_clone,
    entry_wake_wake,
    entry_wake_wake_ref,
    entry_wake_drop,
);

fn entry_waker(entry: Arc<EntryWake>) -> Waker {
    // SAFETY: the vtable functions only ever touch the data pointer as an
    // `Arc<EntryWake>`, which is exactly what `Arc::into_raw` produced and
    // what the clone/drop functions maintain.
    unsafe {
        Waker::from_raw(RawWaker::new(
            Arc::into_raw(entry) as *const (),
            &WAKER_VTABLE,
        ))
    }
}

struct Entry<F> {
    fut: Pin<Box<F>>,
    waker: Waker,
}

/// A set of futures polled together, resolving with the output of whichever
/// member completes first.
///
/// Polling a `FuturesUnordered` drains the ready queue: every new member is
/// polled once when it is pushed, and afterwards a member is polled only when
/// its waker has fired. Dropping the set drops all remaining members.
pub struct FuturesUnordered<F> {
    entries: HashMap<u64, Entry<F>>,
    next_id: u64,
    shared: Arc<Shared>,
}

impl<F: Future> Default for FuturesUnordered<F> {
    fn default() -> FuturesUnordered<F> {
        FuturesUnordered::new()
    }
}

impl<F: Future> FuturesUnordered<F> {
    /// Create a new, empty set.
    pub fn new() -> FuturesUnordered<F> {
        FuturesUnordered {
            entries: HashMap::new(),
            next_id: 0,
            shared: Arc::new(Shared {
                ready: Mutex::new(VecDeque::new()),
                outer: Mutex::new(None),
            }),
        }
    }
    /// Add a future to the set. It is polled once on the next poll of the
    /// set.
    pub fn push(&mut self, future: F) {
        let id = self.next_id;
        self.next_id += 1;
        let entry_wake = Arc::new(EntryWake {
            shared: Arc::clone(&self.shared),
            id,
        });
        let entry = Entry {
            fut: Box::pin(future),
            waker: entry_waker(entry_wake),
        };
        self.shared.ready.lock().unwrap().push_back(id);
        self.entries.insert(id, entry);
    }

    /// Number of members currently in the set.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the set holds no members.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Drop all members.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.shared.ready.lock().unwrap().clear();
    }

    /// Iterate over the members (in map order).
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut F> {
        self.entries.values_mut().map(|entry| {
            // SAFETY: `entry.fut` is a `Pin<Box<F>>` owned by this entry and
            // the caller only gets `&mut F`; nothing polled here requires
            // the `Pin` guarantees, and the entry itself is never moved.
            unsafe { entry.fut.as_mut().get_unchecked_mut() }
        })
    }

    fn poll_next_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>> {
        *self.shared.outer.lock().unwrap() = Some(cx.waker().clone());
        loop {
            let id = match self.shared.ready.lock().unwrap().pop_front() {
                Some(id) => id,
                None => {
                    if self.entries.is_empty() {
                        return Poll::Ready(None);
                    }
                    return Poll::Pending;
                }
            };
            let entry = match self.entries.get_mut(&id) {
                Some(entry) => entry,
                None => continue,
            };
            let mut entry_cx = Context::from_waker(&entry.waker);
            match entry.fut.as_mut().poll(&mut entry_cx) {
                Poll::Ready(output) => {
                    self.entries.remove(&id);
                    return Poll::Ready(Some(output));
                }
                Poll::Pending => {}
            }
        }
    }
}

impl<F: Future> crate::stream::Stream for FuturesUnordered<F> {
    type Item = F::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_inner(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<F: Future> Future for FuturesUnordered<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().poll_next_inner(cx) {
            Poll::Ready(Some(output)) => Poll::Ready(output),
            Poll::Ready(None) => {
                ::core::unreachable!("FuturesUnordered was polled after all members completed")
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
