use std::future::Future;
use std::marker::{PhantomData, PhantomPinned};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::loom::sync::{Arc, Mutex};
use crate::util::{Linked, LinkedList, Pointers};

struct Waiter {
    links: Pointers<Waiter>,
    permits: usize,
    waker: Option<Waker>,
    granted: bool,
}

impl Waiter {
    fn new(permits: usize) -> Waiter {
        Waiter {
            links: Pointers::new(),
            permits,
            waker: None,
            granted: false,
        }
    }
}

impl Linked for Waiter {
    fn pointers(&self) -> &Pointers<Self> {
        &self.links
    }
}

// SAFETY: Waiter is accessed only while the semaphore state mutex is held.
unsafe impl Send for Waiter {}
// SAFETY: Waiter is accessed only while the semaphore state mutex is held.
unsafe impl Sync for Waiter {}

struct State {
    permits: usize,
    closed: bool,
    holder: crate::instrument::TaskId,
    waiters: LinkedList<Waiter>,
}

// SAFETY: the waiter list is only accessed while the semaphore state mutex is
// held; each raw node points into a pinned future that remains alive there.
unsafe impl Send for State {}
// SAFETY: the waiter list is only accessed while the semaphore state mutex is held.
unsafe impl Sync for State {}

struct Inner {
    state: Mutex<State>,
}

pub struct Semaphore {
    inner: Arc<Inner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcquireError;

impl std::fmt::Display for AcquireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("semaphore is closed")
    }
}

impl std::error::Error for AcquireError {}

pub struct Acquire<'a> {
    inner: Arc<Inner>,
    waiter: Waiter,
    _marker: PhantomData<&'a Semaphore>,
    _pin: PhantomPinned,
}

impl<'a> Acquire<'a> {
    fn new(inner: Arc<Inner>, permits: usize) -> Acquire<'a> {
        Acquire {
            inner,
            waiter: Waiter::new(permits),
            _marker: PhantomData,
            _pin: PhantomPinned,
        }
    }
}

impl Future for Acquire<'_> {
    type Output = Result<SemaphorePermit, AcquireError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Cooperative scheduling: an acquire that succeeds immediately (or
        // the hot loop polling it) must yield once its budget is spent.
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        // SAFETY: polling does not move the pinned future allocation.
        let this = unsafe { self.get_unchecked_mut() };
        let mut state = this.inner.state.lock().unwrap();
        if this.waiter.granted {
            this.waiter.granted = false;
            state.holder = crate::instrument::TaskId::current();
            let inner = this.inner.clone();
            return Poll::Ready(Ok(SemaphorePermit {
                inner,
                permits: this.waiter.permits,
            }));
        }
        if state.closed {
            if this.waiter.links.is_linked() {
                let ptr = std::ptr::NonNull::from(&mut this.waiter);
                // SAFETY: the waiter is linked in this semaphore's list.
                unsafe { state.waiters.remove(ptr) };
            }
            return Poll::Ready(Err(AcquireError));
        }
        if !this.waiter.links.is_linked()
            && state.waiters.is_empty()
            && state.permits >= this.waiter.permits
        {
            state.permits -= this.waiter.permits;
            state.holder = crate::instrument::TaskId::current();
            return Poll::Ready(Ok(SemaphorePermit {
                inner: this.inner.clone(),
                permits: this.waiter.permits,
            }));
        }
        this.waiter.waker = Some(cx.waker().clone());
        if !this.waiter.links.is_linked() {
            let ptr = std::ptr::NonNull::from(&mut this.waiter);
            // SAFETY: the future is pinned for the duration of registration;
            // Drop removes the node before the future is moved or destroyed.
            unsafe { state.waiters.push_back(ptr) };
            let holder = state.holder;
            crate::instrument::emit(|| crate::instrument::RuntimeEvent::ResourceContended {
                kind: "semaphore",
                holder,
                waiters: Vec::new(),
            });
        }
        Poll::Pending
    }
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        if self.waiter.links.is_linked() {
            let ptr = std::ptr::NonNull::from(&mut self.waiter);
            // SAFETY: the waiter is linked in this semaphore's list.
            unsafe { state.waiters.remove(ptr) };
        } else if self.waiter.granted {
            self.waiter.granted = false;
            let wakes = release_locked(&mut state, self.waiter.permits);
            drop(state);
            wake_all(wakes);
        }
    }
}

pub struct SemaphorePermit {
    inner: Arc<Inner>,
    permits: usize,
}

impl SemaphorePermit {
    pub fn forget(self) {
        std::mem::forget(self);
    }

    pub fn permits(&self) -> usize {
        self.permits
    }
}

impl Drop for SemaphorePermit {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        let wakes = release_locked(&mut state, self.permits);
        drop(state);
        wake_all(wakes);
    }
}

pub type OwnedSemaphorePermit = SemaphorePermit;

fn release_locked(state: &mut State, permits: usize) -> Vec<Waker> {
    state.permits = state.permits.saturating_add(permits);
    let mut wakes = Vec::new();
    while let Some(ptr) = {
        // SAFETY: the semaphore state mutex is held by the caller.
        unsafe { state.waiters.pop_front() }
    } {
        // SAFETY: ptr was owned by the waiter list and remains live because
        // the waiting future owns the allocation containing it.
        let waiter = unsafe { &mut *ptr.as_ptr() };
        if waiter.permits > state.permits {
            // SAFETY: restore the FIFO head when it cannot yet be granted.
            unsafe { state.waiters.push_front(ptr) };
            break;
        }
        state.permits -= waiter.permits;
        waiter.granted = true;
        state.holder = crate::instrument::TaskId::current();
        if let Some(waker) = waiter.waker.take() {
            wakes.push(waker);
        }
    }
    wakes
}

fn wake_all(wakes: Vec<Waker>) {
    for waker in wakes {
        waker.wake();
    }
}

impl Semaphore {
    pub fn new(permits: usize) -> Semaphore {
        Semaphore {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    permits,
                    closed: false,
                    holder: crate::instrument::TaskId::default(),
                    waiters: LinkedList::new(),
                }),
            }),
        }
    }

    pub fn available_permits(&self) -> usize {
        self.inner.state.lock().unwrap().permits
    }

    pub fn acquire(&self) -> Acquire<'_> {
        self.acquire_many(1)
    }

    pub fn acquire_many(&self, permits: usize) -> Acquire<'_> {
        assert!(
            permits > 0,
            "eddy: semaphore acquire count must be non-zero"
        );
        Acquire::new(self.inner.clone(), permits)
    }

    // NOTE: the receiver must be `std::sync::Arc` (not the loom shim) —
    // `self: Arc<Self>` relies on `std::sync::Arc` implementing `Receiver`,
    // which `loom::sync::Arc` does not. Owned acquisition is not part of any
    // loom model, so this is the one place the shim is not used.
    pub fn acquire_owned(self: std::sync::Arc<Self>) -> Acquire<'static> {
        self.acquire_many_owned(1)
    }

    pub fn acquire_many_owned(self: std::sync::Arc<Self>, permits: usize) -> Acquire<'static> {
        assert!(
            permits > 0,
            "eddy: semaphore acquire count must be non-zero"
        );
        Acquire::new(self.inner.clone(), permits)
    }

    pub fn try_acquire(&self) -> Result<SemaphorePermit, AcquireError> {
        self.try_acquire_many(1)
    }

    pub fn try_acquire_many(&self, permits: usize) -> Result<SemaphorePermit, AcquireError> {
        assert!(
            permits > 0,
            "eddy: semaphore acquire count must be non-zero"
        );
        let mut state = self.inner.state.lock().unwrap();
        if state.closed || !state.waiters.is_empty() || state.permits < permits {
            return Err(AcquireError);
        }
        state.permits -= permits;
        Ok(SemaphorePermit {
            inner: self.inner.clone(),
            permits,
        })
    }

    pub fn add_permits(&self, permits: usize) {
        if permits == 0 {
            return;
        }
        let mut state = self.inner.state.lock().unwrap();
        let wakes = release_locked(&mut state, permits);
        drop(state);
        wake_all(wakes);
    }

    pub fn close(&self) {
        let mut state = self.inner.state.lock().unwrap();
        state.closed = true;
        let mut wakes = Vec::new();
        // SAFETY: every node belongs to this list and remains live in its
        // pinned Acquire future.
        while let Some(ptr) = unsafe { state.waiters.pop_front() } {
            // SAFETY: ptr came from the list and points to a live waiter.
            if let Some(waker) = unsafe { &mut *ptr.as_ptr() }.waker.take() {
                wakes.push(waker);
            }
        }
        drop(state);
        for waker in wakes {
            waker.wake();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.state.lock().unwrap().closed
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use crate::loom::sync::atomic::Ordering as LoomOrdering;

    #[test]
    fn acquire_release_with_two_waiters_never_loses_a_permit() {
        // FIFO chain: with one permit, the first waiter takes it directly and
        // the second parks. The permit then reaches the parked waiter through
        // the release chain (`add_permits`, or the first waiter dropping its
        // grant). A lost wake would leave the parked waiter blocked forever
        // and fail the join assertion (or deadlock the model). The preemption
        // bound keeps the park/wake interleaving space tractable while still
        // exploring both sides of the registration-vs-release race.
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(|| {
            let semaphore = Arc::new(Semaphore::new(1));
            let granted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut threads = Vec::new();
            for _ in 0..2 {
                let semaphore = semaphore.clone();
                let granted = granted.clone();
                threads.push(loom::thread::spawn(move || {
                    let output = loom::future::block_on(semaphore.acquire());
                    assert!(output.is_ok(), "a waiting acquire must never fail");
                    granted.fetch_add(1, LoomOrdering::AcqRel);
                }));
            }
            // Give the second waiter time to park, then release from a
            // third thread (the model also explores the other orderings).
            loom::thread::yield_now();
            semaphore.add_permits(1);
            for thread in threads {
                thread.join().unwrap();
            }
            assert_eq!(granted.load(LoomOrdering::Acquire), 2);
        });
    }

    #[test]
    fn close_racing_with_waiter_registration_wakes_with_an_error() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(|| {
            let semaphore = Arc::new(Semaphore::new(0));
            let waiter_semaphore = semaphore.clone();
            let waiter = loom::thread::spawn(move || {
                let result = loom::future::block_on(waiter_semaphore.acquire());
                assert!(result.is_err(), "a closed semaphore granted a permit");
            });

            loom::thread::yield_now();
            semaphore.close();
            waiter.join().unwrap();
            assert!(semaphore.is_closed());
            assert_eq!(semaphore.available_permits(), 0);
        });
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn permits_are_fifo_and_returned_on_drop() {
        let semaphore = Semaphore::new(1);
        let first = semaphore.try_acquire().unwrap();
        assert!(semaphore.try_acquire().is_err());
        drop(first);
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn close_wakes_waiters_with_an_error() {
        let semaphore = Arc::new(Semaphore::new(0));
        let mut acquire = Box::pin(semaphore.acquire());
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(acquire.as_mut().poll(&mut cx), Poll::Pending));
        semaphore.close();
        assert!(matches!(
            acquire.as_mut().poll(&mut cx),
            Poll::Ready(Err(_))
        ));
    }

    #[test]
    fn dropping_a_pending_acquire_does_not_consume_the_next_permit() {
        let semaphore = Semaphore::new(0);
        let mut acquire = Box::pin(semaphore.acquire());
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(acquire.as_mut().poll(&mut cx).is_pending());
        drop(acquire);

        semaphore.add_permits(1);
        assert!(semaphore.try_acquire().is_ok());
    }
}
