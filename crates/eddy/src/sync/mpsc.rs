use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Waiter {
    granted: AtomicBool,
    /// Whether this waiter holds a reservation slot once granted. A `Reserve`
    /// waiter counts its slot in `State::reserved` at grant time; a plain
    /// `Send` waiter does not.
    reserves: bool,
    waker: Mutex<Option<Waker>>,
}

impl Waiter {
    fn new(reserves: bool) -> Arc<Waiter> {
        Arc::new(Waiter {
            granted: AtomicBool::new(false),
            reserves,
            waker: Mutex::new(None),
        })
    }
}

struct State<T> {
    queue: VecDeque<T>,
    capacity: Option<usize>,
    reserved: usize,
    senders: usize,
    receiver_alive: bool,
    recv_waker: Option<Waker>,
    send_waiters: VecDeque<Arc<Waiter>>,
}

struct Inner<T> {
    state: Mutex<State<T>>,
}

pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

pub struct Receiver<T> {
    inner: Option<Arc<Inner<T>>>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SendError<T>(pub T);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

#[derive(Debug, PartialEq, Eq)]
pub struct TrySendError<T>(pub T);

pub struct Send<'a, T> {
    inner: Arc<Inner<T>>,
    item: Option<T>,
    waiter: Arc<Waiter>,
    registered: bool,
    marker: std::marker::PhantomData<&'a Sender<T>>,
}

pub struct Reserve<'a, T> {
    inner: Arc<Inner<T>>,
    waiter: Arc<Waiter>,
    registered: bool,
    marker: std::marker::PhantomData<&'a Sender<T>>,
}

pub struct Recv<'a, T> {
    receiver: &'a mut Receiver<T>,
}

pub struct Permit<T> {
    inner: Option<Arc<Inner<T>>>,
}

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "eddy: mpsc capacity must be non-zero");
    channel_inner(Some(capacity))
}

pub fn unbounded_channel<T>() -> (Sender<T>, Receiver<T>) {
    channel_inner(None)
}

fn channel_inner<T>(capacity: Option<usize>) -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        state: Mutex::new(State {
            queue: VecDeque::new(),
            capacity,
            reserved: 0,
            senders: 1,
            receiver_alive: true,
            recv_waker: None,
            send_waiters: VecDeque::new(),
        }),
    });
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner: Some(inner) },
    )
}

fn capacity_available<T>(state: &State<T>) -> bool {
    state.capacity.map_or(true, |capacity| {
        state.queue.len() + state.reserved < capacity
    })
}

fn wake_receiver<T>(state: &mut State<T>) {
    if let Some(waker) = state.recv_waker.take() {
        waker.wake();
    }
}

fn wake_one_sender<T>(state: &mut State<T>) {
    if !capacity_available(state) {
        return;
    }
    if let Some(waiter) = state.send_waiters.pop_front() {
        // The reservation is counted exactly once per granted `Reserve`
        // waiter, here at grant time; `Reserve::poll` never increments again.
        if waiter.reserves {
            state.reserved += 1;
        }
        waiter.granted.store(true, Ordering::Release);
        if let Some(waker) = waiter.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Sender<T> {
        self.inner.state.lock().unwrap().senders += 1;
        Sender {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Sender<T> {
    pub fn send(&self, item: T) -> Send<'_, T> {
        Send {
            inner: self.inner.clone(),
            item: Some(item),
            waiter: Waiter::new(false),
            registered: false,
            marker: std::marker::PhantomData,
        }
    }

    pub fn reserve(&self) -> Reserve<'_, T> {
        Reserve {
            inner: self.inner.clone(),
            waiter: Waiter::new(true),
            registered: false,
            marker: std::marker::PhantomData,
        }
    }

    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        let mut state = self.inner.state.lock().unwrap();
        if !state.receiver_alive || !capacity_available(&state) || !state.send_waiters.is_empty() {
            return Err(TrySendError(item));
        }
        state.queue.push_back(item);
        wake_receiver(&mut state);
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        !self.inner.state.lock().unwrap().receiver_alive
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let waker = {
            let mut state = self.inner.state.lock().unwrap();
            state.senders -= 1;
            if state.senders == 0 {
                state.recv_waker.take()
            } else {
                None
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Future for Send<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: polling does not move the pinned future allocation; its
        // fields are only mutated in place.
        let this = unsafe { self.get_unchecked_mut() };
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        let mut state = this.inner.state.lock().unwrap();
        if !state.receiver_alive {
            return Poll::Ready(Err(SendError(this.item.take().unwrap())));
        }
        let granted = this.waiter.granted.swap(false, Ordering::Acquire);
        if granted && capacity_available(&state) {
            state.queue.push_back(this.item.take().unwrap());
            this.registered = false;
            wake_receiver(&mut state);
            return Poll::Ready(Ok(()));
        }
        if granted {
            // The grant was lost to a competing push that refilled the
            // channel; re-register from scratch so the next freed slot is
            // offered to this waiter again.
            this.registered = false;
        }
        if !this.registered && state.send_waiters.is_empty() && capacity_available(&state) {
            state.queue.push_back(this.item.take().unwrap());
            wake_receiver(&mut state);
            return Poll::Ready(Ok(()));
        }
        *this.waiter.waker.lock().unwrap() = Some(cx.waker().clone());
        if !this.registered {
            state.send_waiters.push_back(this.waiter.clone());
            this.registered = true;
        }
        Poll::Pending
    }
}

impl<T> Drop for Send<'_, T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        let granted = self.waiter.granted.swap(false, Ordering::Acquire);
        if self.registered {
            state
                .send_waiters
                .retain(|waiter| !Arc::ptr_eq(waiter, &self.waiter));
        }
        if granted {
            wake_one_sender(&mut state);
        }
    }
}

impl<T> Future for Reserve<'_, T> {
    type Output = Result<Permit<T>, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: polling does not move the pinned future allocation; its
        // fields are only mutated in place.
        let this = unsafe { self.get_unchecked_mut() };
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        let mut state = this.inner.state.lock().unwrap();
        if !state.receiver_alive {
            return Poll::Ready(Err(RecvError));
        }
        let granted = this.waiter.granted.swap(false, Ordering::Acquire);
        if granted {
            // `wake_one_sender` already counted this reservation at grant
            // time, so the permit is valid even if the queue is momentarily
            // full; the reservation guarantees the slot.
            this.registered = false;
            return Poll::Ready(Ok(Permit {
                inner: Some(this.inner.clone()),
            }));
        }
        if !this.registered && state.send_waiters.is_empty() && capacity_available(&state) {
            state.reserved += 1;
            return Poll::Ready(Ok(Permit {
                inner: Some(this.inner.clone()),
            }));
        }
        *this.waiter.waker.lock().unwrap() = Some(cx.waker().clone());
        if !this.registered {
            state.send_waiters.push_back(this.waiter.clone());
            this.registered = true;
        }
        Poll::Pending
    }
}

impl<T> Drop for Reserve<'_, T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        let granted = self.waiter.granted.swap(false, Ordering::Acquire);
        if self.registered {
            state
                .send_waiters
                .retain(|waiter| !Arc::ptr_eq(waiter, &self.waiter));
        }
        if granted {
            // A granted-but-never-polled reservation still holds its slot
            // (counted by `wake_one_sender`): release it before offering the
            // freed slot to the next waiter.
            state.reserved -= 1;
            wake_one_sender(&mut state);
        }
    }
}

impl<T> Permit<T> {
    pub fn send(mut self, item: T) -> Result<(), SendError<T>> {
        let inner = self.inner.take().expect("eddy: mpsc permit used twice");
        let mut state = inner.state.lock().unwrap();
        state.reserved -= 1;
        if !state.receiver_alive {
            return Err(SendError(item));
        }
        state.queue.push_back(item);
        wake_receiver(&mut state);
        Ok(())
    }
}

impl<T> Drop for Permit<T> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let mut state = inner.state.lock().unwrap();
        state.reserved -= 1;
        wake_one_sender(&mut state);
    }
}

impl<T> Receiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        Recv { receiver: self }.await
    }

    pub async fn recv_many(&mut self, output: &mut Vec<T>, limit: usize) -> usize {
        let start = output.len();
        while output.len() - start < limit {
            let Some(item) = self.recv().await else { break };
            output.push(item);
        }
        output.len() - start
    }
}

impl<T> Future for Recv<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Cooperative scheduling: draining a hot channel must yield once the
        // budget is spent, otherwise one consumer starves every other task.
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        // SAFETY: polling does not move the pinned future allocation.
        let this = unsafe { self.get_unchecked_mut() };
        let receiver = &mut *this.receiver;
        let inner = receiver
            .inner
            .as_ref()
            .expect("eddy: receiver dropped")
            .clone();
        let mut state = inner.state.lock().unwrap();
        if let Some(item) = state.queue.pop_front() {
            wake_one_sender(&mut state);
            return Poll::Ready(Some(item));
        }
        if state.senders == 0 {
            drop(state);
            receiver.inner.take();
            return Poll::Ready(None);
        }
        state.recv_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let mut state = inner.state.lock().unwrap();
        state.receiver_alive = false;
        let waiters = std::mem::take(&mut state.send_waiters);
        drop(state);
        for waiter in waiters {
            if let Some(waker) = waiter.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }
}
