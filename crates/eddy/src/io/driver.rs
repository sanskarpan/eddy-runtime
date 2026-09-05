//! The I/O readiness driver: translates kernel events into task wakes.
//!
//! One `DriverShared` lives per multi-thread runtime (inside `Shared`). The
//! worker that parks first claims the poller and blocks in `kevent` /
//! `epoll_wait`; every other parked worker blocks on a condvar. A wake for a
//! worker sleeping on the poller is delivered through the poller's own waker
//! fd (`EVFILT_USER` / `eventfd`) so it interrupts the kernel wait; a wake
//! for a condvar sleeper uses the condvar.

use std::io;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use slab::Slab;

use crate::sys::Fd;

use crate::sys::{set_nonblocking, Event, Interest, Poller, PollerImpl, Ready, Token};
use crate::time::TimerShared;
use crate::util::{Linked, LinkedList, Pointers};

/// Packed `ScheduledIo::packed` layout:
/// `[generation : 32][readiness : 16][shutdown : 1]`
const SHUTDOWN_BIT: u64 = 1 << 16;
const READY_SHIFT: u32 = 17;
const READY_MASK: u64 = 0xFFFF << READY_SHIFT;
const GEN_SHIFT: u32 = 33;
const CONDVAR_PARK_TIMEOUT: Duration = Duration::from_millis(1);

/// A registered fd: one slab entry per fd, addressed by the low 32 bits of
/// the poller token. `packed` carries the registration generation (fd-reuse
/// safety), the current readiness, and the driver shutdown flag in one
/// atomic word.
pub(crate) struct ScheduledIo {
    packed: AtomicU64,
    waiters: Mutex<Waiters>,
    slab_index: AtomicUsize,
    fd: AtomicI64,
}

struct Waiters {
    /// The hot reader and writer wakers, one per direction. Waking the wrong
    /// direction's task is a hang, so the two are strictly separated.
    reader: Option<NonNull<Waiter>>,
    writer: Option<NonNull<Waiter>>,
    /// Overflow waiters when a direction's slot is occupied. Nodes are
    /// `Arc<Waiter>` managed by raw pointer: the slot or list owns one
    /// strong reference (leaked as a raw pointer, restored with
    /// `Arc::from_raw` on removal) while the waiting future owns a second
    /// for identity.
    list: LinkedList<Waiter>,
}

pub(crate) struct Waiter {
    waker: Waker,
    interests: Interest,
    task: crate::instrument::TaskId,
    links: Pointers<Waiter>,
}

impl Linked for Waiter {
    fn pointers(&self) -> &Pointers<Self> {
        &self.links
    }
}

fn interest_satisfied(interest: Interest, ready: Ready) -> bool {
    match (interest.is_readable(), interest.is_writable()) {
        (true, false) => ready.is_readable(),
        (false, true) => ready.is_writable(),
        (true, true) => ready.is_readable() || ready.is_writable(),
        (false, false) => false,
    }
}

fn take_matching(slot: &mut Option<NonNull<Waiter>>, ready: Ready) -> Option<Arc<Waiter>> {
    let node = slot.take()?;
    // SAFETY: `node` is a leaked reference owned by this slot.
    if unsafe { interest_satisfied(node.as_ref().interests, ready) } {
        // SAFETY: `node` remains owned by the slot until this conversion.
        Some(unsafe { from_raw(node) })
    } else {
        *slot = Some(node);
        None
    }
}

// SAFETY: `Waiter` is only ever accessed under the `ScheduledIo::waiters`
// mutex; its raw link pointers refer to other live waiters guarded by the
// same lock, and the embedded `Waker` is Send + Sync.
unsafe impl Send for Waiter {}
// SAFETY: see `Send`; all mutable access is serialized by the mutex.
unsafe impl Sync for Waiter {}

impl Waiters {
    fn new() -> Waiters {
        Waiters {
            reader: None,
            writer: None,
            list: LinkedList::new(),
        }
    }
}

// SAFETY: `ScheduledIo`'s mutable state is either the packed readiness
// atomic or the `waiters` Mutex; the raw waiter pointers inside only ever
// reference live, Arc-managed nodes guarded by that same Mutex.
unsafe impl Send for ScheduledIo {}
// SAFETY: see `Send`; sharing a `ScheduledIo` only exposes atomic reads and
// Mutex-serialized accesses.
unsafe impl Sync for ScheduledIo {}

/// Raw pointer to a live waiter, without retagging its interior links.
fn as_raw(waiter: &Arc<Waiter>) -> NonNull<Waiter> {
    // SAFETY: the caller keeps the `Arc` alive and uses the pointer only
    // while holding the waiters lock; `Arc::as_ptr` performs no retag.
    unsafe { NonNull::new_unchecked(Arc::as_ptr(waiter).cast_mut()) }
}

/// Leak one Arc reference into a raw pointer owned by a slot or the list.
fn into_raw(waiter: Arc<Waiter>) -> NonNull<Waiter> {
    let ptr = as_raw(&waiter);
    std::mem::forget(waiter);
    ptr
}

/// Restore the leaked reference (the reverse of `into_raw`).
unsafe fn from_raw(ptr: NonNull<Waiter>) -> Arc<Waiter> {
    // SAFETY: callers only hand back pointers produced by `into_raw` that
    // are still owned by the slot or list.
    unsafe { Arc::from_raw(ptr.as_ptr()) }
}

impl ScheduledIo {
    fn new(generation: u32, fd: Fd) -> ScheduledIo {
        ScheduledIo {
            packed: AtomicU64::new((generation as u64) << GEN_SHIFT),
            waiters: Mutex::new(Waiters::new()),
            slab_index: AtomicUsize::new(usize::MAX),
            fd: AtomicI64::new(fd as i64),
        }
    }

    fn generation(&self) -> u32 {
        (self.packed.load(Ordering::Relaxed) >> GEN_SHIFT) as u32
    }

    fn fd(&self) -> i32 {
        self.fd.load(Ordering::Relaxed) as i32
    }

    /// Check whether `interest` is currently satisfied, without waiting.
    pub(crate) fn readiness(self: &Arc<Self>, interest: Interest) -> Option<ReadyEvent> {
        let packed = self.packed.load(Ordering::Acquire);
        if packed & SHUTDOWN_BIT != 0 {
            return Some(ReadyEvent {
                ready: Ready::READ_CLOSED
                    .union(Ready::WRITE_CLOSED)
                    .union(Ready::ERROR),
                generation: self.generation(),
                scheduled: Arc::clone(self),
            });
        }
        let ready = Ready::from_bits(((packed & READY_MASK) >> READY_SHIFT) as u16);
        let satisfied = match (interest.is_readable(), interest.is_writable()) {
            (true, false) => ready.is_readable(),
            (false, true) => ready.is_writable(),
            (true, true) => ready.is_readable() || ready.is_writable(),
            (false, false) => false,
        };
        satisfied.then(|| ReadyEvent {
            ready,
            generation: self.generation(),
            scheduled: Arc::clone(self),
        })
    }

    /// Set readiness bits and wake matching waiters. The bits are stored while
    /// holding the waiters lock so a waiter that registers under the same
    /// lock can never miss an event set after its registration.
    fn set_readiness_and_wake(&self, ready: Ready) -> Vec<crate::instrument::TaskId> {
        let mut waiters = self.waiters.lock().unwrap();
        let packed = self.packed.load(Ordering::Relaxed);
        let ready_bits = ((packed & READY_MASK) >> READY_SHIFT) as u16 | ready.bits();
        self.packed.store(
            (packed & !READY_MASK) | ((ready_bits as u64) << READY_SHIFT),
            Ordering::Release,
        );
        let mut popped = Vec::new();
        let mut woke = Vec::new();
        if let Some(waiter) = take_matching(&mut waiters.reader, ready) {
            woke.push(waiter.task);
            popped.push(waiter);
        }
        if let Some(waiter) = take_matching(&mut waiters.writer, ready) {
            woke.push(waiter.task);
            popped.push(waiter);
        }
        // Preserve overflow waiters whose interests were not reported.
        let list_len = waiters.list.len();
        for _ in 0..list_len {
            // SAFETY: the list contains only live nodes inserted under this
            // lock, and each pop restores the list's reference.
            let node = unsafe { waiters.list.pop_front().unwrap() };
            // SAFETY: `node` is a leaked reference owned by the list.
            let waiter = unsafe { from_raw(node) };
            if interest_satisfied(waiter.interests, ready) {
                woke.push(waiter.task);
                popped.push(waiter);
            } else {
                // SAFETY: `waiter` remains live and detached from the list.
                unsafe { waiters.list.push_back(into_raw(waiter)) };
            }
        }
        drop(waiters);
        for waiter in popped {
            waiter.waker.wake_by_ref();
        }
        woke
    }

    /// Clear the bits reported by a `ReadyEvent`. Generation-guarded so a
    /// stale event from a previous registration cannot clear a new one's
    /// readiness.
    fn clear_ready(&self, generation: u32, ready: Ready) {
        let mut packed = self.packed.load(Ordering::Acquire);
        loop {
            if (packed >> GEN_SHIFT) as u32 != generation {
                return;
            }
            if ((packed & READY_MASK) >> READY_SHIFT) as u16 != ready.bits() {
                // New readiness bits were reported since the event snapshot:
                // the current state no longer matches what this event cleared,
                // so leave it for the next poll.
                return;
            }
            let new = packed & !((ready.bits() as u64) << READY_SHIFT);
            match self.packed.compare_exchange_weak(
                packed,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => packed = actual,
            }
        }
    }

    /// Register a waker, returning `Err(ReadyEvent)` if readiness became
    /// satisfied while the lock was held — the caller must then return the
    /// event instead of parking.
    fn add_waiter(
        self: &Arc<Self>,
        interest: Interest,
        waker: Waker,
    ) -> Result<WaiterState, ReadyEvent> {
        let mut waiters = self.waiters.lock().unwrap();
        let packed = self.packed.load(Ordering::Relaxed);
        let ready = Ready::from_bits(((packed & READY_MASK) >> READY_SHIFT) as u16);
        let satisfied = match (interest.is_readable(), interest.is_writable()) {
            (true, false) => ready.is_readable(),
            (false, true) => ready.is_writable(),
            (true, true) => ready.is_readable() || ready.is_writable(),
            (false, false) => false,
        };
        if satisfied || packed & SHUTDOWN_BIT != 0 {
            return Err(ReadyEvent {
                ready,
                generation: self.generation(),
                scheduled: Arc::clone(self),
            });
        }
        let waiter = Arc::new(Waiter {
            waker,
            interests: interest,
            task: crate::instrument::TaskId::current(),
            links: Pointers::new(),
        });
        if interest.is_readable() && waiters.reader.is_none() {
            waiters.reader = Some(into_raw(waiter.clone()));
            return Ok(WaiterState::InSlot {
                slot: Slot::Reader,
                waiter,
            });
        }
        if interest.is_writable() && waiters.writer.is_none() {
            waiters.writer = Some(into_raw(waiter.clone()));
            return Ok(WaiterState::InSlot {
                slot: Slot::Writer,
                waiter,
            });
        }
        // SAFETY: the node is a live, pinned `Arc<Waiter>`; the list takes
        // over one Arc reference (recovered with `from_raw` on pop).
        unsafe { waiters.list.push_back(into_raw(waiter.clone())) };
        Ok(WaiterState::InList { waiter })
    }

    fn remove_slot_waiter(&self, slot: Slot, waiter: &Arc<Waiter>) {
        let mut waiters = self.waiters.lock().unwrap();
        let entry = match slot {
            Slot::Reader => &mut waiters.reader,
            Slot::Writer => &mut waiters.writer,
        };
        if let Some(registered) = entry {
            if *registered == as_raw(waiter) {
                // SAFETY: `registered` is the leaked reference the slot
                // owns; dropping it returns our own reference.
                unsafe { std::mem::drop(from_raw(*registered)) };
                *entry = None;
            }
        }
    }

    fn remove_list_waiter(&self, waiter: &Arc<Waiter>) {
        let mut waiters = self.waiters.lock().unwrap();
        if waiter.links.is_linked() {
            let ptr = as_raw(waiter);
            // SAFETY: `waiter` is a live node linked in this list; removing
            // restores the list's reference, which we drop right away.
            unsafe { waiters.list.remove(ptr) };
            // SAFETY: `ptr` is the leaked reference the list owned.
            unsafe { std::mem::drop(from_raw(ptr)) };
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Slot {
    Reader,
    Writer,
}

/// Where a `Readiness` future parked its waker.
pub(crate) enum WaiterState {
    Unregistered,
    InSlot {
        slot: crate::io::driver::Slot,
        waiter: Arc<Waiter>,
    },
    InList {
        waiter: Arc<Waiter>,
    },
}

/// The future returned by `Registration::readiness`.
pub struct Readiness {
    scheduled: Arc<ScheduledIo>,
    interests: Interest,
    state: WaiterState,
}

impl Readiness {
    pub(crate) fn new(scheduled: Arc<ScheduledIo>, interests: Interest) -> Readiness {
        Readiness {
            scheduled,
            interests,
            state: WaiterState::Unregistered,
        }
    }

    fn detach(&mut self) {
        match &self.state {
            WaiterState::Unregistered => {}
            WaiterState::InSlot { slot, waiter } => {
                self.scheduled.remove_slot_waiter(*slot, waiter);
            }
            WaiterState::InList { waiter } => self.scheduled.remove_list_waiter(waiter),
        }
        self.state = WaiterState::Unregistered;
    }
}

impl std::future::Future for Readiness {
    type Output = ReadyEvent;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<ReadyEvent> {
        let this = self.get_mut();
        if let Some(event) = this.scheduled.readiness(this.interests) {
            this.detach();
            return Poll::Ready(event);
        }
        let waker_changed = match &this.state {
            WaiterState::Unregistered => false,
            WaiterState::InSlot { waiter, .. } | WaiterState::InList { waiter } => {
                !waiter.waker.will_wake(cx.waker())
            }
        };
        if waker_changed {
            // The future moved to a different task (e.g. a new executor):
            // unregister and re-register with the new waker.
            this.detach();
        }
        if let WaiterState::Unregistered = this.state {
            match this
                .scheduled
                .add_waiter(this.interests, cx.waker().clone())
            {
                Ok(state) => {
                    this.state = state;
                    Poll::Pending
                }
                Err(event) => Poll::Ready(event),
            }
        } else {
            // Dispatch removes a waiter before waking it. Re-register after
            // any poll that finds no readiness so a consumed or unrelated
            // event cannot leave this future parked with stale state.
            this.detach();
            match this
                .scheduled
                .add_waiter(this.interests, cx.waker().clone())
            {
                Ok(state) => {
                    this.state = state;
                    Poll::Pending
                }
                Err(event) => Poll::Ready(event),
            }
        }
    }
}

impl Drop for Readiness {
    fn drop(&mut self) {
        self.detach();
    }
}

/// The outcome of a successful `readiness().await`.
pub struct ReadyEvent {
    pub(crate) ready: Ready,
    pub(crate) generation: u32,
    scheduled: Arc<ScheduledIo>,
}

impl ReadyEvent {
    /// The current readiness bits.
    pub fn ready(&self) -> Ready {
        self.ready
    }

    /// Clear the reported readiness so the next `readiness().await` parks
    /// instead of spinning. Essential because readiness reports are
    /// spurious: the condition may have been consumed by another task before
    /// this one ran.
    pub fn clear_ready(&self) {
        self.scheduled.clear_ready(self.generation, self.ready);
    }
}

/// The per-runtime driver shared by every worker and registration.
pub(crate) struct DriverShared {
    poller: PollerImpl,
    slab: Mutex<Slab<Arc<ScheduledIo>>>,
    next_generation: AtomicU32,
    events: Mutex<Vec<Event>>,
    park: Mutex<ParkState>,
    park_condvar: Condvar,
    wake_pending: AtomicBool,
    timer: Arc<TimerShared>,
}

struct ParkState {
    /// The worker currently inside `poller.wait`, if any.
    holder: Option<usize>,
    /// How many workers are blocked on the condvar.
    waiters: usize,
}

// SAFETY: `DriverShared` owns the poller (a Send + Sync raw-fd wrapper) and
// every other mutable field sits behind a Mutex or an atomic.
unsafe impl Send for DriverShared {}
// SAFETY: see `Send`; the poller is thread-safe and all interior state is
// synchronized.
unsafe impl Sync for DriverShared {}

impl DriverShared {
    pub(crate) fn new() -> io::Result<Arc<DriverShared>> {
        let poller = PollerImpl::new()?;
        Ok(Arc::new_cyclic(|weak: &Weak<DriverShared>| {
            let weak = weak.clone();
            let notify = Arc::new(move || {
                if let Some(driver) = weak.upgrade() {
                    if let Err(error) = driver.poller.wake() {
                        tracing::warn!(?error, "eddy: driver wake failed");
                    }
                }
            });
            DriverShared {
                poller,
                slab: Mutex::new(Slab::with_capacity(64)),
                next_generation: AtomicU32::new(1),
                events: Mutex::new(Vec::with_capacity(64)),
                park: Mutex::new(ParkState {
                    holder: None,
                    waiters: 0,
                }),
                park_condvar: Condvar::new(),
                wake_pending: AtomicBool::new(false),
                timer: TimerShared::new(notify),
            }
        }))
    }

    /// Register an fd. The slab slot becomes the poller token.
    pub(crate) fn register(&self, fd: Fd, interest: Interest) -> io::Result<Arc<ScheduledIo>> {
        set_nonblocking(fd)?;
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let scheduled = Arc::new(ScheduledIo::new(generation, fd));
        let index = {
            let mut slab = self.slab.lock().unwrap();
            slab.insert(scheduled.clone())
        };
        scheduled.slab_index.store(index, Ordering::Release);
        let token = Token::new(index, generation);
        if let Err(error) = self.poller.register(fd, token, interest) {
            scheduled.slab_index.store(usize::MAX, Ordering::Release);
            self.slab.lock().unwrap().remove(index);
            return Err(error);
        }
        Ok(scheduled)
    }

    /// Rearm an edge-triggered registration after its owner has attempted an
    /// operation. The generation check keeps a late operation from touching a
    /// slab slot that has already been reused.
    pub(crate) fn reregister(
        &self,
        scheduled: &ScheduledIo,
        fd: Fd,
        interest: Interest,
    ) -> io::Result<()> {
        let index = scheduled.slab_index.load(Ordering::Acquire);
        if index == usize::MAX {
            return Ok(());
        }
        let slab = self.slab.lock().unwrap();
        if !slab
            .get(index)
            .is_some_and(|entry| std::ptr::eq(entry.as_ref(), scheduled))
        {
            return Ok(());
        }
        self.poller
            .reregister(fd, Token::new(index, scheduled.generation()), interest)
    }

    /// Deregister an fd (called from `Registration::drop`). A stale event
    /// already in flight for the old token is discarded by the dispatcher:
    /// either the slab slot is free, or it holds a later registration with a
    /// different generation.
    pub(crate) fn deregister(&self, scheduled: &ScheduledIo, fd: Fd) {
        let _ = self.poller.deregister(fd);
        let index = scheduled.slab_index.swap(usize::MAX, Ordering::AcqRel);
        if index == usize::MAX {
            return;
        }
        let mut slab = self.slab.lock().unwrap();
        if slab
            .get(index)
            .is_some_and(|entry| std::ptr::eq(entry.as_ref(), scheduled))
        {
            slab.remove(index);
        }
    }

    /// Park this worker. The first worker to arrive blocks in the kernel wait
    /// and then dispatches; everyone else blocks on the condvar. The timer
    /// wheel supplies the kernel wait timeout.
    pub(crate) fn park_worker(&self, id: usize) {
        let mut state = self.park.lock().unwrap();
        if state.holder.is_none() {
            state.holder = Some(id);
            drop(state);
            if self.timer.is_paused() {
                // Paused time: never block in the kernel; advance the clock
                // to the next timer deadline instead. The worker loop
                // re-checks its queues after park returns.
                self.wake_pending.store(false, Ordering::Release);
                self.timer.paused_advance();
            } else {
                // If a wake arrived before we claimed the holder, do not
                // block: drain with a zero timeout (H1).
                let timeout = if self.wake_pending.swap(false, Ordering::AcqRel) {
                    Some(Duration::ZERO)
                } else {
                    self.timer.next_timeout()
                };
                let events = {
                    let mut buffer = self.events.lock().unwrap();
                    let _ = self.poller.wait(&mut buffer, timeout);
                    std::mem::take(&mut *buffer)
                };
                self.timer.advance_to_now();
                self.dispatch(&events);
            }
            let mut state = self.park.lock().unwrap();
            state.holder = None;
            // A condvar sleeper may now take over as the driver holder.
            self.park_condvar.notify_one();
        } else {
            state.waiters += 1;
            // The timeout closes the race where a task arrives after the
            // worker's final work check but before it increments waiters.
            let (mut state, _) = self
                .park_condvar
                .wait_timeout(state, CONDVAR_PARK_TIMEOUT)
                .unwrap();
            state.waiters -= 1;
        }
    }

    /// Make sure the driver re-runs so a task routed to the global injector
    /// (or to a specific worker) is serviced. Called after routing a task.
    ///
    /// The wake must not depend on `id`'s driver state: the target thread may
    /// be blocked in `thread::park()` (a nested `block_on`), not in the
    /// driver at all. The injector is shared, so interrupting whichever
    /// worker holds the kernel wait — or a condvar sleeper — is sufficient.
    pub(crate) fn unpark_worker(&self, id: usize) {
        self.wake_pending.store(true, Ordering::Release);
        let state = self.park.lock().unwrap();
        if state.waiters > 0 {
            drop(state);
            self.park_condvar.notify_one();
        } else {
            // The holder, the target between re-check and holder claim, or a
            // different worker holding the kernel wait: one spurious poller
            // return and a work re-check is harmless (H1) and closes the
            // window where the target parks outside the driver.
            let _ = id;
            drop(state);
            if let Err(error) = self.poller.wake() {
                tracing::warn!(?error, "eddy: driver wake failed");
            }
        }
    }

    /// Wake every parked worker (shutdown path): the condvar sleepers and
    /// the kernel wait of the driver holder.
    pub(crate) fn unpark_all(&self) {
        self.wake_pending.store(true, Ordering::Release);
        self.park_condvar.notify_all();
        if let Err(error) = self.poller.wake() {
            tracing::warn!(?error, "eddy: driver wake failed");
        }
    }

    pub(crate) fn timer_driver(&self) -> Arc<TimerShared> {
        self.timer.clone()
    }

    /// Map kernel events to registrations and wake the matching waiters.
    fn dispatch(&self, events: &[Event]) {
        for event in events {
            let token = event.token;
            let Some(scheduled) = self.slab.lock().unwrap().get(token.index()).cloned() else {
                continue;
            };
            if scheduled.generation() != token.generation() {
                // A stale event for an fd whose slab slot was reused.
                continue;
            }
            let woke = scheduled.set_readiness_and_wake(event.ready);
            crate::instrument::emit(|| crate::instrument::RuntimeEvent::IoReady {
                fd: scheduled.fd(),
                readiness: if event.ready.is_readable() && event.ready.is_writable() {
                    "readable|writable"
                } else if event.ready.is_readable() {
                    "readable"
                } else if event.ready.is_writable() {
                    "writable"
                } else if event.ready.is_error() {
                    "error"
                } else {
                    "closed"
                },
                woke,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clear_ready_does_not_consume_a_freshly_reported_event() {
        let scheduled = Arc::new(ScheduledIo::new(7, 0));
        scheduled.set_readiness_and_wake(Ready::READABLE);
        let event = scheduled
            .readiness(Interest::READABLE)
            .expect("readiness reported");
        assert_eq!(event.ready(), Ready::READABLE);
        scheduled.set_readiness_and_wake(Ready::WRITABLE);
        event.clear_ready();
        let now = scheduled.readiness(Interest::WRITABLE);
        assert_eq!(
            now.map(|event| event.ready()),
            Some(Ready::READABLE.union(Ready::WRITABLE))
        );
        let stale = scheduled.readiness(Interest::READABLE).unwrap();
        stale.clear_ready();
        assert!(scheduled.readiness(Interest::WRITABLE).is_none());
        assert!(scheduled.readiness(Interest::READABLE).is_none());
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use std::future::Future;
    use std::mem;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    struct WakeCount(loom::sync::atomic::AtomicUsize);

    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        // SAFETY: every pointer passed to this vtable is an Arc allocation
        // created by `counting_waker` or this function.
        let current = unsafe { loom::sync::Arc::from_raw(data as *const WakeCount) };
        let cloned = current.clone();
        mem::forget(current);
        RawWaker::new(loom::sync::Arc::into_raw(cloned) as *const (), &WAKE_VTABLE)
    }

    unsafe fn wake(data: *const ()) {
        // SAFETY: this call consumes the Arc reference owned by the waker.
        let count = unsafe { loom::sync::Arc::from_raw(data as *const WakeCount) };
        count.0.fetch_add(1, loom::sync::atomic::Ordering::SeqCst);
    }

    unsafe fn wake_by_ref(data: *const ()) {
        // SAFETY: the waker keeps this Arc alive for the duration of the call.
        let count = unsafe { &*(data as *const WakeCount) };
        count.0.fetch_add(1, loom::sync::atomic::Ordering::SeqCst);
    }

    unsafe fn drop_waker(data: *const ()) {
        // SAFETY: this call consumes the Arc reference owned by the waker.
        drop(unsafe { loom::sync::Arc::from_raw(data as *const WakeCount) });
    }

    static WAKE_VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

    fn counting_waker(count: loom::sync::Arc<WakeCount>) -> Waker {
        // SAFETY: `WAKE_VTABLE` matches the Arc data pointer and owns one Arc
        // reference until the resulting Waker is dropped.
        unsafe {
            Waker::from_raw(RawWaker::new(
                loom::sync::Arc::into_raw(count) as *const (),
                &WAKE_VTABLE,
            ))
        }
    }

    #[test]
    fn readiness_registration_racing_with_dispatch_never_loses_a_wake() {
        loom::model(|| {
            let scheduled = Arc::new(ScheduledIo::new(1, 0));
            let dispatch_scheduled = scheduled.clone();
            let wakes = loom::sync::Arc::new(WakeCount(loom::sync::atomic::AtomicUsize::new(0)));
            let waker = counting_waker(wakes.clone());
            let mut readiness = Readiness::new(scheduled, Interest::READABLE);
            let dispatch = loom::thread::spawn(move || {
                dispatch_scheduled.set_readiness_and_wake(Ready::READABLE);
            });

            let first = Pin::new(&mut readiness).poll(&mut Context::from_waker(&waker));
            dispatch.join().unwrap();

            match first {
                Poll::Ready(event) => {
                    assert!(event.ready().is_readable());
                    assert_eq!(wakes.0.load(loom::sync::atomic::Ordering::SeqCst), 0);
                }
                Poll::Pending => {
                    assert_eq!(
                        wakes.0.load(loom::sync::atomic::Ordering::SeqCst),
                        1,
                        "registered readiness waiter was not woken"
                    );
                    let second = Pin::new(&mut readiness).poll(&mut Context::from_waker(&waker));
                    assert!(matches!(second, Poll::Ready(event) if event.ready().is_readable()));
                }
            }
        });
    }
}
