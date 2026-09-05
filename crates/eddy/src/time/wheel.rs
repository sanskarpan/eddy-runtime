//! Hierarchical hashed timing wheel.
//!
//! The wheel has six levels of 64 slots. A timer is linked into exactly one
//! slot, so insertion and cancellation do not scan the set of pending timers.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::{Duration, Instant};

use crate::instrument;
use crate::util::{Linked, LinkedList, Pointers};

pub(crate) const LEVEL_COUNT: usize = 6;
const SLOT_COUNT: usize = 64;
const SLOT_MASK: u64 = SLOT_COUNT as u64 - 1;
const PENDING_LEVEL: u8 = LEVEL_COUNT as u8;
const UNLINKED_LEVEL: u8 = u8::MAX;

/// One timer node. The wheel owns one leaked `Arc` reference while this node
/// is linked; the future that is waiting owns another reference.
pub(crate) struct TimerEntry {
    id: u64,
    deadline: AtomicU64,
    fired: AtomicBool,
    level: AtomicU8,
    slot: AtomicU8,
    waker: Mutex<Option<Waker>>,
    links: Pointers<TimerEntry>,
}

impl TimerEntry {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn new() -> Arc<TimerEntry> {
        static NEXT_TIMER_ID: AtomicU64 = AtomicU64::new(1);
        Arc::new(TimerEntry {
            id: NEXT_TIMER_ID.fetch_add(1, Ordering::Relaxed),
            deadline: AtomicU64::new(0),
            fired: AtomicBool::new(false),
            level: AtomicU8::new(UNLINKED_LEVEL),
            slot: AtomicU8::new(0),
            waker: Mutex::new(None),
            links: Pointers::new(),
        })
    }

    pub(crate) fn is_fired(&self) -> bool {
        self.fired.load(Ordering::Acquire)
    }

    pub(crate) fn reset(&self) {
        self.fired.store(false, Ordering::Release);
    }

    pub(crate) fn clone_waker(&self) -> Option<Waker> {
        self.waker.lock().unwrap().clone()
    }
}

impl Linked for TimerEntry {
    fn pointers(&self) -> &Pointers<Self> {
        &self.links
    }
}

// SAFETY: links are accessed only while the owning TimerShared wheel mutex is
// held; all other fields are atomic or mutex-protected.
unsafe impl Send for TimerEntry {}
// SAFETY: see the Send impl above.
unsafe impl Sync for TimerEntry {}

fn into_raw(entry: Arc<TimerEntry>) -> std::ptr::NonNull<TimerEntry> {
    // SAFETY: the Arc is leaked here and only restored by `from_raw`, so the
    // allocation outlives every list access. `Arc::as_ptr` performs no retag,
    // keeping the node's interior links writable through shared references.
    let ptr = unsafe { std::ptr::NonNull::new_unchecked(Arc::as_ptr(&entry).cast_mut()) };
    std::mem::forget(entry);
    ptr
}

unsafe fn from_raw(ptr: std::ptr::NonNull<TimerEntry>) -> Arc<TimerEntry> {
    // SAFETY: callers return only references previously leaked by into_raw.
    unsafe { Arc::from_raw(ptr.as_ptr()) }
}

struct Level {
    occupied: u64,
    slots: [LinkedList<TimerEntry>; SLOT_COUNT],
}

impl Level {
    fn new() -> Level {
        Level {
            occupied: 0,
            slots: std::array::from_fn(|_| LinkedList::new()),
        }
    }
}

/// Hierarchical hashed timing wheel. `elapsed` is milliseconds since the
/// associated `TimerShared` was created.
///
/// Time is bucketed per level, so a timer may cause an early driver wake at a
/// bucket boundary. `advance_to` compares every candidate's exact deadline to
/// the supplied clock before moving it to `pending`; the public timer therefore
/// never fires early. The extra wake is the bounded cost of the low-resolution
/// wheel.
pub(crate) struct Wheel {
    pub(crate) elapsed: u64,
    levels: Box<[Level; LEVEL_COUNT]>,
    pending: LinkedList<TimerEntry>,
}

// SAFETY: the wheel's intrusive links are only accessed by the owner of the
// TimerShared wheel mutex, and every pointed-to entry is Arc-managed.
unsafe impl Send for Wheel {}

impl Wheel {
    pub(crate) fn new() -> Wheel {
        Wheel {
            elapsed: 0,
            levels: Box::new(std::array::from_fn(|_| Level::new())),
            pending: LinkedList::new(),
        }
    }

    fn level_for(&self, when: u64) -> usize {
        let masked = (when ^ self.elapsed) | SLOT_MASK;
        let significant = 63 - masked.leading_zeros() as usize;
        (significant / 6).min(LEVEL_COUNT - 1)
    }

    fn insert(&mut self, entry: Arc<TimerEntry>) {
        debug_assert!(!entry.links.is_linked());
        let deadline = entry.deadline.load(Ordering::Acquire);
        if deadline <= self.elapsed {
            entry.level.store(PENDING_LEVEL, Ordering::Release);
            // SAFETY: the entry is detached and the wheel takes one Arc ref.
            unsafe { self.pending.push_back(into_raw(entry)) };
            return;
        }

        let level = self.level_for(deadline);
        let slot = ((deadline >> (level * 6)) & SLOT_MASK) as usize;
        entry.level.store(level as u8, Ordering::Release);
        entry.slot.store(slot as u8, Ordering::Release);
        self.levels[level].occupied |= 1u64 << slot;
        // SAFETY: the entry is detached and the wheel takes one Arc ref.
        unsafe { self.levels[level].slots[slot].push_back(into_raw(entry)) };
    }

    pub(crate) fn remove(&mut self, entry: &Arc<TimerEntry>) -> bool {
        if !entry.links.is_linked() {
            return false;
        }

        let level = entry.level.load(Ordering::Acquire);
        let slot = entry.slot.load(Ordering::Acquire) as usize;
        // SAFETY: `entry` is a live Arc and `Arc::as_ptr` performs no retag,
        // so the node's interior links stay writable (see `into_raw`).
        let ptr = unsafe { std::ptr::NonNull::new_unchecked(Arc::as_ptr(entry).cast_mut()) };
        if level == PENDING_LEVEL {
            // SAFETY: the entry is linked in the pending list.
            unsafe { self.pending.remove(ptr) };
        } else if (level as usize) < LEVEL_COUNT && slot < SLOT_COUNT {
            // SAFETY: the entry is linked in this level and slot.
            unsafe { self.levels[level as usize].slots[slot].remove(ptr) };
            if self.levels[level as usize].slots[slot].is_empty() {
                self.levels[level as usize].occupied &= !(1u64 << slot);
            }
        } else {
            debug_assert!(false, "eddy: timer has an invalid wheel location");
            return false;
        }
        entry.level.store(UNLINKED_LEVEL, Ordering::Release);
        // SAFETY: the list's leaked Arc reference belongs to this entry.
        unsafe { std::mem::drop(from_raw(ptr)) };
        true
    }

    fn take_slot(&mut self, level: usize, slot: usize) -> Vec<Arc<TimerEntry>> {
        let list = &mut self.levels[level].slots[slot];
        let mut entries = Vec::new();
        // SAFETY: every node popped is owned by this list.
        while let Some(ptr) = unsafe { list.pop_front() } {
            // SAFETY: ptr is the list's leaked Arc reference.
            let entry = unsafe { from_raw(ptr) };
            entry.level.store(UNLINKED_LEVEL, Ordering::Release);
            entries.push(entry);
        }
        self.levels[level].occupied &= !(1u64 << slot);
        entries
    }

    fn take_pending(&mut self) -> Vec<Arc<TimerEntry>> {
        let mut entries = Vec::new();
        // SAFETY: every node popped is owned by the pending list.
        while let Some(ptr) = unsafe { self.pending.pop_front() } {
            // SAFETY: ptr is the list's leaked Arc reference.
            let entry = unsafe { from_raw(ptr) };
            entry.level.store(UNLINKED_LEVEL, Ordering::Release);
            entries.push(entry);
        }
        entries
    }

    /// Return the next wheel boundary at which work must be processed.
    pub(crate) fn next_event_at(&self) -> Option<u64> {
        if !self.pending.is_empty() {
            return Some(self.elapsed);
        }

        let mut next = None;
        for level in 0..LEVEL_COUNT {
            let occupied = self.levels[level].occupied;
            if occupied == 0 {
                continue;
            }
            let shift = level * 6;
            let current = ((self.elapsed >> shift) & SLOT_MASK) as usize;
            let delta = if current < SLOT_COUNT - 1 {
                let after = occupied >> (current + 1);
                if after != 0 {
                    after.trailing_zeros() as usize + 1
                } else {
                    let lower = occupied & ((1u64 << current) - 1);
                    if lower != 0 {
                        SLOT_COUNT - current + lower.trailing_zeros() as usize
                    } else {
                        SLOT_COUNT
                    }
                }
            } else {
                occupied.trailing_zeros() as usize + 1
            };
            let bucket = (self.elapsed >> shift).saturating_add(delta as u64);
            let at = bucket.checked_shl(shift as u32).unwrap_or(u64::MAX);
            next = Some(next.map_or(at, |current_next: u64| current_next.min(at)));
        }
        next
    }

    /// Move the wheel forward and leave expired entries in `pending`.
    pub(crate) fn advance_to(&mut self, now: u64) {
        if now <= self.elapsed {
            return;
        }

        loop {
            let Some(next) = self.next_event_at() else {
                self.elapsed = now;
                break;
            };
            if next > now {
                self.elapsed = now;
                break;
            }

            self.elapsed = next;
            for level in (1..LEVEL_COUNT).rev() {
                let slot = ((self.elapsed >> (level * 6)) & SLOT_MASK) as usize;
                if self.levels[level].occupied & (1u64 << slot) == 0 {
                    continue;
                }
                for entry in self.take_slot(level, slot) {
                    self.insert(entry);
                }
            }

            let slot = (self.elapsed & SLOT_MASK) as usize;
            if self.levels[0].occupied & (1u64 << slot) != 0 {
                for entry in self.take_slot(0, slot) {
                    if entry.deadline.load(Ordering::Acquire) <= now {
                        entry.level.store(PENDING_LEVEL, Ordering::Release);
                        // SAFETY: the entry is detached and the pending list
                        // takes one Arc reference.
                        unsafe { self.pending.push_back(into_raw(entry)) };
                    } else {
                        self.insert(entry);
                    }
                }
            }

            if !self.pending.is_empty() {
                break;
            }
            if self.next_event_at().is_none() {
                self.elapsed = now;
                break;
            }
        }
    }
}

/// Runtime-shared timer state. The notifier interrupts the runtime's park
/// operation when a newly armed timer may be earlier than its current timeout.
pub(crate) struct TimerShared {
    origin: Instant,
    wheel: Mutex<Wheel>,
    notify: Arc<dyn Fn() + Send + Sync>,
    paused: AtomicBool,
    paused_elapsed: AtomicU64,
    auto_advance: AtomicBool,
}

impl TimerShared {
    pub(crate) fn new(notify: Arc<dyn Fn() + Send + Sync>) -> Arc<TimerShared> {
        Arc::new(TimerShared {
            origin: Instant::now(),
            wheel: Mutex::new(Wheel::new()),
            notify,
            paused: AtomicBool::new(false),
            paused_elapsed: AtomicU64::new(0),
            auto_advance: AtomicBool::new(false),
        })
    }

    pub(crate) fn now_ms(&self) -> u64 {
        if self.paused.load(Ordering::Acquire) {
            self.paused_elapsed.load(Ordering::Acquire)
        } else {
            self.real_elapsed_ms()
        }
    }

    /// The clock as an `Instant`, for deadline arithmetic (paused-aware).
    pub(crate) fn now_instant(&self) -> Instant {
        self.origin + Duration::from_millis(self.now_ms())
    }

    fn real_elapsed_ms(&self) -> u64 {
        self.origin.elapsed().as_millis().min(u64::MAX as u128) as u64
    }

    /// Freeze the clock at its current reading.
    pub(crate) fn pause(&self) {
        if !self.paused.swap(true, Ordering::AcqRel) {
            self.paused_elapsed
                .store(self.real_elapsed_ms(), Ordering::Release);
        }
    }

    /// Unfreeze the clock, resuming real elapsed time.
    pub(crate) fn resume(&self) {
        self.paused.store(false, Ordering::Release);
    }

    pub(crate) fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    pub(crate) fn set_auto_advance(&self, enabled: bool) {
        self.auto_advance.store(enabled, Ordering::Release);
    }

    /// Move the paused clock forward by `duration`, firing whatever becomes
    /// due.
    pub(crate) fn advance(&self, duration: Duration) {
        assert!(self.is_paused(), "eddy: advance requires paused time");
        let ms = duration
            .as_millis()
            .saturating_add(u128::from(duration.subsec_nanos() % 1_000_000 != 0))
            .min(u64::MAX as u128) as u64;
        self.paused_elapsed.fetch_add(ms, Ordering::AcqRel);
        self.advance_to_now();
    }

    /// Advance the paused clock to the next timer deadline, or by the 1 ms
    /// auto-advance step when nothing is pending. Returns whether the clock
    /// moved. Never sleeps; park loops call this instead of blocking when
    /// time is paused.
    pub(crate) fn paused_advance(&self) -> bool {
        if !self.is_paused() {
            return false;
        }
        let step = match self.next_timeout() {
            Some(timeout) => timeout,
            None if self.auto_advance.load(Ordering::Acquire) => Duration::from_millis(1),
            None => return false,
        };
        self.advance(step);
        true
    }

    pub(crate) fn arm(&self, entry: &Arc<TimerEntry>, deadline: Instant, waker: Waker) -> bool {
        let already_due = deadline <= self.now_instant();
        let deadline_instant = deadline;
        let deadline = self.instant_to_ms(deadline);
        let mut wheel = self.wheel.lock().unwrap();
        wheel.remove(entry);
        entry.deadline.store(deadline, Ordering::Release);
        entry.fired.store(false, Ordering::Release);
        *entry.waker.lock().unwrap() = Some(waker);
        instrument::emit(|| instrument::RuntimeEvent::TimerSet {
            id: entry.id(),
            deadline: deadline_instant,
            task: instrument::TaskId::current(),
        });
        if already_due || deadline <= wheel.elapsed {
            entry.fired.store(true, Ordering::Release);
            entry.waker.lock().unwrap().take();
            true
        } else {
            wheel.insert(entry.clone());
            drop(wheel);
            (self.notify)();
            false
        }
    }

    pub(crate) fn cancel(&self, entry: &Arc<TimerEntry>) {
        let mut wheel = self.wheel.lock().unwrap();
        wheel.remove(entry);
        entry.waker.lock().unwrap().take();
        instrument::emit(|| instrument::RuntimeEvent::TimerCancelled { id: entry.id() });
    }

    pub(crate) fn advance_to_now(&self) {
        let now = self.now_ms();
        let mut wheel = self.wheel.lock().unwrap();
        wheel.advance_to(now);
        let entries = wheel.take_pending();
        let mut wakers = Vec::new();
        let mut fired = Vec::new();
        for entry in entries {
            entry.fired.store(true, Ordering::Release);
            let deadline = entry.deadline.load(Ordering::Acquire);
            fired.push((entry.id(), now.saturating_sub(deadline)));
            if let Some(waker) = entry.waker.lock().unwrap().take() {
                wakers.push(waker);
            }
        }
        drop(wheel);
        for (id, lateness) in fired {
            instrument::emit(|| instrument::RuntimeEvent::TimerFired {
                id,
                lateness: Duration::from_millis(lateness),
            });
        }
        for waker in wakers {
            waker.wake();
        }
    }

    pub(crate) fn next_timeout(&self) -> Option<Duration> {
        let now = self.now_ms();
        let wheel = self.wheel.lock().unwrap();
        let at = wheel.next_event_at()?;
        if at <= now {
            Some(Duration::ZERO)
        } else {
            Some(Duration::from_millis(at - now))
        }
    }

    fn instant_to_ms(&self, instant: Instant) -> u64 {
        let Some(duration) = instant.checked_duration_since(self.origin) else {
            return 0;
        };
        let millis = duration.as_millis();
        let rounded = millis + u128::from(duration.subsec_nanos() % 1_000_000 != 0);
        rounded.min(u64::MAX as u128) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(deadline: u64) -> Arc<TimerEntry> {
        let entry = TimerEntry::new();
        entry.deadline.store(deadline, Ordering::Release);
        entry
    }

    #[test]
    fn inserts_and_cancels_in_constant_time_structures() {
        let mut wheel = Wheel::new();
        let first = entry(10);
        let second = entry(20);
        wheel.insert(first.clone());
        wheel.insert(second.clone());
        assert!(wheel.remove(&first));
        assert!(!wheel.remove(&first));
        wheel.advance_to(20);
        let fired = wheel.take_pending();
        assert_eq!(fired.len(), 1);
        assert!(Arc::ptr_eq(&fired[0], &second));
    }

    #[test]
    fn one_hundred_thousand_inserts_and_cancels_complete_quickly() {
        let mut wheel = Wheel::new();
        let mut entries = Vec::with_capacity(100_000);
        let started = std::time::Instant::now();
        for i in 0..100_000u64 {
            let item = entry(1 + (i % 50_000));
            wheel.insert(item.clone());
            entries.push(item);
        }
        for item in &entries {
            assert!(wheel.remove(item));
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "insert+cancel of 100k timers took {elapsed:?}, expected O(1) per op"
        );
    }

    #[test]
    fn timers_do_not_fire_before_their_deadline() {
        let mut wheel = Wheel::new();
        let item = entry(10);
        wheel.insert(item.clone());
        wheel.advance_to(9);
        assert!(wheel.take_pending().is_empty());
        wheel.advance_to(10);
        let fired = wheel.take_pending();
        assert_eq!(fired.len(), 1);
        assert!(Arc::ptr_eq(&fired[0], &item));
    }

    #[test]
    fn bucket_boundary_wakes_do_not_fire_a_timer_early() {
        let mut wheel = Wheel::new();
        let item = entry(65);
        wheel.insert(item.clone());
        // Level zero's bucket boundary is an internal wake point, not the
        // timer's exact deadline.
        wheel.advance_to(64);
        assert!(wheel.take_pending().is_empty());
        wheel.advance_to(65);
        assert_eq!(wheel.take_pending().len(), 1);
    }

    #[test]
    fn timers_fire_in_deadline_order() {
        let mut wheel = Wheel::new();
        let late = entry(30);
        let early = entry(5);
        wheel.insert(late.clone());
        wheel.insert(early.clone());
        wheel.advance_to(5);
        let fired = wheel.take_pending();
        assert_eq!(fired.len(), 1);
        assert!(Arc::ptr_eq(&fired[0], &early));
        wheel.advance_to(30);
        let fired = wheel.take_pending();
        assert_eq!(fired.len(), 1);
        assert!(Arc::ptr_eq(&fired[0], &late));
    }

    #[test]
    fn cascades_across_all_levels() {
        let mut wheel = Wheel::new();
        let deadline = 1u64 << 30;
        let entry = entry(deadline);
        wheel.insert(entry.clone());
        wheel.advance_to(deadline);
        let fired = wheel.take_pending();
        assert_eq!(fired.len(), 1);
        assert!(Arc::ptr_eq(&fired[0], &entry));
    }

    #[test]
    fn empty_wheel_jumps_without_scanning_time() {
        let mut wheel = Wheel::new();
        wheel.advance_to(u64::MAX);
        assert_eq!(wheel.elapsed, u64::MAX);
    }
}

#[cfg(all(test, not(loom)))]
mod property_tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;

    fn entry(deadline: u64) -> Arc<TimerEntry> {
        let entry = TimerEntry::new();
        entry.deadline.store(deadline, Ordering::Release);
        entry
    }

    proptest! {
        #[test]
        fn cancelled_entries_never_appear_in_pending(
            deadlines in prop::collection::vec(1u64..128, 1..32),
            cancelled in prop::collection::vec(any::<bool>(), 1..32),
        ) {
            let mut wheel = Wheel::new();
            let entries: Vec<_> = deadlines.into_iter().map(entry).collect();
            for item in &entries {
                wheel.insert(item.clone());
            }

            let mut expected = HashSet::new();
            for (index, item) in entries.iter().enumerate() {
                if cancelled.get(index).copied().unwrap_or(false) {
                    assert!(wheel.remove(item));
                } else {
                    expected.insert(item.id());
                }
            }

            let mut actual = HashSet::new();
            loop {
                wheel.advance_to(128);
                actual.extend(wheel.take_pending().into_iter().map(|item| item.id()));
                if wheel.elapsed >= 128 {
                    break;
                }
            }
            prop_assert_eq!(actual, expected);
        }
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use std::mem;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    struct WakeCount(loom::sync::atomic::AtomicUsize);

    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        // SAFETY: the pointer is an Arc allocation owned by this vtable.
        let current = unsafe { loom::sync::Arc::from_raw(data as *const WakeCount) };
        let cloned = current.clone();
        mem::forget(current);
        RawWaker::new(loom::sync::Arc::into_raw(cloned) as *const (), &WAKE_VTABLE)
    }

    unsafe fn wake(data: *const ()) {
        // SAFETY: wake consumes the Arc reference represented by the waker.
        let count = unsafe { loom::sync::Arc::from_raw(data as *const WakeCount) };
        count.0.fetch_add(1, loom::sync::atomic::Ordering::SeqCst);
    }

    unsafe fn wake_by_ref(data: *const ()) {
        // SAFETY: the waker keeps this allocation alive during the call.
        let count = unsafe { &*(data as *const WakeCount) };
        count.0.fetch_add(1, loom::sync::atomic::Ordering::SeqCst);
    }

    unsafe fn drop_waker(data: *const ()) {
        // SAFETY: drop consumes the Arc reference represented by the waker.
        drop(unsafe { loom::sync::Arc::from_raw(data as *const WakeCount) });
    }

    static WAKE_VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_waker, wake, wake_by_ref, drop_waker);

    fn counting_waker(count: loom::sync::Arc<WakeCount>) -> Waker {
        // SAFETY: the vtable matches the Arc data pointer and owns one Arc ref.
        unsafe {
            Waker::from_raw(RawWaker::new(
                loom::sync::Arc::into_raw(count) as *const (),
                &WAKE_VTABLE,
            ))
        }
    }

    #[test]
    fn timer_fire_racing_with_cancel_wakes_at_most_once() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(|| {
            let entry = TimerEntry::new();
            let wakes = loom::sync::Arc::new(WakeCount(loom::sync::atomic::AtomicUsize::new(0)));
            *entry.waker.lock().unwrap() = Some(counting_waker(wakes.clone()));

            // The wheel itself is covered by the deterministic unit and
            // property tests above. This model isolates the one shared
            // ownership rule at the fire/cancel boundary: exactly one side
            // may claim an armed entry and its waker.
            let armed = loom::sync::Arc::new(loom::sync::Mutex::new(true));

            let fire_armed = armed.clone();
            let fire_entry = entry.clone();
            let fire = loom::thread::spawn(move || {
                let waker = {
                    let mut armed = fire_armed.lock().unwrap();
                    if *armed {
                        *armed = false;
                        fire_entry.fired.store(true, Ordering::Release);
                        fire_entry.waker.lock().unwrap().take()
                    } else {
                        None
                    }
                };
                if let Some(waker) = waker {
                    waker.wake();
                }
            });

            let cancel_armed = armed.clone();
            let cancel_entry = entry.clone();
            let cancel = loom::thread::spawn(move || {
                let mut armed = cancel_armed.lock().unwrap();
                if *armed {
                    *armed = false;
                    cancel_entry.waker.lock().unwrap().take();
                }
            });

            fire.join().unwrap();
            cancel.join().unwrap();
            let count = wakes.0.load(loom::sync::atomic::Ordering::SeqCst);
            assert!(count <= 1, "timer fired more than once");
            assert_eq!(count, if entry.is_fired() { 1 } else { 0 });
        });
    }
}
