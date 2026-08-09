//! Hierarchical hashed timing wheel.
//!
//! The wheel has six levels of 64 slots. A timer is linked into exactly one
//! slot, so insertion and cancellation do not scan the set of pending timers.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::{Duration, Instant};

use crate::util::{Linked, LinkedList, Pointers};

pub(crate) const LEVEL_COUNT: usize = 6;
const SLOT_COUNT: usize = 64;
const SLOT_MASK: u64 = SLOT_COUNT as u64 - 1;
const PENDING_LEVEL: u8 = LEVEL_COUNT as u8;
const UNLINKED_LEVEL: u8 = u8::MAX;

/// One timer node. The wheel owns one leaked `Arc` reference while this node
/// is linked; the future that is waiting owns another reference.
pub(crate) struct TimerEntry {
    deadline: AtomicU64,
    fired: AtomicBool,
    level: AtomicU8,
    slot: AtomicU8,
    waker: Mutex<Option<Waker>>,
    links: Pointers<TimerEntry>,
}

impl TimerEntry {
    pub(crate) fn new() -> Arc<TimerEntry> {
        Arc::new(TimerEntry {
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
}

impl Linked for TimerEntry {
    fn pointers(&self) -> &Pointers<Self> {
        &self.links
    }

    fn pointers_mut(&mut self) -> &mut Pointers<Self> {
        &mut self.links
    }
}

// SAFETY: links are accessed only while the owning TimerShared wheel mutex is
// held; all other fields are atomic or mutex-protected.
unsafe impl Send for TimerEntry {}
// SAFETY: see the Send impl above.
unsafe impl Sync for TimerEntry {}

fn into_raw(entry: Arc<TimerEntry>) -> std::ptr::NonNull<TimerEntry> {
    let ptr = std::ptr::NonNull::from(Arc::as_ref(&entry));
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
        let ptr = std::ptr::NonNull::from(Arc::as_ref(entry));
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
                    if entry.deadline.load(Ordering::Acquire) <= self.elapsed {
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
}

impl TimerShared {
    pub(crate) fn new(notify: Arc<dyn Fn() + Send + Sync>) -> Arc<TimerShared> {
        Arc::new(TimerShared {
            origin: Instant::now(),
            wheel: Mutex::new(Wheel::new()),
            notify,
        })
    }

    pub(crate) fn now_ms(&self) -> u64 {
        self.origin.elapsed().as_millis().min(u64::MAX as u128) as u64
    }

    pub(crate) fn arm(&self, entry: &Arc<TimerEntry>, deadline: Instant, waker: Waker) -> bool {
        let already_due = deadline <= Instant::now();
        let deadline = self.instant_to_ms(deadline);
        let mut wheel = self.wheel.lock().unwrap();
        wheel.remove(entry);
        entry.deadline.store(deadline, Ordering::Release);
        entry.fired.store(false, Ordering::Release);
        *entry.waker.lock().unwrap() = Some(waker);
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
    }

    pub(crate) fn advance_to_now(&self) {
        let now = self.now_ms();
        let mut wheel = self.wheel.lock().unwrap();
        wheel.advance_to(now);
        let entries = wheel.take_pending();
        let mut wakers = Vec::new();
        for entry in entries {
            entry.fired.store(true, Ordering::Release);
            if let Some(waker) = entry.waker.lock().unwrap().take() {
                wakers.push(waker);
            }
        }
        drop(wheel);
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
