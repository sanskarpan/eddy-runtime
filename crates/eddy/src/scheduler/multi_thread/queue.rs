//! A fixed-capacity Chase-Lev local queue.
//!
//! The owner pushes and pops at the tail. Thieves reserve ranges from the
//! head, copy those tasks into their own local queue, and then commit the
//! reservation. `real_head` is the committed head and `steal_head` is the
//! reservation frontier. Packing both into one atomic word lets the owner see
//! a consistent snapshot while a steal is in flight.
//!
//! A claimed-but-uncommitted range is still physically occupied: the thief
//! reads its slots between the claim CAS and the commit, so the owner must
//! never push into it. Commits therefore land **in order**: each thief
//! advances `real_head` from the claim's start to its end, and retries until
//! every earlier claim has committed. This keeps `real_head` from ever
//! passing a slot that a thief has not finished copying.

#![allow(dead_code)]

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;

use crate::loom::sync::atomic::{fence, AtomicU32, AtomicU64, Ordering};
use crate::loom::sync::Arc;

pub(crate) const LOCAL_QUEUE_CAPACITY: usize = 256;
const INDEX_MASK: u32 = LOCAL_QUEUE_CAPACITY as u32 - 1;

/// The owner has exclusive access to `push_back` and `pop`; cloned handles are
/// read-only thieves. The queue is `Send`/`Sync` only when its elements are
/// `Send`, which is the boundary the multi-thread scheduler requires.
pub(crate) struct Local<T: 'static, const CAP: usize = LOCAL_QUEUE_CAPACITY> {
    inner: Arc<Inner<T, CAP>>,
}

struct Inner<T: 'static, const CAP: usize> {
    /// Packed as `[steal_head: 32][real_head: 32]`.
    head: AtomicU64,
    /// Written by the owner and acquired by thieves.
    tail: AtomicU32,
    buffer: Box<[UnsafeCell<MaybeUninit<T>>; CAP]>,
}

// SAFETY: the owner writes a slot before publishing `tail`, and thieves only
// read a slot after claiming it through `head`. The queue never exposes a
// concurrent mutable reference to an element.
unsafe impl<T: Send + 'static, const CAP: usize> Send for Inner<T, CAP> {}
// SAFETY: all cross-thread access is synchronized by the queue protocol above.
unsafe impl<T: Send + 'static, const CAP: usize> Sync for Inner<T, CAP> {}

impl<T: 'static, const CAP: usize> Local<T, CAP> {
    pub(crate) fn new() -> Local<T, CAP> {
        Local {
            inner: Arc::new(Inner {
                head: AtomicU64::new(pack(0, 0)),
                tail: AtomicU32::new(0),
                buffer: Box::new(std::array::from_fn(|_| {
                    UnsafeCell::new(MaybeUninit::uninit())
                })),
            }),
        }
    }

    pub(crate) fn push_back(&mut self, value: T) -> Result<(), T> {
        let tail = self.inner.tail.load(Ordering::Relaxed);
        let head = self.inner.head.load(Ordering::Acquire);
        let (_, real_head) = unpack(head);
        if tail.wrapping_sub(real_head) as usize >= CAP {
            return Err(value);
        }

        // SAFETY: only the owner writes this slot, and the capacity check
        // proves that no live element occupies it. The check uses the
        // *committed* head: a claimed-but-uncommitted range is still being
        // read by its thief, so it counts as occupied until its commit lands.
        unsafe { (*self.slot(tail).get()).write(value) };
        // Release publishes the initialized slot to a thief that acquires the
        // tail after observing a matching head snapshot.
        self.inner
            .tail
            .store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Owner-only insert when the queue is full: move the OLDER half of the
    /// current items into `overflow`, then insert `value` locally.
    ///
    /// The older half is the steal region, so the owner reclaims it through
    /// the same `steal_into` reservation protocol thieves use (which is
    /// already race-safe against in-flight steals). Moving the older half
    /// instead of the new task avoids ping-pong: the injected half drains
    /// other workers' queues while the new task stays on this worker.
    pub(crate) fn push_overflow(&mut self, value: T, overflow: &mut Vec<T>) {
        let mut staging = Local::<T, LOCAL_QUEUE_CAPACITY>::new();
        let mut pending = Some(value);
        let mut unproductive = 0;
        loop {
            match self.push_back(pending.take().expect("eddy: overflow lost the task")) {
                Ok(()) => return,
                Err(value) => {
                    let Some(first) = self.steal_into(&mut staging) else {
                        // L8: every slot is claimed-but-uncommitted, so neither
                        // push nor steal can make progress; commits are
                        // imminent, so yield instead of spinning hot.
                        unproductive += 1;
                        if unproductive >= 16 {
                            std::thread::yield_now();
                            unproductive = 0;
                        }
                        pending = Some(value);
                        continue;
                    };
                    overflow.push(first);
                    while let Some(item) = staging.pop() {
                        overflow.push(item);
                    }
                    unproductive = 0;
                    pending = Some(value);
                }
            }
        }
    }

    pub(crate) fn pop(&mut self) -> Option<T> {
        let tail = self.inner.tail.load(Ordering::Relaxed);
        let initial_head = self.inner.head.load(Ordering::Acquire);
        let (initial_steal_head, _) = unpack(initial_head);
        if tail == initial_steal_head {
            return None;
        }
        let new_tail = tail.wrapping_sub(1);
        self.inner.tail.store(new_tail, Ordering::Relaxed);
        // This fence orders the owner's tail withdrawal against a thief's
        // sequentially-consistent head claim, matching the Chase-Lev pop.
        fence(Ordering::SeqCst);

        let snapshot = self.inner.head.load(Ordering::SeqCst);
        let (steal_head, real_head) = unpack(snapshot);
        let available = tail.wrapping_sub(steal_head) as usize;
        if available == 0 || real_head != steal_head {
            self.inner.tail.store(tail, Ordering::Relaxed);
            return None;
        }

        if available == 1 {
            // The final item is claimed through the same head word as thieves.
            // If the CAS loses, a thief owns it and the owner must restore the
            // tail without reading the slot.
            let desired = pack(steal_head.wrapping_add(1), real_head.wrapping_add(1));
            if self
                .inner
                .head
                .compare_exchange(snapshot, desired, Ordering::SeqCst, Ordering::Relaxed)
                .is_err()
            {
                self.inner.tail.store(tail, Ordering::Relaxed);
                return None;
            }
            // Keep the tail one past the consumed item. This is the empty
            // queue representation: both counters now point at the same
            // logical position, and it also preserves the next push index.
            self.inner.tail.store(tail, Ordering::Relaxed);
        }

        // SAFETY: the owner removed this slot from the queue, and either the
        // non-final path excludes thieves from the tail item or the final CAS
        // exclusively claimed it.
        Some(unsafe { (*self.slot(new_tail).get()).assume_init_read() })
    }

    /// Steal up to half of the available range into `dst`, returning one task
    /// for the worker to poll immediately. The remaining stolen tasks stay in
    /// `dst`'s local queue.
    pub(crate) fn steal_into<const CAP2: usize>(&self, dst: &mut Local<T, CAP2>) -> Option<T> {
        loop {
            let snapshot = self.inner.head.load(Ordering::Acquire);
            let (steal_head, real_head) = unpack(snapshot);
            let tail = self.inner.tail.load(Ordering::Acquire);
            let available = tail.wrapping_sub(steal_head) as usize;
            if available == 0 || available > CAP {
                return None;
            }

            let room = dst.available_space();
            if room == 0 {
                return None;
            }
            let count = available.min(room).min(available.max(1) / 2).max(1);
            let claimed_end = steal_head.wrapping_add(count as u32);
            let desired = pack(claimed_end, real_head);

            // A failed CAS means another thief changed the reservation
            // frontier. Re-read both heads and retry without touching a slot.
            if self
                .inner
                .head
                .compare_exchange(snapshot, desired, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            let mut first = None;
            for offset in 0..count {
                // SAFETY: this range was exclusively reserved by the CAS
                // above, and the release store to `tail` published every slot.
                let value = unsafe {
                    (*self.slot(steal_head.wrapping_add(offset as u32)).get()).assume_init_read()
                };
                if first.is_none() {
                    first = Some(value);
                } else {
                    // `room` was measured before the reservation and `dst`
                    // has no other owner, so this cannot fail.
                    if let Err(_value) = dst.push_back(value) {
                        panic!("eddy: destination queue filled during steal");
                    }
                }
            }

            self.commit_real_head(steal_head, claimed_end);
            return first;
        }
    }

    pub(crate) fn len(&self) -> usize {
        let (_, real_head) = unpack(self.inner.head.load(Ordering::Acquire));
        self.inner
            .tail
            .load(Ordering::Acquire)
            .wrapping_sub(real_head) as usize
    }

    fn available_space(&self) -> usize {
        CAP.saturating_sub(self.len())
    }

    fn slot(&self, index: u32) -> &UnsafeCell<MaybeUninit<T>> {
        &self.inner.buffer[(index & (CAP as u32 - 1)) as usize]
    }

    /// Commit `real_head` forward to `claimed_end` for a claim that started
    /// at `claim_start` (the `steal_head` observed when the claim was made).
    ///
    /// Commits land in order: the CAS only succeeds when `real_head` still
    /// equals `claim_start`, i.e. every earlier claim has already committed.
    /// Otherwise the thief retries. This is what keeps the owner from
    /// reusing slots that an in-flight thief has claimed but not finished
    /// copying — were `real_head` to jump over the claim, the owner would
    /// treat the thief's slots as free and overwrite them mid-copy.
    fn commit_real_head(&self, claim_start: u32, claimed_end: u32) {
        loop {
            let current = self.inner.head.load(Ordering::Acquire);
            let (steal_head, real_head) = unpack(current);
            if real_head == claimed_end {
                return;
            }
            // The CAS succeeds only while `real_head` still equals
            // `claim_start`, i.e. every earlier claim has already committed;
            // otherwise it fails and the loop retries. An earlier claim's
            // commit can therefore never be skipped.
            let expected = pack(steal_head, claim_start);
            let desired = pack(steal_head, claimed_end);
            if self
                .inner
                .head
                .compare_exchange(expected, desired, Ordering::Release, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
            // The commit must wait for an earlier claim to land. Yield so
            // the earlier thief can finish instead of burning the CPU.
            crate::loom::thread::yield_now();
        }
    }
}

impl<T: 'static, const CAP: usize> Clone for Local<T, CAP> {
    fn clone(&self) -> Self {
        Local {
            inner: self.inner.clone(),
        }
    }
}

impl<T: 'static, const CAP: usize> Drop for Inner<T, CAP> {
    fn drop(&mut self) {
        let real_head = unpack(self.head.load(Ordering::Relaxed)).1;
        let tail = self.tail.load(Ordering::Relaxed);
        let remaining = tail.wrapping_sub(real_head) as usize;
        debug_assert!(remaining <= CAP);
        for offset in 0..remaining {
            // SAFETY: after the final queue handle is dropped no thief can be
            // active. Every slot from the committed head to tail is initialized.
            unsafe {
                self.buffer[((real_head.wrapping_add(offset as u32)) & (CAP as u32 - 1)) as usize]
                    .get()
                    .cast::<T>()
                    .drop_in_place();
            }
        }
    }
}

fn pack(steal_head: u32, real_head: u32) -> u64 {
    (u64::from(steal_head) << 32) | u64::from(real_head)
}

fn unpack(value: u64) -> (u32, u32) {
    ((value >> 32) as u32, value as u32)
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn owner_pushes_and_pops_in_lifo_order() {
        let mut queue = Local::<u32>::new();
        queue.push_back(1).unwrap();
        queue.push_back(2).unwrap();
        queue.push_back(3).unwrap();

        assert_eq!(queue.pop(), Some(3));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn push_rejects_the_257th_item_without_dropping_it() {
        let mut queue = Local::<u32>::new();
        for value in 0..LOCAL_QUEUE_CAPACITY as u32 {
            queue.push_back(value).unwrap();
        }

        assert_eq!(queue.push_back(LOCAL_QUEUE_CAPACITY as u32), Err(256));
        assert_eq!(queue.len(), LOCAL_QUEUE_CAPACITY);
    }

    #[test]
    fn stealing_moves_half_and_leaves_the_owner_with_the_other_half() {
        let mut source = Local::<u32>::new();
        let mut destination = Local::<u32>::new();
        for value in 0..8 {
            source.push_back(value).unwrap();
        }

        let first = source.steal_into(&mut destination).unwrap();
        assert_eq!(first, 0);
        assert_eq!(source.len(), 4);
        assert_eq!(destination.len(), 3);

        let mut stolen = vec![first];
        while let Some(value) = destination.pop() {
            stolen.push(value);
        }
        while let Some(value) = source.pop() {
            stolen.push(value);
        }
        stolen.sort_unstable();
        assert_eq!(stolen, (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn push_overflow_moves_the_older_half_and_keeps_the_new_task_local() {
        let mut queue = Local::<u32>::new();
        for value in 0..LOCAL_QUEUE_CAPACITY as u32 {
            queue.push_back(value).unwrap();
        }

        let mut overflow = Vec::new();
        queue.push_overflow(LOCAL_QUEUE_CAPACITY as u32, &mut overflow);

        // The OLDER half moved out; the queue still holds the newer half
        // plus the new task at its tail.
        assert_eq!(overflow.len(), LOCAL_QUEUE_CAPACITY / 2);
        assert_eq!(queue.len(), LOCAL_QUEUE_CAPACITY / 2 + 1);
        let mut moved = overflow.clone();
        moved.sort_unstable();
        assert_eq!(
            moved,
            (0..LOCAL_QUEUE_CAPACITY as u32 / 2).collect::<Vec<_>>()
        );
        // The new task is polled first (LIFO): it stayed on this worker.
        assert_eq!(queue.pop(), Some(LOCAL_QUEUE_CAPACITY as u32));

        // Half the queue is now free: the next overflow is a long way off.
        assert!(queue.push_back(1_000).is_ok());

        let mut values = overflow;
        while let Some(value) = queue.pop() {
            values.push(value);
        }
        values.sort_unstable();
        // 256 was already popped above; everything else must still be here.
        let mut expected = (0..LOCAL_QUEUE_CAPACITY as u32).collect::<Vec<_>>();
        expected.push(1_000);
        assert_eq!(values, expected);
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use crate::loom::thread;

    #[test]
    fn two_thieves_do_not_duplicate_or_drop_items() {
        loom::model(|| {
            let mut source = Local::<u32>::new();
            for value in 0..4 {
                source.push_back(value).unwrap();
            }

            let thief_a = source.clone();
            let thief_b = source.clone();
            let a = thread::spawn(move || {
                let mut destination = Local::<u32>::new();
                let first = thief_a.steal_into(&mut destination);
                let mut values = first.into_iter().collect::<Vec<_>>();
                while let Some(value) = destination.pop() {
                    values.push(value);
                }
                values
            });
            let b = thread::spawn(move || {
                let mut destination = Local::<u32>::new();
                let first = thief_b.steal_into(&mut destination);
                let mut values = first.into_iter().collect::<Vec<_>>();
                while let Some(value) = destination.pop() {
                    values.push(value);
                }
                values
            });

            let mut values = a.join().unwrap();
            values.extend(b.join().unwrap());
            while let Some(value) = source.pop() {
                values.push(value);
            }
            values.sort_unstable();
            assert_eq!(values, vec![0, 1, 2, 3]);
        });
    }

    #[test]
    fn push_overflow_racing_a_steal_never_loses_or_duplicates_tasks() {
        // Tiny capacity keeps the model's state space small while still
        // forcing the overflow path (full queue) under a concurrent steal.
        loom::model(|| {
            let mut owner = Local::<u32, 4>::new();
            for value in 0..4 {
                owner.push_back(value).unwrap();
            }
            let thief = owner.clone();
            let thief_thread = thread::spawn(move || {
                let mut destination = Local::<u32, 4>::new();
                let mut values = Vec::new();
                if let Some(first) = thief.steal_into(&mut destination) {
                    values.push(first);
                }
                while let Some(value) = destination.pop() {
                    values.push(value);
                }
                values
            });

            let mut overflow = Vec::new();
            owner.push_overflow(99, &mut overflow);

            let mut values = thief_thread.join().unwrap();
            while let Some(value) = owner.pop() {
                values.push(value);
            }
            values.extend(overflow);
            values.sort_unstable();
            assert_eq!(values, vec![0, 1, 2, 3, 99]);
        });
    }

    #[test]
    fn owner_push_back_never_reuses_a_slot_claimed_by_an_unfinished_steal() {
        // C1 regression: two thieves claim overlapping-frontier ranges while
        // the owner pushes. If a commit jumps `real_head` past a claim that
        // is still being copied, the owner pushes into the thief's slot and
        // the task is either duplicated (owner + thief both run it) or lost.
        loom::model(|| {
            let mut owner = Local::<u32, 4>::new();
            for value in 0..4 {
                owner.push_back(value).unwrap();
            }
            let thief_a = owner.clone();
            let thief_b = owner.clone();
            let a = thread::spawn(move || {
                let mut destination = Local::<u32, 4>::new();
                let mut values = Vec::new();
                if let Some(first) = thief_a.steal_into(&mut destination) {
                    values.push(first);
                }
                while let Some(value) = destination.pop() {
                    values.push(value);
                }
                values
            });
            let b = thread::spawn(move || {
                let mut destination = Local::<u32, 4>::new();
                let mut values = Vec::new();
                if let Some(first) = thief_b.steal_into(&mut destination) {
                    values.push(first);
                }
                while let Some(value) = destination.pop() {
                    values.push(value);
                }
                values
            });

            // The queue is full (with claims possibly in flight), so pushes
            // may legitimately be rejected; every accepted value must still
            // appear exactly once.
            let mut rejected = Vec::new();
            for value in 4..8 {
                if owner.push_back(value).is_err() {
                    rejected.push(value);
                }
            }

            let mut values = a.join().unwrap();
            values.extend(b.join().unwrap());
            while let Some(value) = owner.pop() {
                values.push(value);
            }
            values.extend(rejected);
            values.sort_unstable();
            assert_eq!(values, (0..8).collect::<Vec<_>>());
        });
    }
}
