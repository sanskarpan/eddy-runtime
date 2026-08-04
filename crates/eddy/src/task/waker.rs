//! Hand-built `RawWaker`: a data pointer (`NonNull<Header>`) plus a vtable
//! of four functions that must maintain the task's refcount EXACTLY:
//!   clone       -> +1   (a new Waker now exists)
//!   wake        -> consumes the reference (schedule, then possibly -1)
//!   wake_by_ref -> does NOT consume (schedule takes its own +1 if needed)
//!   drop        -> -1
//! Getting `wake` vs `wake_by_ref` backwards is the canonical bug: `wake`
//! must not leave a reference behind (else every woken task leaks), and
//! `wake_by_ref` must not consume one (else a task still queued elsewhere
//! gets freed under it -> use-after-free).

use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::task::{RawWaker, RawWakerVTable, Waker};

use super::raw::{Header, RawTask};

static WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref_waker, drop_waker);

/// Builds a brand-new, independently-owned `Waker` — this takes a FRESH
/// reference (`ref_inc`), exactly like cloning an existing `Waker` would.
/// `header` only needs to point at a live task; the caller does not need to
/// already hold a reference of their own to spend.
///
/// No production call site yet in this slice (`harness::poll` uses the
/// zero-cost `waker_ref` instead) — this is the general-purpose
/// constructor a future sync primitive (e.g. storing a waker in a channel's
/// waiter list) will call directly; exercised thoroughly by tests today.
#[allow(dead_code)]
pub(crate) fn waker_from_raw(header: NonNull<Header>) -> Waker {
    // SAFETY: `header` points at a live task (function precondition).
    unsafe { header.as_ref().state.ref_inc() };
    let raw = RawWaker::new(header.as_ptr() as *const (), &WAKER_VTABLE);
    // SAFETY: WAKER_VTABLE's four functions uphold the refcount contract
    // documented above; the `ref_inc` just above accounts for the
    // reference this `Waker` now owns.
    unsafe { Waker::from_raw(raw) }
}

/// A borrowed waker for the hot poll path: constructing it does NOT touch
/// the refcount at all (unlike `waker_from_raw`), because it doesn't own a
/// reference — it borrows the caller's own, for the lifetime `'a`. This is
/// safe specifically because a `WakerRef` only ever exists for the
/// duration of one `poll()` call, during which the harness itself already
/// holds a live reference to the task. Any code that needs a Waker to
/// OUTLIVE the poll call (e.g. to wake the task from a channel later) must
/// call `.clone()` on it, which goes through the real `clone_waker` and
/// takes its own fresh reference — exactly like cloning any other `Waker`.
pub(crate) struct WakerRef<'a> {
    waker: ManuallyDrop<Waker>,
    _marker: std::marker::PhantomData<&'a Header>,
}

impl<'a> Deref for WakerRef<'a> {
    type Target = Waker;
    fn deref(&self) -> &Waker {
        &self.waker
    }
}

/// Takes `NonNull<Header>`, NOT `&Header`, deliberately. `Header` is the
/// first field of a much larger `Cell<F, S>` allocation, and a `RawWaker`
/// built from it can be `.clone()`d into a real, persisted `Waker` that
/// later gets used to reach OTHER fields of that `Cell` (e.g. `schedule`
/// reads `core.scheduler`). If the pointer were derived from a `&Header`
/// reference (whose provenance a strict-provenance checker like Miri scopes
/// to exactly `size_of::<Header>()` bytes), that later access through a
/// clone would be use-after-the-declared-bounds — confirmed by Miri as a
/// genuine Stacked Borrows violation before this fix. Deriving directly
/// from the `NonNull<Header>` that ultimately traces back to
/// `RawTask::new`'s `Box::into_raw(cell) as *mut Header` cast preserves
/// provenance over the whole allocation, matching what `waker_from_raw`
/// already did correctly.
pub(crate) fn waker_ref(header: NonNull<Header>) -> WakerRef<'static> {
    let raw = RawWaker::new(header.as_ptr() as *const (), &WAKER_VTABLE);
    WakerRef {
        // SAFETY: never actually dropped (ManuallyDrop) — this borrows the
        // caller's existing reference for `'a` instead of owning one; see
        // the doc comment above.
        waker: ManuallyDrop::new(unsafe { Waker::from_raw(raw) }),
        _marker: std::marker::PhantomData,
    }
}

/// SAFETY: `ptr` was produced by `waker_from_raw`/`clone_waker`, i.e. it is
/// a `NonNull<Header>` cast to `*const ()`, per the `Waker` contract that
/// only these functions ever construct a `RawWaker` with `&WAKER_VTABLE`.
unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    header.as_ref().state.ref_inc(); // +1
    RawWaker::new(ptr, &WAKER_VTABLE)
}

/// SAFETY: see `clone_waker`. Consumes the reference this RawWaker
/// represented — must not leave one behind.
unsafe fn wake_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    RawTask::from_raw(header).wake_by_val();
}

/// SAFETY: see `clone_waker`. Borrows — must leave the refcount unchanged;
/// any reference the schedule path needs is taken fresh inside
/// `transition_to_notified_by_ref`.
unsafe fn wake_by_ref_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    RawTask::from_raw(header).wake_by_ref();
}

/// SAFETY: see `clone_waker`. -1.
unsafe fn drop_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    RawTask::from_raw(header).drop_reference();
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::task::raw::RawTask;
    use crate::task::{Notified, Schedule};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone, Default)]
    struct RecordingSchedule(Rc<RefCell<Vec<RawTask>>>);
    // SAFETY: this test scheduler is only used on one thread.
    unsafe impl Sync for RecordingSchedule {}
    impl Schedule for RecordingSchedule {
        fn schedule(&self, task: Notified<Self>) {
            self.0.borrow_mut().push(task.into_raw());
        }
    }

    #[test]
    fn clone_drop_ten_thousand_times_returns_to_baseline() {
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(std::future::pending::<()>(), sched);
        let base = raw.header().state.snapshot().ref_count();
        let w = waker_from_raw(raw.header);
        for _ in 0..10_000 {
            let cloned = w.clone();
            drop(cloned);
        }
        drop(w);
        assert_eq!(raw.header().state.snapshot().ref_count(), base);
        // This test never polls `raw`, so the run-queue-slot reference from
        // spawn (the other half of the initial refcount=2) was never
        // consumed by a poll — drop it explicitly alongside the
        // conceptual "JoinHandle" reference so the task doesn't leak.
        raw.poll();
        raw.drop_reference();
    }

    #[test]
    fn wake_on_pending_task_schedules_exactly_once() {
        let sched = RecordingSchedule::default();
        let scheduled = sched.0.clone();
        let raw = RawTask::new(std::future::pending::<()>(), sched);
        raw.poll(); // Pending, no self-wake -> parked, no schedule yet
        assert_eq!(scheduled.borrow().len(), 0);
        let w = waker_from_raw(raw.header);
        w.wake_by_ref();
        assert_eq!(scheduled.borrow().len(), 1);
        scheduled.borrow_mut().pop().unwrap().drop_reference();
        raw.drop_reference();
    }

    #[test]
    #[allow(clippy::waker_clone_wake)]
    fn owned_wake_consumes_only_the_owned_waker_reference() {
        let sched = RecordingSchedule::default();
        let scheduled = sched.0.clone();
        let raw = RawTask::new(std::future::pending::<()>(), sched);
        raw.poll();
        let waker = waker_from_raw(raw.header);
        waker.clone().wake();
        assert_eq!(scheduled.borrow().len(), 1);
        scheduled.borrow_mut().pop().unwrap().drop_reference();
        drop(waker);
        raw.drop_reference();
    }

    #[test]
    fn double_wake_before_poll_schedules_once() {
        let sched = RecordingSchedule::default();
        let scheduled = sched.0.clone();
        let raw = RawTask::new(std::future::pending::<()>(), sched);
        raw.poll();
        let w = waker_from_raw(raw.header);
        w.wake_by_ref();
        w.wake_by_ref();
        assert_eq!(scheduled.borrow().len(), 1);
        scheduled.borrow_mut().pop().unwrap().drop_reference();
        raw.drop_reference();
    }

    #[test]
    fn wake_during_poll_requeues_after_pending_not_lost() {
        let sched = RecordingSchedule::default();
        let scheduled = sched.0.clone();
        struct SelfWaking;
        impl std::future::Future for SelfWaking {
            type Output = ();
            fn poll(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<()> {
                // Simulate "reactor fires mid-poll": wake from inside poll,
                // before returning Pending.
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
        let raw = RawTask::new(SelfWaking, sched);
        raw.poll();
        // Must be re-queued (OkNotified path), not lost.
        assert_eq!(scheduled.borrow().len(), 1);
        scheduled.borrow_mut().pop().unwrap().drop_reference();
        raw.drop_reference();
    }

    #[test]
    fn wake_after_completion_is_noop() {
        let sched = RecordingSchedule::default();
        let scheduled = sched.0.clone();
        let raw = RawTask::new(async {}, sched);
        let w = waker_from_raw(raw.header);
        raw.poll(); // completes immediately
        w.wake_by_ref();
        assert_eq!(scheduled.borrow().len(), 0);
        raw.drop_reference();
    }

    #[test]
    fn waker_outlives_task_keeps_it_alive_until_dropped() {
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(async {}, sched);
        let w = waker_from_raw(raw.header);
        raw.poll();
        raw.drop_reference(); // drop the "JoinHandle" conceptual ref
                              // Task still alive: waker holds a reference.
        assert!(raw.header().state.snapshot().ref_count() >= 1);
        drop(w); // last reference -> dealloc happens here, not before
    }
}

#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use crate::task::raw::RawTask;
    use crate::task::{Notified, Schedule};

    #[derive(Clone)]
    struct LoomSchedule(loom::sync::Arc<loom::sync::Mutex<Vec<RawTask>>>);
    // SAFETY: all access to the raw-task list is protected by the loom mutex;
    // each entry retains a live task reference until the test drains it.
    unsafe impl Sync for LoomSchedule {}
    impl Schedule for LoomSchedule {
        fn schedule(&self, task: Notified<Self>) {
            self.0.lock().unwrap().push(task.into_raw());
        }
    }

    struct PollGate(loom::sync::Arc<loom::sync::atomic::AtomicBool>);

    impl std::future::Future for PollGate {
        type Output = ();

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            self.0.store(true, loom::sync::atomic::Ordering::Release);
            loom::thread::yield_now();
            std::task::Poll::Pending
        }
    }

    #[test]
    fn wake_during_poll_is_never_lost() {
        loom::model(|| {
            let sched = LoomSchedule(loom::sync::Arc::new(loom::sync::Mutex::new(Vec::new())));
            let queued = sched.0.clone();
            let started = loom::sync::Arc::new(loom::sync::atomic::AtomicBool::new(false));
            let raw = RawTask::new(PollGate(started.clone()), sched);
            let w = waker_from_raw(raw.header);

            // Reactor thread races with the worker thread polling.
            let th = loom::thread::spawn(move || {
                while !started.load(loom::sync::atomic::Ordering::Acquire) {
                    loom::thread::yield_now();
                }
                w.wake_by_ref();
            });
            raw.poll();
            th.join().unwrap();

            // The barrier ensures the wake happens after RUNNING is set and
            // before the poll returns, so exactly one requeue is mandatory.
            let queued_count = queued.lock().unwrap().len();
            assert_eq!(queued_count, 1, "lost or duplicate wake: {queued_count}");
            for t in queued.lock().unwrap().drain(..) {
                t.drop_reference();
            }
            raw.drop_reference();
        });
    }

    #[test]
    fn refcount_is_exact_under_concurrent_clone_drop() {
        loom::model(|| {
            let drops = loom::sync::Arc::new(loom::sync::atomic::AtomicUsize::new(0));
            struct DropFuture(loom::sync::Arc<loom::sync::atomic::AtomicUsize>);
            impl std::future::Future for DropFuture {
                type Output = ();

                fn poll(
                    self: std::pin::Pin<&mut Self>,
                    _cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Self::Output> {
                    std::task::Poll::Pending
                }
            }
            impl Drop for DropFuture {
                fn drop(&mut self) {
                    self.0.fetch_add(1, loom::sync::atomic::Ordering::SeqCst);
                }
            }

            let sched = LoomSchedule(loom::sync::Arc::new(loom::sync::Mutex::new(Vec::new())));
            let raw = RawTask::new(DropFuture(drops.clone()), sched);
            raw.poll(); // consume and release the initial queue reference
            let w = waker_from_raw(raw.header);
            let w1 = w.clone();
            let w2 = w.clone();
            drop(w);

            let t1 = loom::thread::spawn(move || drop(w1));
            let t2 = loom::thread::spawn(move || drop(w2));
            t1.join().unwrap();
            t2.join().unwrap();

            raw.drop_reference(); // the conceptual JoinHandle reference
            assert_eq!(drops.load(loom::sync::atomic::Ordering::SeqCst), 1);
        });
    }
}
