//! The poll driver: each `Vtable` function is generic over `F, S` and
//! operates on the typed `Cell` after casting the header pointer back.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};
#[cfg(feature = "instrumentation")]
use std::time::Instant;

use super::raw::{Cell, Header, Stage};
use super::state::{TransitionToIdle, TransitionToRunning};
use super::waker::waker_ref;
use super::{JoinErrorRepr, Notified, RawTask, Schedule};

/// SAFETY: every call site upholds the same precondition as
/// `RawTask::from_raw` — `header` points at a live `Cell<F, S>` for this
/// exact `F`/`S`.
unsafe fn cell_ptr<F: Future, S: Schedule>(header: NonNull<Header>) -> *mut Cell<F, S> {
    header.as_ptr() as *mut Cell<F, S>
}

pub(super) unsafe fn poll<F: Future, S: Schedule>(header: NonNull<Header>) {
    let raw = RawTask::from_raw(header);
    match raw.header().state.transition_to_running() {
        TransitionToRunning::Failed(snap) => {
            // Either already COMPLETE (some other path already finalized
            // and dropped this reference — nothing to do), or CANCELLED
            // before this queue entry was ever polled. In the cancelled
            // case, THIS poll call is the sole queue entry for the task
            // (the state machine guarantees at most one outstanding queue
            // claim at a time, and `transition_to_notified_*` refuses to
            // create new ones once CANCELLED is set), so it is safe —
            // nothing else can be concurrently touching `stage` — to
            // finalize it here using the reference this call already owns.
            if snap.is_cancelled() && !snap.is_complete() {
                let cell = cell_ptr::<F, S>(header);
                complete::<F, S>(header, cell, Err(JoinErrorRepr::Cancelled));
            } else {
                // A stale or duplicate queue entry still owns a reference.
                // Release it even though this entry must not poll the task.
                raw.drop_reference();
            }
            return;
        }
        TransitionToRunning::Success => {}
    }

    let cell = cell_ptr::<F, S>(header);
    // Zero-refcount-cost: borrows the reference `poll`'s caller already
    // holds for the duration of this call. Does NOT decrement anything on
    // drop (see `WakerRef`), unlike a real owned `Waker` would. Built from
    // `header` (`NonNull<Header>`) directly, NOT `raw.header()` (`&Header`)
    // — see `waker_ref`'s doc comment for why that distinction matters.
    let waker = waker_ref(header);
    let mut cx = Context::from_waker(&waker);
    #[cfg(feature = "instrumentation")]
    let poll_started = Instant::now();
    #[cfg(feature = "instrumentation")]
    let task_id = raw.header().task_id;
    #[cfg(feature = "instrumentation")]
    let worker = crate::scheduler::current_worker_id().unwrap_or(0) as u32;
    #[cfg(feature = "instrumentation")]
    crate::instrument::emit(|| crate::instrument::RuntimeEvent::TaskPollStart {
        id: task_id,
        worker,
        at: poll_started,
    });

    // SAFETY: we hold the RUNNING bit, which is the runtime's mutual-
    // exclusion guarantee that no other thread touches `stage` concurrently.
    let poll_result = {
        let _stage_guard = (*cell).header.stage_lock.lock().unwrap();
        let stage = &mut *(*cell).core.stage.get();
        match stage {
            Stage::Running(fut) => {
                // SAFETY: the future was placed here by `RawTask::new` and is
                // never moved for the lifetime of the allocation (the `Cell`
                // is heap-allocated once and never relocated).
                let pin = Pin::new_unchecked(fut);
                let _budget = crate::coop::budget_guard();
                let _task = (*cell).header.task_id.enter();
                catch_unwind(AssertUnwindSafe(|| pin.poll(&mut cx)))
            }
            _ => unreachable!("eddy: poll called on a task not in Running stage"),
        }
    };

    #[cfg(feature = "instrumentation")]
    {
        let duration = poll_started.elapsed();
        let result = match &poll_result {
            Ok(Poll::Ready(_)) => crate::instrument::PollResult::Ready,
            Ok(Poll::Pending) => crate::instrument::PollResult::Pending,
            Err(_) => crate::instrument::PollResult::Panicked,
        };
        let now = crate::instrument::monotonic_nanos();
        let previous = raw
            .header()
            .last_poll_ns
            .swap(now, std::sync::atomic::Ordering::Relaxed);
        if previous != 0 {
            raw.header().idle_ns.fetch_add(
                now.saturating_sub(previous),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        raw.header()
            .polls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        raw.header().busy_ns.fetch_add(
            duration.as_nanos().min(u64::MAX as u128) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        let bucket = duration.as_nanos().max(1).ilog2().min(15) as usize;
        raw.header().poll_histogram[bucket].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        crate::instrument::task_polled(&raw.header().metrics, duration);
        crate::instrument::emit(|| crate::instrument::RuntimeEvent::TaskPollEnd {
            id: task_id,
            worker,
            duration,
            result,
        });
        if duration >= std::time::Duration::from_millis(100) {
            let location = raw.header().location.clone();
            crate::instrument::emit(|| crate::instrument::RuntimeEvent::BlockingDetected {
                task: task_id,
                poll_duration: duration,
                location,
            });
        }
    }

    match poll_result {
        Ok(Poll::Ready(out)) => complete::<F, S>(header, cell, Ok(out)),
        Ok(Poll::Pending) => match raw.header().state.transition_to_idle() {
            // Parked with no wake pending: the reference this poll call was
            // holding (the one that let it be popped off a queue and run)
            // no longer corresponds to anything — nothing is queued, and
            // any future resurrection comes through a NEW reference taken
            // by `transition_to_notified_by_ref`'s `Submit` branch. If we
            // didn't drop it here, every parked task would leak forever.
            TransitionToIdle::Ok => raw.drop_reference(),
            // A wake landed during this poll: the SAME reference is reused
            // as the queue's reference, so it is deliberately NOT dropped
            // here — `raw.schedule()` consumes it directly.
            TransitionToIdle::OkNotified => raw.schedule(),
            TransitionToIdle::Cancelled => {
                complete::<F, S>(header, cell, Err(JoinErrorRepr::Cancelled))
            }
        },
        Err(panic) => complete::<F, S>(header, cell, Err(JoinErrorRepr::Panic(panic))),
    }
}

/// Marks the task Finished and wakes the JoinHandle, WITHOUT touching the
/// refcount. Used by `complete` (below) and by `shutdown`'s truly-idle
/// branch, which needs the mark-complete behavior but does NOT own a
/// dedicated "run-queue" reference to spend (that reference was already
/// dropped earlier, when the task originally went idle).
unsafe fn finalize<F: Future, S: Schedule>(
    header: NonNull<Header>,
    cell: *mut Cell<F, S>,
    result: std::result::Result<F::Output, JoinErrorRepr>,
) {
    {
        let _stage_guard = (*cell).header.stage_lock.lock().unwrap();
        *(*cell).core.stage.get() = Stage::Finished(result);
    }
    let completed = RawTask::from_raw(header)
        .header()
        .state
        .transition_to_complete();
    wake_join_handle::<F, S>(header);
    if !completed.has_join_interest() {
        discard_finished_output::<F, S>(cell);
    }
    (*cell)
        .core
        .scheduler
        .task_complete(RawTask::from_raw(header));
}

/// `finalize` plus dropping the run-queue reference this call chain owns.
/// Only correct to call from a context that actually holds that reference:
/// `poll`'s Ready/Pending-cancelled/panic paths (this poll call owns it
/// throughout), and `poll`'s `Failed`-and-cancelled arm (the queue entry
/// being processed owns it). NOT correct from `shutdown`'s truly-idle
/// branch — see `finalize`.
unsafe fn complete<F: Future, S: Schedule>(
    header: NonNull<Header>,
    cell: *mut Cell<F, S>,
    result: std::result::Result<F::Output, JoinErrorRepr>,
) {
    finalize::<F, S>(header, cell, result);
    // The run-queue reference is done: nothing will poll this task again.
    RawTask::from_raw(header).drop_reference();
}

unsafe fn wake_join_handle<F: Future, S: Schedule>(header: NonNull<Header>) {
    let trailer = trailer_ptr::<F, S>(header);
    if let Some(waker) = (*trailer).waker.lock().take() {
        waker.wake();
    }
}

unsafe fn trailer_ptr<F: Future, S: Schedule>(header: NonNull<Header>) -> *mut super::raw::Trailer {
    std::ptr::addr_of_mut!((*cell_ptr::<F, S>(header)).trailer)
}

unsafe fn discard_finished_output<F: Future, S: Schedule>(cell: *mut Cell<F, S>) {
    let _stage_guard = (*cell).header.stage_lock.lock().unwrap();
    let stage = &mut *(*cell).core.stage.get();
    if matches!(stage, Stage::Finished(_)) {
        *stage = Stage::Consumed;
    }
}

pub(super) unsafe fn schedule<F: Future, S: Schedule>(header: NonNull<Header>) {
    let cell = cell_ptr::<F, S>(header);
    let scheduler = &(*cell).core.scheduler;
    scheduler.schedule(Notified::<S>::new(RawTask::from_raw(header)));
}

pub(super) unsafe fn dealloc<F: Future, S: Schedule>(header: NonNull<Header>) {
    let cell = cell_ptr::<F, S>(header);
    let raw = RawTask::from_raw(header);
    // The final reference may be released by a foreign thread. Restore a
    // queue reference and let the scheduler destroy the allocation on its
    // owning thread, so a `!Send` future is never dropped remotely.
    if (*cell).core.scheduler.is_owner_thread() || (*cell).core.scheduler.can_dealloc_remotely() {
        raw.dealloc_now();
        return;
    }
    raw.header().state.ref_inc();
    (*cell)
        .core
        .scheduler
        .defer_dealloc(Notified::new_dealloc(raw));
}

pub(super) unsafe fn dealloc_now<F: Future, S: Schedule>(header: NonNull<Header>) {
    let cell = cell_ptr::<F, S>(header);
    let task_id = (*cell).header.task_id;
    #[cfg(feature = "instrumentation")]
    let (total_polls, total_busy, total_idle) = (
        (*cell)
            .header
            .polls
            .load(std::sync::atomic::Ordering::Relaxed),
        std::time::Duration::from_nanos(
            (*cell)
                .header
                .busy_ns
                .load(std::sync::atomic::Ordering::Relaxed),
        ),
        std::time::Duration::from_nanos(
            (*cell)
                .header
                .idle_ns
                .load(std::sync::atomic::Ordering::Relaxed),
        ),
    );
    #[cfg(not(feature = "instrumentation"))]
    let (total_polls, total_busy, total_idle) =
        (0, std::time::Duration::ZERO, std::time::Duration::ZERO);
    crate::instrument::emit(|| crate::instrument::RuntimeEvent::TaskDropped {
        id: task_id,
        total_polls,
        total_busy,
        total_idle,
    });
    crate::instrument::task_finished(&(*cell).header.metrics);
    // SAFETY: the deferred deallocation entry owns the final reference and
    // is run only after the scheduler has transferred it to its owner.
    drop(Box::from_raw(cell));
    crate::task::raw::record_dealloc();
}

pub(super) unsafe fn try_read_output<F: Future, S: Schedule>(
    header: NonNull<Header>,
    dst: *mut (),
    waker: &Waker,
) -> bool {
    let cell = cell_ptr::<F, S>(header);
    let result = {
        let _stage_guard = (*cell).header.stage_lock.lock().unwrap();
        let stage = &mut *(*cell).core.stage.get();
        match stage {
            Stage::Finished(_) => {
                let Stage::Finished(result) = std::mem::replace(stage, Stage::Consumed) else {
                    unreachable!()
                };
                Some(result)
            }
            _ => None,
        }
    };

    if let Some(result) = result {
        let dst = dst as *mut Option<std::result::Result<F::Output, JoinErrorRepr>>;
        std::ptr::write(dst, Some(result));
        // The JoinHandle reference ends here. Decrement only after the stage
        // lock is released so deallocation cannot invalidate a live guard.
        RawTask::from_raw(header).drop_reference();
        true
    } else {
        let trailer = trailer_ptr::<F, S>(header);
        *(*trailer).waker.lock() = Some(waker.clone());
        if !RawTask::from_raw(header).header().state.set_join_waker() {
            // Completion won the race after the stage check. Wake the newly
            // registered waiter so it observes the finished output next time.
            if let Some(waker) = (*trailer).waker.lock().take() {
                waker.wake();
            }
        }
        false
    }
}

pub(super) unsafe fn drop_join_handle_slow<F: Future, S: Schedule>(header: NonNull<Header>) {
    let raw = RawTask::from_raw(header);
    let before = raw.header().state.unset_join_interest();
    let trailer = trailer_ptr::<F, S>(header);
    raw.header().state.unset_join_waker();
    *(*trailer).waker.lock() = None;
    if before.is_complete() {
        discard_finished_output::<F, S>(cell_ptr::<F, S>(header));
    }
    raw.drop_reference();
}

pub(super) unsafe fn shutdown<F: Future, S: Schedule>(header: NonNull<Header>) {
    let raw = RawTask::from_raw(header);
    // `set_cancelled` also permanently blocks every future
    // `transition_to_notified_*` call from this point on (see its doc
    // comment in state.rs) — so `before`, captured atomically with the
    // CANCELLED bit going up, durably describes the task's fate. Nothing
    // can change `is_running()`/`is_notified()` back to "there's new work
    // to claim" after this CAS.
    let before = raw.header().state.set_cancelled();
    if before.is_complete() || before.is_cancelled() {
        return; // already finished; cancellation after the fact is a no-op
    }
    crate::instrument::emit(|| crate::instrument::RuntimeEvent::TaskAborted {
        id: raw.header().task_id,
    });
    if before.is_running() {
        // A poll is in flight RIGHT NOW. We must NOT touch `stage` here —
        // that poll already holds exclusive access to it. It will observe
        // CANCELLED itself via `transition_to_idle` and finish the task
        // safely from the owner thread.
        return;
    }
    if before.is_notified() {
        // A queue entry already exists (initial spawn, or a pending
        // re-queue) holding its own reference. THAT entry's eventual
        // `poll()` will observe CANCELLED via `transition_to_running`'s
        // `Failed` case and finalize using its own reference. Touching
        // `stage` here too would finalize the task twice.
        return;
    }
    // Truly idle: no queue entry, nothing running, not complete. No other
    // path can touch `stage` (nothing is queued to poll it, and no new
    // queue entry can form post-cancellation), so it's safe to finalize
    // directly. Deliberately `finalize`, NOT `complete`: the run-queue
    // reference was already dropped earlier (when the task went idle via
    // `TransitionToIdle::Ok`) — this call doesn't own a reference to spend,
    // it's just marking the already-referenced-elsewhere task Finished.
    let cell = cell_ptr::<F, S>(header);
    finalize::<F, S>(header, cell, Err(JoinErrorRepr::Cancelled));
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::super::raw::RawTask;
    use super::super::{JoinErrorRepr, Notified, Schedule};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::task::Poll;

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
    fn ready_future_completes_and_output_is_readable() {
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(async { 7i32 }, sched);
        raw.poll(); // ready immediately, no waker registration needed
        let mut out: Option<std::result::Result<i32, JoinErrorRepr>> = None;
        let noop = futures::task::noop_waker();
        // SAFETY: `raw.header` points at a live `Cell<i32-future, _>`; `out`
        // is a live local of the matching `Result<i32, JoinErrorRepr>` type.
        let got = unsafe {
            (raw.header().vtable.try_read_output)(raw.header, &mut out as *mut _ as *mut (), &noop)
        };
        assert!(got);
        assert!(matches!(out, Some(Ok(7))));
        // NOTE: no explicit `raw.drop_reference()` here — a successful
        // `try_read_output` already drops the JoinHandle's conceptual
        // reference internally (matching real `JoinHandle::poll` usage).
        // Calling it again would be a double-drop / use-after-free.
    }

    #[test]
    fn pending_future_registers_and_wakes_on_notify() {
        let sched = RecordingSchedule::default();
        let scheduled = sched.0.clone();
        struct Once(bool);
        impl std::future::Future for Once {
            type Output = i32;
            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> Poll<i32> {
                if self.0 {
                    Poll::Ready(1)
                } else {
                    self.0 = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
        let raw = RawTask::new(Once(false), sched);
        raw.poll(); // Pending, but wakes itself -> NOTIFIED -> re-queued via OkNotified path
        assert_eq!(scheduled.borrow().len(), 1);
        let requeued = scheduled.borrow_mut().pop().unwrap();
        requeued.poll(); // Ready(1) this time
        raw.drop_reference();
    }

    #[test]
    fn shutdown_before_first_poll_defers_to_the_queued_entry() {
        // Regression for review finding #1/#3: cancelling a task that still
        // has a live queue entry (here: the initial spawn state itself,
        // which is always NOTIFIED) must NOT finalize it directly — that
        // would drop the queue entry's reference out from under it. It
        // must defer, and the eventual `poll()` of that entry must finish
        // the job via `transition_to_running`'s `Failed` arm.
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(std::future::pending::<i32>(), sched);
        // SAFETY: `raw.header` points at a live task; `shutdown` is safe to
        // call at any point in the task's lifecycle (see its own doc).
        unsafe { (raw.header().vtable.shutdown)(raw.header) };
        // Not finalized yet — deferred to the (not-yet-run) queue entry.
        assert!(!raw.header().state.snapshot().is_complete());
        raw.poll(); // the deferred queue entry: transition_to_running -> Failed(cancelled)
        assert!(raw.header().state.snapshot().is_complete());
        let mut out: Option<std::result::Result<i32, JoinErrorRepr>> = None;
        let noop = futures::task::noop_waker();
        // SAFETY: see the Ready-path test above.
        unsafe {
            (raw.header().vtable.try_read_output)(raw.header, &mut out as *mut _ as *mut (), &noop);
        }
        assert!(matches!(out, Some(Err(JoinErrorRepr::Cancelled))));
        // No explicit drop_reference here — try_read_output already
        // dropped it (see the note in the Ready-path test above).
    }

    #[test]
    fn shutdown_while_truly_idle_finalizes_without_extra_drop() {
        // Regression: a task parked (Pending, not notified — its run-queue
        // reference already released) that gets cancelled must finalize
        // via `finalize`, NOT `complete` — it doesn't own a dedicated
        // reference to spend. Exercise this by holding a second, real
        // reference (an extra waker clone) across the abort and confirming
        // it's still exactly one reference short of zero afterward — i.e.
        // shutdown did not silently steal a decrement from it.
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(std::future::pending::<i32>(), sched);
        raw.poll(); // Pending, not notified -> Idle::Ok -> its own run-queue ref is dropped
        let extra = crate::task::waker::waker_from_raw(raw.header); // a real, independent +1
        let before = raw.header().state.snapshot().ref_count();
        // SAFETY: `raw.header` points at a live task.
        unsafe { (raw.header().vtable.shutdown)(raw.header) }; // truly idle -> finalize path
        assert!(raw.header().state.snapshot().is_complete());
        // finalize() must not have dropped anything: refcount unchanged.
        assert_eq!(raw.header().state.snapshot().ref_count(), before);
        drop(extra); // -1
        raw.drop_reference(); // the conceptual JoinHandle ref -> should hit zero, no crash
    }

    #[test]
    fn panicking_future_yields_join_error_panic() {
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(
            async {
                panic!("boom");
                #[allow(unreachable_code)]
                0i32
            },
            sched,
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| raw.poll()));
        assert!(
            result.is_ok(),
            "poll() must catch the future's panic internally"
        );
        let mut out: Option<std::result::Result<i32, JoinErrorRepr>> = None;
        let noop = futures::task::noop_waker();
        // SAFETY: see the Ready-path test above.
        unsafe {
            (raw.header().vtable.try_read_output)(raw.header, &mut out as *mut _ as *mut (), &noop);
        }
        assert!(matches!(out, Some(Err(JoinErrorRepr::Panic(_)))));
        // No explicit drop_reference here — try_read_output already
        // dropped it (see the note in the Ready-path test above).
    }
}
