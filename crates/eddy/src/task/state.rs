//! The task's atomic state machine: one `AtomicUsize` packed as
//! `[ refcount : 48 bits ][ flags : 16 bits ]`. Every transition is a
//! single CAS (`fetch_update`). See the plan doc / SPEC.md §4 for why this
//! shape (RUNNING vs NOTIFIED) prevents the canonical lost-wakeup bug.

use crate::loom::sync::atomic::{AtomicUsize, Ordering};

pub(crate) const RUNNING: usize = 0b0000_0001;
pub(crate) const COMPLETE: usize = 0b0000_0010;
pub(crate) const NOTIFIED: usize = 0b0000_0100;
pub(crate) const CANCELLED: usize = 0b0000_1000;
pub(crate) const JOIN_INTEREST: usize = 0b0001_0000;
pub(crate) const JOIN_WAKER_SET: usize = 0b0010_0000;

const REF_ONE: usize = 1 << 16;
const REF_MASK: usize = !(REF_ONE - 1);
/// Abort rather than silently wrap the refcount on overflow. This can only
/// be reached by a Waker-cloning bug (leaking millions of clones), never by
/// normal use.
const REF_MAX: usize = 1 << 40;
const REF_MAX_COUNT: usize = REF_MAX >> 16;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) struct Snapshot(usize);

impl Snapshot {
    // Used by tests to assert exact refcounts; no production call site
    // yet (nothing currently branches on the raw count outside of the
    // ref_inc/ref_dec overflow guard, which reads the atomic directly).
    #[allow(dead_code)]
    pub(crate) fn ref_count(self) -> usize {
        (self.0 & REF_MASK) >> 16
    }
    pub(crate) fn is_running(self) -> bool {
        self.0 & RUNNING != 0
    }
    pub(crate) fn is_complete(self) -> bool {
        self.0 & COMPLETE != 0
    }
    pub(crate) fn is_notified(self) -> bool {
        self.0 & NOTIFIED != 0
    }
    pub(crate) fn is_cancelled(self) -> bool {
        self.0 & CANCELLED != 0
    }
    // No production call site yet — will gate `into_abort_handle`-style
    // APIs in a later phase; kept as part of the flag surface now.
    #[allow(dead_code)]
    pub(crate) fn has_join_interest(self) -> bool {
        self.0 & JOIN_INTEREST != 0
    }
    #[allow(dead_code)] // read by future console instrumentation, not yet wired
    pub(crate) fn has_join_waker(self) -> bool {
        self.0 & JOIN_WAKER_SET != 0
    }

    fn set_running(&mut self) {
        self.0 |= RUNNING;
    }
    fn unset_running(&mut self) {
        self.0 &= !RUNNING;
    }
    fn set_complete(&mut self) {
        self.0 |= COMPLETE;
    }
    fn set_notified(&mut self) {
        self.0 |= NOTIFIED;
    }
    fn unset_notified(&mut self) {
        self.0 &= !NOTIFIED;
    }
    fn set_cancelled(&mut self) {
        self.0 |= CANCELLED;
    }
    fn unset_join_interest(&mut self) {
        self.0 &= !JOIN_INTEREST;
    }
    fn set_join_waker(&mut self) {
        self.0 |= JOIN_WAKER_SET;
    }
    fn unset_join_waker(&mut self) {
        self.0 &= !JOIN_WAKER_SET;
    }
    fn ref_inc(&mut self) {
        if self.ref_count() >= REF_MAX_COUNT {
            std::process::abort();
        }
        self.0 += REF_ONE;
    }
}

pub(crate) struct State {
    val: AtomicUsize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToRunning {
    Success,
    /// Carries the snapshot from the moment the transition failed, so the
    /// caller can tell WHY (already complete: nothing to do; cancelled and
    /// not yet complete: this poll call is the one queue entry responsible
    /// for finalizing it — see `harness::poll`'s `Failed` arm).
    Failed(Snapshot),
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToIdle {
    Ok,
    OkNotified,
    Cancelled,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToNotifiedByVal {
    Submit,
    DoNothing,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToNotifiedByRef {
    Submit,
    DoNothing,
}

impl State {
    /// refcount=2 (JoinHandle + initial run-queue slot), JOIN_INTEREST |
    /// NOTIFIED set. The spawn path consumes the queue slot directly by
    /// pushing to the scheduler — it does not go through
    /// `transition_to_notified_*`.
    pub(crate) fn new() -> State {
        let raw = (2 * REF_ONE) | JOIN_INTEREST | NOTIFIED;
        State {
            val: AtomicUsize::new(raw),
        }
    }

    // No production call site yet — every current caller goes through a
    // `transition_to_*` method instead of reading a bare snapshot; tests
    // use this directly to assert intermediate state.
    #[allow(dead_code)]
    pub(crate) fn snapshot(&self) -> Snapshot {
        // Acquire: synchronizes-with the Release of whatever transition
        // produced the value we're about to branch on.
        Snapshot(self.val.load(Ordering::Acquire))
    }

    fn fetch_update_action<F, T>(&self, mut f: F) -> T
    where
        F: FnMut(Snapshot) -> (T, Option<Snapshot>),
    {
        let mut cur = Snapshot(self.val.load(Ordering::Acquire));
        loop {
            let (out, next) = f(cur);
            let Some(next) = next else { return out };
            // AcqRel: Acquire so we observe whatever the losing thread of a
            // concurrent CAS published; Release so the next reader of this
            // state observes our flag changes.
            match self
                .val
                .compare_exchange_weak(cur.0, next.0, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return out,
                Err(actual) => cur = Snapshot(actual),
            }
        }
    }

    pub(crate) fn transition_to_running(&self) -> TransitionToRunning {
        self.fetch_update_action(|mut snap| {
            if snap.is_complete() || snap.is_cancelled() || snap.is_running() {
                return (TransitionToRunning::Failed(snap), None);
            }
            debug_assert!(
                !snap.is_running(),
                "eddy: task polled while already running"
            );
            snap.set_running();
            snap.unset_notified();
            (TransitionToRunning::Success, Some(snap))
        })
    }

    pub(crate) fn transition_to_idle(&self) -> TransitionToIdle {
        self.fetch_update_action(|mut snap| {
            debug_assert!(snap.is_running());
            snap.unset_running();
            if snap.is_cancelled() {
                (TransitionToIdle::Cancelled, Some(snap))
            } else if snap.is_notified() {
                (TransitionToIdle::OkNotified, Some(snap))
            } else {
                (TransitionToIdle::Ok, Some(snap))
            }
        })
    }

    /// Called from three different contexts (see `harness::complete`): a
    /// normal Ready result (RUNNING still set at this point), a cancelled
    /// task being finalized (RUNNING may already be clear — cancellation
    /// can be observed after `transition_to_idle` has already cleared it,
    /// or via `shutdown`/`poll`'s `Failed` arm on a task that was never
    /// running at all). So this does NOT assert `is_running()` — the
    /// invariant that actually must hold is "never completed twice".
    pub(crate) fn transition_to_complete(&self) -> Snapshot {
        self.fetch_update_action(|mut snap| {
            debug_assert!(!snap.is_complete(), "eddy: task completed twice");
            snap.unset_running();
            snap.set_complete();
            snap.unset_notified();
            (snap, Some(snap))
        })
    }

    /// `is_cancelled()` is checked alongside `is_complete()`/`is_notified()`
    /// so that ONCE a task is cancelled, every subsequent wake is
    /// permanently a no-op — no `Submit` ever fires again for it. This is
    /// what makes `shutdown`'s "before" snapshot durably describe the
    /// task's fate: without it, a wake racing in right after cancellation
    /// could take the `Submit` branch and queue a reference that nothing
    /// would ever reclaim (the eventual poll of that queue entry fails via
    /// `transition_to_running`'s `Failed` case, whose only job is to
    /// finalize a task that was cancelled BEFORE it was ever queued — not
    /// one cancelled AFTER, which this check prevents from happening).
    pub(crate) fn transition_to_notified_by_ref(&self) -> TransitionToNotifiedByRef {
        self.fetch_update_action(|mut snap| {
            if snap.is_complete() || snap.is_notified() || snap.is_cancelled() {
                return (TransitionToNotifiedByRef::DoNothing, None);
            }
            if snap.is_running() {
                snap.set_notified();
                return (TransitionToNotifiedByRef::DoNothing, Some(snap));
            }
            snap.set_notified();
            snap.ref_inc();
            (TransitionToNotifiedByRef::Submit, Some(snap))
        })
    }

    /// See `transition_to_notified_by_ref` for why `is_cancelled()` is
    /// checked here too.
    pub(crate) fn transition_to_notified_by_val(&self) -> TransitionToNotifiedByVal {
        self.fetch_update_action(|mut snap| {
            if snap.is_complete() || snap.is_notified() || snap.is_cancelled() {
                return (TransitionToNotifiedByVal::DoNothing, None);
            }
            if snap.is_running() {
                snap.set_notified();
                return (TransitionToNotifiedByVal::DoNothing, Some(snap));
            }
            snap.set_notified();
            (TransitionToNotifiedByVal::Submit, Some(snap))
        })
    }

    /// Returns the snapshot from BEFORE cancellation was applied, so the
    /// caller can tell whether a poll was already in flight (`is_running()`)
    /// or the task had already finished (`is_complete()`) — touching the
    /// future's storage directly is only safe when neither is true; see
    /// `harness::shutdown`.
    pub(crate) fn set_cancelled(&self) -> Snapshot {
        self.fetch_update_action(|mut snap| {
            let before = snap;
            snap.set_cancelled();
            (before, Some(snap))
        })
    }

    /// Returns the snapshot *before* clearing, so the caller can tell
    /// whether the task was already complete (JoinHandle drop-detach path).
    pub(crate) fn unset_join_interest(&self) -> Snapshot {
        self.fetch_update_action(|mut snap| {
            let before = snap;
            snap.unset_join_interest();
            (before, Some(snap))
        })
    }

    /// Fails (returns false) if the task is already complete — caller
    /// should read the output immediately instead of waiting for a wake.
    pub(crate) fn set_join_waker(&self) -> bool {
        self.fetch_update_action(|mut snap| {
            if snap.is_complete() {
                return (false, None);
            }
            snap.set_join_waker();
            (true, Some(snap))
        })
    }

    #[allow(dead_code)] // symmetrical API; not exercised until a future slice re-registers a waker
    pub(crate) fn unset_join_waker(&self) {
        self.fetch_update_action(|mut snap| {
            snap.unset_join_waker();
            ((), Some(snap))
        });
    }

    /// +1. Relaxed is sound: the caller already holds a live reference, so
    /// there is nothing new to synchronize-with (contrast with `ref_dec`,
    /// which publishes the *last* access before deallocation).
    pub(crate) fn ref_inc(&self) {
        let prev = self.val.fetch_add(REF_ONE, Ordering::Relaxed);
        if (prev & REF_MASK) >> 16 >= REF_MAX_COUNT {
            std::process::abort();
        }
    }

    /// -1. Returns true if this was the last reference and the caller must
    /// deallocate. Release on the decrement + an Acquire fence on the path
    /// that observes zero: every access made through any now-dropped
    /// reference happens-before the dealloc.
    pub(crate) fn ref_dec(&self) -> bool {
        let prev = self.val.fetch_sub(REF_ONE, Ordering::Release);
        let prev_count = (prev & REF_MASK) >> 16;
        debug_assert!(prev_count >= 1, "eddy: refcount underflow");
        if prev_count == 1 {
            self.val.load(Ordering::Acquire);
            true
        } else {
            false
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn initial_state_has_refcount_2_and_join_interest_and_notified() {
        let s = State::new();
        let snap = s.snapshot();
        assert_eq!(snap.ref_count(), 2);
        assert!(snap.has_join_interest());
        assert!(snap.is_notified());
        assert!(!snap.is_running());
        assert!(!snap.is_complete());
    }

    #[test]
    fn running_then_idle_without_wake_parks() {
        let s = State::new();
        assert!(matches!(
            s.transition_to_running(),
            TransitionToRunning::Success
        ));
        assert!(matches!(s.transition_to_idle(), TransitionToIdle::Ok));
        assert!(!s.snapshot().is_running());
    }

    #[test]
    fn transition_to_running_rejects_a_duplicate_poll() {
        let s = State::new();
        assert!(matches!(
            s.transition_to_running(),
            TransitionToRunning::Success
        ));
        assert!(matches!(
            s.transition_to_running(),
            TransitionToRunning::Failed(snapshot) if snapshot.is_running()
        ));
    }

    #[test]
    fn wake_by_ref_while_running_defers_to_transition_to_idle() {
        let s = State::new();
        s.transition_to_running();
        assert!(matches!(
            s.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::DoNothing
        ));
        assert!(s.snapshot().is_notified());
        assert!(matches!(
            s.transition_to_idle(),
            TransitionToIdle::OkNotified
        ));
    }

    #[test]
    fn wake_by_ref_while_idle_submits_and_takes_a_ref() {
        let s = State::new();
        s.transition_to_running();
        s.transition_to_idle();
        let before = s.snapshot().ref_count();
        assert!(matches!(
            s.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::Submit
        ));
        assert_eq!(s.snapshot().ref_count(), before + 1);
    }

    #[test]
    fn wake_by_val_while_idle_submits_without_taking_a_ref() {
        let s = State::new();
        s.transition_to_running();
        s.transition_to_idle();
        let before = s.snapshot().ref_count();
        assert!(matches!(
            s.transition_to_notified_by_val(),
            TransitionToNotifiedByVal::Submit
        ));
        assert_eq!(s.snapshot().ref_count(), before);
    }

    #[test]
    fn double_wake_before_poll_is_idempotent() {
        let s = State::new();
        s.transition_to_running();
        s.transition_to_idle();
        assert!(matches!(
            s.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::Submit
        ));
        let before = s.snapshot().ref_count();
        assert!(matches!(
            s.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::DoNothing
        ));
        assert_eq!(s.snapshot().ref_count(), before);
    }

    #[test]
    fn wake_after_complete_is_noop() {
        let s = State::new();
        s.transition_to_running();
        s.transition_to_complete();
        assert!(matches!(
            s.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::DoNothing
        ));
        assert!(matches!(
            s.transition_to_notified_by_val(),
            TransitionToNotifiedByVal::DoNothing
        ));
    }

    #[test]
    fn ref_inc_dec_returns_true_only_when_last() {
        let s = State::new(); // refcount 2
        s.ref_inc(); // 3
        assert!(!s.ref_dec()); // 2
        assert!(!s.ref_dec()); // 1
        assert!(s.ref_dec()); // 0 -> last
    }
}
