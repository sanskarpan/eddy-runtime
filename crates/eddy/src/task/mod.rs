//! Task representation: one heap allocation per task (Header + Core +
//! Trailer), a manual vtable for type erasure, and a packed atomic state
//! machine. See docs/superpowers/plans/2026-08-03-eddy-phase0-3-spine.md.

use std::future::Future;

mod harness;
mod join;
mod raw;
mod state;
mod waker;

pub(crate) use raw::RawTask;

pub use join::{AbortHandle, JoinError, JoinHandle};

/// Implemented by every scheduler a task can run on. `schedule` is called
/// with a reference-carrying `Notified<S>` any time the task transitions
/// from idle to needing a poll (initial spawn, or a wake with `Submit`).
pub(crate) trait Schedule: Sized + Sync + 'static {
    fn schedule(&self, task: Notified<Self>);

    /// Defer destruction of a task to the scheduler's owning thread. This is
    /// required for current-thread tasks whose future may be `!Send`.
    fn defer_dealloc(&self, task: Notified<Self>) {
        std::mem::forget(task);
    }

    fn is_owner_thread(&self) -> bool {
        true
    }

    fn can_dealloc_remotely(&self) -> bool {
        false
    }

    /// Remove a completed task from the scheduler's ownership registry.
    fn task_complete(&self, _task: RawTask) {}

    /// Register a task-owned reference used by scheduler shutdown.
    fn register_task(&self, _task: RawTask) {}
}

/// A `RawTask` that is known to carry exactly one "queued" reference. Wraps
/// `RawTask` instead of exposing it directly so schedulers can't
/// accidentally forget to eventually poll-or-drop it.
///
/// The current-thread implementation can move the pointer across its
/// injection queue regardless of whether the future is `Send`; the future is
/// only touched on the owning thread and destruction is deferred there.
pub(crate) struct Notified<S: Schedule> {
    pub(crate) raw: RawTask,
    kind: NotifiedKind,
    consumed: bool,
    _marker: std::marker::PhantomData<S>,
}

#[derive(Copy, Clone)]
pub(crate) enum NotifiedKind {
    Poll,
    Dealloc,
}

impl<S: Schedule> Notified<S> {
    pub(crate) fn new(raw: RawTask) -> Notified<S> {
        Notified {
            raw,
            kind: NotifiedKind::Poll,
            consumed: false,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn new_dealloc(raw: RawTask) -> Notified<S> {
        Notified {
            raw,
            kind: NotifiedKind::Dealloc,
            consumed: false,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn run(mut self) {
        self.consumed = true;
        match self.kind {
            NotifiedKind::Poll => self.raw.poll(),
            NotifiedKind::Dealloc => self.raw.dealloc_now(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn into_raw(mut self) -> RawTask {
        self.consumed = true;
        self.raw
    }
}

impl<S: Schedule> Drop for Notified<S> {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        match self.kind {
            NotifiedKind::Poll => self.raw.drop_reference(),
            NotifiedKind::Dealloc => self.raw.dealloc_now(),
        }
    }
}

// SAFETY: `CurrentThread` contains only synchronized shared state, and its
// scheduler routes polling and destruction back to its owner thread. Other
// scheduler implementations must add their own explicit Send implementation
// when their task ownership model is ready.
unsafe impl Send for Notified<crate::scheduler::CurrentThread> {}

#[derive(Debug)]
pub(crate) enum JoinErrorRepr {
    Panic(Box<dyn std::any::Any + Send + 'static>),
    Cancelled,
}

/// Allocates the task and returns both halves: the `Notified<S>` the
/// caller (a scheduler) must push onto its run queue exactly once, and the
/// `JoinHandle<T>` for the caller (user code) to await. Deliberately does
/// NOT call `S::schedule` itself — the initial state (`State::new`)
/// already accounts for the run-queue reference, so routing it through
/// `transition_to_notified_*` here would incorrectly try to take a second
/// one.
pub(crate) fn spawn<F, S>(future: F, scheduler: S) -> (Notified<S>, JoinHandle<F::Output>)
where
    F: Future + 'static,
    S: Schedule,
{
    let raw = RawTask::new(future, scheduler);
    let handle = JoinHandle::new(raw);
    (Notified::new(raw), handle)
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
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
    fn spawn_does_not_call_schedule_synchronously() {
        let sched = RecordingSchedule::default();
        let (notified, handle) = spawn(async { 5 }, sched.clone());
        assert_eq!(sched.0.borrow().len(), 0);
        notified.run();
        drop(handle);
    }
}
