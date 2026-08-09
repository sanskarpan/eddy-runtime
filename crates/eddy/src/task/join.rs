//! `JoinHandle<T>`: a `Future<Output = Result<T, JoinError>>` that reads a
//! completed task's output through the vtable's `try_read_output`.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::raw::RawTask;
use super::JoinErrorRepr;

#[derive(Debug)]
pub enum JoinError {
    Panic(Box<dyn std::any::Any + Send + 'static>),
    Cancelled,
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinError::Panic(_) => write!(f, "task panicked"),
            JoinError::Cancelled => write!(f, "task was cancelled"),
        }
    }
}
impl std::error::Error for JoinError {}

impl From<JoinErrorRepr> for JoinError {
    fn from(r: JoinErrorRepr) -> Self {
        match r {
            JoinErrorRepr::Panic(p) => JoinError::Panic(p),
            JoinErrorRepr::Cancelled => JoinError::Cancelled,
        }
    }
}

pub struct JoinHandle<T> {
    raw: Option<RawTask>,
    // The handle holds only a type-erased task pointer; `T` is tracked for
    // variance and auto-trait purposes. `PhantomData<T>` makes the handle
    // `Send`/`Sync` exactly when `T` is, which is sound: awaiting the handle
    // moves `T` out of the task core onto the awaiting thread, so a `!Send`
    // output (current-thread tasks) must keep the handle pinned to its
    // owning thread.
    _marker: PhantomData<T>,
}

// `JoinHandle<T>` never stores a `T` inline (only a pointer to the task
// that will eventually produce one via `try_read_output`), so it never
// needs to be pinned — explicit rather than relying on `PhantomData<T>`'s
// own Unpin-ness, which does track `T`.
impl<T> Unpin for JoinHandle<T> {}

// SAFETY: the header, state, and output all live in the shared task
// allocation, and every operation (`abort`, `try_read_output`, refcounts)
// is safe from any thread once `T: Send` allows the output to cross
// threads.
unsafe impl<T: Send> Send for JoinHandle<T> {}
// SAFETY: see `Send`; sharing the handle only shares the task pointer.
unsafe impl<T: Sync> Sync for JoinHandle<T> {}

impl<T> JoinHandle<T> {
    /// Takes ownership of the "JoinHandle" reference baked into the task's
    /// initial state (`State::new`'s refcount=2) — does NOT `ref_inc`,
    /// because it's consuming a reference that already exists rather than
    /// cloning a new one.
    pub(crate) fn new(raw: RawTask) -> JoinHandle<T> {
        JoinHandle {
            raw: Some(raw),
            _marker: PhantomData,
        }
    }

    pub fn abort(&self) {
        if let Some(raw) = self.raw {
            // SAFETY: `raw` is valid for as long as this handle holds a
            // reference (guaranteed by construction / not yet dropped).
            unsafe { (raw.header().vtable.shutdown)(raw.header) }
        }
    }

    /// Hands out an independent `AbortHandle` sharing the same task —
    /// takes a fresh reference (unlike `JoinHandle::new`, this doesn't
    /// consume the caller's own).
    pub fn abort_handle(&self) -> AbortHandle {
        let raw = self.raw.expect("eddy: JoinHandle polled after completion");
        raw.header().state.ref_inc();
        AbortHandle { raw }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let self_mut = self.get_mut();
        let raw = self_mut
            .raw
            .expect("eddy: JoinHandle polled after completion");
        let mut out: Option<std::result::Result<T, JoinErrorRepr>> = None;
        // SAFETY: `dst` points at a live `Option<Result<T, JoinErrorRepr>>`
        // on this stack frame for the duration of the call, and `T` matches
        // the task's own output type by construction (a `JoinHandle<T>` is
        // only ever created by `spawn` for a future whose Output is `T`).
        let ready = unsafe {
            (raw.header().vtable.try_read_output)(
                raw.header,
                &mut out as *mut _ as *mut (),
                cx.waker(),
            )
        };
        if ready {
            self_mut.raw = None;
            let result = out.expect("eddy: try_read_output reported ready with no value");
            Poll::Ready(result.map_err(JoinError::from))
        } else {
            Poll::Pending
        }
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if let Some(raw) = self.raw.take() {
            // SAFETY: see `abort`.
            unsafe { (raw.header().vtable.drop_join_handle_slow)(raw.header) }
        }
    }
}

pub struct AbortHandle {
    raw: RawTask,
}

// SAFETY: `AbortHandle` only touches the task header: `abort` is a state
// transition plus a scheduler notification (which routes to the owning
// thread for current-thread tasks), and refcounting is atomic. It never
// reads or writes the possibly-`!Send` future or output.
unsafe impl Send for AbortHandle {}
// SAFETY: see `Send`; all operations are interior-mutating through the
// shared header.
unsafe impl Sync for AbortHandle {}

impl AbortHandle {
    pub fn abort(&self) {
        // SAFETY: `raw` stays valid for `AbortHandle`'s lifetime — it holds
        // a counted reference for as long as it exists (taken in
        // `JoinHandle::abort_handle`, released in `Drop` below).
        unsafe { (self.raw.header().vtable.shutdown)(self.raw.header) }
    }
}

impl Clone for AbortHandle {
    fn clone(&self) -> AbortHandle {
        self.raw.header().state.ref_inc();
        AbortHandle { raw: self.raw }
    }
}

impl Drop for AbortHandle {
    fn drop(&mut self) {
        self.raw.drop_reference();
    }
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

    fn spawn_handle<F: std::future::Future<Output = i32>>(fut: F) -> (RawTask, JoinHandle<i32>) {
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(fut, sched);
        let handle = JoinHandle::new(raw);
        (raw, handle)
    }

    #[test]
    fn join_handle_resolves_to_output() {
        let (raw, mut handle) = spawn_handle(async { 42 });
        raw.poll();
        let noop = futures::task::noop_waker();
        let mut cx = Context::from_waker(&noop);
        let out = Pin::new(&mut handle).poll(&mut cx);
        assert!(matches!(out, Poll::Ready(Ok(42))));
    }

    #[test]
    fn abort_before_poll_yields_cancelled() {
        let (raw, mut handle) = spawn_handle(std::future::pending::<i32>());
        handle.abort();
        raw.poll(); // the deferred queue entry finalizes itself as Cancelled
        let noop = futures::task::noop_waker();
        let mut cx = Context::from_waker(&noop);
        let out = Pin::new(&mut handle).poll(&mut cx);
        assert!(matches!(out, Poll::Ready(Err(JoinError::Cancelled))));
    }

    #[test]
    fn dropping_join_handle_detaches_task_which_keeps_running() {
        let (raw, handle) = spawn_handle(async { 1 });
        // Hold an independent reference so the task's own completion
        // (which drops the run-queue reference) doesn't free it before
        // this test can inspect the final state — `raw` itself is just a
        // bare pointer, not a counted handle.
        let keepalive = super::super::waker::waker_from_raw(raw.header);
        drop(handle); // detach, not abort
        raw.poll(); // must still run to completion normally, not be cancelled
        assert!(raw.header().state.snapshot().is_complete());
        drop(keepalive);
    }

    #[test]
    fn abort_handle_keeps_task_alive_independent_of_join_handle_drop() {
        let (raw, handle) = spawn_handle(std::future::pending::<i32>());
        let abort_handle = handle.abort_handle();
        drop(handle); // detach — task still alive via abort_handle's own reference
        assert!(!raw.header().state.snapshot().is_complete());
        abort_handle.abort();
        raw.poll(); // deferred queue entry finalizes as Cancelled
        assert!(raw.header().state.snapshot().is_complete());
        drop(abort_handle); // releases the last reference -> dealloc
    }
}
