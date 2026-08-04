//! One heap allocation per task: `Header` (type-erased control block) +
//! `Core<F, S>` (the future, inline) + `Trailer` (JoinHandle waker). See
//! SPEC.md §4 for the "why one allocation" rationale.

use std::cell::UnsafeCell;
use std::future::Future;
use std::ptr::NonNull;
use std::task::Waker;

use super::state::State;
use super::{JoinErrorRepr, Schedule};

#[repr(C)]
pub(crate) struct Cell<F: Future, S> {
    pub(crate) header: Header,
    pub(crate) core: Core<F, S>,
    pub(crate) trailer: Trailer,
}

pub(crate) struct Header {
    pub(crate) state: State,
    pub(crate) vtable: &'static Vtable,
    /// Serializes access between the owner-thread poller and a JoinHandle
    /// that is read from another thread.
    pub(crate) stage_lock: std::sync::Mutex<()>,
    // Unused until Phase 4 (multi-thread scheduler routes cross-thread
    // wakes by comparing this) and the injector (intrusive list threaded
    // through `queue_next`) — present now so the layout matches SPEC.md §4
    // and doesn't need to shift once those land.
    #[allow(dead_code)]
    pub(crate) owner_id: UnsafeCell<u32>,
    #[allow(dead_code)]
    pub(crate) queue_next: UnsafeCell<Option<NonNull<Header>>>,
}

impl Header {
    fn new<F: Future, S: Schedule>() -> Header {
        Header {
            state: State::new(),
            vtable: vtable::<F, S>(),
            stage_lock: std::sync::Mutex::new(()),
            owner_id: UnsafeCell::new(0),
            queue_next: UnsafeCell::new(None),
        }
    }
}

pub(crate) struct Core<F: Future, S> {
    pub(crate) scheduler: S,
    pub(crate) stage: UnsafeCell<Stage<F>>,
}

pub(crate) enum Stage<F: Future> {
    Running(F),
    Finished(std::result::Result<F::Output, JoinErrorRepr>),
    Consumed,
}

pub(crate) struct Trailer {
    pub(crate) waker: parking_lot::Mutex<Option<Waker>>,
}

impl Trailer {
    fn new() -> Trailer {
        Trailer {
            waker: parking_lot::Mutex::new(None),
        }
    }
}

/// One word: type-erased handle to a task. The vtable is reached through
/// the header, so this never needs to carry type information itself —
/// that's what lets it live in an intrusive, non-generic linked list.
#[derive(Copy, Clone)]
pub(crate) struct RawTask {
    pub(crate) header: NonNull<Header>,
}

impl RawTask {
    /// Allocates one `Cell<F, S>`, initializes it, and returns the raw
    /// handle. Caller (spawn) owns both references baked into the initial
    /// state (JoinHandle ref + run-queue ref) and must account for both.
    pub(crate) fn new<F: Future, S: Schedule>(future: F, scheduler: S) -> RawTask {
        let cell = Box::new(Cell {
            header: Header::new::<F, S>(),
            core: Core {
                scheduler,
                stage: UnsafeCell::new(Stage::Running(future)),
            },
            trailer: Trailer::new(),
        });
        // SAFETY: `Box::into_raw` never returns null; `Cell` is `repr(C)`
        // with `header` as its first field, so casting the `Cell` pointer
        // to `*mut Header` and back to `NonNull` is a valid reinterpretation
        // of the same address for the lifetime of the allocation.
        let ptr = Box::into_raw(cell) as *mut Header;
        RawTask {
            // SAFETY: `ptr` is `Box::into_raw`'s result reinterpreted per
            // the comment above — never null.
            header: unsafe { NonNull::new_unchecked(ptr) },
        }
    }

    /// SAFETY: `header` must point at a live `Header` embedded (at offset 0)
    /// in a `Cell<F, S>` allocation, for the same `F`/`S` the vtable was
    /// built for.
    pub(crate) unsafe fn from_raw(header: NonNull<Header>) -> RawTask {
        RawTask { header }
    }

    pub(crate) fn header(&self) -> &Header {
        // SAFETY: the task is alive for as long as `self` exists (callers
        // hold a counted reference), so the header is valid to read.
        unsafe { self.header.as_ref() }
    }

    pub(crate) fn poll(self) {
        // SAFETY: `poll` is one of the six vtable functions built for this
        // task's exact (F, S); the header pointer is valid per the type's
        // invariant.
        unsafe { (self.header().vtable.poll)(self.header) }
    }

    pub(crate) fn schedule(self) {
        // SAFETY: see `poll`.
        unsafe { (self.header().vtable.schedule)(self.header) }
    }

    pub(crate) fn dealloc(self) {
        // SAFETY: see `poll`. Caller must have observed `ref_dec() == true`
        // (this was the last reference) before calling.
        unsafe { (self.header().vtable.dealloc)(self.header) }
    }

    pub(crate) fn dealloc_now(self) {
        // SAFETY: the caller owns the deferred deallocation reference and
        // invokes this only on the scheduler's owning thread.
        unsafe { (self.header().vtable.dealloc_now)(self.header) }
    }

    /// Drops this reference, deallocating if it was the last one.
    pub(crate) fn drop_reference(self) {
        if self.header().state.ref_dec() {
            self.dealloc();
        }
    }

    pub(crate) fn drop_reference_on_owner(self) {
        if self.header().state.ref_dec() {
            self.dealloc_now();
        }
    }

    /// SAFETY: caller is consuming exactly one reference to this task (the
    /// same discipline as dropping a `Waker`).
    pub(crate) unsafe fn wake_by_val(self) {
        use super::state::TransitionToNotifiedByVal as T;
        match self.header().state.transition_to_notified_by_val() {
            T::Submit => self.schedule(),
            T::DoNothing => self.drop_reference(),
        }
    }

    /// SAFETY: caller must not treat this as consuming a reference (the
    /// same discipline as `Waker::wake_by_ref`).
    pub(crate) unsafe fn wake_by_ref(self) {
        use super::state::TransitionToNotifiedByRef as T;
        match self.header().state.transition_to_notified_by_ref() {
            T::Submit => self.schedule(),
            T::DoNothing => {}
        }
    }
}

pub(crate) struct Vtable {
    pub(crate) poll: unsafe fn(NonNull<Header>),
    pub(crate) schedule: unsafe fn(NonNull<Header>),
    pub(crate) dealloc: unsafe fn(NonNull<Header>),
    pub(crate) dealloc_now: unsafe fn(NonNull<Header>),
    pub(crate) try_read_output: unsafe fn(NonNull<Header>, *mut (), &Waker) -> bool,
    pub(crate) drop_join_handle_slow: unsafe fn(NonNull<Header>),
    pub(crate) shutdown: unsafe fn(NonNull<Header>),
}

pub(crate) fn vtable<F: Future, S: Schedule>() -> &'static Vtable {
    &Vtable {
        poll: super::harness::poll::<F, S>,
        schedule: super::harness::schedule::<F, S>,
        dealloc: super::harness::dealloc::<F, S>,
        dealloc_now: super::harness::dealloc_now::<F, S>,
        try_read_output: super::harness::try_read_output::<F, S>,
        drop_join_handle_slow: super::harness::drop_join_handle_slow::<F, S>,
        shutdown: super::harness::shutdown::<F, S>,
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::task::Notified;

    struct NoopSchedule;
    impl Schedule for NoopSchedule {
        fn schedule(&self, _task: Notified<Self>) {}
    }

    #[test]
    fn cell_layout_is_repr_c_header_first() {
        // Header must be at offset 0 so `NonNull<Header>` obtained from a
        // `NonNull<Cell<F, S>>` cast is always valid.
        let cell: Cell<std::future::Ready<i32>, NoopSchedule> = Cell {
            header: Header::new::<std::future::Ready<i32>, NoopSchedule>(),
            core: Core {
                scheduler: NoopSchedule,
                stage: std::cell::UnsafeCell::new(Stage::Consumed),
            },
            trailer: Trailer::new(),
        };
        let cell_ptr: *const Cell<_, _> = &cell;
        let header_ptr: *const Header = &cell.header;
        assert_eq!(cell_ptr as usize, header_ptr as usize);
    }
}
