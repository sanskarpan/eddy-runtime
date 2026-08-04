//! Ambient runtime handle + builder for the current-thread executor. Only
//! `new_current_thread` exists in this slice; `new_multi_thread` arrives
//! with Phase 4.

use std::cell::RefCell;
use std::future::Future;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::scheduler::CurrentThread;
use crate::task::JoinHandle;

thread_local! {
    static CURRENT: RefCell<Option<Handle>> = const { RefCell::new(None) };
}

#[derive(Clone)]
pub struct Handle {
    scheduler: CurrentThread,
    _not_send: PhantomData<Rc<()>>,
}

impl Handle {
    /// # Panics
    /// Panics with a message naming the problem if called outside of a
    /// runtime's `block_on` — deliberately a clear panic rather than a
    /// silent no-op.
    pub fn current() -> Handle {
        CURRENT.with(|c| {
            c.borrow().clone().expect(
                "no eddy runtime running on this thread: call Handle::current() from inside Runtime::block_on",
            )
        })
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        self.scheduler.spawn(future)
    }

    /// Enter this handle as the ambient runtime on the current thread.
    pub fn enter(&self) -> EnterGuard {
        EnterGuard::new(self.scheduler.clone())
    }
}

/// RAII guard installing a `Handle` as the ambient thread-local runtime for
/// the duration of `block_on`. Restores whatever was there before on drop.
pub struct EnterGuard {
    previous: Option<Handle>,
}

impl EnterGuard {
    pub(crate) fn new(scheduler: CurrentThread) -> EnterGuard {
        let previous = CURRENT.with(|c| {
            c.replace(Some(Handle {
                scheduler,
                _not_send: PhantomData,
            }))
        });
        EnterGuard { previous }
    }
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = self.previous.take());
    }
}

pub struct Runtime {
    scheduler: CurrentThread,
    // A current-thread runtime must not be moved to another thread while it
    // owns local futures and their executor state.
    _not_send: PhantomData<Rc<()>>,
}

impl Runtime {
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.scheduler.block_on(future)
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        self.scheduler.spawn(future)
    }
}

pub struct Builder {
    // Only current-thread flavor exists in this slice.
}

impl Builder {
    pub fn new_current_thread() -> Builder {
        Builder {}
    }

    pub fn build(self) -> Runtime {
        Runtime {
            scheduler: CurrentThread::new(),
            _not_send: PhantomData,
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.scheduler.shutdown();
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn builder_new_current_thread_block_on_42() {
        let rt = Builder::new_current_thread().build();
        assert_eq!(rt.block_on(async { 42 }), 42);
    }

    #[test]
    #[should_panic(expected = "no eddy runtime")]
    fn handle_current_outside_runtime_panics_clearly() {
        let _ = Handle::current();
    }

    #[test]
    fn handle_current_inside_block_on_works() {
        let rt = Builder::new_current_thread().build();
        let ok = rt.block_on(async { Handle::current().spawn(async { 1 }).await.unwrap() == 1 });
        assert!(ok);
    }
}
