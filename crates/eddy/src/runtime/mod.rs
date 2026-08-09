//! Runtime construction and ambient handles for both executor flavors.

use std::cell::RefCell;
use std::future::Future;
use std::sync::Arc;

use crate::blocking::{DEFAULT_KEEP_ALIVE, DEFAULT_MAX_BLOCKING_THREADS};
use crate::scheduler::{CurrentThread, MultiThread, MultiThreadHandle, MultiThreadOptions};
use crate::task::JoinHandle;
use crate::time::TimerShared;

thread_local! {
    static CURRENT: RefCell<Option<Handle>> = const { RefCell::new(None) };
}

#[derive(Clone)]
enum Scheduler {
    Current(CurrentThread),
    Multi(MultiThread),
}

#[derive(Clone)]
pub struct Handle {
    scheduler: Scheduler,
}

impl Handle {
    /// # Panics
    /// Panics with a message naming the problem if called outside of a
    /// runtime's `block_on` — deliberately a clear panic rather than a
    /// silent no-op.
    pub fn current() -> Handle {
        Self::try_current().expect(
            "no eddy runtime running on this thread: call Handle::current() from inside Runtime::block_on",
        )
    }

    /// The ambient runtime handle of the current thread, if any.
    pub(crate) fn try_current() -> Option<Handle> {
        CURRENT.with(|c| c.borrow().clone())
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match &self.scheduler {
            Scheduler::Current(scheduler) => scheduler.spawn(future),
            Scheduler::Multi(scheduler) => scheduler.spawn(future),
        }
    }

    /// Spawn a potentially `!Send` future on a current-thread runtime.
    ///
    /// Multi-thread runtimes reject this operation because the future may be
    /// polled by any worker.
    pub fn spawn_local<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        match &self.scheduler {
            Scheduler::Current(scheduler) => scheduler.spawn(future),
            Scheduler::Multi(_) => panic!("eddy: spawn_local requires a current-thread runtime"),
        }
    }

    /// Spawn a blocking closure on the runtime's dedicated blocking pool.
    ///
    /// The closure runs on a separate thread so it may perform blocking
    /// work — synchronous I/O, CPU-heavy computation — without stalling
    /// worker threads. Once the task has started it is **not cancellable**:
    /// dropping or aborting the returned handle does not interrupt it, and
    /// the task always runs to completion (this surprises people; queued
    /// tasks are cancelled only when the runtime shuts down).
    pub fn spawn_blocking<F, R>(&self, task: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + Unpin + 'static,
        R: Send + 'static,
    {
        match &self.scheduler {
            Scheduler::Current(scheduler) => scheduler.spawn_blocking(task),
            Scheduler::Multi(scheduler) => scheduler.spawn_blocking(task),
        }
    }

    /// Run `fut` on the current thread while a takeover thread services this
    /// worker's run queue, so the future may borrow non-`'static` data and
    /// perform blocking work. Only available on multi-thread runtimes.
    ///
    /// # Panics
    /// Panics if called from outside a multi-thread runtime's worker thread;
    /// on current-thread runtimes it always panics (there is no queue to
    /// hand off) rather than deadlocking.
    pub fn block_in_place<F, R>(&self, fut: F) -> R
    where
        F: Future<Output = R>,
    {
        match &self.scheduler {
            Scheduler::Current(_) => panic!(
                "eddy: block_in_place requires a multi-thread runtime; \
                 current-thread runtimes cannot hand off their run queue"
            ),
            Scheduler::Multi(scheduler) => scheduler.block_in_place(fut),
        }
    }

    /// Enter this handle as the ambient runtime on the current thread.
    pub fn enter(&self) -> EnterGuard {
        EnterGuard::new(self.clone())
    }

    pub(crate) fn from_multi(scheduler: MultiThreadHandle) -> Handle {
        Handle {
            scheduler: Scheduler::Multi(MultiThread::from_handle(&scheduler)),
        }
    }

    pub(crate) fn from_current(scheduler: CurrentThread) -> Handle {
        Handle {
            scheduler: Scheduler::Current(scheduler),
        }
    }

    /// The I/O readiness driver of this handle's runtime, if it has one.
    /// Current-thread runtimes do not yet provide a driver.
    pub(crate) fn io_driver(&self) -> Option<std::sync::Arc<crate::io::driver::DriverShared>> {
        match &self.scheduler {
            Scheduler::Multi(scheduler) => Some(scheduler.io_driver()),
            Scheduler::Current(_) => None,
        }
    }

    pub(crate) fn timer_driver(&self) -> Option<Arc<TimerShared>> {
        match &self.scheduler {
            Scheduler::Multi(scheduler) => Some(scheduler.timer_driver()),
            Scheduler::Current(scheduler) => Some(scheduler.timer_driver()),
        }
    }
}

/// RAII guard installing a `Handle` as the ambient thread-local runtime for
/// the duration of `block_on`. Restores whatever was there before on drop.
pub struct EnterGuard {
    previous: Option<Handle>,
}

impl EnterGuard {
    pub(crate) fn new(handle: Handle) -> EnterGuard {
        let previous = CURRENT.with(|c| {
            c.replace(Some(Handle {
                scheduler: handle.scheduler,
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
    scheduler: Scheduler,
}

impl Runtime {
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        match &self.scheduler {
            Scheduler::Current(scheduler) => scheduler.block_on(future),
            Scheduler::Multi(scheduler) => scheduler.block_on(future),
        }
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match &self.scheduler {
            Scheduler::Current(scheduler) => scheduler.spawn(future),
            Scheduler::Multi(scheduler) => scheduler.spawn(future),
        }
    }

    pub fn spawn_local<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        match &self.scheduler {
            Scheduler::Current(scheduler) => scheduler.spawn(future),
            Scheduler::Multi(_) => panic!("eddy: spawn_local requires a current-thread runtime"),
        }
    }

    /// Spawn a blocking closure on the runtime's blocking pool; see
    /// [`Handle::spawn_blocking`] for semantics.
    pub fn spawn_blocking<F, R>(&self, task: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + Unpin + 'static,
        R: Send + 'static,
    {
        match &self.scheduler {
            Scheduler::Current(scheduler) => scheduler.spawn_blocking(task),
            Scheduler::Multi(scheduler) => scheduler.spawn_blocking(task),
        }
    }

    /// Run a future on the current thread with this worker's queue handed to
    /// a takeover thread; see [`Handle::block_in_place`].
    pub fn block_in_place<F, R>(&self, fut: F) -> R
    where
        F: Future<Output = R>,
    {
        match &self.scheduler {
            Scheduler::Current(_) => panic!(
                "eddy: block_in_place requires a multi-thread runtime; \
                 current-thread runtimes cannot hand off their run queue"
            ),
            Scheduler::Multi(scheduler) => scheduler.block_in_place(fut),
        }
    }

    /// Shut down the runtime gracefully, waiting up to `timeout` for worker
    /// threads to drain and exit. Stragglers are detached and exit on their
    /// own once their queues are empty.
    pub fn shutdown_timeout(&self, timeout: std::time::Duration) {
        match &self.scheduler {
            Scheduler::Current(scheduler) => scheduler.shutdown(),
            Scheduler::Multi(scheduler) => scheduler.shutdown_timeout(timeout),
        }
    }
}

pub struct Builder {
    flavor: Flavor,
    worker_threads: usize,
    thread_name: String,
    thread_stack_size: Option<usize>,
    lifo_slot: bool,
    max_blocking_threads: usize,
    keep_alive: std::time::Duration,
    on_thread_start: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
    on_thread_stop: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
    on_thread_park: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
    on_thread_unpark: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
}

enum Flavor {
    Current,
    Multi,
}

impl Builder {
    pub fn new_current_thread() -> Builder {
        Builder {
            flavor: Flavor::Current,
            worker_threads: 1,
            thread_name: "eddy-worker".to_string(),
            thread_stack_size: None,
            lifo_slot: true,
            max_blocking_threads: DEFAULT_MAX_BLOCKING_THREADS,
            keep_alive: DEFAULT_KEEP_ALIVE,
            on_thread_start: None,
            on_thread_stop: None,
            on_thread_park: None,
            on_thread_unpark: None,
        }
    }

    pub fn new_multi_thread() -> Builder {
        Builder {
            flavor: Flavor::Multi,
            worker_threads: std::thread::available_parallelism()
                .map(|parallelism| parallelism.get())
                .unwrap_or(1),
            thread_name: "eddy-worker".to_string(),
            thread_stack_size: None,
            lifo_slot: true,
            max_blocking_threads: DEFAULT_MAX_BLOCKING_THREADS,
            keep_alive: DEFAULT_KEEP_ALIVE,
            on_thread_start: None,
            on_thread_stop: None,
            on_thread_park: None,
            on_thread_unpark: None,
        }
    }

    pub fn worker_threads(mut self, worker_threads: usize) -> Builder {
        assert!(
            worker_threads > 0,
            "eddy: worker_threads must be greater than zero"
        );
        self.worker_threads = worker_threads;
        self
    }

    /// Set the base name of the worker threads; each gets `-<id>` appended.
    pub fn thread_name(mut self, name: impl Into<String>) -> Builder {
        self.thread_name = name.into();
        self
    }

    /// Set the stack size of each worker thread.
    pub fn thread_stack_size(mut self, stack_size: usize) -> Builder {
        self.thread_stack_size = Some(stack_size);
        self
    }

    /// Disable the LIFO slot optimization, which also disables its
    /// preemption-pair latency trade-off.
    pub fn disable_lifo_slot(mut self) -> Builder {
        self.lifo_slot = false;
        self
    }

    /// Set the maximum number of blocking-pool threads. Defaults to 512 —
    /// deliberately large, since these threads are typically blocked on I/O
    /// rather than consuming CPU. Pool threads are spawned lazily on demand.
    pub fn max_blocking_threads(mut self, max: usize) -> Builder {
        assert!(
            max > 0,
            "eddy: max_blocking_threads must be greater than zero"
        );
        self.max_blocking_threads = max;
        self
    }

    /// Set how long an idle blocking-pool thread waits for work before
    /// exiting. Defaults to 10 seconds.
    pub fn keep_alive(mut self, keep_alive: std::time::Duration) -> Builder {
        self.keep_alive = keep_alive;
        self
    }

    /// Run `f` when a worker thread starts.
    pub fn on_thread_start(mut self, f: impl Fn() + Send + Sync + 'static) -> Builder {
        self.on_thread_start = Some(std::sync::Arc::new(f));
        self
    }

    /// Run `f` when a worker thread stops.
    pub fn on_thread_stop(mut self, f: impl Fn() + Send + Sync + 'static) -> Builder {
        self.on_thread_stop = Some(std::sync::Arc::new(f));
        self
    }

    /// Run `f` every time a worker thread parks.
    pub fn on_thread_park(mut self, f: impl Fn() + Send + Sync + 'static) -> Builder {
        self.on_thread_park = Some(std::sync::Arc::new(f));
        self
    }

    /// Run `f` every time a worker thread unparks.
    pub fn on_thread_unpark(mut self, f: impl Fn() + Send + Sync + 'static) -> Builder {
        self.on_thread_unpark = Some(std::sync::Arc::new(f));
        self
    }

    pub fn build(self) -> Runtime {
        let scheduler = match self.flavor {
            Flavor::Current => Scheduler::Current(CurrentThread::new(
                self.max_blocking_threads,
                self.keep_alive,
            )),
            Flavor::Multi => Scheduler::Multi(MultiThread::new_with_options(MultiThreadOptions {
                worker_threads: self.worker_threads,
                thread_name: self.thread_name,
                thread_stack_size: self.thread_stack_size,
                lifo_slot: self.lifo_slot,
                max_blocking_threads: self.max_blocking_threads,
                keep_alive: self.keep_alive,
                on_thread_start: self.on_thread_start,
                on_thread_stop: self.on_thread_stop,
                on_thread_park: self.on_thread_park,
                on_thread_unpark: self.on_thread_unpark,
            })),
        };
        Runtime { scheduler }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        match &self.scheduler {
            Scheduler::Current(scheduler) => scheduler.shutdown(),
            Scheduler::Multi(scheduler) => scheduler.shutdown(),
        }
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
