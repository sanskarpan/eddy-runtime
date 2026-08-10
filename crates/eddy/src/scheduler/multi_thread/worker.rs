//! Multi-thread scheduler workers.
//!
//! Each worker owns one local Chase-Lev queue. Wakes from another thread enter
//! the intrusive global injector; same-worker wakes use the non-stealable LIFO
//! slot before falling back to the local queue. The scheduler is intentionally
//! independent of I/O and timers: those drivers only need a `Waker`.

use std::cell::{Cell, RefCell, UnsafeCell};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle as ThreadJoinHandle, Thread};
use std::time::{Duration, Instant};

use super::{Injector, Local};
use crate::blocking::{self, BlockingPool};
use crate::io::driver::DriverShared;
use crate::task::{self, JoinHandle, Notified, RawTask, Schedule};
use crate::time::TimerShared;
use crate::util::FastRand;

const GLOBAL_QUEUE_INTERVAL: u32 = 61;
const MAX_LIFO_POLLS_PER_TICK: u8 = 3;

thread_local! {
    static WORKER_ID: Cell<Option<usize>> = const { Cell::new(None) };
    static LIFO_SLOT: RefCell<Option<Notified<MultiThreadHandle>>> = const { RefCell::new(None) };
}

pub(crate) struct MultiThreadOptions {
    pub(crate) worker_threads: usize,
    pub(crate) thread_name: String,
    pub(crate) thread_stack_size: Option<usize>,
    pub(crate) lifo_slot: bool,
    pub(crate) max_blocking_threads: usize,
    pub(crate) keep_alive: Duration,
    pub(crate) on_thread_start: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(crate) on_thread_stop: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(crate) on_thread_park: Option<Arc<dyn Fn() + Send + Sync>>,
    pub(crate) on_thread_unpark: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Default for MultiThreadOptions {
    fn default() -> MultiThreadOptions {
        MultiThreadOptions {
            worker_threads: 1,
            thread_name: "eddy-worker".to_string(),
            thread_stack_size: None,
            lifo_slot: true,
            max_blocking_threads: crate::blocking::DEFAULT_MAX_BLOCKING_THREADS,
            keep_alive: crate::blocking::DEFAULT_KEEP_ALIVE,
            on_thread_start: None,
            on_thread_stop: None,
            on_thread_park: None,
            on_thread_unpark: None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct MultiThread {
    shared: Arc<Shared>,
}

#[derive(Clone)]
pub(crate) struct MultiThreadHandle {
    shared: Arc<Shared>,
    target: usize,
}

struct WorkerShared {
    queue: UnsafeCell<Local<Notified<MultiThreadHandle>>>,
    thread: Mutex<Option<Thread>>,
    finished: AtomicBool,
}

// SAFETY: only the worker identified by `WORKER_ID` mutates this queue. Other
// threads use the queue's immutable thief handle or route tasks to the global
// injector instead.
unsafe impl Send for WorkerShared {}
// SAFETY: the interior-mutable queue is confined to its owner worker; reads
// by thieves go through the queue's own synchronization.
unsafe impl Sync for WorkerShared {}

struct Shared {
    workers: Vec<WorkerShared>,
    inject: Injector<MultiThreadHandle>,
    io: Arc<DriverShared>,
    shutdown: AtomicBool,
    tasks: Mutex<Vec<RawTask>>,
    threads: Mutex<Vec<ThreadJoinHandle<()>>>,
    blocking: BlockingPool,
    next_worker: AtomicUsize,
    steal_count: AtomicUsize,
    searching: AtomicUsize,
    idle: IdleState,
    lifo_slot_enabled: bool,
    hooks: WorkerHooks,
}

// SAFETY: `WorkerShared` owns the only interior-mutable local queues and all
// other mutable state is behind synchronization primitives.
unsafe impl Send for Shared {}
// SAFETY: see `Send`; the same confinement argument applies to sharing.
unsafe impl Sync for Shared {}

/// Bitmap of which workers are currently parked, for O(1) "who to unpark".
struct IdleState {
    workers: usize,
    parked: AtomicUsize,
}

impl IdleState {
    fn new(workers: usize) -> IdleState {
        IdleState {
            workers,
            parked: AtomicUsize::new(0),
        }
    }

    /// Park this worker: set its bit (after a final work re-check), then
    /// block on the I/O driver's park — the first worker to park takes the
    /// kernel readiness wait, the rest the driver condvar. The driver now
    /// uses the timer wheel's next deadline for its kernel wait timeout.
    fn park(&self, id: usize, shared: &Shared) {
        self.parked.fetch_or(1 << id, Ordering::SeqCst);
        if has_work(shared, id) {
            self.parked.fetch_and(!(1 << id), Ordering::SeqCst);
            return;
        }
        if let Some(hook) = &shared.hooks.on_thread_park {
            hook();
        }
        shared.io.park_worker(id);
        if let Some(hook) = &shared.hooks.on_thread_unpark {
            hook();
        }
        self.parked.fetch_and(!(1 << id), Ordering::SeqCst);
    }

    /// Wake one parked worker, if any. Double-unparks are harmless: the
    /// driver wake is sticky.
    fn unpark_any(&self, shared: &Shared) {
        let parked = self.parked.load(Ordering::SeqCst);
        if parked == 0 {
            return;
        }
        let id = parked.trailing_zeros() as usize;
        if id >= self.workers {
            return;
        }
        self.parked.fetch_and(!(1 << id), Ordering::SeqCst);
        // L3: workers block in `park_worker`, never in `thread::park()`, so
        // route through the driver (holder wake / condvar notify / stale
        // wake) instead of an inert `thread::unpark`.
        shared.io.unpark_worker(id);
    }
}

struct WorkerHooks {
    on_thread_start: Option<Arc<dyn Fn() + Send + Sync>>,
    on_thread_stop: Option<Arc<dyn Fn() + Send + Sync>>,
    on_thread_park: Option<Arc<dyn Fn() + Send + Sync>>,
    on_thread_unpark: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl WorkerHooks {
    fn from_options(options: &MultiThreadOptions) -> WorkerHooks {
        WorkerHooks {
            on_thread_start: options.on_thread_start.clone(),
            on_thread_stop: options.on_thread_stop.clone(),
            on_thread_park: options.on_thread_park.clone(),
            on_thread_unpark: options.on_thread_unpark.clone(),
        }
    }
}

impl MultiThread {
    pub(crate) fn from_handle(handle: &MultiThreadHandle) -> MultiThread {
        MultiThread {
            shared: handle.shared.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new(worker_threads: usize) -> MultiThread {
        MultiThread::new_with_options(MultiThreadOptions {
            worker_threads,
            ..MultiThreadOptions::default()
        })
    }

    pub(crate) fn new_with_options(options: MultiThreadOptions) -> MultiThread {
        let worker_threads = options.worker_threads.max(1);
        let workers = (0..worker_threads)
            .map(|_| WorkerShared {
                queue: UnsafeCell::new(Local::<Notified<MultiThreadHandle>>::new()),
                thread: Mutex::new(None),
                finished: AtomicBool::new(false),
            })
            .collect();
        let io = DriverShared::new().expect(
            "eddy: failed to start the I/O driver: ensure the runtime thread supports \
                     the platform's readiness backend",
        );
        let shared = Arc::new(Shared {
            workers,
            inject: Injector::new(),
            io,
            shutdown: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
            threads: Mutex::new(Vec::new()),
            blocking: BlockingPool::new(options.max_blocking_threads, options.keep_alive),
            next_worker: AtomicUsize::new(0),
            steal_count: AtomicUsize::new(0),
            searching: AtomicUsize::new(0),
            idle: IdleState::new(worker_threads),
            lifo_slot_enabled: options.lifo_slot,
            hooks: WorkerHooks::from_options(&options),
        });

        for id in 0..worker_threads {
            let worker_shared = shared.clone();
            let mut builder = thread::Builder::new().name(format!("{}-{id}", options.thread_name));
            if let Some(stack_size) = options.thread_stack_size {
                builder = builder.stack_size(stack_size);
            }
            let thread = builder
                .spawn(move || worker_loop(worker_shared, id, None))
                .expect("eddy: failed to start worker thread");
            shared.workers[id]
                .thread
                .lock()
                .unwrap()
                .replace(thread.thread().clone());
            shared.threads.lock().unwrap().push(thread);
        }

        MultiThread { shared }
    }

    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        assert!(
            !self.shared.shutdown.load(Ordering::Acquire),
            "eddy: cannot spawn on a shut down runtime"
        );
        let target = WORKER_ID.with(|id| {
            id.get().unwrap_or_else(|| {
                self.shared.next_worker.fetch_add(1, Ordering::Relaxed) % self.shared.workers.len()
            })
        });
        let scheduler = MultiThreadHandle {
            shared: self.shared.clone(),
            target,
        };
        let (notified, handle) = task::spawn(future, scheduler.clone());
        scheduler.register_task(notified.raw);
        scheduler.schedule(notified);
        handle
    }

    pub(crate) fn spawn_blocking<F, R>(&self, task: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + Unpin + 'static,
        R: Send + 'static,
    {
        let handle = crate::runtime::Handle::from_multi(MultiThreadHandle {
            shared: self.shared.clone(),
            target: self.shared.next_worker.fetch_add(1, Ordering::Relaxed)
                % self.shared.workers.len(),
        });
        blocking::spawn_on_pool(&self.shared.blocking, handle, task)
    }

    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        // A worker calling `block_on` (nested) must not keep its identity:
        // spawns inside the root future would route to this thread's local
        // queue, which only `worker_loop` services — and this thread is
        // blocked here. Clear it for the duration, as `block_in_place` does.
        let was_worker = WORKER_ID.with(|worker_id| worker_id.take());
        let scheduler = MultiThreadHandle {
            shared: self.shared.clone(),
            target: self.shared.next_worker.fetch_add(1, Ordering::Relaxed)
                % self.shared.workers.len(),
        };
        let _enter =
            crate::runtime::EnterGuard::new(crate::runtime::Handle::from_multi(scheduler.clone()));
        let signal = Arc::new(RootSignal {
            thread: thread::current(),
            woken: AtomicBool::new(true),
        });
        let waker = root_waker(signal.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);

        let output = loop {
            if signal.woken.swap(false, Ordering::Acquire) {
                if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                    break output;
                }
            }
            thread::park();
        };
        if let Some(id) = was_worker {
            WORKER_ID.with(|worker_id| worker_id.set(Some(id)));
        }
        output
    }

    /// Run `fut` on the calling thread. On a worker thread the run queue is
    /// handed to a takeover thread that services it until `fut` completes;
    /// on any other thread `fut` is simply polled in place.
    pub(crate) fn block_in_place<F: Future>(&self, fut: F) -> F::Output {
        match current_worker_id() {
            Some(id) => block_in_place_on_worker(&self.shared, id, fut),
            None => inline_block_in_place(fut),
        }
    }

    pub(crate) fn shutdown(&self) {
        if self.shared.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        self.shared.inject.close();
        let tasks = {
            let mut tasks = self.shared.tasks.lock().unwrap();
            std::mem::take(&mut *tasks)
        };
        for task in &tasks {
            // SAFETY: every registry entry retains a live reference and the
            // vtable belongs to the task's `MultiThreadHandle` scheduler.
            unsafe { (task.header().vtable.shutdown)(task.header) };
        }
        self.unpark_all();
        let threads = std::mem::take(&mut *self.shared.threads.lock().unwrap());
        for thread in threads {
            thread.join().expect("eddy: worker thread panicked");
        }
        for task in tasks {
            task.drop_reference();
        }
        self.shared.blocking.shutdown();
    }

    /// Graceful shutdown with a deadline: workers have already been asked to
    /// drain and exit, so if they finish within `timeout` they are joined;
    /// stragglers are detached (they exit on their own once drained).
    pub(crate) fn shutdown_timeout(&self, timeout: Duration) {
        if self.shared.shutdown.swap(true, Ordering::AcqRel) {
            return;
        }
        self.shared.inject.close();
        let tasks = {
            let mut tasks = self.shared.tasks.lock().unwrap();
            std::mem::take(&mut *tasks)
        };
        for task in &tasks {
            // SAFETY: see `shutdown`.
            unsafe { (task.header().vtable.shutdown)(task.header) };
        }
        self.unpark_all();
        let threads = std::mem::take(&mut *self.shared.threads.lock().unwrap());
        let deadline = Instant::now() + timeout;
        for (id, thread) in threads.into_iter().enumerate() {
            if !self.shared.workers[id].finished.load(Ordering::Acquire)
                && Instant::now() < deadline
            {
                loop {
                    if self.shared.workers[id].finished.load(Ordering::Acquire) {
                        break;
                    }
                    if Instant::now() >= deadline {
                        break;
                    }
                    thread::sleep(Duration::from_millis(1));
                }
            }
            if self.shared.workers[id].finished.load(Ordering::Acquire) {
                thread.join().expect("eddy: worker thread panicked");
            }
        }
        for task in tasks {
            task.drop_reference();
        }
        self.shared.blocking.shutdown();
    }

    /// Number of successful steals across all workers; used by tests and
    /// future metrics.
    #[allow(dead_code)]
    pub(crate) fn steal_count(&self) -> usize {
        self.shared.steal_count.load(Ordering::Relaxed)
    }

    fn unpark_all(&self) {
        for worker in &self.shared.workers {
            if let Some(thread) = worker.thread.lock().unwrap().as_ref() {
                thread.unpark();
            }
        }
        // Wake the worker currently inside the kernel readiness wait too;
        // `thread::unpark` cannot interrupt `kevent`/`epoll_wait`.
        self.shared.io.unpark_all();
    }

    /// The runtime's I/O driver, for `Registration` and friends.
    pub(crate) fn io_driver(&self) -> Arc<DriverShared> {
        self.shared.io.clone()
    }

    pub(crate) fn timer_driver(&self) -> Arc<TimerShared> {
        self.shared.io.timer_driver()
    }
}

impl Schedule for MultiThreadHandle {
    fn schedule(&self, task: Notified<Self>) {
        if self.shared.shutdown.load(Ordering::Acquire) {
            drop(task);
            return;
        }

        let on_target = WORKER_ID.with(|id| id.get() == Some(self.target));
        if on_target {
            let mut task = Some(task);
            if self.shared.lifo_slot_enabled {
                let placed_in_lifo = LIFO_SLOT.with(|slot| {
                    let mut slot = slot.borrow_mut();
                    if slot.is_none() {
                        *slot = task.take();
                        true
                    } else {
                        false
                    }
                });
                if placed_in_lifo {
                    return;
                }
            }
            let task = task.expect("eddy: task lost while placing in LIFO slot");
            // SAFETY: `on_target` proves this worker thread owns the queue;
            // no other thread can hold a mutable reference to it.
            let queue = unsafe { &mut *self.shared.workers[self.target].queue.get() };
            if let Err(task) = queue.push_back(task) {
                // Full: move the older half to the injector, keep `task`
                // local (see `push_overflow` for why not the reverse).
                let mut overflow = Vec::new();
                queue.push_overflow(task, &mut overflow);
                self.shared.inject.push_batch(overflow);
                self.unpark_target();
            }
        } else {
            self.shared.inject.push(task);
            self.unpark_target();
        }
    }

    fn defer_dealloc(&self, task: Notified<Self>) {
        self.schedule(task);
    }

    fn can_dealloc_remotely(&self) -> bool {
        true
    }

    fn task_complete(&self, task: RawTask) {
        let removed = {
            let mut tasks = self.shared.tasks.lock().unwrap();
            tasks
                .iter()
                .position(|registered| registered.header == task.header)
                .map(|index| tasks.swap_remove(index))
        };
        if let Some(task) = removed {
            task.drop_reference();
        }
    }

    fn register_task(&self, task: RawTask) {
        task.header().state.ref_inc();
        self.shared.tasks.lock().unwrap().push(task);
    }
}

impl MultiThreadHandle {
    fn unpark_target(&self) {
        // Workers park in the driver's `park_worker` (kernel readiness wait
        // or condvar), never in `thread::park()`, so the driver signal is
        // the only wake that matters: it interrupts the kernel wait when the
        // target is the driver holder, the condvar notify wakes it when it
        // is a condvar sleeper, and `unpark_worker` additionally covers the
        // window where the target has claimed neither yet.
        self.shared.io.unpark_worker(self.target);
    }
}

// SAFETY: `Notified` contains only a type-erased task pointer. The scheduler
// implementation below is the synchronization boundary that guarantees a
// task is polled by one worker and that its Send future is never moved while
// borrowed.
unsafe impl Send for Notified<MultiThreadHandle> {}

fn worker_loop(shared: Arc<Shared>, id: usize, takeover: Option<Arc<Takeover>>) {
    WORKER_ID.with(|worker_id| worker_id.set(Some(id)));
    let scheduler = MultiThreadHandle {
        shared: shared.clone(),
        target: id,
    };
    if takeover.is_none() {
        if let Some(hook) = &shared.hooks.on_thread_start {
            hook();
        }
    }
    let _enter = crate::runtime::EnterGuard::new(crate::runtime::Handle::from_multi(scheduler));
    let mut tick = 0u32;
    let mut lifo_polls = 0u8;
    let mut rand = FastRand::new(id as u32 + 1);

    loop {
        if let Some(task) = next_lifo(&mut lifo_polls) {
            task.run();
            continue;
        }

        // SAFETY: this worker owns queue `id` (WORKER_ID == id), so a
        // mutable borrow cannot alias any other live reference.
        let task = unsafe { &mut *shared.workers[id].queue.get() }.pop();
        if let Some(task) = task {
            lifo_polls = 0;
            task.run();
            continue;
        }

        tick = tick.wrapping_add(1);
        let global_first = tick % GLOBAL_QUEUE_INTERVAL == 0;
        if global_first {
            if let Some(task) = take_injected(&shared, id) {
                task.run();
                continue;
            }
        }

        if let Some(task) = search_and_steal(&shared, id, &mut rand) {
            shared.steal_count.fetch_add(1, Ordering::Relaxed);
            task.run();
            continue;
        }

        if !global_first {
            if let Some(task) = take_injected(&shared, id) {
                task.run();
                continue;
            }
        }

        let takeover_requested = takeover
            .as_ref()
            .is_some_and(|takeover| takeover.requested.load(Ordering::Acquire));
        if shared.shutdown.load(Ordering::Acquire) || takeover_requested {
            // SAFETY: this worker owns queue `id`; reading it here can only
            // race with the owner's own operations.
            let local_empty = unsafe { (&*shared.workers[id].queue.get()).len() == 0 };
            let lifo_empty = LIFO_SLOT.with(|slot| slot.borrow().is_none());
            // A takeover thread hands back early; the returning worker picks
            // up whatever remains in the shared injector itself.
            let inject_empty = takeover_requested || shared.inject.len() == 0;
            if local_empty && lifo_empty && inject_empty {
                break;
            }
        }
        shared.idle.park(id, &shared);
    }
    let lifo = LIFO_SLOT.with(|slot| slot.borrow_mut().take());
    WORKER_ID.with(|worker_id| worker_id.set(None));
    match &takeover {
        Some(takeover) => {
            *takeover.lifo.lock().unwrap() = lifo;
        }
        None => {
            shared.workers[id].finished.store(true, Ordering::Release);
            if let Some(hook) = &shared.hooks.on_thread_stop {
                hook();
            }
        }
    }
}

/// The thread that services worker `id`'s queues while the worker itself
/// runs a future passed to `block_in_place`.
fn takeover_thread(shared: Arc<Shared>, id: usize, takeover: Arc<Takeover>) {
    let queue = takeover
        .queue
        .lock()
        .unwrap()
        .take()
        .expect("eddy: takeover queue missing");
    // SAFETY: the original worker handed ownership of queue `id` to this
    // thread before spawning it, and will not touch it until it is returned.
    unsafe { *shared.workers[id].queue.get() = queue };
    LIFO_SLOT.with(|slot| *slot.borrow_mut() = takeover.lifo.lock().unwrap().take());
    WORKER_ID.with(|worker_id| worker_id.set(Some(id)));
    *shared.workers[id].thread.lock().unwrap() = Some(std::thread::current());
    worker_loop(shared.clone(), id, Some(takeover.clone()));
    // Return the queue to the blocked worker.
    // SAFETY: this thread still owns queue `id`; the returning worker only
    // touches it after observing the hand-back below.
    let queue = unsafe { std::mem::replace(&mut *shared.workers[id].queue.get(), Local::new()) };
    *takeover.queue.lock().unwrap() = Some(queue);
    takeover.returned.store(true, Ordering::Release);
    takeover.condvar.notify_one();
}

/// State exchanged between a worker blocked in `block_in_place` and its
/// takeover thread.
struct Takeover {
    /// The local run queue, owned by whichever thread currently services it.
    queue: Mutex<Option<Local<Notified<MultiThreadHandle>>>>,
    /// The LIFO slot contents, transferred along with the queue.
    lifo: Mutex<Option<Notified<MultiThreadHandle>>>,
    /// Set by the worker once it wants its queue back.
    requested: AtomicBool,
    /// Set by the takeover thread after it has put the queue back. The
    /// worker must wait for this, NOT for a non-empty `queue`: before the
    /// takeover thread starts, `queue` legitimately holds the startup
    /// hand-off and is not the return value yet.
    returned: AtomicBool,
    condvar: Condvar,
    thread: Mutex<Option<ThreadJoinHandle<()>>>,
}

/// The `WORKER_ID` of the current thread, if it is a worker.
pub(crate) fn current_worker_id() -> Option<usize> {
    WORKER_ID.with(|worker_id| worker_id.get())
}

/// Run `fut` on this worker thread while a freshly spawned takeover thread
/// services the worker's queues. Clears this thread's worker identity for
/// the duration so spawns inside `fut` route to the takeover thread.
fn block_in_place_on_worker<F: Future>(shared: &Arc<Shared>, id: usize, fut: F) -> F::Output {
    // SAFETY: this worker owns queue `id`; the empty queue left behind
    // becomes the takeover thread's entry point.
    let queue = unsafe { std::mem::replace(&mut *shared.workers[id].queue.get(), Local::new()) };
    let lifo = LIFO_SLOT.with(|slot| slot.borrow_mut().take());
    WORKER_ID.with(|worker_id| worker_id.set(None));

    let takeover = Arc::new(Takeover {
        queue: Mutex::new(Some(queue)),
        lifo: Mutex::new(lifo),
        requested: AtomicBool::new(false),
        returned: AtomicBool::new(false),
        condvar: Condvar::new(),
        thread: Mutex::new(None),
    });
    let thread = thread::Builder::new()
        .name(format!("eddy-takeover-{id}"))
        .spawn({
            let shared = shared.clone();
            let takeover = takeover.clone();
            move || takeover_thread(shared, id, takeover)
        })
        .expect("eddy: failed to start takeover thread");
    // Wakes for this worker must now reach the takeover thread.
    *shared.workers[id].thread.lock().unwrap() = Some(thread.thread().clone());
    *takeover.thread.lock().unwrap() = Some(thread);

    // Poll the future to completion on this thread. A panic must still run
    // the hand-back below: the takeover thread keeps servicing the queue
    // until `requested` is set, so skipping it would leave two threads
    // touching one `UnsafeCell` queue.
    let output = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let signal = Arc::new(RootSignal {
            thread: thread::current(),
            woken: AtomicBool::new(true),
        });
        let waker = root_waker(signal.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        let mut fut = std::pin::pin!(fut);
        loop {
            if signal.woken.swap(false, Ordering::Acquire) {
                if let std::task::Poll::Ready(output) = fut.as_mut().poll(&mut cx) {
                    return output;
                }
            }
            thread::park();
        }
    }));

    // Ask for the queue back, waking the takeover thread at the driver
    // level in case it is inside a kernel readiness wait; `unpark_worker`
    // also covers the takeover thread not having claimed the holder yet.
    takeover.requested.store(true, Ordering::Release);
    shared.io.unpark_worker(id);
    let queue = {
        let mut state = takeover.queue.lock().unwrap();
        while !takeover.returned.load(Ordering::Acquire) {
            state = takeover
                .condvar
                .wait(state)
                .expect("eddy: takeover condvar poisoned");
        }
        state.take().expect("eddy: takeover queue missing")
    };
    let lifo = takeover.lifo.lock().unwrap().take();
    // SAFETY: the takeover thread has handed the queue back and will not
    // touch it again; only this worker may access it from here on.
    unsafe { *shared.workers[id].queue.get() = queue };
    LIFO_SLOT.with(|slot| *slot.borrow_mut() = lifo);
    WORKER_ID.with(|worker_id| worker_id.set(Some(id)));
    *shared.workers[id].thread.lock().unwrap() = Some(thread::current());
    if let Some(thread) = takeover.thread.lock().unwrap().take() {
        thread.join().expect("eddy: takeover thread panicked");
    }

    match output {
        Ok(output) => output,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

/// Poll `fut` in place on a thread that is not a worker (e.g. the `block_on`
/// thread). There is no run queue to hand off, so this simply blocks the
/// calling thread until the future completes.
fn inline_block_in_place<F: Future>(fut: F) -> F::Output {
    let signal = Arc::new(RootSignal {
        thread: thread::current(),
        woken: AtomicBool::new(true),
    });
    let waker = root_waker(signal.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    loop {
        if signal.woken.swap(false, Ordering::Acquire) {
            if let std::task::Poll::Ready(output) = fut.as_mut().poll(&mut cx) {
                return output;
            }
        }
        thread::park();
    }
}

/// Whether this worker currently has any work of its own (used to decide
/// whether parking would lose work that arrived mid-transition).
fn has_work(shared: &Shared, id: usize) -> bool {
    if shared.inject.len() > 0 {
        return true;
    }
    if LIFO_SLOT.with(|slot| slot.borrow().is_some()) {
        return true;
    }
    // SAFETY: this worker owns queue `id`.
    unsafe { (&*shared.workers[id].queue.get()).len() > 0 }
}

/// Try to steal work, honoring the searching cap: at most half the workers
/// may be searching at once, otherwise an idle storm hits every queue. When
/// the LAST searching worker gives up empty-handed, it hands the baton to a
/// parked worker so parallelism never collapses to a single searcher.
fn search_and_steal(
    shared: &Shared,
    id: usize,
    rand: &mut FastRand,
) -> Option<Notified<MultiThreadHandle>> {
    if shared.searching.load(Ordering::Acquire) >= shared.workers.len() / 2 {
        return None;
    }
    loop {
        let count = shared.searching.load(Ordering::Relaxed);
        if count >= shared.workers.len() / 2 {
            return None;
        }
        if shared
            .searching
            .compare_exchange_weak(count, count + 1, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            break;
        }
    }
    let task = steal_work(shared, id, rand);
    let prev = shared.searching.fetch_sub(1, Ordering::AcqRel);
    if task.is_none() && prev == 1 {
        shared.idle.unpark_any(shared);
    }
    task
}

fn next_lifo(lifo_polls: &mut u8) -> Option<Notified<MultiThreadHandle>> {
    if *lifo_polls >= MAX_LIFO_POLLS_PER_TICK {
        *lifo_polls = 0;
        return None;
    }
    let task = LIFO_SLOT.with(|slot| slot.borrow_mut().take());
    if task.is_some() {
        *lifo_polls += 1;
    }
    task
}

fn take_injected(shared: &Shared, id: usize) -> Option<Notified<MultiThreadHandle>> {
    let task = shared.inject.pop()?;
    // SAFETY: this worker owns queue `id`; no other thread mutates it.
    let queue = unsafe { &mut *shared.workers[id].queue.get() };
    match queue.push_back(task) {
        Ok(()) => queue.pop(),
        // L10: the local queue is full; run the popped task directly instead
        // of re-pushing to the injector (which would hide it from thieves
        // for no benefit and force a second injector pop).
        Err(task) => Some(task),
    }
}

fn steal_work(
    shared: &Shared,
    id: usize,
    rand: &mut FastRand,
) -> Option<Notified<MultiThreadHandle>> {
    let workers = shared.workers.len();
    if workers < 2 {
        return None;
    }
    let start = rand.fastrand_n(workers as u32) as usize;
    for offset in 0..workers {
        let victim = (start + offset) % workers;
        if victim == id {
            continue;
        }
        // SAFETY: the victim queue is only read through `steal_into`'s
        // synchronized thief operations, never mutated by this worker.
        let source = unsafe { &*shared.workers[victim].queue.get() };
        // SAFETY: this worker owns queue `id`; `steal_into` transfers a
        // task into it without aliasing any other live reference.
        let destination = unsafe { &mut *shared.workers[id].queue.get() };
        if let Some(task) = source.steal_into(destination) {
            return Some(task);
        }
    }
    None
}

struct RootSignal {
    thread: Thread,
    woken: AtomicBool,
}

fn root_waker(signal: Arc<RootSignal>) -> std::task::Waker {
    let raw = Arc::into_raw(signal) as *const ();
    // SAFETY: the vtable treats `raw` as an `Arc<RootSignal>` and preserves
    // exactly one reference for every raw waker operation.
    unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(raw, &ROOT_VTABLE)) }
}

static ROOT_VTABLE: std::task::RawWakerVTable =
    std::task::RawWakerVTable::new(root_clone, root_wake, root_wake_by_ref, root_drop);

unsafe fn root_clone(ptr: *const ()) -> std::task::RawWaker {
    Arc::increment_strong_count(ptr as *const RootSignal);
    std::task::RawWaker::new(ptr, &ROOT_VTABLE)
}

unsafe fn root_wake(ptr: *const ()) {
    let signal = Arc::from_raw(ptr as *const RootSignal);
    signal.woken.store(true, Ordering::Release);
    signal.thread.unpark();
}

unsafe fn root_wake_by_ref(ptr: *const ()) {
    let signal = std::mem::ManuallyDrop::new(Arc::from_raw(ptr as *const RootSignal));
    signal.woken.store(true, Ordering::Release);
    signal.thread.unpark();
}

unsafe fn root_drop(ptr: *const ()) {
    Arc::decrement_strong_count(ptr as *const RootSignal);
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::sync::atomic::AtomicI32;
    use std::task::{Context, Poll};

    #[test]
    fn spawn_and_join_round_trip() {
        let rt = MultiThread::new(2);
        let out = rt.block_on(async {
            let handle = crate::runtime::Handle::current().spawn(async { 40 });
            handle.await.unwrap() + 2
        });
        assert_eq!(out, 42);
        rt.shutdown();
    }

    #[test]
    fn nested_spawns_run_to_completion() {
        let rt = MultiThread::new(2);
        let out = rt.block_on(async {
            let handle = crate::runtime::Handle::current().spawn(async {
                let inner = crate::runtime::Handle::current().spawn(async { 9 });
                inner.await.unwrap() + 1
            });
            handle.await.unwrap()
        });
        assert_eq!(out, 10);
        rt.shutdown();
    }

    #[test]
    fn wake_from_another_thread_makes_progress() {
        let rt = MultiThread::new(2);
        let signal = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let signal_for_thread = signal.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            signal_for_thread.store(true, Ordering::Release);
        });
        let slot = Arc::new(Mutex::new(None::<std::task::Waker>));
        let handle = rt.spawn({
            let slot = slot.clone();
            let signal = signal.clone();
            let done = done.clone();
            async move {
                std::future::poll_fn(move |cx| {
                    if signal.load(Ordering::Acquire) {
                        Poll::Ready(())
                    } else {
                        *slot.lock().unwrap() = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await;
                done.store(true, Ordering::Release);
            }
        });
        // Keep waking the task from this thread while it waits for the
        // signal; each wake enters through the global injector.
        std::thread::sleep(Duration::from_millis(5));
        loop {
            if let Some(waker) = slot.lock().unwrap().take() {
                waker.wake();
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
            if done.load(Ordering::Acquire) {
                break;
            }
        }
        rt.block_on(async move {
            handle.await.unwrap();
        });
        rt.shutdown();
    }

    #[test]
    fn shutdown_joins_workers_and_cancels_pending_tasks() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let dropped_for_task = dropped.clone();
        let rt = MultiThread::new(2);
        struct Pending {
            _probe: Arc<AtomicUsize>,
        }
        impl Future for Pending {
            type Output = ();
            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
                Poll::Pending
            }
        }
        impl Drop for Pending {
            fn drop(&mut self) {
                self._probe.fetch_add(1, Ordering::SeqCst);
            }
        }
        let handle = rt.spawn(Pending {
            _probe: dropped_for_task,
        });
        drop(handle);
        rt.shutdown(); // cancels the pending task and drops its future exactly once
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn stress_100k_tasks_across_8_workers() {
        let rt = MultiThread::new(8);
        let counter = Arc::new(AtomicI32::new(0));
        let mut handles = Vec::new();
        // Test hygiene: alternate fast and slow tasks. Round-robin routing
        // sends one parity to odd workers and the other to even workers, so
        // the fast workers drain their share and must steal from the
        // still-loaded slow workers — a deterministic steal, unlike the
        // previous uniform load which could complete without any stealing.
        for i in 0..100_000 {
            let counter = counter.clone();
            let slow = i % 2 == 0;
            handles.push(rt.spawn(async move {
                if slow {
                    for _ in 0..10 {
                        crate::future::yield_now().await;
                    }
                }
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        }
        rt.block_on(async move {
            for handle in handles {
                handle.await.unwrap();
            }
        });
        assert_eq!(counter.load(Ordering::SeqCst), 100_000);
        assert!(
            rt.steal_count() > 0,
            "expected work to be stolen between workers"
        );
        rt.shutdown();
    }
}
