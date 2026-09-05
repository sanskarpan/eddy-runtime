//! The current-thread scheduler. Local work is kept in a FIFO queue while
//! wakes from other threads use the injection queue. Task ownership is
//! serialized by the scheduler's locks so a task containing a `!Send` future
//! is never polled or destroyed by a foreign thread.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread::{Thread, ThreadId};
use std::time::Duration;

use crate::blocking::{self, BlockingPool};
use crate::task::{self, JoinHandle, Notified, RawTask, Schedule};
use crate::time::TimerShared;

const GLOBAL_QUEUE_INTERVAL: u32 = 61;

struct Inner {
    local: Mutex<VecDeque<Notified<CurrentThread>>>,
    injection: Mutex<VecDeque<Notified<CurrentThread>>>,
    deferred: Mutex<VecDeque<Notified<CurrentThread>>>,
    owner: Mutex<Option<ThreadId>>,
    owner_thread: ThreadId,
    unparker: Arc<Mutex<Option<Thread>>>,
    tick: AtomicU32,
    tasks: Mutex<Vec<RawTask>>,
    closed: AtomicBool,
    shutdown_complete: AtomicBool,
    timer: Arc<TimerShared>,
    blocking: BlockingPool,
    metrics: Arc<crate::instrument::MetricsState>,
}

// `Inner` is plain `Send + Sync` (every field is behind a Mutex or an
// atomic; `Notified` moves only a raw task pointer). Cross-thread wakes are
// safe: they only enqueue and unpark, never touching a `!Send` future. The
// `!Send + !Sync` confinement lives on `Runtime` itself (runtime/mod.rs),
// because `shutdown` polls and destroys `!Send` futures on the drop thread.

#[derive(Clone)]
pub(crate) struct CurrentThread(Arc<Inner>);

impl CurrentThread {
    pub(crate) fn new(max_blocking_threads: usize, keep_alive: Duration) -> CurrentThread {
        // The unparker slot records the thread currently in `block_on`,
        // which need not be the construction thread; the timer notifier must
        // therefore read the slot at fire time rather than capture the
        // construction thread.
        let unparker = Arc::new(Mutex::new(None::<Thread>));
        let unparker_for_timer = unparker.clone();
        let timer = TimerShared::new(Arc::new(move || {
            if let Some(thread) = unparker_for_timer.lock().unwrap().clone() {
                thread.unpark();
            }
        }));
        let metrics = crate::instrument::MetricsState::new(1);
        CurrentThread(Arc::new(Inner {
            local: Mutex::new(VecDeque::new()),
            injection: Mutex::new(VecDeque::new()),
            deferred: Mutex::new(VecDeque::new()),
            owner: Mutex::new(None),
            owner_thread: std::thread::current().id(),
            unparker,
            tick: AtomicU32::new(0),
            tasks: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            shutdown_complete: AtomicBool::new(false),
            timer,
            blocking: BlockingPool::new_with_metrics(
                max_blocking_threads,
                keep_alive,
                metrics.clone(),
            ),
            metrics,
        }))
    }

    fn is_owner_thread(&self) -> bool {
        let current = std::thread::current().id();
        *self.0.owner.lock().unwrap() == Some(current)
            || (self.0.closed.load(Ordering::Acquire) && self.0.owner_thread == current)
    }

    fn unpark_owner(&self) {
        if let Some(thread) = self.0.unparker.lock().unwrap().clone() {
            thread.unpark();
        }
    }

    fn enqueue(&self, task: Notified<CurrentThread>) {
        let current = std::thread::current().id();
        let owner = self.0.owner.lock().unwrap();
        if self.0.closed.load(Ordering::Acquire) && self.0.shutdown_complete.load(Ordering::Acquire)
        {
            drop(task);
            return;
        }

        if *owner == Some(current) && !self.0.closed.load(Ordering::Acquire) {
            crate::instrument::task_queued(&self.0.metrics);
            self.0.local.lock().unwrap().push_back(task);
            return;
        }
        drop(owner);

        let mut injection = self.0.injection.lock().unwrap();
        if self.0.closed.load(Ordering::Acquire) && self.0.shutdown_complete.load(Ordering::Acquire)
        {
            drop(injection);
            drop(task);
            return;
        }
        crate::instrument::task_queued(&self.0.metrics);
        injection.push_back(task);
        drop(injection);
        self.unpark_owner();
    }

    fn next_task(&self) -> Option<Notified<CurrentThread>> {
        let tick = self.0.tick.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        let _owner = self.0.owner.lock().unwrap();

        if let Some(task) = self.0.deferred.lock().unwrap().pop_front() {
            return Some(task);
        }
        if tick % GLOBAL_QUEUE_INTERVAL == 0 {
            if let Some(task) = self.0.injection.lock().unwrap().pop_front() {
                return Some(task);
            }
        }
        if let Some(task) = self.0.local.lock().unwrap().pop_front() {
            return Some(task);
        }
        self.0.injection.lock().unwrap().pop_front()
    }

    #[track_caller]
    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        self.spawn_named(future, None)
    }

    #[track_caller]
    pub(crate) fn spawn_named<F>(&self, future: F, name: Option<String>) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        assert!(
            !self.0.closed.load(Ordering::Acquire),
            "eddy: cannot spawn on a shut down runtime"
        );
        let (notified, handle) = task::spawn_with_name(future, self.clone(), name);
        self.register_task(notified.raw);
        crate::instrument::task_scheduled(&self.0.metrics);
        self.enqueue(notified);
        handle
    }

    pub(crate) fn spawn_blocking<F, R>(&self, task: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + Unpin + 'static,
        R: Send + 'static,
    {
        assert!(
            !self.0.closed.load(Ordering::Acquire),
            "eddy: cannot spawn on a shut down runtime"
        );
        let handle = crate::runtime::Handle::from_current(self.clone());
        blocking::spawn_on_pool(&self.0.blocking, handle, task)
    }

    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        {
            let mut owner = self.0.owner.lock().unwrap();
            assert!(
                owner.is_none(),
                "eddy: block_on called while runtime is running"
            );
            *owner = Some(std::thread::current().id());
        }
        *self.0.unparker.lock().unwrap() = Some(std::thread::current());
        let _cleanup = BlockOnCleanup {
            scheduler: self.clone(),
        };
        let _enter =
            crate::runtime::EnterGuard::new(crate::runtime::Handle::from_current(self.clone()));

        let signal = Arc::new(RootSignal {
            thread: std::thread::current(),
            woken: AtomicBool::new(true),
        });
        let waker = thread_waker(signal.clone());
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);

        loop {
            self.drain_deferred();
            if signal.woken.swap(false, Ordering::Acquire) {
                if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                    return output;
                }
            }

            match self.next_task() {
                Some(task) => run_task(task, &self.0.metrics),
                None => {
                    self.0.timer.advance_to_now();
                    // H4: `advance_to_now` may have just fired a timer whose
                    // waker belongs to a *spawned* task. Waking it enqueues
                    // the task on this thread without unparking (the
                    // owner-thread enqueue fast path), so parking here —
                    // bounded or not — would strand that work. Re-check the
                    // queues and loop instead of sleeping.
                    if !self.0.deferred.lock().unwrap().is_empty()
                        || !self.0.local.lock().unwrap().is_empty()
                        || !self.0.injection.lock().unwrap().is_empty()
                    {
                        continue;
                    }
                    // Paused time: the scheduler never blocks; the clock is
                    // advanced to the next timer deadline instead.
                    if self.0.timer.paused_advance() {
                        continue;
                    }
                    crate::instrument::worker_parked(&self.0.metrics);
                    match self.0.timer.next_timeout() {
                        Some(timeout) => std::thread::park_timeout(timeout),
                        None => std::thread::park(),
                    }
                }
            }
        }
    }

    pub(crate) fn timer_driver(&self) -> Arc<TimerShared> {
        self.0.timer.clone()
    }

    pub(crate) fn metrics(&self) -> crate::instrument::RuntimeMetrics {
        let scheduler = self.clone();
        crate::instrument::RuntimeMetrics::from_state(
            self.0.metrics.clone(),
            Some(Arc::new(move || {
                crate::instrument::snapshot_tasks(&scheduler.0.tasks.lock().unwrap())
            })),
        )
    }

    fn drain_deferred(&self) {
        while let Some(task) = self.0.deferred.lock().unwrap().pop_front() {
            run_task(task, &self.0.metrics);
        }
    }

    pub(crate) fn shutdown(&self) {
        if self.0.closed.swap(true, Ordering::AcqRel) {
            return;
        }

        let mut tasks = self.0.tasks.lock().unwrap();
        let registered = std::mem::take(&mut *tasks);
        drop(tasks);

        for task in &registered {
            // The registry reference keeps the allocation alive while the
            // shutdown pass handles queued and parked tasks.
            // SAFETY: Runtime is !Send and Drop runs on the owner thread;
            // shutdown never runs concurrently with block_on.
            unsafe { (task.header().vtable.shutdown)(task.header) };
        }

        loop {
            let task = self
                .0
                .deferred
                .lock()
                .unwrap()
                .pop_front()
                .or_else(|| self.0.local.lock().unwrap().pop_front())
                .or_else(|| self.0.injection.lock().unwrap().pop_front());
            let Some(task) = task else { break };
            run_task(task, &self.0.metrics);
        }

        // Every registered task is now complete and its future/output has
        // been destroyed on the owner thread. Remaining references may be
        // released remotely without touching `!Send` state.
        self.0.shutdown_complete.store(true, Ordering::Release);
        for task in registered {
            task.drop_reference_on_owner();
        }
        self.0.blocking.shutdown();
    }
}

impl Schedule for CurrentThread {
    fn schedule(&self, task: Notified<Self>) {
        self.enqueue(task);
    }

    fn defer_dealloc(&self, task: Notified<Self>) {
        if self.0.closed.load(Ordering::Acquire) && self.0.shutdown_complete.load(Ordering::Acquire)
        {
            // There is no owner left to destroy a `!Send` future. Leaking the
            // final deferred entry is preferable to dropping it remotely.
            if !CurrentThread::is_owner_thread(self) {
                std::mem::forget(task);
                return;
            }
        }
        self.0.deferred.lock().unwrap().push_back(task);
        self.unpark_owner();
    }

    fn metrics(&self) -> Arc<crate::instrument::MetricsState> {
        self.0.metrics.clone()
    }

    fn is_owner_thread(&self) -> bool {
        CurrentThread::is_owner_thread(self)
    }

    fn can_dealloc_remotely(&self) -> bool {
        self.0.shutdown_complete.load(Ordering::Acquire)
    }

    fn task_complete(&self, task: RawTask) {
        let removed = {
            let mut tasks = self.0.tasks.lock().unwrap();
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
        let mut tasks = self.0.tasks.lock().unwrap();
        assert!(
            !self.0.closed.load(Ordering::Acquire),
            "eddy: cannot register a task on a shut down runtime"
        );
        task.header().state.ref_inc();
        tasks.push(task);
    }
}

fn run_task(task: Notified<CurrentThread>, metrics: &crate::instrument::MetricsState) {
    crate::instrument::task_dequeued(metrics);
    let started = std::time::Instant::now();
    task.run();
    crate::instrument::worker_busy(metrics, started.elapsed());
}

struct BlockOnCleanup {
    scheduler: CurrentThread,
}

impl Drop for BlockOnCleanup {
    fn drop(&mut self) {
        *self.scheduler.0.unparker.lock().unwrap() = None;
        let current = std::thread::current().id();
        let mut owner = self.scheduler.0.owner.lock().unwrap();
        if *owner == Some(current) {
            *owner = None;
        }
    }
}

struct RootSignal {
    thread: Thread,
    woken: AtomicBool,
}

fn thread_waker(signal: Arc<RootSignal>) -> Waker {
    let raw = Arc::into_raw(signal) as *const ();
    // SAFETY: the vtable treats `raw` as an `Arc<RootSignal>` pointer created
    // by `Arc::into_raw` and maintains its ownership exactly.
    unsafe { Waker::from_raw(RawWaker::new(raw, &THREAD_WAKER_VTABLE)) }
}

static THREAD_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    thread_waker_clone,
    thread_waker_wake,
    thread_waker_wake_by_ref,
    thread_waker_drop,
);

/// SAFETY: `ptr` was produced by `thread_waker` or this function.
unsafe fn thread_waker_clone(ptr: *const ()) -> RawWaker {
    Arc::increment_strong_count(ptr as *const RootSignal);
    RawWaker::new(ptr, &THREAD_WAKER_VTABLE)
}

/// SAFETY: see `thread_waker_clone`; this consumes one Arc reference.
unsafe fn thread_waker_wake(ptr: *const ()) {
    let signal = Arc::from_raw(ptr as *const RootSignal);
    signal.woken.store(true, Ordering::Release);
    signal.thread.unpark();
}

/// SAFETY: see `thread_waker_clone`; `ManuallyDrop` preserves the reference.
unsafe fn thread_waker_wake_by_ref(ptr: *const ()) {
    let signal = std::mem::ManuallyDrop::new(Arc::from_raw(ptr as *const RootSignal));
    signal.woken.store(true, Ordering::Release);
    signal.thread.unpark();
}

/// SAFETY: see `thread_waker_clone`; this releases one Arc reference.
unsafe fn thread_waker_drop(ptr: *const ()) {
    Arc::decrement_strong_count(ptr as *const RootSignal);
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn block_on_ready_future_returns_immediately() {
        let rt = CurrentThread::new(
            crate::blocking::DEFAULT_MAX_BLOCKING_THREADS,
            crate::blocking::DEFAULT_KEEP_ALIVE,
        );
        assert_eq!(rt.block_on(async { 42 }), 42);
    }

    #[test]
    fn spawn_1000_tasks_all_complete() {
        let rt = CurrentThread::new(
            crate::blocking::DEFAULT_MAX_BLOCKING_THREADS,
            crate::blocking::DEFAULT_KEEP_ALIVE,
        );
        rt.block_on(async {
            let mut handles = Vec::new();
            for i in 0..1000 {
                handles.push(crate::runtime::Handle::current().spawn(async move { i }));
            }
            let mut total = 0;
            for handle in handles {
                total += handle.await.unwrap();
            }
            assert_eq!(total, (0..1000).sum::<i32>());
        });
    }

    #[test]
    fn nested_spawn_works() {
        let rt = CurrentThread::new(
            crate::blocking::DEFAULT_MAX_BLOCKING_THREADS,
            crate::blocking::DEFAULT_KEEP_ALIVE,
        );
        let out = rt.block_on(async {
            let handle = crate::runtime::Handle::current().spawn(async {
                let inner = crate::runtime::Handle::current().spawn(async { 9 });
                inner.await.unwrap()
            });
            handle.await.unwrap()
        });
        assert_eq!(out, 9);
    }

    #[test]
    fn non_send_spawned_future_compiles_and_runs() {
        let rt = CurrentThread::new(
            crate::blocking::DEFAULT_MAX_BLOCKING_THREADS,
            crate::blocking::DEFAULT_KEEP_ALIVE,
        );
        let value = std::rc::Rc::new(5);
        let out = rt.block_on(async move {
            crate::runtime::Handle::current()
                .spawn_local(async move { *value + 1 })
                .await
                .unwrap()
        });
        assert_eq!(out, 6);
    }

    #[test]
    fn wake_from_another_thread_makes_progress() {
        let rt = CurrentThread::new(
            crate::blocking::DEFAULT_MAX_BLOCKING_THREADS,
            crate::blocking::DEFAULT_KEEP_ALIVE,
        );
        rt.block_on(async {
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            let waker_slot = Arc::new(parking_lot::Mutex::new(None::<Waker>));
            let slot_for_thread = waker_slot.clone();
            std::thread::spawn(move || {
                rx.recv().unwrap();
                std::thread::sleep(std::time::Duration::from_millis(10));
                if let Some(waker) = slot_for_thread.lock().take() {
                    waker.wake();
                }
            });

            struct WaitOnce {
                armed: bool,
                slot: Arc<parking_lot::Mutex<Option<Waker>>>,
                tx: std::sync::mpsc::Sender<()>,
            }
            impl Future for WaitOnce {
                type Output = ();
                fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                    if self.armed {
                        return Poll::Ready(());
                    }
                    self.armed = true;
                    *self.slot.lock() = Some(cx.waker().clone());
                    self.tx.send(()).unwrap();
                    Poll::Pending
                }
            }

            WaitOnce {
                armed: false,
                slot: waker_slot,
                tx,
            }
            .await;
        });
    }

    #[test]
    fn root_pending_future_is_not_repolled_without_a_wake() {
        let rt = CurrentThread::new(
            crate::blocking::DEFAULT_MAX_BLOCKING_THREADS,
            crate::blocking::DEFAULT_KEEP_ALIVE,
        );
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let task_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let waker_slot = Arc::new(parking_lot::Mutex::new(None::<Waker>));
        let (tx, rx) = std::sync::mpsc::channel();
        let slot_for_thread = waker_slot.clone();
        std::thread::spawn(move || {
            rx.recv().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
            if let Some(waker) = slot_for_thread.lock().take() {
                waker.wake();
            }
        });

        let polls_for_root = polls.clone();
        let task_ran_for_task = task_ran.clone();
        let slot_for_root = waker_slot.clone();
        rt.block_on(async move {
            crate::runtime::Handle::current().spawn(async move {
                task_ran_for_task.store(true, Ordering::Release);
                tx.send(()).unwrap();
            });
            std::future::poll_fn(move |cx| {
                let poll = polls_for_root.fetch_add(1, Ordering::Relaxed);
                if poll == 0 {
                    *slot_for_root.lock() = Some(cx.waker().clone());
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
        });
        assert!(task_ran.load(Ordering::Acquire));
        assert_eq!(polls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn timer_notify_unparks_the_block_on_thread_not_the_construction_thread() {
        // H3 regression: the timer notifier reads the `unparker` slot at
        // fire time, so a timer armed by a foreign (blocking-pool) thread
        // wakes the thread currently in `block_on`, even when that differs
        // from the construction thread. The old closure captured the
        // construction thread and its wake was lost. (The wheel's
        // park_timeout bounds most parks, so this guards the empty-wheel
        // window rather than a deterministic old-code hang.)
        let rt = CurrentThread::new(
            crate::blocking::DEFAULT_MAX_BLOCKING_THREADS,
            crate::blocking::DEFAULT_KEEP_ALIVE,
        );
        let (tx, rx) = std::sync::mpsc::channel();
        let done = std::thread::spawn(move || {
            rt.block_on(async {
                // Arm a timer from the blocking-pool thread while this
                // thread is parked, then complete via the normal wake path.
                let handle = crate::runtime::Handle::current().spawn_blocking(move || {
                    let mut sleep =
                        std::pin::pin!(crate::time::sleep(std::time::Duration::from_millis(30)));
                    let waker = std::task::Waker::noop();
                    let mut cx = Context::from_waker(waker);
                    let _ = sleep.as_mut().poll(&mut cx);
                });
                handle.await.unwrap();
            });
            tx.send(()).unwrap();
        });
        // The block_on thread parks with an empty wheel and no timeout while
        // the pool thread arms the timer; the arm's notify must hit the
        // block_on thread (unparker slot), not the construction thread.
        // Bounded by recv_timeout so a lost wake fails instead of hanging.
        rx.recv_timeout(std::time::Duration::from_secs(10))
            .expect("H3: block_on thread was never woken after a foreign timer arm");
        done.join().unwrap();
    }

    #[test]
    fn spawned_task_timer_does_not_strand_work_at_park() {
        // H4 regression: a timer fired by `advance_to_now` wakes a spawned
        // task, which the owner-thread enqueue path queues *without* an
        // unpark. If `block_on` then park()s unconditionally, the queued
        // task is stranded forever. The asleep-in-a-spawned-task pattern is
        // exactly what the watch/broadcast async tests exercise; this one
        // guards it directly and would hang without the queue re-check.
        let rt = CurrentThread::new(
            crate::blocking::DEFAULT_MAX_BLOCKING_THREADS,
            crate::blocking::DEFAULT_KEEP_ALIVE,
        );
        rt.block_on(async {
            let handle = crate::runtime::Handle::current().spawn(async {
                crate::time::sleep(std::time::Duration::from_millis(10)).await;
                42
            });
            // Bounded so a lost wake fails loudly instead of hanging CI.
            let value = crate::time::timeout(std::time::Duration::from_secs(10), handle)
                .await
                .expect("H4: spawned-task timer wake was lost")
                .unwrap();
            assert_eq!(value, 42);
        });
    }
}
