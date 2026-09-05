//! The blocking pool: background threads for CPU-bound or blocking work that
//! must not stall a worker thread.
//!
//! `spawn_blocking` runs a `FnOnce` closure on a dedicated thread whose only
//! job is to run blocking tasks. Threads are spawned lazily on demand, exit
//! after an idle `keep_alive` period, and are capped at `max_blocking_threads`
//! (default 512 — deliberately large, since these threads are blocked on I/O
//! rather than consuming CPU).
//!
//! Tasks running on the pool are NOT cancellable once they have started: the
//! closure runs to completion. Dropping the returned `JoinHandle` (or aborting
//! it) has no effect on a task that is already running; tasks still queued
//! when the pool shuts down are cancelled and their handles observe
//! `JoinError::Cancelled`.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::runtime::{EnterGuard, Handle};
use crate::task::{self, JoinHandle, Notified, Schedule};

pub(crate) const DEFAULT_MAX_BLOCKING_THREADS: usize = 512;
pub(crate) const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub(crate) struct BlockingPool {
    inner: Arc<Inner>,
}

struct Inner {
    queue: Mutex<QueueState>,
    condvar: Condvar,
    closed: AtomicBool,
    num_threads: AtomicUsize,
    max_threads: usize,
    keep_alive: Duration,
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
    metrics: Arc<crate::instrument::MetricsState>,
}

struct QueueState {
    jobs: VecDeque<Notified<BlockingPool>>,
    /// Threads currently waiting for a job (or about to take one).
    idle: usize,
}

impl BlockingPool {
    pub(crate) fn new_with_metrics(
        max_threads: usize,
        keep_alive: Duration,
        metrics: Arc<crate::instrument::MetricsState>,
    ) -> BlockingPool {
        BlockingPool {
            inner: Arc::new(Inner {
                queue: Mutex::new(QueueState {
                    jobs: VecDeque::new(),
                    idle: 0,
                }),
                condvar: Condvar::new(),
                closed: AtomicBool::new(false),
                num_threads: AtomicUsize::new(0),
                max_threads,
                keep_alive,
                threads: Mutex::new(Vec::new()),
                metrics,
            }),
        }
    }

    /// Queue a task, spawning a fresh thread if nobody is idle and the cap
    /// allows it. Idle-thread accounting and the spawn decision are
    /// serialized by the queue lock so the pool can never exceed the cap.
    fn submit(&self, task: Notified<BlockingPool>) {
        let mut state = self.inner.queue.lock().unwrap();
        if self.inner.closed.load(Ordering::Acquire) {
            drop(state);
            cancel_task(task);
            return;
        }
        state.jobs.push_back(task);
        crate::instrument::task_scheduled(&self.inner.metrics);
        crate::instrument::task_queued(&self.inner.metrics);
        let spawn = state.idle == 0
            && self.inner.num_threads.load(Ordering::Relaxed) < self.inner.max_threads;
        if spawn {
            self.inner.num_threads.fetch_add(1, Ordering::Relaxed);
        }
        drop(state);
        self.inner.condvar.notify_one();
        if spawn {
            self.spawn_thread();
        }
    }

    fn spawn_thread(&self) {
        let pool = self.inner.clone();
        let thread = std::thread::Builder::new()
            .name("eddy-blocking".to_string())
            .spawn(move || thread_loop(pool))
            .expect("eddy: failed to start blocking thread");
        self.inner.threads.lock().unwrap().push(thread);
    }

    /// Stop accepting tasks, cancel everything still queued (their handles
    /// see `JoinError::Cancelled`), wait for in-flight jobs to finish, then
    /// join every thread. Idle threads are woken and exit.
    pub(crate) fn shutdown(&self) {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let queued = {
            let mut state = self.inner.queue.lock().unwrap();
            std::mem::take(&mut state.jobs)
        };
        for task in queued {
            crate::instrument::task_dequeued(&self.inner.metrics);
            cancel_task(task);
        }
        self.inner.condvar.notify_all();
        let threads = std::mem::take(&mut *self.inner.threads.lock().unwrap());
        for thread in threads {
            thread.join().expect("eddy: blocking thread panicked");
        }
    }
}

impl Schedule for BlockingPool {
    fn schedule(&self, task: Notified<Self>) {
        self.submit(task);
    }

    fn defer_dealloc(&self, task: Notified<Self>) {
        self.submit(task);
    }

    fn can_dealloc_remotely(&self) -> bool {
        true
    }

    fn metrics(&self) -> Arc<crate::instrument::MetricsState> {
        self.inner.metrics.clone()
    }
}

// SAFETY: `BlockingPool` is `Arc<Inner>` with every mutable field guarded by
// a Mutex or atomic, and tasks carry only a type-erased pointer whose future
// is `Send` (enforced by the `spawn_blocking` API). Moving the pointer across
// threads and polling on the pool threads is therefore sound.
unsafe impl Send for Notified<BlockingPool> {}

/// Mark a task as cancelled (waking any awaiters with `JoinError::Cancelled`)
/// and release the queued reference. Used when the pool is closed, since the
/// task's closure must never run.
///
/// `vtable.shutdown` only *defers* for a queued (NOTIFIED) task: its queue
/// entry is the one that finalizes the task. So the entry itself is run
/// afterwards — its poll observes CANCELLED via `transition_to_running` and
/// completes the task without ever touching the closure.
fn cancel_task(task: Notified<BlockingPool>) {
    // SAFETY: the vtable belongs to the `BlockingPool` scheduler and `task`
    // holds one live reference for the entire call.
    unsafe { (task.raw.header().vtable.shutdown)(task.raw.header) };
    task.run();
}

fn thread_loop(pool: Arc<Inner>) {
    loop {
        let mut state = pool.queue.lock().unwrap();
        if let Some(job) = state.jobs.pop_front() {
            // A thread that woke from the wait already removed its own idle
            // count; a freshly spawned thread was never counted at all.
            drop(state);
            let started = std::time::Instant::now();
            crate::instrument::task_dequeued(&pool.metrics);
            job.run();
            crate::instrument::worker_busy(&pool.metrics, started.elapsed());
            continue;
        }
        if pool.closed.load(Ordering::Acquire) {
            break;
        }
        state.idle += 1;
        let (guard, timed_out) = pool
            .condvar
            .wait_timeout(state, pool.keep_alive)
            .expect("eddy: blocking pool condvar poisoned");
        let mut state = guard;
        state.idle -= 1;
        if timed_out.timed_out() && state.jobs.is_empty() && !pool.closed.load(Ordering::Acquire) {
            break;
        }
    }
    pool.num_threads.fetch_sub(1, Ordering::Relaxed);
}

/// The task future for `spawn_blocking`: runs the closure exactly once on the
/// first poll (which happens on a pool thread), installing the runtime's
/// ambient handle so the closure can use `Handle::current()` — including
/// spawning further blocking tasks.
pub(crate) struct BlockingTask<F> {
    handle: Handle,
    task: Option<F>,
}

impl<F> BlockingTask<F> {
    pub(crate) fn new(handle: Handle, task: F) -> BlockingTask<F> {
        BlockingTask {
            handle,
            task: Some(task),
        }
    }
}

impl<F, R> Future for BlockingTask<F>
where
    F: FnOnce() -> R + Unpin,
{
    type Output = R;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<R> {
        let this = self.get_mut();
        let task = this
            .task
            .take()
            .expect("eddy: blocking task polled after completion");
        let _enter = EnterGuard::new(this.handle.clone());
        Poll::Ready(task())
    }
}

/// Shared helpers for the schedulers that own a blocking pool.
pub(crate) fn spawn_on_pool<F, R>(pool: &BlockingPool, handle: Handle, task: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + Unpin + 'static,
    R: Send + 'static,
{
    let (notified, join) = task::spawn(BlockingTask::new(handle, task), pool.clone());
    pool.submit(notified);
    join
}
