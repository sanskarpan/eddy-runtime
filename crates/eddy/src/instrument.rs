//! Runtime event emission (SPEC §15).
//!
//! The runtime emits `RuntimeEvent`s as tasks are spawned, polled, woken,
//! and dropped; a subscriber (the unix-socket console feed) consumes the
//! stream. Emission is a single TLS call that is a no-op when no subscriber
//! is installed, keeping the disabled cost near zero.
//!
//! Phase 11 establishes the event type and the emission seam
//! (`BudgetExhausted`); the remaining emission points and the unix-socket
//! subscriber land in Phase 13.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Unique id of a spawned task, assigned from a process-wide counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(u64);

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

#[allow(dead_code)] // wired in Phase 13
impl TaskId {
    /// Allocate the next task id.
    pub(crate) fn next() -> TaskId {
        TaskId(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed))
    }

    /// The id of the task currently being polled on this thread, if any.
    pub(crate) fn current() -> TaskId {
        TaskId(0)
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Where a task's wake came from — the causality edge of the wake graph.
#[derive(Debug, Clone)]
#[allow(dead_code)] // wired in Phase 13
pub enum WakeSource {
    /// A socket reported readable/writable.
    Io { fd: i32 },
    /// A timer fired.
    Timer { id: u64 },
    /// Another task woke this one.
    Task(TaskId),
    /// A channel produced a value or freed capacity.
    Channel { kind: &'static str },
    /// A thread outside the runtime (the waker was cloned and sent off).
    External,
}

/// The spawn location of a task, captured at spawn time.
#[derive(Debug, Clone)]
#[allow(dead_code)] // wired in Phase 13
pub struct Location {
    pub file: &'static str,
    pub line: u32,
}

/// How a task poll ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // wired in Phase 13
pub enum PollResult {
    Ready,
    Pending,
    Panicked,
}

/// The event stream the consoles consume.
#[derive(Debug, Clone)]
#[allow(dead_code)] // emission points wired in Phase 13
pub enum RuntimeEvent {
    TaskSpawned {
        id: TaskId,
        name: Option<String>,
        location: Location,
        parent: Option<TaskId>,
    },
    TaskPollStart {
        id: TaskId,
        worker: u32,
        at: Instant,
    },
    TaskPollEnd {
        id: TaskId,
        worker: u32,
        duration: Duration,
        result: PollResult,
    },
    TaskWoken {
        id: TaskId,
        by: WakeSource,
    },
    TaskDropped {
        id: TaskId,
        total_polls: u64,
        total_busy: Duration,
        total_idle: Duration,
    },
    TaskAborted {
        id: TaskId,
    },
    WorkerPark {
        worker: u32,
        timeout: Option<Duration>,
    },
    WorkerUnpark {
        worker: u32,
        reason: UnparkReason,
    },
    WorkerSteal {
        thief: u32,
        victim: u32,
        count: usize,
    },
    QueueDepth {
        worker: u32,
        local: usize,
        global: usize,
        lifo: bool,
    },
    IoRegistered {
        fd: i32,
        interest: &'static str,
        task: TaskId,
    },
    IoReady {
        fd: i32,
        readiness: &'static str,
        woke: Vec<TaskId>,
    },
    TimerSet {
        id: u64,
        deadline: Instant,
        task: TaskId,
    },
    TimerFired {
        id: u64,
        lateness: Duration,
    },
    TimerCancelled {
        id: u64,
    },
    BlockingDetected {
        task: TaskId,
        poll_duration: Duration,
        location: Location,
    },
    BudgetExhausted {
        task: TaskId,
    },
    ResourceContended {
        kind: &'static str,
        holder: TaskId,
        waiters: Vec<TaskId>,
    },
}

/// Why a parked worker was woken.
#[derive(Debug, Clone)]
#[allow(dead_code)] // wired in Phase 13
pub enum UnparkReason {
    /// A task was queued for it.
    Task,
    /// The runtime is shutting down.
    Shutdown,
    /// It was stolen idle and given work by another worker.
    Steal,
    /// The driver reported readiness events.
    Event,
}

thread_local! {
    static SUBSCRIBER: std::cell::RefCell<Option<&'static (dyn Fn(RuntimeEvent) + Sync)>> =
        const { std::cell::RefCell::new(None) };
}

/// Install the process-wide event sink. Panics if already set.
#[allow(dead_code)] // wired in Phase 13
pub(crate) fn set_subscriber(sink: &'static (dyn Fn(RuntimeEvent) + Sync)) {
    SUBSCRIBER.with(|s| {
        assert!(
            s.borrow().is_none(),
            "eddy: an instrumentation subscriber is already installed"
        );
        *s.borrow_mut() = Some(sink);
    });
}

/// Remove the event sink (runtime shutdown).
#[allow(dead_code)] // wired in Phase 13
pub(crate) fn clear_subscriber() {
    SUBSCRIBER.with(|s| *s.borrow_mut() = None);
}

/// Emit an event; a no-op when no subscriber is installed.
pub(crate) fn emit(event: RuntimeEvent) {
    SUBSCRIBER.with(|s| {
        if let Some(sink) = *s.borrow() {
            sink(event);
        }
    });
}
