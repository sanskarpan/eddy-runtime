//! Runtime event emission (SPEC §15).
//!
//! The runtime emits `RuntimeEvent`s as tasks are spawned, polled, woken,
//! and dropped; a subscriber (the unix-socket console feed) consumes the
//! stream. Emission is a single TLS call that is a no-op when no subscriber
//! is installed, keeping the disabled cost near zero.
//!
//! Phase 13 adds the emission points, runtime metrics, task snapshots, and
//! the Unix-socket subscriber.

#[cfg(feature = "instrumentation")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

thread_local! {
    static CURRENT_TASK: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Unique id of a spawned task, assigned from a process-wide counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct TaskId(u64);

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);
#[cfg(feature = "instrumentation")]
static ORIGIN: OnceLock<Instant> = OnceLock::new();

#[cfg(feature = "instrumentation")]
pub(crate) fn monotonic_nanos() -> u64 {
    ORIGIN
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

impl TaskId {
    /// Numeric task identifier used by event consumers and metrics tools.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Allocate the next task id.
    pub(crate) fn next() -> TaskId {
        TaskId(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed))
    }

    /// The id of the task currently being polled on this thread, if any.
    pub(crate) fn current() -> TaskId {
        TaskId(CURRENT_TASK.with(|c| c.get()))
    }

    /// Install this id as the current task for the duration of a poll.
    pub(crate) fn enter(self) -> CurrentTaskGuard {
        CurrentTaskGuard {
            previous: CURRENT_TASK.with(|c| c.replace(self.0)),
        }
    }
}

pub(crate) struct CurrentTaskGuard {
    previous: u64,
}

impl Drop for CurrentTaskGuard {
    fn drop(&mut self) {
        CURRENT_TASK.with(|c| c.set(self.previous));
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
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // wired in Phase 13
pub struct Location {
    pub file: &'static str,
    pub line: u32,
}

/// The state observed for a task in a task dump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Queued,
    Running,
    Idle,
    Complete,
    Cancelled,
}

/// A point-in-time view of a task retained by the runtime registry.
///
/// The location is the spawn call site. Eddy does not retain an await-point
/// backtrace because futures do not expose that information without wrapping
/// every awaitable; callers that need one should use a tracing span around the
/// operation being awaited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSnapshot {
    pub id: TaskId,
    pub name: Option<String>,
    pub location: Location,
    pub parent: Option<TaskId>,
    pub state: TaskState,
    pub worker: u32,
    pub polls: u64,
    pub total_busy: Duration,
    pub total_idle: Duration,
    pub scheduled: u64,
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

#[allow(dead_code)]
pub type Subscriber = Arc<dyn Fn(RuntimeEvent) + Send + Sync + 'static>;
#[allow(dead_code)]
type Sink = Subscriber;

#[cfg(feature = "instrumentation")]
static SUBSCRIBER_ACTIVE: AtomicBool = AtomicBool::new(false);

#[allow(dead_code)]
fn subscriber() -> &'static Mutex<Option<Sink>> {
    static SUBSCRIBER: OnceLock<Mutex<Option<Sink>>> = OnceLock::new();
    SUBSCRIBER.get_or_init(|| Mutex::new(None))
}

/// Install the process-wide event sink. A mutex, rather than thread-local
/// storage, is required because task polls and wakes happen on any worker.
#[cfg(feature = "instrumentation")]
pub fn set_subscriber(sink: Subscriber) {
    *subscriber().lock().unwrap() = Some(sink);
    SUBSCRIBER_ACTIVE.store(true, Ordering::Release);
}

/// Remove the event sink. Primarily useful for tests and controlled runtime
/// shutdown; replacing a sink is safe and does not race event delivery.
#[cfg(feature = "instrumentation")]
pub fn clear_subscriber() {
    SUBSCRIBER_ACTIVE.store(false, Ordering::Release);
    *subscriber().lock().unwrap() = None;
}

/// Emit an event. Passing a closure keeps construction out of the disabled
/// binary: the no-feature implementation is inlined to an empty function.
#[cfg(feature = "instrumentation")]
#[inline]
pub(crate) fn emit(make: impl FnOnce() -> RuntimeEvent) {
    if !SUBSCRIBER_ACTIVE.load(Ordering::Acquire) {
        return;
    }
    let sink = subscriber().lock().unwrap().clone();
    if let Some(sink) = sink {
        sink(make());
    }
}

#[cfg(not(feature = "instrumentation"))]
#[inline(always)]
pub(crate) fn emit(make: impl FnOnce() -> RuntimeEvent) {
    drop(make);
}

/// A cheap snapshot of scheduler metrics for one runtime.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RuntimeMetricsSnapshot {
    pub active_tasks: u64,
    pub queue_depth: u64,
    pub total_polls: u64,
    pub total_busy: Duration,
    pub scheduled_tasks: u64,
    pub steals: u64,
    pub parks: u64,
    pub worker_busy_ratio: f64,
}

pub(crate) struct MetricsState {
    active_tasks: AtomicU64,
    queue_depth: AtomicU64,
    total_polls: AtomicU64,
    total_busy_ns: AtomicU64,
    scheduled_tasks: AtomicU64,
    steals: AtomicU64,
    parks: AtomicU64,
    worker_busy_ns: AtomicU64,
    worker_count: u64,
    started: Instant,
}

impl MetricsState {
    pub(crate) fn new(worker_count: usize) -> Arc<MetricsState> {
        Arc::new(MetricsState {
            active_tasks: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            total_polls: AtomicU64::new(0),
            total_busy_ns: AtomicU64::new(0),
            scheduled_tasks: AtomicU64::new(0),
            steals: AtomicU64::new(0),
            parks: AtomicU64::new(0),
            worker_busy_ns: AtomicU64::new(0),
            worker_count: worker_count.max(1) as u64,
            started: Instant::now(),
        })
    }
}

static METRICS: OnceLock<Arc<MetricsState>> = OnceLock::new();

pub(crate) fn global_metrics() -> Arc<MetricsState> {
    METRICS.get_or_init(|| MetricsState::new(1)).clone()
}

/// Programmatic access to runtime-wide counters.
#[derive(Clone)]
pub struct RuntimeMetrics {
    state: Arc<MetricsState>,
    task_provider: Option<Arc<dyn Fn() -> Vec<TaskSnapshot> + Send + Sync>>,
}

impl RuntimeMetrics {
    pub fn current() -> RuntimeMetrics {
        RuntimeMetrics::from_state(global_metrics(), None)
    }

    pub(crate) fn from_state(
        state: Arc<MetricsState>,
        task_provider: Option<Arc<dyn Fn() -> Vec<TaskSnapshot> + Send + Sync>>,
    ) -> RuntimeMetrics {
        RuntimeMetrics {
            state,
            task_provider,
        }
    }

    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        let metrics = &self.state;
        let elapsed_ns = metrics.started.elapsed().as_nanos();
        let capacity_ns = elapsed_ns.saturating_mul(metrics.worker_count as u128);
        let busy_ns = metrics.worker_busy_ns.load(Ordering::Relaxed) as u128;
        RuntimeMetricsSnapshot {
            active_tasks: metrics.active_tasks.load(Ordering::Relaxed),
            queue_depth: metrics.queue_depth.load(Ordering::Relaxed),
            total_polls: metrics.total_polls.load(Ordering::Relaxed),
            total_busy: Duration::from_nanos(metrics.total_busy_ns.load(Ordering::Relaxed)),
            scheduled_tasks: metrics.scheduled_tasks.load(Ordering::Relaxed),
            steals: metrics.steals.load(Ordering::Relaxed),
            parks: metrics.parks.load(Ordering::Relaxed),
            worker_busy_ratio: if capacity_ns == 0 {
                0.0
            } else {
                (busy_ns as f64 / capacity_ns as f64).min(1.0)
            },
        }
    }

    /// Return snapshots of tasks that are still registered with this runtime.
    pub fn task_snapshots(&self) -> Vec<TaskSnapshot> {
        self.task_provider
            .as_ref()
            .map_or_else(Vec::new, |provider| provider())
    }

    /// Alias intended for watchdogs and diagnostic commands.
    pub fn dump_tasks(&self) -> Vec<TaskSnapshot> {
        self.task_snapshots()
    }
}

#[cfg(feature = "instrumentation")]
pub(crate) fn task_spawned(metrics: &MetricsState) {
    metrics.active_tasks.fetch_add(1, Ordering::Relaxed);
}

#[cfg(feature = "instrumentation")]
pub(crate) fn task_finished(metrics: &MetricsState) {
    metrics.active_tasks.fetch_sub(1, Ordering::Relaxed);
}

#[cfg(feature = "instrumentation")]
pub(crate) fn task_polled(metrics: &MetricsState, duration: Duration) {
    metrics.total_polls.fetch_add(1, Ordering::Relaxed);
    metrics.total_busy_ns.fetch_add(
        duration.as_nanos().min(u64::MAX as u128) as u64,
        Ordering::Relaxed,
    );
}

#[cfg(feature = "instrumentation")]
pub(crate) fn task_scheduled(metrics: &MetricsState) {
    metrics.scheduled_tasks.fetch_add(1, Ordering::Relaxed);
}

#[cfg(feature = "instrumentation")]
pub(crate) fn task_queued(metrics: &MetricsState) {
    metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
}

#[cfg(feature = "instrumentation")]
pub(crate) fn task_dequeued(metrics: &MetricsState) {
    metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
}

#[cfg(feature = "instrumentation")]
pub(crate) fn worker_stolen(metrics: &MetricsState, count: usize) {
    metrics.steals.fetch_add(count as u64, Ordering::Relaxed);
}

#[cfg(feature = "instrumentation")]
pub(crate) fn worker_parked(metrics: &MetricsState) {
    metrics.parks.fetch_add(1, Ordering::Relaxed);
}

#[cfg(feature = "instrumentation")]
pub(crate) fn worker_busy(metrics: &MetricsState, duration: Duration) {
    metrics.worker_busy_ns.fetch_add(
        duration.as_nanos().min(u64::MAX as u128) as u64,
        Ordering::Relaxed,
    );
}

#[cfg(not(feature = "instrumentation"))]
pub(crate) fn task_spawned(_: &MetricsState) {}
#[cfg(not(feature = "instrumentation"))]
pub(crate) fn task_finished(_: &MetricsState) {}
#[cfg(not(feature = "instrumentation"))]
#[allow(dead_code)]
pub(crate) fn task_polled(_: &MetricsState, _: Duration) {}
#[cfg(not(feature = "instrumentation"))]
pub(crate) fn task_scheduled(_: &MetricsState) {}
#[cfg(not(feature = "instrumentation"))]
pub(crate) fn task_queued(_: &MetricsState) {}
#[cfg(not(feature = "instrumentation"))]
pub(crate) fn task_dequeued(_: &MetricsState) {}
#[cfg(not(feature = "instrumentation"))]
pub(crate) fn worker_stolen(_: &MetricsState, _: usize) {}
#[cfg(not(feature = "instrumentation"))]
pub(crate) fn worker_parked(_: &MetricsState) {}
#[cfg(not(feature = "instrumentation"))]
pub(crate) fn worker_busy(_: &MetricsState, _: Duration) {}

pub(crate) fn snapshot_tasks(tasks: &[crate::task::RawTask]) -> Vec<TaskSnapshot> {
    tasks
        .iter()
        .map(|task| {
            let header = task.header();
            let state = header.state.snapshot();
            let task_state = if state.is_complete() {
                TaskState::Complete
            } else if state.is_cancelled() {
                TaskState::Cancelled
            } else if state.is_running() {
                TaskState::Running
            } else if state.is_notified() {
                TaskState::Queued
            } else {
                TaskState::Idle
            };
            TaskSnapshot {
                id: header.task_id,
                name: header.name.clone(),
                location: header.location.clone(),
                parent: header.parent,
                state: task_state,
                worker: task.owner_id(),
                polls: header.polls.load(Ordering::Relaxed),
                total_busy: Duration::from_nanos(header.busy_ns.load(Ordering::Relaxed)),
                total_idle: Duration::from_nanos(header.idle_ns.load(Ordering::Relaxed)),
                scheduled: header.scheduled.load(Ordering::Relaxed),
            }
        })
        .collect()
}

/// Encode one event as a little-endian `u32` payload length followed by a
/// stable tag-and-fields payload. The framing is deliberately implemented
/// here instead of adding a serialization dependency to the runtime.
#[cfg(feature = "instrumentation")]
pub(crate) fn encode_frame(event: &RuntimeEvent) -> Vec<u8> {
    let mut payload = Vec::new();
    fn u8(out: &mut Vec<u8>, value: u8) {
        out.push(value);
    }
    fn u32(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_le_bytes());
    }
    fn u64(out: &mut Vec<u8>, value: u64) {
        out.extend_from_slice(&value.to_le_bytes());
    }
    fn bytes(out: &mut Vec<u8>, value: &[u8]) {
        assert!(
            value.len() <= u32::MAX as usize,
            "eddy: event field is too large"
        );
        u32(out, value.len() as u32);
        out.extend_from_slice(value);
    }
    fn string(out: &mut Vec<u8>, value: &str) {
        bytes(out, value.as_bytes());
    }
    fn id(out: &mut Vec<u8>, value: TaskId) {
        u64(out, value.0);
    }
    fn duration(out: &mut Vec<u8>, value: Duration) {
        u64(out, value.as_nanos().min(u64::MAX as u128) as u64);
    }
    fn location(out: &mut Vec<u8>, value: &Location) {
        string(out, value.file);
        u32(out, value.line);
    }
    fn instant(out: &mut Vec<u8>, value: Instant) {
        let nanos = ORIGIN
            .get()
            .map(|origin| value.saturating_duration_since(*origin))
            .unwrap_or_default();
        duration(out, nanos);
    }
    fn source(out: &mut Vec<u8>, value: &WakeSource) {
        match value {
            WakeSource::Io { fd } => {
                u8(out, 0);
                u32(out, *fd as u32);
            }
            WakeSource::Timer { id: timer } => {
                u8(out, 1);
                u64(out, *timer);
            }
            WakeSource::Task(task) => {
                u8(out, 2);
                id(out, *task);
            }
            WakeSource::Channel { kind } => {
                u8(out, 3);
                string(out, kind);
            }
            WakeSource::External => u8(out, 4),
        }
    }
    fn unpark_reason(out: &mut Vec<u8>, value: &UnparkReason) {
        u8(
            out,
            match value {
                UnparkReason::Task => 0,
                UnparkReason::Shutdown => 1,
                UnparkReason::Steal => 2,
                UnparkReason::Event => 3,
            },
        );
    }

    match event {
        RuntimeEvent::TaskSpawned {
            id: task,
            name,
            location: source_location,
            parent,
        } => {
            u8(&mut payload, 0);
            id(&mut payload, *task);
            match name {
                Some(name) => {
                    u8(&mut payload, 1);
                    string(&mut payload, name);
                }
                None => u8(&mut payload, 0),
            }
            location(&mut payload, source_location);
            match parent {
                Some(parent) => {
                    u8(&mut payload, 1);
                    id(&mut payload, *parent);
                }
                None => u8(&mut payload, 0),
            }
        }
        RuntimeEvent::TaskPollStart {
            id: task,
            worker,
            at,
        } => {
            u8(&mut payload, 1);
            id(&mut payload, *task);
            u32(&mut payload, *worker);
            instant(&mut payload, *at);
        }
        RuntimeEvent::TaskPollEnd {
            id: task,
            worker,
            duration: elapsed,
            result,
        } => {
            u8(&mut payload, 2);
            id(&mut payload, *task);
            u32(&mut payload, *worker);
            duration(&mut payload, *elapsed);
            u8(
                &mut payload,
                match result {
                    PollResult::Ready => 0,
                    PollResult::Pending => 1,
                    PollResult::Panicked => 2,
                },
            );
        }
        RuntimeEvent::TaskWoken { id: task, by } => {
            u8(&mut payload, 3);
            id(&mut payload, *task);
            source(&mut payload, by);
        }
        RuntimeEvent::TaskDropped {
            id: task,
            total_polls,
            total_busy,
            total_idle,
        } => {
            u8(&mut payload, 4);
            id(&mut payload, *task);
            u64(&mut payload, *total_polls);
            duration(&mut payload, *total_busy);
            duration(&mut payload, *total_idle);
        }
        RuntimeEvent::TaskAborted { id: task } => {
            u8(&mut payload, 5);
            id(&mut payload, *task);
        }
        RuntimeEvent::WorkerPark { worker, timeout } => {
            u8(&mut payload, 6);
            u32(&mut payload, *worker);
            match timeout {
                Some(value) => {
                    u8(&mut payload, 1);
                    duration(&mut payload, *value);
                }
                None => u8(&mut payload, 0),
            }
        }
        RuntimeEvent::WorkerUnpark { worker, reason } => {
            u8(&mut payload, 7);
            u32(&mut payload, *worker);
            unpark_reason(&mut payload, reason);
        }
        RuntimeEvent::WorkerSteal {
            thief,
            victim,
            count,
        } => {
            u8(&mut payload, 8);
            u32(&mut payload, *thief);
            u32(&mut payload, *victim);
            u64(&mut payload, *count as u64);
        }
        RuntimeEvent::QueueDepth {
            worker,
            local,
            global,
            lifo,
        } => {
            u8(&mut payload, 9);
            u32(&mut payload, *worker);
            u64(&mut payload, *local as u64);
            u64(&mut payload, *global as u64);
            u8(&mut payload, u8::from(*lifo));
        }
        RuntimeEvent::IoRegistered { fd, interest, task } => {
            u8(&mut payload, 10);
            u32(&mut payload, *fd as u32);
            string(&mut payload, interest);
            id(&mut payload, *task);
        }
        RuntimeEvent::IoReady {
            fd,
            readiness,
            woke,
        } => {
            u8(&mut payload, 11);
            u32(&mut payload, *fd as u32);
            string(&mut payload, readiness);
            assert!(
                woke.len() <= u32::MAX as usize,
                "eddy: event field is too large"
            );
            u32(&mut payload, woke.len() as u32);
            for task in woke {
                id(&mut payload, *task);
            }
        }
        RuntimeEvent::TimerSet {
            id: timer,
            deadline,
            task,
        } => {
            u8(&mut payload, 12);
            u64(&mut payload, *timer);
            instant(&mut payload, *deadline);
            id(&mut payload, *task);
        }
        RuntimeEvent::TimerFired {
            id: timer,
            lateness,
        } => {
            u8(&mut payload, 13);
            u64(&mut payload, *timer);
            duration(&mut payload, *lateness);
        }
        RuntimeEvent::TimerCancelled { id: timer } => {
            u8(&mut payload, 14);
            u64(&mut payload, *timer);
        }
        RuntimeEvent::BlockingDetected {
            task,
            poll_duration,
            location: source_location,
        } => {
            u8(&mut payload, 15);
            id(&mut payload, *task);
            duration(&mut payload, *poll_duration);
            location(&mut payload, source_location);
        }
        RuntimeEvent::BudgetExhausted { task } => {
            u8(&mut payload, 16);
            id(&mut payload, *task);
        }
        RuntimeEvent::ResourceContended {
            kind,
            holder,
            waiters,
        } => {
            u8(&mut payload, 17);
            string(&mut payload, kind);
            id(&mut payload, *holder);
            assert!(
                waiters.len() <= u32::MAX as usize,
                "eddy: event field is too large"
            );
            u32(&mut payload, waiters.len() as u32);
            for task in waiters {
                id(&mut payload, *task);
            }
        }
    }
    assert!(
        payload.len() <= u32::MAX as usize,
        "eddy: event frame is too large"
    );
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&payload);
    frame
}

/// Install a Unix-domain event stream. Each connected client receives
/// length-prefixed event frames; slow clients are dropped rather than
/// blocking scheduler workers.
#[cfg(all(unix, feature = "instrumentation"))]
pub fn install_unix_socket(path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::net::UnixListener;
    use std::sync::mpsc;
    use std::thread;

    const CLIENT_QUEUE: usize = 64;
    let path = path.as_ref().to_owned();
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    let clients = Arc::new(Mutex::new(Vec::<mpsc::SyncSender<Vec<u8>>>::new()));
    let clients_for_accept = clients.clone();
    thread::Builder::new()
        .name("eddy-instrumentation".to_string())
        .spawn(move || {
            for stream in listener.incoming().flatten() {
                let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(CLIENT_QUEUE);
                thread::Builder::new()
                    .name("eddy-instrumentation-client".to_string())
                    .spawn(move || {
                        let mut stream = stream;
                        for frame in receiver {
                            if stream.write_all(&frame).is_err() {
                                break;
                            }
                        }
                    })
                    .expect("eddy: failed to start instrumentation client");
                clients_for_accept.lock().unwrap().push(sender);
            }
        })
        .map_err(std::io::Error::other)?;

    set_subscriber(Arc::new(move |event| {
        let frame = encode_frame(&event);
        let mut clients = clients.lock().unwrap();
        clients.retain(|client| client.try_send(frame.clone()).is_ok());
    }));
    Ok(())
}

#[cfg(all(test, feature = "instrumentation"))]
mod tests {
    use super::*;

    #[test]
    fn frame_prefix_is_the_exact_payload_length() {
        let frame = encode_frame(&RuntimeEvent::BudgetExhausted { task: TaskId(7) });
        let payload_len = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(payload_len, frame.len() - 4);
        assert_eq!(frame[4], 16); // BudgetExhausted's stable event tag.
    }

    #[cfg(unix)]
    #[test]
    fn unix_socket_delivers_a_complete_length_prefixed_frame() {
        use std::io::Read;
        use std::os::unix::net::UnixStream;
        use std::time::{Duration, Instant};

        let path = std::env::temp_dir().join(format!(
            "eddy-instrumentation-{}-{}",
            std::process::id(),
            TaskId::next().0
        ));
        install_unix_socket(&path).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut stream = loop {
            match UnixStream::connect(&path) {
                Ok(stream) => break stream,
                Err(error) if Instant::now() < deadline => {
                    assert!(
                        error.kind() == std::io::ErrorKind::NotFound
                            || error.kind() == std::io::ErrorKind::ConnectionRefused
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("could not connect to instrumentation socket: {error}"),
            }
        };
        std::thread::sleep(Duration::from_millis(10));
        emit(|| RuntimeEvent::BudgetExhausted { task: TaskId(9) });

        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).unwrap();
        let payload_len = u32::from_le_bytes(prefix) as usize;
        let mut payload = vec![0; payload_len];
        stream.read_exact(&mut payload).unwrap();
        assert!(payload.first().is_some_and(|tag| *tag <= 17));
        clear_subscriber();
        let _ = std::fs::remove_file(path);
    }
}
