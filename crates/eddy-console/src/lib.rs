//! A small terminal viewer for eddy's instrumentation socket.
//!
//! The runtime deliberately has no serialization dependency.  The console
//! therefore owns the matching decoder for its stable, tagged wire format.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs, Wrap};
use ratatui::Frame;

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
const MAX_HISTORY: usize = 64;

/// A location included in a task event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Location {
    pub file: String,
    pub line: u32,
}

/// A source for a task wake.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WakeSource {
    Io { fd: i32 },
    Timer { id: u64 },
    Task(u64),
    Channel { kind: String },
    External,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PollResult {
    Ready,
    Pending,
    Panicked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnparkReason {
    Task,
    Shutdown,
    Steal,
    Event,
}

/// One decoded instrumentation event. Durations and timestamps are nanoseconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    TaskSpawned {
        id: u64,
        name: Option<String>,
        location: Location,
        parent: Option<u64>,
    },
    TaskPollStart {
        id: u64,
        worker: u32,
        at_ns: u64,
    },
    TaskPollEnd {
        id: u64,
        worker: u32,
        duration_ns: u64,
        result: PollResult,
    },
    TaskWoken {
        id: u64,
        by: WakeSource,
    },
    TaskDropped {
        id: u64,
        total_polls: u64,
        total_busy_ns: u64,
        total_idle_ns: u64,
    },
    TaskAborted {
        id: u64,
    },
    WorkerPark {
        worker: u32,
        timeout_ns: Option<u64>,
    },
    WorkerUnpark {
        worker: u32,
        reason: UnparkReason,
    },
    WorkerSteal {
        thief: u32,
        victim: u32,
        count: u64,
    },
    QueueDepth {
        worker: u32,
        local: u64,
        global: u64,
        lifo: bool,
    },
    IoRegistered {
        fd: i32,
        interest: String,
        task: u64,
    },
    IoReady {
        fd: i32,
        readiness: String,
        woke: Vec<u64>,
    },
    TimerSet {
        id: u64,
        deadline_ns: u64,
        task: u64,
    },
    TimerFired {
        id: u64,
        lateness_ns: u64,
    },
    TimerCancelled {
        id: u64,
    },
    BlockingDetected {
        task: u64,
        poll_duration_ns: u64,
        location: Location,
    },
    BudgetExhausted {
        task: u64,
    },
    ResourceContended {
        kind: String,
        holder: u64,
        waiters: Vec<u64>,
    },
}

/// Errors from the length-prefixed event protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    FrameTooLarge(u32),
    UnknownTag(u8),
    Truncated,
    InvalidUtf8,
    InvalidValue(&'static str),
    TrailingBytes { tag: u8, remaining: usize },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge(size) => write!(f, "event frame is too large ({size} bytes)"),
            Self::UnknownTag(tag) => write!(f, "unknown event tag {tag}"),
            Self::Truncated => f.write_str("truncated event frame"),
            Self::InvalidUtf8 => f.write_str("event contains invalid UTF-8"),
            Self::InvalidValue(value) => write!(f, "invalid {value} in event frame"),
            Self::TrailingBytes { tag, remaining } => {
                write!(f, "event tag {tag} has {remaining} trailing bytes")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

/// Incremental decoder for `[little-endian u32 length][payload]` frames.
#[derive(Default)]
pub struct FrameDecoder {
    buffer: Vec<u8>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add arbitrary bytes, returning every complete event now available.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Event>, DecodeError> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        loop {
            if self.buffer.len() < 4 {
                break;
            }
            let size = u32::from_le_bytes(self.buffer[..4].try_into().unwrap());
            if size as usize > MAX_FRAME_SIZE {
                return Err(DecodeError::FrameTooLarge(size));
            }
            let frame_size = 4 + size as usize;
            if self.buffer.len() < frame_size {
                break;
            }
            let payload = self.buffer[4..frame_size].to_vec();
            self.buffer.drain(..frame_size);
            events.push(decode_payload(&payload)?);
        }
        Ok(events)
    }

    /// A clean socket close is valid only between complete frames.
    pub fn finish(&self) -> Result<(), DecodeError> {
        if self.buffer.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::Truncated)
        }
    }
}

struct Payload<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Payload<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(DecodeError::Truncated)?;
        if end > self.bytes.len() {
            return Err(DecodeError::Truncated);
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn string(&mut self) -> Result<String, DecodeError> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec()).map_err(|_| DecodeError::InvalidUtf8)
    }

    fn optional_string(&mut self) -> Result<Option<String>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.string()?)),
            _ => Err(DecodeError::InvalidValue("optional string marker")),
        }
    }

    fn optional_u64(&mut self) -> Result<Option<u64>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            _ => Err(DecodeError::InvalidValue("optional integer marker")),
        }
    }

    fn location(&mut self) -> Result<Location, DecodeError> {
        Ok(Location {
            file: self.string()?,
            line: self.u32()?,
        })
    }

    fn id_list(&mut self) -> Result<Vec<u64>, DecodeError> {
        let count = self.u32()? as usize;
        if count > self.bytes.len().saturating_sub(self.offset) / 8 {
            return Err(DecodeError::Truncated);
        }
        (0..count).map(|_| self.u64()).collect()
    }

    fn done(&self, tag: u8) -> Result<(), DecodeError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes {
                tag,
                remaining: self.bytes.len() - self.offset,
            })
        }
    }
}

fn decode_payload(bytes: &[u8]) -> Result<Event, DecodeError> {
    let mut p = Payload::new(bytes);
    let tag = p.u8()?;
    let event = match tag {
        0 => Event::TaskSpawned {
            id: p.u64()?,
            name: p.optional_string()?,
            location: p.location()?,
            parent: p.optional_u64()?,
        },
        1 => Event::TaskPollStart {
            id: p.u64()?,
            worker: p.u32()?,
            at_ns: p.u64()?,
        },
        2 => Event::TaskPollEnd {
            id: p.u64()?,
            worker: p.u32()?,
            duration_ns: p.u64()?,
            result: match p.u8()? {
                0 => PollResult::Ready,
                1 => PollResult::Pending,
                2 => PollResult::Panicked,
                _ => return Err(DecodeError::InvalidValue("poll result")),
            },
        },
        3 => Event::TaskWoken {
            id: p.u64()?,
            by: decode_wake_source(&mut p)?,
        },
        4 => Event::TaskDropped {
            id: p.u64()?,
            total_polls: p.u64()?,
            total_busy_ns: p.u64()?,
            total_idle_ns: p.u64()?,
        },
        5 => Event::TaskAborted { id: p.u64()? },
        6 => Event::WorkerPark {
            worker: p.u32()?,
            timeout_ns: p.optional_u64()?,
        },
        7 => Event::WorkerUnpark {
            worker: p.u32()?,
            reason: decode_unpark_reason(&mut p)?,
        },
        8 => Event::WorkerSteal {
            thief: p.u32()?,
            victim: p.u32()?,
            count: p.u64()?,
        },
        9 => Event::QueueDepth {
            worker: p.u32()?,
            local: p.u64()?,
            global: p.u64()?,
            lifo: match p.u8()? {
                0 => false,
                1 => true,
                _ => return Err(DecodeError::InvalidValue("lifo marker")),
            },
        },
        10 => Event::IoRegistered {
            fd: p.u32()? as i32,
            interest: p.string()?,
            task: p.u64()?,
        },
        11 => Event::IoReady {
            fd: p.u32()? as i32,
            readiness: p.string()?,
            woke: p.id_list()?,
        },
        12 => Event::TimerSet {
            id: p.u64()?,
            deadline_ns: p.u64()?,
            task: p.u64()?,
        },
        13 => Event::TimerFired {
            id: p.u64()?,
            lateness_ns: p.u64()?,
        },
        14 => Event::TimerCancelled { id: p.u64()? },
        15 => Event::BlockingDetected {
            task: p.u64()?,
            poll_duration_ns: p.u64()?,
            location: p.location()?,
        },
        16 => Event::BudgetExhausted { task: p.u64()? },
        17 => Event::ResourceContended {
            kind: p.string()?,
            holder: p.u64()?,
            waiters: p.id_list()?,
        },
        _ => return Err(DecodeError::UnknownTag(tag)),
    };
    p.done(tag)?;
    Ok(event)
}

fn decode_wake_source(p: &mut Payload<'_>) -> Result<WakeSource, DecodeError> {
    Ok(match p.u8()? {
        0 => WakeSource::Io {
            fd: p.u32()? as i32,
        },
        1 => WakeSource::Timer { id: p.u64()? },
        2 => WakeSource::Task(p.u64()?),
        3 => WakeSource::Channel { kind: p.string()? },
        4 => WakeSource::External,
        _ => return Err(DecodeError::InvalidValue("wake source")),
    })
}

fn decode_unpark_reason(p: &mut Payload<'_>) -> Result<UnparkReason, DecodeError> {
    Ok(match p.u8()? {
        0 => UnparkReason::Task,
        1 => UnparkReason::Shutdown,
        2 => UnparkReason::Steal,
        3 => UnparkReason::Event,
        _ => return Err(DecodeError::InvalidValue("unpark reason")),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskState {
    Queued,
    Running,
    Idle,
    Complete,
    Cancelled,
}

impl TaskState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Idle => "idle",
            Self::Complete => "done",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Debug)]
pub struct TaskView {
    pub id: u64,
    pub name: String,
    pub state: TaskState,
    pub worker: u32,
    pub polls: u64,
    pub total_busy_ns: u64,
    pub total_idle_ns: u64,
    pub scheduled: u64,
    pub location: Option<Location>,
    pub warnings: Vec<String>,
    pub poll_durations_ns: Vec<u64>,
    pub wake_sources: Vec<String>,
}

impl TaskView {
    fn new(id: u64) -> Self {
        Self {
            id,
            name: "<unnamed>".to_string(),
            state: TaskState::Queued,
            worker: 0,
            polls: 0,
            total_busy_ns: 0,
            total_idle_ns: 0,
            scheduled: 1,
            location: None,
            warnings: Vec::new(),
            poll_durations_ns: Vec::new(),
            wake_sources: Vec::new(),
        }
    }

    fn warning(&mut self, warning: &str) {
        if !self.warnings.iter().any(|value| value == warning) {
            self.warnings.push(warning.to_string());
        }
    }

    fn duration(&self) -> u64 {
        self.total_busy_ns.saturating_add(self.total_idle_ns)
    }
}

#[derive(Clone, Debug, Default)]
pub struct WorkerView {
    pub local_queue: u64,
    pub global_queue: u64,
    pub parks: u64,
    pub steals: u64,
    pub busy_ns: u64,
    pub polls: u64,
}

#[derive(Clone, Debug)]
pub struct ResourceView {
    pub kind: String,
    pub holder: u64,
    pub waiters: Vec<u64>,
}

/// An operation that is currently waiting for an asynchronous resource.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingOperation {
    pub task: u64,
    pub resource: PendingResource,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingResource {
    Io { fd: i32, interest: String },
    Timer { id: u64, deadline_ns: u64 },
    Resource { kind: String, holder: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum View {
    Tasks,
    Workers,
    Resources,
    AsyncOps,
}

/// Input and optional raw-stream recording configuration for the console.
#[derive(Clone, Debug)]
pub struct ConsoleOptions {
    pub socket: PathBuf,
    pub record: Option<PathBuf>,
    pub replay: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SortKey {
    Id,
    Total,
    Busy,
    Idle,
    Polls,
}

/// In-memory projection of the event stream used by both the renderer and tests.
#[derive(Debug)]
pub struct ConsoleModel {
    pub tasks: BTreeMap<u64, TaskView>,
    pub workers: BTreeMap<u32, WorkerView>,
    pub resources: BTreeMap<String, ResourceView>,
    pub pending_operations: Vec<PendingOperation>,
    pub connected: bool,
    pub disconnect_reason: Option<String>,
    pub event_count: u64,
    pub view: View,
    pub selected_task: usize,
    pub paused: bool,
    show_help: bool,
    sort: SortKey,
    filter: String,
    filter_input: Option<String>,
}

impl Default for ConsoleModel {
    fn default() -> Self {
        Self {
            tasks: BTreeMap::new(),
            workers: BTreeMap::new(),
            resources: BTreeMap::new(),
            pending_operations: Vec::new(),
            connected: true,
            disconnect_reason: None,
            event_count: 0,
            view: View::Tasks,
            selected_task: 0,
            paused: false,
            show_help: false,
            sort: SortKey::Id,
            filter: String::new(),
            filter_input: None,
        }
    }
}

impl ConsoleModel {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, event: Event) {
        if self.paused {
            return;
        }
        self.event_count = self.event_count.saturating_add(1);
        match event {
            Event::TaskSpawned {
                id, name, location, ..
            } => {
                let task = self.tasks.entry(id).or_insert_with(|| TaskView::new(id));
                task.name = name.unwrap_or_else(|| "<unnamed>".to_string());
                task.location = Some(location);
                task.state = TaskState::Queued;
                task.scheduled = task.scheduled.max(1);
            }
            Event::TaskPollStart { id, worker, .. } => {
                self.clear_task_pending(id);
                let task = self.task_mut(id);
                task.worker = worker;
                task.state = TaskState::Running;
                self.workers.entry(worker).or_default();
            }
            Event::TaskPollEnd {
                id,
                worker,
                duration_ns,
                result,
            } => {
                let task = self.task_mut(id);
                task.worker = worker;
                task.polls = task.polls.saturating_add(1);
                task.total_busy_ns = task.total_busy_ns.saturating_add(duration_ns);
                push_history(&mut task.poll_durations_ns, duration_ns);
                task.state = match result {
                    PollResult::Ready => TaskState::Complete,
                    PollResult::Pending => TaskState::Idle,
                    PollResult::Panicked => {
                        task.warning("panic");
                        TaskState::Cancelled
                    }
                };
                let worker_view = self.workers.entry(worker).or_default();
                worker_view.busy_ns = worker_view.busy_ns.saturating_add(duration_ns);
                worker_view.polls = worker_view.polls.saturating_add(1);
            }
            Event::TaskWoken { id, by } => {
                self.clear_woken_pending(id, &by);
                let task = self.task_mut(id);
                task.state = TaskState::Queued;
                task.scheduled = task.scheduled.saturating_add(1);
                push_history(&mut task.wake_sources, wake_source_name(&by));
            }
            Event::TaskDropped {
                id,
                total_polls,
                total_busy_ns,
                total_idle_ns,
            } => {
                self.clear_task_pending(id);
                let task = self.task_mut(id);
                task.state = TaskState::Complete;
                task.polls = total_polls;
                task.total_busy_ns = total_busy_ns;
                task.total_idle_ns = total_idle_ns;
            }
            Event::TaskAborted { id } => {
                self.clear_task_pending(id);
                self.task_mut(id).state = TaskState::Cancelled;
            }
            Event::WorkerPark { worker, .. } => {
                self.workers.entry(worker).or_default().parks += 1;
            }
            Event::WorkerUnpark { worker, .. } => {
                self.workers.entry(worker).or_default();
            }
            Event::WorkerSteal {
                thief,
                victim,
                count,
            } => {
                self.workers.entry(thief).or_default().steals = self
                    .workers
                    .get(&thief)
                    .map_or(count, |worker| worker.steals.saturating_add(count));
                self.workers.entry(victim).or_default();
            }
            Event::QueueDepth {
                worker,
                local,
                global,
                ..
            } => {
                let view = self.workers.entry(worker).or_default();
                view.local_queue = local;
                view.global_queue = global;
            }
            Event::BlockingDetected {
                task,
                poll_duration_ns,
                location: _,
            } => {
                let task = self.task_mut(task);
                task.warning("blocking");
                push_history(&mut task.poll_durations_ns, poll_duration_ns);
            }
            Event::BudgetExhausted { task } => self.task_mut(task).warning("budget"),
            Event::ResourceContended {
                kind,
                holder,
                waiters,
            } => {
                self.pending_operations.retain(|operation| {
                    !matches!(&operation.resource, PendingResource::Resource { kind: current_kind, .. } if *current_kind == kind)
                });
                self.pending_operations
                    .extend(waiters.iter().copied().map(|task| PendingOperation {
                        task,
                        resource: PendingResource::Resource {
                            kind: kind.clone(),
                            holder,
                        },
                    }));
                self.resources.insert(
                    kind.clone(),
                    ResourceView {
                        kind,
                        holder,
                        waiters,
                    },
                );
            }
            Event::IoRegistered { fd, interest, task } => {
                self.pending_operations.retain(|operation| {
                    !matches!(
                        &operation.resource,
                        PendingResource::Io { fd: current_fd, .. }
                            if *current_fd == fd && operation.task == task
                    )
                });
                self.pending_operations.push(PendingOperation {
                    task,
                    resource: PendingResource::Io { fd, interest },
                });
            }
            Event::IoReady { fd, woke, .. } => {
                self.pending_operations.retain(|operation| {
                    !matches!(
                        &operation.resource,
                        PendingResource::Io { fd: current_fd, .. }
                            if *current_fd == fd && woke.contains(&operation.task)
                    )
                });
            }
            Event::TimerSet {
                id,
                deadline_ns,
                task,
            } => {
                self.pending_operations.retain(|operation| {
                    !matches!(&operation.resource, PendingResource::Timer { id: current_id, .. } if *current_id == id)
                });
                self.pending_operations.push(PendingOperation {
                    task,
                    resource: PendingResource::Timer { id, deadline_ns },
                });
            }
            Event::TimerFired { id, .. } | Event::TimerCancelled { id } => {
                self.pending_operations.retain(|operation| {
                    !matches!(&operation.resource, PendingResource::Timer { id: current_id, .. } if *current_id == id)
                });
            }
        }
    }

    pub fn disconnected(&mut self, reason: impl Into<String>) {
        self.connected = false;
        self.disconnect_reason = Some(reason.into());
    }

    fn task_mut(&mut self, id: u64) -> &mut TaskView {
        self.tasks.entry(id).or_insert_with(|| TaskView::new(id))
    }

    fn clear_task_pending(&mut self, task: u64) {
        self.pending_operations
            .retain(|operation| operation.task != task);
    }

    fn clear_woken_pending(&mut self, task: u64, source: &WakeSource) {
        match source {
            WakeSource::Io { fd } => self.pending_operations.retain(|operation| {
                !matches!(&operation.resource, PendingResource::Io { fd: current_fd, .. } if *current_fd == *fd && operation.task == task)
            }),
            WakeSource::Timer { id } => self.pending_operations.retain(|operation| {
                !matches!(&operation.resource, PendingResource::Timer { id: current_id, .. } if *current_id == *id && operation.task == task)
            }),
            WakeSource::Task(_) | WakeSource::Channel { .. } | WakeSource::External => {
                self.clear_task_pending(task);
            }
        }
    }

    fn sorted_tasks(&self) -> Vec<&TaskView> {
        let mut tasks: Vec<_> = self
            .tasks
            .values()
            .filter(|task| self.matches_filter(task))
            .collect();
        tasks.sort_by(|left, right| {
            let ordering = match self.sort {
                SortKey::Id => left.id.cmp(&right.id),
                SortKey::Total => left.duration().cmp(&right.duration()),
                SortKey::Busy => left.total_busy_ns.cmp(&right.total_busy_ns),
                SortKey::Idle => left.total_idle_ns.cmp(&right.total_idle_ns),
                SortKey::Polls => left.polls.cmp(&right.polls),
            };
            ordering.then_with(|| left.id.cmp(&right.id))
        });
        tasks
    }

    fn matches_filter(&self, task: &TaskView) -> bool {
        if self.filter.is_empty() {
            return true;
        }
        let filter = self.filter.to_ascii_lowercase();
        task.name.to_ascii_lowercase().contains(&filter)
            || task.state.as_str().to_ascii_lowercase().contains(&filter)
    }

    fn select_next(&mut self, amount: isize) {
        let count = self
            .tasks
            .values()
            .filter(|task| self.matches_filter(task))
            .count();
        if count == 0 {
            self.selected_task = 0;
        } else {
            self.selected_task =
                (self.selected_task as isize + amount).rem_euclid(count as isize) as usize;
        }
    }
}

fn push_history<T>(history: &mut Vec<T>, value: T) {
    if history.len() == MAX_HISTORY {
        history.remove(0);
    }
    history.push(value);
}

fn wake_source_name(source: &WakeSource) -> String {
    match source {
        WakeSource::Io { fd } => format!("io:{fd}"),
        WakeSource::Timer { id } => format!("timer:{id}"),
        WakeSource::Task(id) => format!("task:{id}"),
        WakeSource::Channel { kind } => format!("channel:{kind}"),
        WakeSource::External => "external".to_string(),
    }
}

fn format_duration(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    } else if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.2}us", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}

fn shorten(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_string();
    }
    let mut output: String = value.chars().take(width.saturating_sub(1)).collect();
    output.push('~');
    output
}

/// Whether terminal colors should be emitted. `NO_COLOR` is presence-based,
/// including when it is set to an empty value.
pub fn colors_enabled() -> bool {
    std::env::var_os("NO_COLOR").is_none()
}

fn colored_style(color: bool, foreground: Option<Color>, background: Option<Color>) -> Style {
    let mut style = Style::default();
    if color {
        if let Some(foreground) = foreground {
            style = style.fg(foreground);
        }
        if let Some(background) = background {
            style = style.bg(background);
        }
    }
    style
}

/// Render the current model. This is public so a non-interactive frontend can
/// reuse the projection and so tests can render with ratatui's TestBackend.
pub fn render(frame: &mut Frame<'_>, model: &ConsoleModel) {
    render_with_color(frame, model, colors_enabled());
}

fn render_with_color(frame: &mut Frame<'_>, model: &ConsoleModel, color: bool) {
    let area = frame.area();
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(4),
            Constraint::Length(1),
        ])
        .split(area);

    let connection = if model.connected {
        Span::styled(
            " CONNECTED ",
            colored_style(color, Some(Color::Green), None),
        )
    } else {
        Span::styled(
            " DISCONNECTED ",
            colored_style(color, Some(Color::Red), None),
        )
    };
    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            " eddy-console ",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        connection,
        Span::raw(format!(
            "  events:{}  tasks:{}",
            model.event_count,
            model.tasks.len()
        )),
    ]))
    .block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(title, vertical[0]);

    if model.show_help {
        render_help(frame, vertical[1]);
    } else {
        let tabs = Tabs::new(["Tasks", "Workers", "Resources", "Async Ops"])
            .select(match model.view {
                View::Tasks => 0,
                View::Workers => 1,
                View::Resources => 2,
                View::AsyncOps => 3,
            })
            .block(Block::default().borders(Borders::ALL).title(" View "))
            .highlight_style(
                colored_style(color, Some(Color::Cyan), None).add_modifier(Modifier::BOLD),
            );
        let content = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(1)])
            .split(vertical[1]);
        frame.render_widget(tabs, content[0]);
        match model.view {
            View::Tasks => render_tasks(frame, model, content[1], color),
            View::Workers => render_workers(frame, model, content[1]),
            View::Resources => render_resources(frame, model, content[1]),
            View::AsyncOps => render_async_ops(frame, model, content[1]),
        }
    }

    let footer = if let Some(reason) = &model.disconnect_reason {
        format!(" {reason}  |  q quit  r reconnect externally")
    } else if let Some(filter) = &model.filter_input {
        format!(" FILTER: {filter}_  |  enter apply  esc cancel  backspace delete")
    } else if !model.filter.is_empty() {
        format!(
            " FILTER: {}  |  / edit  esc clear  1-4 views  q quit",
            model.filter
        )
    } else if model.paused {
        " PAUSED  |  space resume  1-4 views  ? help  q quit".to_string()
    } else {
        " / filter  1-4 views  t/b/i/p sort  arrows select  space pause  ? help  q quit".to_string()
    };
    frame.render_widget(Paragraph::new(footer), vertical[2]);
}

fn render_tasks(frame: &mut Frame<'_>, model: &ConsoleModel, area: Rect, color: bool) {
    let rows = model
        .sorted_tasks()
        .into_iter()
        .enumerate()
        .map(|(index, task)| {
            let warning = if task.warnings.is_empty() {
                " ".to_string()
            } else {
                format!("!{}", task.warnings.join(","))
            };
            let location = task.location.as_ref().map_or_else(String::new, |location| {
                format!("{}:{}", shorten(&location.file, 22), location.line)
            });
            let style = if index == model.selected_task {
                colored_style(color, None, Some(Color::DarkGray))
            } else if !task.warnings.is_empty() {
                colored_style(color, Some(Color::Yellow), None)
            } else {
                Style::default()
            };
            Row::new([
                Cell::from(task.id.to_string()),
                Cell::from(shorten(&task.name, 20)),
                Cell::from(task.state.as_str()),
                Cell::from(format_duration(task.duration())),
                Cell::from(format_duration(task.total_busy_ns)),
                Cell::from(format_duration(task.total_idle_ns)),
                Cell::from(task.polls.to_string()),
                Cell::from(task.scheduled.to_string()),
                Cell::from(warning),
                Cell::from(location),
            ])
            .style(style)
        });
    let header = Row::new([
        "ID", "NAME", "STATE", "TOTAL", "BUSY", "IDLE", "POLLS", "SCHED", "WARN", "LOCATION",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));
    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(20),
            Constraint::Length(9),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(7),
            Constraint::Length(7),
            Constraint::Length(12),
            Constraint::Min(12),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title(" Tasks "));
    frame.render_widget(table, area);
}

fn render_workers(frame: &mut Frame<'_>, model: &ConsoleModel, area: Rect) {
    let rows = model.workers.iter().map(|(id, worker)| {
        Row::new([
            id.to_string(),
            worker.polls.to_string(),
            format_duration(worker.busy_ns),
            worker.local_queue.to_string(),
            worker.global_queue.to_string(),
            worker.parks.to_string(),
            worker.steals.to_string(),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(14),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new([
            "WORKER", "POLLS", "BUSY", "LOCAL Q", "GLOBAL Q", "PARKS", "STEALS",
        ])
        .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(Block::default().borders(Borders::ALL).title(" Workers "));
    frame.render_widget(table, area);
}

fn render_resources(frame: &mut Frame<'_>, model: &ConsoleModel, area: Rect) {
    let rows = model.resources.values().map(|resource| {
        Row::new([
            resource.kind.clone(),
            resource.holder.to_string(),
            if resource.waiters.is_empty() {
                "-".to_string()
            } else {
                resource
                    .waiters
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            },
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(24),
            Constraint::Length(12),
            Constraint::Min(20),
        ],
    )
    .header(
        Row::new(["RESOURCE", "HOLDER", "WAITERS"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(Block::default().borders(Borders::ALL).title(" Resources "));
    frame.render_widget(table, area);
}

fn render_async_ops(frame: &mut Frame<'_>, model: &ConsoleModel, area: Rect) {
    let rows = model.pending_operations.iter().map(|operation| {
        let resource = match &operation.resource {
            PendingResource::Io { fd, interest } => format!("io fd {fd} ({interest})"),
            PendingResource::Timer { id, deadline_ns } => {
                format!("timer {id} (deadline {deadline_ns}ns)")
            }
            PendingResource::Resource { kind, holder } => {
                format!("{kind} (held by task {holder})")
            }
        };
        Row::new([operation.task.to_string(), resource])
    });
    let table = Table::new(rows, [Constraint::Length(12), Constraint::Min(30)])
        .header(
            Row::new(["TASK", "WAITING ON"]).style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .block(Block::default().borders(Borders::ALL).title(" Async Ops "));
    frame.render_widget(table, area);
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    let help = Paragraph::new(vec![
        Line::from("1 Tasks     2 Workers     3 Resources     4 Async Ops"),
        Line::from("t/b/i/p     sort task totals, busy, idle, or polls"),
        Line::from("Up/Down    select a task     Space pause/resume"),
        Line::from("q/Esc       quit                 ? close help"),
        Line::from("The view stays available after the runtime socket closes."),
    ])
    .block(Block::default().borders(Borders::ALL).title(" Help "))
    .wrap(Wrap { trim: true });
    frame.render_widget(help, area);
}

#[cfg(unix)]
mod terminal {
    use super::*;
    use crossterm::event::{self, Event as TerminalEvent, KeyCode, KeyEvent};
    use crossterm::execute;
    use crossterm::terminal::{
        disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
    };
    use ratatui::backend::CrosstermBackend;
    use ratatui::Terminal;
    use std::fs::File;
    use std::io::{self, Read, Stdout, Write};
    use std::os::unix::net::UnixStream;
    use std::path::{Path, PathBuf};
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::thread;
    use std::time::Duration;

    enum ReaderMessage {
        Events(Vec<Event>),
        Disconnected(String),
    }

    struct TerminalSession {
        terminal: Terminal<CrosstermBackend<Stdout>>,
    }

    impl TerminalSession {
        fn new() -> io::Result<Self> {
            enable_raw_mode()?;
            let mut stdout = io::stdout();
            if let Err(error) = execute!(stdout, EnterAlternateScreen, crossterm::cursor::Hide) {
                let _ = disable_raw_mode();
                return Err(error);
            }
            match Terminal::new(CrosstermBackend::new(stdout)) {
                Ok(terminal) => Ok(Self { terminal }),
                Err(error) => {
                    let _ = disable_raw_mode();
                    Err(error)
                }
            }
        }
    }

    impl Drop for TerminalSession {
        fn drop(&mut self) {
            let _ = disable_raw_mode();
            let _ = execute!(
                self.terminal.backend_mut(),
                LeaveAlternateScreen,
                crossterm::cursor::Show
            );
            let _ = self.terminal.show_cursor();
        }
    }

    pub fn run(options: &ConsoleOptions) -> io::Result<()> {
        if options.replay.is_some() && options.record.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--record cannot be used with --replay",
            ));
        }
        let reader: Box<dyn Read + Send> = if let Some(path) = &options.replay {
            Box::new(File::open(path).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("could not open replay file {}: {error}", path.display()),
                )
            })?)
        } else {
            Box::new(UnixStream::connect(&options.socket).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("could not connect to {}: {error}", options.socket.display()),
                )
            })?)
        };
        let mut session = TerminalSession::new()?;
        let receiver = spawn_reader(reader, options.record.clone());
        let mut model = ConsoleModel::new();
        loop {
            while let Ok(message) = receiver.try_recv() {
                match message {
                    ReaderMessage::Events(events) => {
                        for event in events {
                            model.apply(event);
                        }
                    }
                    ReaderMessage::Disconnected(reason) => model.disconnected(reason),
                }
            }
            session.terminal.draw(|frame| render(frame, &model))?;
            if event::poll(Duration::from_millis(100))? {
                if let TerminalEvent::Key(key) = event::read()? {
                    if handle_key(&mut model, key) {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn spawn_reader<R>(reader: R, record: Option<PathBuf>) -> Receiver<ReaderMessage>
    where
        R: Read + Send + 'static,
    {
        let (sender, receiver) = mpsc::channel();
        thread::Builder::new()
            .name("eddy-console-reader".to_string())
            .spawn(move || read_stream(reader, &sender, record.as_deref()))
            .expect("failed to start eddy-console reader");
        receiver
    }

    fn send(sender: &Sender<ReaderMessage>, message: ReaderMessage) -> bool {
        sender.send(message).is_ok()
    }

    fn read_stream<R: Read>(mut reader: R, sender: &Sender<ReaderMessage>, record: Option<&Path>) {
        let mut recorder = match record {
            Some(path) => match File::create(path) {
                Ok(file) => Some(file),
                Err(error) => {
                    let _ = send(
                        sender,
                        ReaderMessage::Disconnected(format!(
                            "could not create record file {}: {error}",
                            path.display()
                        )),
                    );
                    return;
                }
            },
            None => None,
        };
        let mut decoder = FrameDecoder::new();
        let mut bytes = [0_u8; 8192];
        loop {
            match reader.read(&mut bytes) {
                Ok(0) => {
                    let message = match decoder.finish() {
                        Ok(()) => "runtime disconnected".to_string(),
                        Err(error) => format!("runtime disconnected with protocol error: {error}"),
                    };
                    let _ = send(sender, ReaderMessage::Disconnected(message));
                    return;
                }
                Ok(count) => {
                    if let Some(recorder) = &mut recorder {
                        if let Err(error) = recorder.write_all(&bytes[..count]) {
                            let _ = send(
                                sender,
                                ReaderMessage::Disconnected(format!(
                                    "could not write record file: {error}"
                                )),
                            );
                            return;
                        }
                    }
                    match decoder.push(&bytes[..count]) {
                        Ok(events) if !events.is_empty() => {
                            if !send(sender, ReaderMessage::Events(events)) {
                                return;
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let _ = send(
                                sender,
                                ReaderMessage::Disconnected(format!("protocol error: {error}")),
                            );
                            return;
                        }
                    }
                }
                Err(error) => {
                    let _ = send(
                        sender,
                        ReaderMessage::Disconnected(format!("socket error: {error}")),
                    );
                    return;
                }
            }
        }
    }

    fn handle_key(model: &mut ConsoleModel, key: KeyEvent) -> bool {
        if let Some(filter) = &mut model.filter_input {
            match key.code {
                KeyCode::Enter => {
                    model.filter = std::mem::take(filter);
                    model.filter_input = None;
                    model.selected_task = 0;
                }
                KeyCode::Esc => model.filter_input = None,
                KeyCode::Backspace => {
                    filter.pop();
                }
                KeyCode::Char(character) => filter.push(character),
                _ => {}
            }
            return false;
        }

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return true,
            KeyCode::Char('/') => model.filter_input = Some(model.filter.clone()),
            KeyCode::Char('1') => model.view = View::Tasks,
            KeyCode::Char('2') => model.view = View::Workers,
            KeyCode::Char('3') => model.view = View::Resources,
            KeyCode::Char('4') => model.view = View::AsyncOps,
            KeyCode::Char(' ') => model.paused = !model.paused,
            KeyCode::Char('?') => model.show_help = !model.show_help,
            KeyCode::Char('t') => model.sort = SortKey::Total,
            KeyCode::Char('b') => model.sort = SortKey::Busy,
            KeyCode::Char('i') => model.sort = SortKey::Idle,
            KeyCode::Char('p') => model.sort = SortKey::Polls,
            KeyCode::Char('c') if !model.filter.is_empty() => {
                model.filter.clear();
                model.selected_task = 0;
            }
            KeyCode::Up => model.select_next(-1),
            KeyCode::Down => model.select_next(1),
            _ => {}
        }
        false
    }

    pub fn default_socket() -> PathBuf {
        std::env::var_os("EDDY_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/eddy-instrumentation.sock"))
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io::Write;

        #[test]
        fn reader_reports_clean_disconnect_after_a_synthetic_frame() {
            let (mut writer, mut reader) = UnixStream::pair().unwrap();
            let (sender, receiver) = mpsc::channel();
            let thread = thread::spawn(move || read_stream(&mut reader, &sender, None));

            let mut payload = vec![16_u8];
            payload.extend_from_slice(&9_u64.to_le_bytes());
            let mut bytes = (payload.len() as u32).to_le_bytes().to_vec();
            bytes.extend_from_slice(&payload);
            writer.write_all(&bytes).unwrap();

            match receiver.recv().unwrap() {
                ReaderMessage::Events(events) => {
                    assert_eq!(events, vec![Event::BudgetExhausted { task: 9 }]);
                }
                ReaderMessage::Disconnected(reason) => panic!("unexpected disconnect: {reason}"),
            }
            drop(writer);
            match receiver.recv().unwrap() {
                ReaderMessage::Disconnected(reason) => assert_eq!(reason, "runtime disconnected"),
                ReaderMessage::Events(_) => panic!("expected disconnect"),
            }
            thread.join().unwrap();
        }

        #[test]
        fn recording_preserves_frames_for_replay() {
            let payload = [16_u8, 9, 0, 0, 0, 0, 0, 0, 0];
            let bytes = {
                let mut bytes = (payload.len() as u32).to_le_bytes().to_vec();
                bytes.extend_from_slice(&payload);
                bytes
            };
            let path = std::env::temp_dir().join(format!(
                "eddy-console-record-{}-{}.bin",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let (sender, receiver) = mpsc::channel();
            read_stream(std::io::Cursor::new(bytes.clone()), &sender, Some(&path));

            match receiver.recv().unwrap() {
                ReaderMessage::Events(events) => {
                    assert_eq!(events, vec![Event::BudgetExhausted { task: 9 }]);
                }
                ReaderMessage::Disconnected(reason) => panic!("unexpected disconnect: {reason}"),
            }
            assert!(matches!(
                receiver.recv().unwrap(),
                ReaderMessage::Disconnected(_)
            ));
            assert_eq!(std::fs::read(&path).unwrap(), bytes);

            let (sender, receiver) = mpsc::channel();
            let replay = File::open(&path).unwrap();
            read_stream(replay, &sender, None);
            match receiver.recv().unwrap() {
                ReaderMessage::Events(events) => {
                    assert_eq!(events, vec![Event::BudgetExhausted { task: 9 }]);
                }
                ReaderMessage::Disconnected(reason) => panic!("unexpected replay error: {reason}"),
            }
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(unix)]
pub use terminal::default_socket;

/// Run the interactive console against a Unix-domain instrumentation socket.
#[cfg(unix)]
pub fn run(path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    run_with_options(ConsoleOptions {
        socket: path.as_ref().to_owned(),
        record: None,
        replay: None,
    })
}

/// Run the console from a live socket or a previously recorded raw stream.
#[cfg(unix)]
pub fn run_with_options(options: ConsoleOptions) -> std::io::Result<()> {
    terminal::run(&options)
}

/// The console is only able to consume the runtime's Unix socket on Unix.
#[cfg(not(unix))]
pub fn run(_: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "eddy-console requires a Unix-domain socket",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut output = (payload.len() as u32).to_le_bytes().to_vec();
        output.extend_from_slice(payload);
        output
    }

    fn u32_bytes(value: u32) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn u64_bytes(value: u64) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn string_bytes(value: &str) -> Vec<u8> {
        let mut bytes = u32_bytes(value.len() as u32);
        bytes.extend_from_slice(value.as_bytes());
        bytes
    }

    #[test]
    fn decoder_accepts_fragmented_synthetic_frames() {
        let mut payload = vec![0_u8];
        payload.extend(u64_bytes(7));
        payload.push(1);
        payload.extend(string_bytes("worker-task"));
        payload.extend(string_bytes("src/main.rs"));
        payload.extend(u32_bytes(42));
        payload.push(0);
        let bytes = frame(&payload);
        let mut decoder = FrameDecoder::new();
        assert!(decoder.push(&bytes[..2]).unwrap().is_empty());
        assert!(decoder.push(&bytes[2..9]).unwrap().is_empty());
        let events = decoder.push(&bytes[9..]).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0],
            Event::TaskSpawned {
                id: 7,
                name: Some("worker-task".to_string()),
                location: Location {
                    file: "src/main.rs".to_string(),
                    line: 42,
                },
                parent: None,
            }
        );
        assert!(decoder.finish().is_ok());
    }

    #[test]
    fn decoder_rejects_bad_and_truncated_frames() {
        let mut decoder = FrameDecoder::new();
        assert_eq!(
            decoder.push(&frame(&[99])).unwrap_err(),
            DecodeError::UnknownTag(99)
        );
        let mut decoder = FrameDecoder::new();
        let bytes = frame(&[16]);
        assert!(decoder.push(&bytes[..bytes.len() - 1]).unwrap().is_empty());
        assert_eq!(decoder.finish().unwrap_err(), DecodeError::Truncated);
        let mut decoder = FrameDecoder::new();
        assert_eq!(
            decoder.push(&(u32::MAX).to_le_bytes()).unwrap_err(),
            DecodeError::FrameTooLarge(u32::MAX)
        );
    }

    #[test]
    fn model_and_render_show_tasks_workers_resources_and_warnings() {
        let mut model = ConsoleModel::new();
        model.apply(Event::TaskSpawned {
            id: 7,
            name: Some("acceptor".to_string()),
            location: Location {
                file: "server.rs".to_string(),
                line: 12,
            },
            parent: None,
        });
        model.apply(Event::TaskPollStart {
            id: 7,
            worker: 2,
            at_ns: 1,
        });
        model.apply(Event::TaskPollEnd {
            id: 7,
            worker: 2,
            duration_ns: 150_000_000,
            result: PollResult::Pending,
        });
        model.apply(Event::BlockingDetected {
            task: 7,
            poll_duration_ns: 150_000_000,
            location: Location {
                file: "server.rs".to_string(),
                line: 12,
            },
        });
        model.apply(Event::QueueDepth {
            worker: 2,
            local: 3,
            global: 1,
            lifo: true,
        });
        model.apply(Event::WorkerPark {
            worker: 2,
            timeout_ns: None,
        });
        model.apply(Event::ResourceContended {
            kind: "mutex".to_string(),
            holder: 7,
            waiters: vec![8, 9],
        });

        assert_eq!(model.tasks[&7].state, TaskState::Idle);
        assert_eq!(model.workers[&2].local_queue, 3);
        assert_eq!(model.resources["mutex"].waiters, vec![8, 9]);

        let backend = TestBackend::new(120, 30);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| render(frame, &model)).unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("acceptor"));
        assert!(rendered.contains("blocking"));
        assert!(rendered.contains("Tasks"));
    }

    #[test]
    fn disconnected_model_keeps_the_last_view() {
        let mut model = ConsoleModel::new();
        model.apply(Event::BudgetExhausted { task: 4 });
        model.disconnected("runtime disconnected");
        assert!(!model.connected);
        assert_eq!(model.tasks[&4].warnings, vec!["budget"]);
        assert_eq!(
            model.disconnect_reason.as_deref(),
            Some("runtime disconnected")
        );
    }

    #[test]
    fn task_filter_matches_name_and_state_without_changing_the_event_model() {
        let mut model = ConsoleModel::new();
        for (id, name) in [(1, "acceptor"), (2, "worker")] {
            model.apply(Event::TaskSpawned {
                id,
                name: Some(name.to_string()),
                location: Location {
                    file: "test.rs".to_string(),
                    line: id as u32,
                },
                parent: None,
            });
        }
        model.tasks.get_mut(&2).unwrap().state = TaskState::Running;

        model.filter = "ACCEPT".to_string();
        assert_eq!(
            model
                .sorted_tasks()
                .iter()
                .map(|task| task.id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        model.filter = "running".to_string();
        assert_eq!(
            model
                .sorted_tasks()
                .iter()
                .map(|task| task.id)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(model.tasks.len(), 2);
    }

    #[test]
    fn async_operations_track_io_timers_and_resource_waiters() {
        let mut model = ConsoleModel::new();
        model.apply(Event::IoRegistered {
            fd: 5,
            interest: "read".to_string(),
            task: 1,
        });
        model.apply(Event::TimerSet {
            id: 8,
            deadline_ns: 123,
            task: 2,
        });
        model.apply(Event::ResourceContended {
            kind: "semaphore".to_string(),
            holder: 3,
            waiters: vec![4, 5],
        });

        assert_eq!(
            model.pending_operations,
            vec![
                PendingOperation {
                    task: 1,
                    resource: PendingResource::Io {
                        fd: 5,
                        interest: "read".to_string(),
                    },
                },
                PendingOperation {
                    task: 2,
                    resource: PendingResource::Timer {
                        id: 8,
                        deadline_ns: 123,
                    },
                },
                PendingOperation {
                    task: 4,
                    resource: PendingResource::Resource {
                        kind: "semaphore".to_string(),
                        holder: 3,
                    },
                },
                PendingOperation {
                    task: 5,
                    resource: PendingResource::Resource {
                        kind: "semaphore".to_string(),
                        holder: 3,
                    },
                },
            ]
        );

        model.view = View::AsyncOps;
        let backend = TestBackend::new(80, 12);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render_with_color(frame, &model, false))
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("Async Ops"));
        assert!(rendered.contains("semaphore"));

        model.apply(Event::IoReady {
            fd: 5,
            readiness: "readable".to_string(),
            woke: vec![1],
        });
        model.apply(Event::TimerFired {
            id: 8,
            lateness_ns: 0,
        });
        assert_eq!(model.pending_operations.len(), 2);
        assert!(model
            .pending_operations
            .iter()
            .all(|operation| matches!(operation.resource, PendingResource::Resource { .. })));
    }

    #[test]
    fn no_color_styles_do_not_set_terminal_colors() {
        assert_eq!(
            colored_style(false, Some(Color::Green), Some(Color::DarkGray)),
            Style::default()
        );
        assert_ne!(
            colored_style(true, Some(Color::Green), Some(Color::DarkGray)),
            Style::default()
        );
    }
}
