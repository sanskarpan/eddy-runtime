# SPEC.md — `eddy`: An Async Runtime from Scratch

> **Backend: Rust 2021 (MSRV 1.80+)** — executor, work-stealing scheduler, reactor (epoll/kqueue/IOCP/io_uring), timer wheel, async I/O types, sync primitives, blocking pool, instrumentation
> **Frontend: React 18 + TypeScript + Vite + Tailwind + shadcn/ui + D3 + Recharts** (web console) **and ratatui** (TUI, tokio-console-style)
> **Targets: Linux (epoll + io_uring), macOS/BSD (kqueue), Windows (IOCP)**

---

## §1 Language Decision — Rust, and there is genuinely no alternative

This is the only project in this series where the language choice is *forced by the problem statement itself*.

**`async`/`await` in Rust is a language feature with no runtime attached.** The standard library ships `Future`, `Poll`, `Context`, `Waker`, `RawWakerVTable`, and `Pin` — the entire *interface* — and then stops. There is no executor, no reactor, no timer, no I/O. That gap is exactly what Tokio, async-std, smol, glommio, and monoio fill.

No other mainstream language has this shape:

| Language | Async model | Can you build a runtime? |
|---|---|---|
| **Rust** | Poll-based futures, **runtime is a library** | ✅ This is the design. `std` defines the contract and leaves the implementation to you. |
| Go | Goroutines + runtime scheduler **in the compiler/runtime** | ❌ You cannot replace `go`'s M:N scheduler; it's baked into the toolchain |
| JavaScript | Event loop **in the engine** (libuv, V8 microtask queue) | ❌ You can write a scheduler *on top*, but the real loop is inaccessible |
| C# | `Task` + `SynchronizationContext`, runtime thread pool | ⚠️ You can write a custom `TaskScheduler`, but you're layering on a CLR pool you don't own |
| Python | `asyncio` event loop is replaceable (uvloop proves it) | ⚠️ Closest analogue, but you inherit the GIL and can't do work-stealing across cores |
| Zig | `async` was **removed** in 0.11 and is being redesigned | ⚠️ Interesting, but the target is moving |
| C++ | Coroutines TS: you *must* write the promise type and scheduler | ✅ Genuinely possible — and you'd spend the project debugging use-after-free in your task allocator |

The deciding factor is **`Pin`**. Rust is the only language that has confronted the self-referential-future problem head-on and produced a type-system answer. An async function compiles to a state machine that holds references *into itself* (`let x = ...; foo(&x).await;` stores both `x` and `&x`), which means it must never move after first poll. `Pin<&mut T>` encodes that. Implementing a runtime is the only way to actually understand why `Pin` exists — and no other language will teach you, because no other language has the problem.

### What you get to build that the language deliberately leaves empty

```rust
// std gives you exactly this and nothing more:
pub trait Future {
    type Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>;
}
pub struct Context<'a> { waker: &'a Waker, /* … */ }
pub struct Waker { waker: RawWaker }
pub struct RawWaker { data: *const (), vtable: &'static RawWakerVTable }
pub struct RawWakerVTable {
    clone: unsafe fn(*const ()) -> RawWaker,
    wake: unsafe fn(*const ()),
    wake_by_ref: unsafe fn(*const ()),
    drop: unsafe fn(*const ()),
}
```

Everything else — who calls `poll`, what a `Waker` actually does, where tasks live, how the OS tells you a socket is readable, how `sleep(3s)` works — **is your job**.

### Crates

**Deliberately minimal.** The whole point is to not use a runtime.

| Crate | Role |
|---|---|
| `libc` | `epoll_*`, `kqueue`, `eventfd`, `timerfd`, `socket`, `accept4`, `fcntl(O_NONBLOCK)` |
| `windows-sys` | IOCP: `CreateIoCompletionPort`, `GetQueuedCompletionStatusEx`, `WSARecv` |
| `io-uring` | **raw ring bindings only**, no runtime — for the completion-based backend |
| `crossbeam-utils` | `CachePadded`, `Backoff` — we write our own queues |
| `slab` | O(1) keyed storage for I/O registrations and timers |
| `parking_lot` | faster `Mutex`/`Condvar` for the injector and park/unpark |
| `pin-project-lite` | safe structural pinning in combinators (`Join`, `Select`, `Timeout`) |
| `tracing` | structured instrumentation feeding the console |
| `loom` | **concurrency model checker** — exhaustively explores interleavings of our lock-free queues |
| `criterion` | benchmarks against Tokio |

**Explicitly forbidden in the runtime crate:** `tokio`, `async-std`, `smol`, `futures` (the executor parts), `mio`, `polling`. We build all of it. `futures` is allowed *in tests only*, as a reference oracle for combinator semantics.

### Frontend: two consoles, both justified

An async runtime's core problem is that **you cannot see what it's doing**. Tasks multiplex onto threads, logs interleave, and a stalled system looks identical to an idle one. That's precisely why `tokio-console` exists.

- **ratatui TUI** (`eddy-console`) — the daily driver. htop-style task list, poll-time histograms, works over SSH. This is how you'd actually debug a server.
- **React web console** — for the things that genuinely need pixels and don't fit a terminal:
  - **Task lifecycle swimlanes** (D3) — every task as a horizontal lane, colored by state (idle/scheduled/running), across time. Makes starvation and long polls instantly visible.
  - **Worker/queue heatmap** (D3) — per-worker local queue depth over time, with steal events drawn as arrows between lanes. Work-stealing becomes a picture.
  - **Wake causality graph** (D3 + dagre) — "task 7 woke task 12 which woke task 3". This is the async equivalent of a call graph and nothing else shows it.
  - **Poll duration histograms** (Recharts) — p50/p99/max per task, with a red line at the "you are blocking the executor" threshold.

Both consume the same `tracing`-based event stream over a Unix socket, so instrumentation is written once.

---

## §2 What This Project Covers

| Area | Concepts |
|---|---|
| Futures | `Future`, `Poll`, laziness, the poll contract, cancellation-by-drop, fusing |
| `Pin` | Self-referential state machines, `Unpin`, structural pinning, why `Pin` exists at all |
| Waker | `RawWaker`, `RawWakerVTable`, manual vtable construction, `Arc` refcount discipline, `will_wake` |
| Task | Heap-allocated task cell, header/core/trailer layout, atomic state machine, refcounts, `JoinHandle` |
| Executor | Poll loop, ready queue, `block_on`, current-thread vs multi-thread |
| Scheduler | Work-stealing deque (Chase-Lev), LIFO slot, global injector, park/unpark, fairness budget |
| Reactor | epoll (level vs edge-triggered), kqueue, IOCP, readiness registration, `Slab`-keyed sources |
| io_uring | Completion vs readiness model, SQ/CQ rings, **buffer ownership and cancellation safety** |
| Timers | Hierarchical timing wheel (6 levels × 64 slots), O(1) insert/cancel/expire, `sleep`/`timeout`/`interval` |
| Async I/O | `AsyncRead`/`AsyncWrite`, `TcpListener`/`TcpStream`/`UdpSocket`, buffered adapters, `split` |
| Sync | `Mutex`, `RwLock`, `Semaphore`, `Notify`, `oneshot`, `mpsc`, `broadcast`, `watch` — all async, all fair |
| Combinators | `join!`, `try_join!`, `select!`, `race`, `timeout`, `FuturesUnordered` |
| Blocking | `spawn_blocking`, dynamically-sized thread pool, `block_in_place` |
| Cancellation | Drop-based cancellation, `AbortHandle`, `CancellationToken`, cancel-safety analysis |
| Fairness | Cooperative budget (yield after N ops), `yield_now`, starvation prevention |
| Observability | Per-task poll histograms, wake causality, queue depths, steal counts, blocking detection |
| Correctness | `loom` model checking, Miri, differential testing against Tokio |

---

## §3 Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │              User async code                  │
                    │   rt.block_on(async { ... spawn(...).await })  │
                    └──────────────────────┬───────────────────────┘
                                           │
        ┌──────────────────────────────────▼──────────────────────────────┐
        │                          Runtime                                 │
        │  ┌────────────────────────────────────────────────────────────┐ │
        │  │                     Scheduler                                │ │
        │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐    ┌─────────────┐ │ │
        │  │  │ Worker 0 │ │ Worker 1 │ │ Worker N │    │  Injector   │ │ │
        │  │  │ ┌──────┐ │ │ ┌──────┐ │ │ ┌──────┐ │    │  (global,   │ │ │
        │  │  │ │LIFO  │ │ │ │LIFO  │ │ │ │LIFO  │ │◄──►│   MPMC)     │ │ │
        │  │  │ ├──────┤ │ │ ├──────┤ │ │ ├──────┤ │    └─────────────┘ │ │
        │  │  │ │local │ │ │ │local │ │ │ │local │ │                    │ │
        │  │  │ │ deque│◄┼─┼─┤ deque│◄┼─┼─┤ deque│ │   ← work stealing  │ │
        │  │  │ │ (256)│ │ │ │ (256)│ │ │ │ (256)│ │                    │ │
        │  │  │ └──────┘ │ │ └──────┘ │ │ └──────┘ │                    │ │
        │  │  └────┬─────┘ └────┬─────┘ └────┬─────┘                    │ │
        │  └───────┼────────────┼────────────┼──────────────────────────┘ │
        │          │            │            │                             │
        │          └────────────┴────────────┘                             │
        │                       │  park() when no work                     │
        │  ┌────────────────────▼────────────────────────────────────────┐ │
        │  │                      Driver                                  │ │
        │  │  ┌──────────────────┐  ┌────────────────┐  ┌──────────────┐ │ │
        │  │  │   I/O Reactor    │  │  Timer Wheel   │  │  Blocking    │ │ │
        │  │  │ epoll / kqueue / │  │ 6 levels × 64  │  │  Pool        │ │ │
        │  │  │ IOCP / io_uring  │  │ O(1) all ops   │  │ (dynamic)    │ │ │
        │  │  │                  │  │                │  │              │ │ │
        │  │  │ Slab<ScheduledIo>│  │ Slab<TimerEntry│  │ VecDeque<Job>│ │ │
        │  │  └────────┬─────────┘  └───────┬────────┘  └──────┬───────┘ │ │
        │  └───────────┼────────────────────┼──────────────────┼─────────┘ │
        └──────────────┼────────────────────┼──────────────────┼───────────┘
                       │  waker.wake()      │                  │
                       ▼                    ▼                  ▼
                  Task pushed back onto a run queue
```

**The single most important structural fact:** the reactor does not "run tasks". It converts OS events into `Waker::wake()` calls, and `wake()` pushes a task onto a run queue. That indirection — event → waker → queue → poll — is the entire architecture, and every component exists to serve it.

---

## §4 Task Representation

A task is a single heap allocation containing everything: refcount, state, scheduler pointer, the future, and eventually the output.

```rust
// crates/eddy/src/task/raw.rs

/// Memory layout of one task allocation:
///
///   ┌────────────────────────────────────────────┐
///   │ Header                                      │
///   │   state: AtomicUsize   (bitflags + refcount)│
///   │   vtable: &'static Vtable                   │
///   │   owner_id: u32                             │
///   │   queue_next: UnsafeCell<Option<NonNull>>   │ ← intrusive list link
///   ├────────────────────────────────────────────┤
///   │ Core                                        │
///   │   scheduler: S                              │
///   │   stage: UnsafeCell<Stage<F>>               │
///   │     Running(F) | Finished(F::Output) | Consumed
///   ├────────────────────────────────────────────┤
///   │ Trailer                                     │
///   │   waker: UnsafeCell<Option<Waker>>          │ ← JoinHandle's waker
///   └────────────────────────────────────────────┘
///
/// THREE reasons for one allocation instead of Arc<Mutex<Box<dyn Future>>>:
///
///   1. ONE malloc per task instead of three. At a million tasks/sec this is
///      the dominant cost.
///   2. The `queue_next` link is INTRUSIVE — the global injector is a linked
///      list threaded through the tasks themselves, so pushing a task to it
///      allocates nothing.
///   3. The future is stored INLINE, so polling doesn't chase a pointer, and
///      `Stage` can be overwritten in place with the output — no separate
///      result allocation.
#[repr(C)]
pub(crate) struct Cell<F: Future, S> {
    pub header: Header,
    pub core: Core<F, S>,
    pub trailer: Trailer,
}

pub(crate) struct Header {
    /// Packed: [ refcount : 48 ][ flags : 16 ]
    ///
    /// Flags:
    ///   RUNNING   — a worker is polling right now
    ///   COMPLETE  — the future returned Ready
    ///   NOTIFIED  — a wake arrived; task is queued or will be
    ///   CANCELLED — abort requested
    ///   JOIN_WAKER_SET — the JoinHandle registered a waker
    ///
    /// Every state transition is a single CAS. The invariant that makes this
    /// sound: a task is polled by at most ONE thread at a time, enforced by
    /// the RUNNING bit, and `wake()` on an already-RUNNING task sets NOTIFIED
    /// so the poller re-queues it on the way out instead of dropping the wake.
    pub state: AtomicUsize,
    pub vtable: &'static Vtable,
    pub owner_id: u32,
    pub queue_next: UnsafeCell<Option<NonNull<Header>>>,
}

/// Manual vtable — this is what makes the task type-erased without a trait
/// object. `RawTask` is a bare NonNull<Header>; the vtable is reached through
/// the header, so a task pointer is one word, not two.
pub(crate) struct Vtable {
    pub poll: unsafe fn(NonNull<Header>),
    pub schedule: unsafe fn(NonNull<Header>),
    pub dealloc: unsafe fn(NonNull<Header>),
    pub try_read_output: unsafe fn(NonNull<Header>, *mut (), &Waker),
    pub drop_join_handle_slow: unsafe fn(NonNull<Header>),
    pub shutdown: unsafe fn(NonNull<Header>),
}
```

### Refcount discipline

```
A task holds up to three references:
  1. The scheduler (while queued or running)
  2. The JoinHandle (until dropped or awaited)
  3. Every live Waker cloned from it

Deallocation happens when the count hits zero. Getting this wrong gives you
either a leak (every task ever spawned stays alive) or a use-after-free (the
reactor wakes a freed task). Both are caught by the loom tests in §14.
```

---

## §5 The Waker

The single hardest piece of `unsafe` in the project, and the one `std` most conspicuously leaves to you.

```rust
// crates/eddy/src/task/waker.rs

/// A Waker is a fat pointer built by hand: a data pointer plus a vtable of
/// four function pointers.
///
/// Our data pointer is a `NonNull<Header>` — the task itself. Each vtable
/// function must maintain the task's refcount EXACTLY:
///
///   clone       → +1  (a new Waker exists)
///   wake        → consumes the reference (schedule, then -1)
///   wake_by_ref → does NOT consume (schedule, refcount unchanged)
///   drop        → -1
///
/// Getting `wake` vs `wake_by_ref` backwards is the classic bug: `wake`
/// takes `self` by value and must therefore *not* leave a reference behind,
/// while `wake_by_ref` takes `&self` and must. One leaks every task; the other
/// frees tasks that are still queued.
pub(crate) unsafe fn waker_from_raw(ptr: NonNull<Header>) -> Waker {
    Waker::from_raw(RawWaker::new(ptr.as_ptr() as *const (), &WAKER_VTABLE))
}

static WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref_waker, drop_waker);

unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    header.as_ref().state.ref_inc();               // +1
    RawWaker::new(ptr, &WAKER_VTABLE)
}

unsafe fn wake_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    // Consumes our reference: schedule() takes ownership of it.
    RawTask::from_raw(header).wake_by_val();
}

unsafe fn wake_by_ref_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    RawTask::from_raw(header).wake_by_ref();       // refcount unchanged
}

unsafe fn drop_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    if header.as_ref().state.ref_dec() {           // -1, true if it hit zero
        (header.as_ref().vtable.dealloc)(header);
    }
}
```

### The transition that everything depends on

```rust
/// wake() must handle the case where the task is CURRENTLY BEING POLLED.
///
/// Sequence that breaks a naive implementation:
///   1. Worker begins polling task T (sets RUNNING)
///   2. T's poll registers a waker with the reactor and is about to return Pending
///   3. Reactor thread fires and calls wake() on T
///   4. Naive wake sees "already running, nothing to do" and drops the wake
///   5. T returns Pending, is never re-queued, and hangs forever
///
/// The fix: wake() on a RUNNING task sets NOTIFIED. When the poller finishes
/// and sees NOTIFIED, it re-queues the task immediately instead of parking it.
/// This is a lost-wakeup bug and it is *the* canonical async runtime bug.
fn transition_to_notified(state: &AtomicUsize) -> TransitionToNotified {
    state.fetch_update(AcqRel, Acquire, |cur| {
        if cur & COMPLETE != 0 { return None; }              // already done
        if cur & NOTIFIED != 0 { return None; }              // already queued
        if cur & RUNNING != 0 {
            return Some(cur | NOTIFIED);                     // ← the critical case
        }
        Some(cur | NOTIFIED)                                 // needs scheduling
    })
    .map_or(TransitionToNotified::DoNothing, |prev| {
        if prev & RUNNING != 0 { TransitionToNotified::DoNothing }  // poller will re-queue
        else { TransitionToNotified::Submit }
    })
}
```

---

## §6 Scheduler

Two schedulers behind one `Handle` enum — matching Tokio's structure, because the two have genuinely different tradeoffs.

### 6.1 Current-thread scheduler

A `VecDeque<Task>` and a poll loop. No stealing, no atomics on the hot path, supports `!Send` futures. This is the one to build first — it's ~200 lines and proves the task/waker machinery works.

### 6.2 Multi-thread work-stealing scheduler

```rust
// crates/eddy/src/scheduler/multi_thread/worker.rs

pub(crate) struct Core {
    /// Poll counter. Every `GLOBAL_QUEUE_INTERVAL` ticks the worker checks the
    /// global queue *first*, even if its local queue is full. Without this,
    /// a worker with a self-replenishing local queue never drains the injector
    /// and externally-spawned tasks starve indefinitely.
    tick: u32,

    /// The LIFO slot: a single-task fast path.
    ///
    /// When task A wakes task B, B goes here instead of the queue tail, so B
    /// runs next on the same worker while A's data is still in L1/L2. On
    /// message-passing and request/response chains this is worth a large
    /// constant factor.
    ///
    /// The cost is fairness, so:
    ///   - the slot holds exactly ONE task
    ///   - it is not stealable
    ///   - MAX_LIFO_POLLS_PER_TICK (3) caps consecutive LIFO polls
    lifo_slot: Option<Notified>,
    lifo_polls: u8,

    /// Fixed 256-entry ring buffer. Owner pushes/pops one end; thieves steal
    /// from the other. Head is a packed atomic holding both a "real head" and
    /// a "steal head" in one word, so a steal in progress is visible atomically.
    run_queue: queue::Local,

    is_searching: bool,
    park: Option<Parker>,
    rand: FastRand,
}

const LOCAL_QUEUE_CAPACITY: usize = 256;
const GLOBAL_QUEUE_INTERVAL: u32 = 61;    // prime, to avoid resonance with other periods
const MAX_LIFO_POLLS_PER_TICK: u8 = 3;

impl Core {
    /// Task acquisition order. Every clause exists to fix a specific failure.
    fn next_task(&mut self, worker: &Worker) -> Option<Notified> {
        // Every 61 ticks: global FIRST. Fixes injector starvation.
        if self.tick % GLOBAL_QUEUE_INTERVAL == 0 {
            return worker.inject().pop().or_else(|| self.next_local_task());
        }
        self.next_local_task().or_else(|| worker.inject().pop())
    }

    fn next_local_task(&mut self) -> Option<Notified> {
        if self.lifo_polls < MAX_LIFO_POLLS_PER_TICK {
            if let Some(task) = self.lifo_slot.take() {
                self.lifo_polls += 1;
                return Some(task);
            }
        }
        self.lifo_polls = 0;
        self.run_queue.pop()
    }

    /// Steal from a RANDOM starting worker, taking HALF its queue.
    ///
    /// Random start prevents the convoy effect where every idle worker
    /// hammers worker 0. Half (not one) amortizes the atomic cost of stealing
    /// and gives the thief enough work to stay busy.
    fn steal_work(&mut self, worker: &Worker) -> Option<Notified> {
        let num = worker.shared.remotes.len();
        let start = self.rand.fastrand_n(num as u32) as usize;
        for i in 0..num {
            let i = (start + i) % num;
            if i == worker.index { continue; }
            if let Some(task) = worker.shared.remotes[i].steal.steal_into(&mut self.run_queue) {
                return Some(task);
            }
        }
        None
    }
}
```

### 6.3 Queue overflow

```rust
/// When the 256-slot local queue is full, move HALF of it to the injector —
/// and move the OLDER half, keeping the new task local.
///
/// This ordering is deliberate: a task pulled back off the front of the global
/// queue would otherwise land on the same overloaded worker and immediately
/// overflow again, producing a pathological ping-pong.
fn push_overflow(&mut self, task: Notified, inject: &Inject) {
    let half = LOCAL_QUEUE_CAPACITY / 2;
    let batch = self.run_queue.pop_n(half);        // oldest half
    inject.push_batch(batch);
    self.run_queue.push_back(task);                // newest stays hot
}
```

### 6.4 The Chase-Lev deque

```rust
/// Lock-free single-producer / multi-consumer ring buffer.
///
/// The head is a single AtomicU64 packing TWO 32-bit values:
///   [ steal_head : 32 ][ real_head : 32 ]
///
/// A thief CASes steal_head forward first, claiming a range, then copies
/// tasks out, then CASes real_head to match. The owner reads both: if they
/// differ, a steal is in flight and the owner must not touch that range.
///
/// Packing them in one word means the owner sees a consistent snapshot with a
/// single atomic load — with two separate atomics there is a window where the
/// owner observes a torn state and can hand the same task to two threads.
pub(crate) struct Local<T: 'static> {
    inner: Arc<Inner<T>>,
}

struct Inner<T: 'static> {
    head: AtomicU64,          // packed (steal, real)
    tail: AtomicU32,          // owner-only writes
    buffer: Box<[UnsafeCell<MaybeUninit<T>>; LOCAL_QUEUE_CAPACITY]>,
}
```

---

## §7 The Reactor

### 7.1 Readiness model (epoll / kqueue / IOCP)

```rust
// crates/eddy/src/io/driver.rs

pub(crate) struct Driver {
    poll: Poller,                        // platform-specific
    events: Events,
    /// Slab index IS the epoll token. O(1) event → registration lookup with
    /// no hashing, and the index is stable for the life of the registration.
    resources: Mutex<Slab<Arc<ScheduledIo>>>,
    waker_fd: WakerFd,                   // eventfd/pipe/kqueue-user to interrupt epoll_wait
}

/// Per-file-descriptor state, shared between the reactor and the task.
pub(crate) struct ScheduledIo {
    /// Packed: [ generation : 32 ][ readiness : 16 ][ shutdown : 1 ]
    ///
    /// The GENERATION counter is what makes fd reuse safe. Close fd 7, open a
    /// new socket that also gets fd 7, and a stale event for the old one can
    /// still be in flight. Comparing generations discards it.
    readiness: AtomicUsize,
    /// Separate waker lists for read and write. A socket can be
    /// read-waited by one task and write-waited by another simultaneously,
    /// and waking the wrong one is a hang.
    waiters: Mutex<Waiters>,
}

struct Waiters {
    reader: Option<Waker>,
    writer: Option<Waker>,
    list: LinkedList<Waiter>,   // for readiness futures beyond the fast path
}
```

**Level-triggered by default.** Edge-triggered is faster but requires draining the fd until `EWOULDBLOCK` on every wake, and a single missed drain is a permanent hang. Level-triggered is forgiving and the difference is small for typical workloads. Edge-triggered is available behind a flag, with the drain requirement documented and enforced by the `AsyncRead` impl.

### 7.2 The `epoll_wait` timeout comes from the timer wheel

```rust
/// The runtime blocks in exactly one place: epoll_wait. Its timeout must be
/// the next timer deadline, or timers fire late.
///
/// Three cases:
///   - tasks are ready       → timeout 0 (poll and return immediately)
///   - a timer is pending    → timeout = deadline - now
///   - nothing at all        → block indefinitely until an event or the waker fd
///
/// This is the coupling point between the I/O driver and the timer driver, and
/// it's why they live behind one `Driver` rather than being independent.
fn park(&mut self, timers: &TimerWheel, has_ready_tasks: bool) {
    let timeout = if has_ready_tasks {
        Some(Duration::ZERO)
    } else {
        timers.next_expiration().map(|d| d.saturating_duration_since(Instant::now()))
    };
    self.poll.wait(&mut self.events, timeout);
    self.dispatch_events();
    timers.advance_to(Instant::now());
}
```

### 7.3 io_uring — a genuinely different model

```rust
// crates/eddy/src/io/uring.rs

/// io_uring is COMPLETION-based, not readiness-based, and that difference is
/// not cosmetic — it breaks a core assumption of Rust's future model.
///
/// THE PROBLEM:
///   Rust futures are cancelled by dropping them. With epoll that's free: you
///   just stop caring that the fd is readable. With io_uring you have already
///   handed the kernel a pointer to your buffer, and the kernel WILL write
///   into it whenever it gets around to it — including after your future has
///   been dropped and the buffer freed.
///
///   Dropping a future with an in-flight io_uring read is a use-after-free
///   that the borrow checker cannot see, because the kernel isn't a Rust
///   reference.
///
/// THREE POSSIBLE FIXES, and why we implement the third:
///
///   1. Copy into a runtime-owned buffer, then memcpy out. Sound, keeps the
///      familiar AsyncRead API, costs one extra copy per operation — which
///      partly defeats the point of using io_uring.
///
///   2. Leak the buffer on cancellation until the CQE arrives. Sound but
///      unbounded: a cancel-heavy workload leaks without limit.
///
///   3. OWNED BUFFERS: the API takes the buffer by value and gives it back
///      on completion. `read_at(buf: Vec<u8>) -> (Result<usize>, Vec<u8>)`.
///      Cancellation transfers ownership to the driver, which holds the buffer
///      until the CQE arrives and then drops it. Zero copies, sound, and the
///      API change is honest about what's actually happening.
///
/// This is why tokio-uring, monoio, and compio all have a different I/O trait
/// from Tokio. It is not gratuitous — it's forced by the model.
pub trait AsyncReadOwned {
    fn read(&self, buf: Vec<u8>) -> impl Future<Output = (io::Result<usize>, Vec<u8>)>;
}

pub(crate) struct UringDriver {
    ring: IoUring,
    /// Operations whose future was dropped before completion. The buffer lives
    /// here until the kernel reports the CQE, then it is dropped. This bounds
    /// the leak to "in-flight cancelled ops" instead of "forever".
    orphaned: Slab<OrphanedOp>,
    ops: Slab<OpState>,
}
```

---

## §8 The Timer Wheel

```rust
// crates/eddy/src/time/wheel.rs

/// Hierarchical hashed timing wheel (Varghese & Lauck, 1987).
///
///   Level 0: 64 slots × 1 ms      → 64 ms range
///   Level 1: 64 slots × 64 ms     → ~4 s
///   Level 2: 64 slots × 4 s       → ~4 min
///   Level 3: 64 slots × 4 min     → ~4.5 hr
///   Level 4: 64 slots × 4.5 hr    → ~12 days
///   Level 5: 64 slots × 12 days   → ~2 years
///
/// WHY NOT A BINARY HEAP:
///   A heap gives O(log n) insert and O(log n) cancel. The wheel gives O(1)
///   for both. That sounds marginal until you consider the actual workload: a
///   server with 100k connections sets a timeout on EVERY request and cancels
///   almost all of them when the response arrives first. The dominant
///   operation is insert-then-cancel, at request rate. O(1) vs O(log n) on
///   100k entries is a real, measurable difference.
///
/// 64 slots per level is not arbitrary: it makes slot indexing a shift and a
/// mask rather than a division, and one level's occupancy fits in a u64
/// bitmap so "which slot is next non-empty" is a single trailing_zeros().
pub(crate) struct Wheel {
    elapsed: u64,                  // ms since wheel creation
    levels: Box<[Level; 6]>,
    pending: LinkedList<TimerEntry>,   // due now, awaiting fire
}

struct Level {
    /// Bitmap of occupied slots. `occupied.trailing_zeros()` finds the next
    /// non-empty slot in one instruction instead of scanning 64 lists.
    occupied: u64,
    slots: [LinkedList<TimerEntry>; 64],
}

impl Wheel {
    fn level_for(&self, when: u64) -> usize {
        const SLOT_MASK: u64 = (1 << 6) - 1;
        // XOR the current time with the deadline: the highest differing bit
        // tells you which level's granularity the difference falls into.
        let masked = (when ^ self.elapsed) | SLOT_MASK;
        let leading_zeros = masked.leading_zeros() as usize;
        let significant = 63 - leading_zeros;
        significant / 6
    }

    /// Advancing: fire everything in level 0, then CASCADE — when level 0
    /// wraps, redistribute level 1's current slot down into level 0.
    ///
    /// Cascading is amortized O(1): each timer is moved down at most 6 times
    /// over its entire lifetime, once per level.
    pub fn advance_to(&mut self, now: u64) -> Vec<TimerEntry> {
        let mut fired = Vec::new();
        while self.elapsed < now {
            match self.next_expiration() {
                Some(exp) if exp.deadline <= now => {
                    self.elapsed = exp.deadline;
                    if exp.level == 0 {
                        fired.extend(self.take_slot(0, exp.slot));
                    } else {
                        // Cascade down one level
                        for entry in self.take_slot(exp.level, exp.slot) {
                            self.insert(entry);
                        }
                    }
                }
                _ => { self.elapsed = now; break; }
            }
        }
        fired
    }
}
```

---

## §9 Async I/O Types

```rust
pub trait AsyncRead {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>)
        -> Poll<io::Result<()>>;
}

pub trait AsyncWrite {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8])
        -> Poll<io::Result<usize>>;
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

/// ReadBuf tracks THREE regions: filled, initialized, and the rest.
///
/// The `initialized` distinction exists to avoid zeroing buffers you're about
/// to overwrite, without ever exposing uninitialized memory to safe code. It's
/// a small API but it is where a naive `&mut [u8]` design leaks UB.
pub struct ReadBuf<'a> {
    buf: &'a mut [MaybeUninit<u8>],
    filled: usize,
    initialized: usize,
}
```

Types: `TcpListener`, `TcpStream`, `UdpSocket`, `UnixListener`, `UnixStream`, plus `BufReader`/`BufWriter` adapters, `split()`/`into_split()`, and `copy`/`copy_bidirectional`.

All sockets are `O_NONBLOCK` from creation (`accept4(SOCK_NONBLOCK)` where available), and every operation follows:

```rust
loop {
    let ev = self.io.readiness(Interest::READABLE).await?;   // wait for readiness
    match self.io.try_io(|| syscall()) {
        Ok(n) => return Ok(n),
        Err(e) if e.kind() == WouldBlock => {
            ev.clear_ready();                                  // spurious; re-arm
            continue;
        }
        Err(e) => return Err(e),
    }
}
```

**The `clear_ready()` step is essential.** epoll can report readiness spuriously; without clearing and re-waiting, the task spins.

---

## §10 Synchronization Primitives

All async, all with an intrusive waiter list, all **fair (FIFO)**.

| Primitive | Notes |
|---|---|
| `Mutex<T>` | `lock().await` returns a guard. Guard is `Send` if `T: Send`. Intrusive FIFO wait list. |
| `RwLock<T>` | Write-preferring, to prevent writer starvation under sustained read load. |
| `Semaphore` | The building block — `Mutex` is a 1-permit semaphore. Supports multi-permit acquire. |
| `Notify` | `notified().await` / `notify_one()` / `notify_waiters()`. **Stores one permit** so a notify that arrives before the wait isn't lost. |
| `oneshot` | Single value. Sender half detects receiver drop and vice versa. |
| `mpsc` | Bounded (with backpressure) and unbounded. `send().await` on a full bounded channel is a proper await, not a spin. |
| `broadcast` | Ring buffer with per-receiver cursor; slow receivers get `RecvError::Lagged(n)` rather than stalling the sender. |
| `watch` | Single latest value, `changed().await`, seen/unseen versioning. |

```rust
/// The intrusive waiter pattern used by all of them.
///
/// The Waiter node lives INSIDE the future, on the caller's stack (or inside
/// the task allocation), and is linked into the primitive's list. Zero
/// allocation per wait — critical when a Mutex is contended by thousands of
/// tasks.
///
/// This requires `Pin`: the node is self-referential once linked (the list
/// holds a pointer to it), so the future must not move. This is the clearest
/// real-world demonstration of WHY Pin exists, and worth building a
/// documentation example around.
struct Waiter {
    pointers: linked_list::Pointers<Waiter>,
    waker: UnsafeCell<Option<Waker>>,
    state: AtomicUsize,
    _pin: PhantomPinned,          // ← makes the future !Unpin
}
```

---

## §11 Combinators & Cancellation

```rust
join!(a, b, c)          // all, concurrently, on one task
try_join!(a, b, c)      // all, short-circuit on first Err
select!{ ... }          // first to complete; others DROPPED (cancelled)
race(a, b)              // like select but same-typed
timeout(dur, fut)       // Err(Elapsed) if the future doesn't finish
FuturesUnordered        // dynamic set, polls only woken members
```

### Cancel safety — the most under-documented concept in async Rust

```rust
/// `select!` DROPS the losing futures. If a future is not "cancel safe",
/// dropping it mid-operation loses data.
///
/// NOT cancel safe — this can lose bytes:
///     select! {
///         _ = stream.read(&mut buf) => {}   // may have read into buf, then dropped
///         _ = other => {}
///     }
///
/// Cancel safe — the message stays in the channel:
///     select! {
///         msg = rx.recv() => {}             // recv() is cancel safe by design
///         _ = other => {}
///     }
///
/// Every future we ship documents its cancel safety. This isn't optional
/// polish — silently losing a message on cancellation is a data-loss bug that
/// shows up only under load, and the only defense is documenting it and
/// testing for it (§14.4).
```

Plus `AbortHandle`, `JoinHandle::abort()`, and a `CancellationToken` for structured cancellation trees.

---

## §12 Blocking Pool

```rust
/// The problem: `poll` must not block. A blocking call inside a task stalls
/// its worker thread, and with N workers, N blocking tasks deadlock the
/// entire runtime.
///
/// spawn_blocking moves the work to a separate, dynamically-sized pool that is
/// allowed to block. The pool grows to max_blocking_threads (default 512) and
/// idle threads exit after keep_alive (default 10s).
///
/// 512 is deliberately large: these threads are expected to be blocked on I/O,
/// not consuming CPU, so the usual "threads ≈ cores" rule doesn't apply.
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where F: FnOnce() -> R + Send + 'static, R: Send + 'static;

/// block_in_place: for the multi-thread runtime only. Converts the CURRENT
/// worker into a blocking thread and hands its queue to a replacement, so
/// blocking work can borrow from the surrounding scope without `'static`.
pub fn block_in_place<F, R>(f: F) -> R where F: FnOnce() -> R;
```

### Blocking detection

Instrumentation records every poll duration. A poll exceeding a threshold (default 100 ms) emits a `blocking_detected` event naming the task and its spawn location. **This alone justifies the console** — "why is my server slow" is almost always "someone called a sync function in an async fn", and nothing else surfaces it.

---

## §13 Fairness & the Cooperative Budget

```rust
/// A task that always returns Ready never yields, and starves its worker.
///
///     loop { let msg = rx.recv().await; }     // always-ready channel
///
/// Every resource operation consumes from a per-task budget (128 ops). When
/// the budget is exhausted, the operation returns Pending and wakes itself
/// immediately, forcing a trip through the scheduler so other tasks run.
///
/// This is invisible to users and is why a busy channel doesn't hang a
/// Tokio server. It's also the thing people are most surprised to learn
/// exists, which makes it a good console visualization: show budget
/// consumption per task and mark forced yields.
const BUDGET: u8 = 128;

pub(crate) fn poll_proceed(cx: &mut Context<'_>) -> Poll<RestoreOnPending> {
    CURRENT.with(|cell| {
        let mut budget = cell.get();
        match budget.0 {
            Some(0) => {
                cx.waker().wake_by_ref();        // re-queue at the back
                Poll::Pending
            }
            Some(n) => { cell.set(Budget(Some(n - 1))); Poll::Ready(/* … */) }
            None => Poll::Ready(/* … */),        // unconstrained (e.g. inside block_on)
        }
    })
}
```

Plus `yield_now()` for explicit cooperation, and `unconstrained(fut)` to opt out.

---

## §14 Correctness

An async runtime is a lock-free concurrent data structure with a public API. Testing it requires more than unit tests.

### 14.1 `loom` — exhaustive interleaving exploration

```rust
/// loom replaces atomics, Mutex, and threads with instrumented versions and
/// EXHAUSTIVELY explores every valid interleaving permitted by the C11 memory
/// model — including the weak-memory reorderings that only show up on ARM.
///
/// A `#[test]` that passes a million times on x86 can still be broken. loom
/// either proves the model has no interleaving that breaks the invariant, or
/// hands you the exact schedule that does.
///
/// This is not optional for this project. The task state machine, the
/// Chase-Lev deque, and the intrusive waiter lists are all places where a
/// missed interleaving is a use-after-free.
#[test]
fn loom_task_wake_during_poll() {
    loom::model(|| {
        let task = task::spawn(async { /* … */ });
        let waker = task.waker();

        let th = loom::thread::spawn(move || waker.wake());   // reactor thread
        task.poll();                                          // worker thread
        th.join().unwrap();

        // The invariant: the task is either completed, or queued for another
        // poll. It must NEVER be dropped-and-forgotten (lost wakeup) or
        // queued twice (double-free).
        assert!(task.is_complete() || task.is_queued());
    });
}
```

Loom models required for: task state transitions, refcount/dealloc, Chase-Lev push/pop/steal, injector push/pop, `Notify` permit, `oneshot` send/recv/drop, semaphore acquire/release.

### 14.2 Miri

Every `unsafe` path — the task allocation, waker vtable, intrusive lists, `ReadBuf` — under Miri with stacked borrows. Miri catches the pointer-provenance errors loom doesn't.

### 14.3 Differential testing against Tokio

```rust
/// Identical workloads run on eddy and Tokio must produce identical
/// observable behavior — same results, same ordering guarantees where they
/// exist. Where behavior legitimately differs (scheduling order), assert only
/// the guaranteed properties.
```

### 14.4 Cancel-safety property tests

```rust
/// For every future we ship: drop it at a random poll count and assert no
/// data is lost. For channels specifically: cancel `recv()` mid-flight and
/// assert the message is still there for the next receiver.
```

### 14.5 Stress and deadlock detection

10-minute soak runs at max concurrency, with a watchdog that dumps all task states if no progress occurs for 30 seconds.

---

## §15 Observability

The runtime emits `tracing` events over a Unix socket. Both consoles consume the same stream.

```rust
pub enum RuntimeEvent {
    TaskSpawned  { id: TaskId, name: Option<String>, location: Location, parent: Option<TaskId> },
    TaskPollStart{ id: TaskId, worker: u32, at: Instant },
    TaskPollEnd  { id: TaskId, worker: u32, duration: Duration, result: PollResult },
    TaskWoken    { id: TaskId, by: WakeSource },     // ← the causality edge
    TaskDropped  { id: TaskId, total_polls: u64, total_busy: Duration, total_idle: Duration },
    TaskAborted  { id: TaskId },

    WorkerPark   { worker: u32, timeout: Option<Duration> },
    WorkerUnpark { worker: u32, reason: UnparkReason },
    WorkerSteal  { thief: u32, victim: u32, count: usize },
    QueueDepth   { worker: u32, local: usize, global: usize, lifo: bool },

    IoRegistered { fd: RawFd, interest: Interest, task: TaskId },
    IoReady      { fd: RawFd, readiness: Ready, woke: Vec<TaskId> },

    TimerSet     { id: TimerId, deadline: Instant, task: TaskId },
    TimerFired   { id: TimerId, lateness: Duration },   // ← how late? measures wheel accuracy
    TimerCancelled { id: TimerId },

    BlockingDetected { task: TaskId, poll_duration: Duration, location: Location },
    BudgetExhausted  { task: TaskId },
    ResourceContended{ kind: ResourceKind, holder: TaskId, waiters: Vec<TaskId> },
}

pub enum WakeSource {
    Io { fd: RawFd },
    Timer { id: TimerId },
    Task(TaskId),           // ← task-to-task causality
    Channel { kind: &'static str },
    External,               // from a non-runtime thread
}
```

### TUI (`eddy-console`)

ratatui. Task list with `Total / Busy / Idle / Polls / Sched` columns, sortable. Enter on a task shows poll-duration and scheduled-duration histograms. Warning lamps for blocking-detected and never-yielded tasks. Resource view showing mutex holders and waiter queues.

### Web console — four views the terminal can't do

**View 1 · Task Lifecycle Swimlanes ⭐**
D3. X = time, one lane per task, segments colored: gray idle, yellow scheduled-but-not-running, green running. Starvation is *visually obvious* — a lane that's solid yellow for 200 ms was ready and ignored. Hovering a segment shows the poll duration; clicking jumps to the spawn location.

**View 2 · Worker & Queue Heatmap ⭐**
D3. One row per worker, X = time, cell color = local queue depth. Steal events drawn as arrows from victim row to thief row. A saturated worker next to idle ones is immediately visible, as is the moment stealing kicks in. Global queue depth as a separate strip.

**View 3 · Wake Causality Graph ⭐**
D3 + dagre. Nodes = tasks, edges = "woke". This is the async equivalent of a call graph and nothing else produces it: it shows that task 7 (the acceptor) woke task 12 (a connection handler) which woke task 3 (a database pool), tracing a request's actual path through the concurrency structure. Cycles are highlighted — a wake cycle is usually a busy-loop bug.

**View 4 · Poll Duration Distribution**
Recharts. Per-task histograms with p50/p99/max, and a red threshold line at 100 ms labeled "blocking the executor". Sorting by p99 immediately identifies the one handler that's calling a sync function.

Stack: `react` · `vite` · `typescript` · `tailwindcss` · `shadcn/ui` · `d3` · `@dagrejs/dagre` · `recharts` · `zustand` (event stream state) · WebSocket bridge from the Unix socket.

---

## §16 Public API

```rust
// Runtime construction
let rt = Runtime::new()?;                                    // multi-thread, cores workers
let rt = Builder::new_multi_thread()
    .worker_threads(4)
    .max_blocking_threads(256)
    .thread_name("eddy-worker")
    .enable_io().enable_time()
    .global_queue_interval(61)
    .disable_lifo_slot()
    .on_thread_park(|| { /* … */ })
    .build()?;
let rt = Builder::new_current_thread().enable_all().build()?;

rt.block_on(async { /* … */ });
let handle = rt.spawn(async { /* … */ });     // JoinHandle<T>
handle.abort();
let out = handle.await?;                       // Result<T, JoinError>

// Ambient handle
let h = Handle::current();
h.spawn(fut);
h.spawn_blocking(|| { /* … */ });
h.block_on(fut);

// Time
sleep(Duration::from_secs(1)).await;
sleep_until(instant).await;
timeout(Duration::from_secs(5), fut).await?;
let mut ticker = interval(Duration::from_millis(100));
ticker.tick().await;

// Net
let listener = TcpListener::bind("0.0.0.0:8080").await?;
let (stream, addr) = listener.accept().await?;

// Macros
#[eddy::main] async fn main() { }
#[eddy::test] async fn my_test() { }
select! { … }  join!(…)  try_join!(…)
```

Deliberately **Tokio-compatible in shape**, so the differential tests are meaningful and so a reader can transfer what they learn.

---

## §17 File Structure

```
eddy/
├── crates/
│   ├── eddy/
│   │   └── src/
│   │       ├── task/          # raw.rs, header.rs, state.rs, waker.rs, join.rs, harness.rs
│   │       ├── scheduler/
│   │       │   ├── current_thread/
│   │       │   └── multi_thread/  # worker.rs, queue.rs (Chase-Lev), inject.rs, idle.rs, park.rs
│   │       ├── io/            # driver.rs, scheduled_io.rs, registration.rs, poll_evented.rs
│   │       │   ├── sys/       # epoll.rs, kqueue.rs, iocp.rs
│   │       │   └── uring.rs   # completion-based backend
│   │       ├── time/          # wheel.rs, level.rs, entry.rs, sleep.rs, timeout.rs, interval.rs
│   │       ├── net/           # tcp/, udp/, unix/
│   │       ├── sync/          # mutex.rs, rwlock.rs, semaphore.rs, notify.rs,
│   │       │                  # oneshot.rs, mpsc/, broadcast.rs, watch.rs, batch_semaphore.rs
│   │       ├── future/        # join.rs, select.rs, timeout.rs, futures_unordered.rs, poll_fn.rs
│   │       ├── blocking/      # pool.rs, schedule.rs
│   │       ├── coop.rs        # cooperative budget
│   │       ├── util/          # linked_list.rs, slab.rs, rand.rs, wake_list.rs
│   │       └── trace/         # events.rs, subscriber.rs, socket.rs
│   ├── eddy-macros/          # #[eddy::main], #[eddy::test], select!
│   ├── eddy-console/         # ratatui TUI
│   └── eddy-console-web/     # WebSocket bridge for the React app
├── console-ui/                # React app
├── tests/
│   ├── loom/                  # model checking
│   ├── differential/          # vs Tokio
│   ├── cancel_safety/
│   └── stress/
├── benches/                   # vs Tokio: spawn, ping-pong, echo server, timer churn
└── examples/                  # echo, chat, http, proxy, graceful shutdown
```

---

## §18 Correctness Properties

1. **No lost wakeups.** A `wake()` that races with an in-progress poll always results in another poll. Verified by loom.
2. **No double-schedule.** A task is never in two queues, never polled by two threads. Verified by loom.
3. **No leaks.** Every spawned task is eventually deallocated: completed, aborted, or dropped at shutdown. Verified by allocation counting.
4. **No use-after-free.** Verified by Miri over all `unsafe` paths.
5. **Work conservation.** If any task is ready and any worker is idle, that worker acquires work within a bounded number of steal attempts.
6. **Injector liveness.** A task in the global queue is picked up within `GLOBAL_QUEUE_INTERVAL` polls of any worker.
7. **Fairness.** No ready task waits indefinitely while others run. Enforced by the budget and the global-queue interval.
8. **Timer accuracy.** A timer fires no earlier than its deadline, and no later than deadline + one tick + scheduling latency.
9. **Timer O(1).** Insert, cancel, and expire are constant-time regardless of the number of pending timers.
10. **Cancel safety.** Every documented cancel-safe future loses no data when dropped mid-poll. Property-tested.
11. **`Pin` soundness.** No `!Unpin` future is ever moved after first poll.
12. **Shutdown completeness.** `Runtime::drop` cancels all tasks, joins all threads, and closes all fds. No thread outlives the runtime.
13. **io_uring soundness.** No buffer handed to the kernel is freed before its CQE arrives, including on cancellation.

---

## §19 Performance Targets

Measured against Tokio on the same hardware. **Within 2× is a success** — Tokio has had seven years of tuning, and the goal is to understand the design, not to win.

| Benchmark | Tokio | eddy target |
|---|---|---|
| `spawn` + join, 1M tasks | ~1.2 s | < 2.4 s |
| Task allocation size | 88 B | < 128 B |
| Ping-pong (2 tasks, 1M round trips) | ~0.9 s | < 1.8 s |
| Echo server, 10k conns, 64 B msgs | ~1.1 M req/s | > 550 k req/s |
| `sleep(1ms)` × 100k concurrent | ~130 ms | < 260 ms |
| Timer insert/cancel | ~40 ns | < 80 ns |
| `mpsc` send/recv, bounded(100) | ~55 ns | < 110 ns |
| `Mutex` uncontended lock/unlock | ~25 ns | < 50 ns |
| Work-steal latency (idle → task) | ~2 µs | < 5 µs |
| Poll overhead (empty future) | ~15 ns | < 30 ns |
| io_uring echo (Linux 6.x) | tokio-uring ~1.6 M/s | > 1.2 M/s |
| Instrumentation overhead (enabled) | — | < 5 % |
