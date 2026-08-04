# CLAUDE CODE PROMPT — `eddy`: An Async Runtime from Scratch

## Project Mission

Build a Tokio-class async runtime from scratch:

- **Backend: Rust** — hand-built task allocation with a manual vtable, `RawWakerVTable` by hand, current-thread and multi-thread work-stealing schedulers (Chase-Lev deque, LIFO slot, global injector), reactor over epoll/kqueue/IOCP plus a completion-based io_uring backend, hierarchical timing wheel, async I/O types, a full set of async sync primitives, blocking pool, cooperative budget, and `tracing`-based instrumentation
- **Frontend: ratatui TUI + React/TypeScript/Vite/Tailwind/shadcn/D3/Recharts web console** — task lifecycle swimlanes, worker/queue heatmap, wake causality graph, poll-duration histograms

**Read `runtime-SPEC.md` and `runtime-CHECKLIST.md` before writing any code.**

### Four rules that override everything

1. **No async runtime dependencies.** `tokio`, `async-std`, `smol`, `mio`, `polling`, `futures-executor` are forbidden in `eddy`. They are allowed *in tests only*, as oracles. Add a CI check on `cargo tree` — this rule will otherwise erode.

2. **Build Phases 1–3 first: task, waker, current-thread executor.** ~800 lines that let you `block_on(async { 42 })`. Everything else is elaboration. If the waker refcounting is wrong, no amount of scheduler work will make the system correct.

3. **Every lock-free structure gets a `loom` test in the same commit.** A `#[test]` that passes a million times on x86 proves nothing — x86's TSO hides reorderings that ARM exposes. loom exhaustively explores every interleaving the C11 model permits. This is not optional for a project whose bugs are use-after-free.

4. **Run CI on ARM64.** Weak memory ordering surfaces bugs that x86 will never show you.

---

## Phase 0 — The Day One Spike

```bash
cargo new --lib eddy && cd eddy
cargo add libc slab parking_lot pin-project-lite tracing crossbeam-utils
cargo add --dev loom criterion proptest futures tokio
```

```rust
// examples/spike.rs — build a Waker by hand, poll a future to completion.
// No dependencies. If this is wrong, everything above it is unfixable.
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

struct Shared { woken: AtomicBool }

unsafe fn clone(p: *const ()) -> RawWaker {
    Arc::increment_strong_count(p as *const Shared);   // +1
    RawWaker::new(p, &VTABLE)
}
unsafe fn wake(p: *const ()) {
    let arc = Arc::from_raw(p as *const Shared);       // takes ownership
    arc.woken.store(true, Ordering::Release);
    // arc dropped here → -1, which is correct: wake() CONSUMES the waker
}
unsafe fn wake_by_ref(p: *const ()) {
    let arc = std::mem::ManuallyDrop::new(Arc::from_raw(p as *const Shared));
    arc.woken.store(true, Ordering::Release);
    // ManuallyDrop → refcount unchanged, which is correct: wake_by_ref BORROWS
}
unsafe fn drop_it(p: *const ()) { Arc::decrement_strong_count(p as *const Shared); }

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_it);

fn main() {
    let shared = Arc::new(Shared { woken: AtomicBool::new(false) });
    let waker = unsafe {
        Waker::from_raw(RawWaker::new(Arc::into_raw(shared.clone()) as *const (), &VTABLE))
    };
    let mut cx = Context::from_waker(&waker);
    let mut fut = Box::pin(async { 42 });
    assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(42));
    println!("waker + poll works");
}
```

---

## Phase 1 — The Task Allocation

```rust
// crates/eddy/src/task/raw.rs

/// ONE heap allocation per task, holding everything.
///
/// The obvious design — Arc<Mutex<Pin<Box<dyn Future>>>> — costs three
/// allocations and a lock per poll. This costs one allocation and no lock,
/// for three reasons:
///
///   1. One malloc instead of three. At a million spawns/sec this dominates.
///
///   2. `queue_next` is an INTRUSIVE list link. The global injector is a
///      linked list threaded through the tasks themselves, so pushing a task
///      onto it allocates nothing. A Vec-based queue would allocate on every
///      push under contention.
///
///   3. The future is stored INLINE. Polling doesn't chase a pointer, and when
///      it completes, `Stage` is overwritten in place with the output — no
///      separate result allocation.
#[repr(C)]
pub(crate) struct Cell<F: Future, S> {
    pub header: Header,
    pub core: Core<F, S>,
    pub trailer: Trailer,
}

pub(crate) struct Header {
    /// Packed: [ refcount : 48 ][ flags : 16 ]
    pub state: AtomicUsize,
    /// Manual vtable. This is what type-erases the task WITHOUT a trait
    /// object: `RawTask` is a bare NonNull<Header>, one word, and the vtable
    /// is reached through the header. A `Box<dyn Future>` would be two words
    /// and couldn't be stored in an intrusive list.
    pub vtable: &'static Vtable,
    pub owner_id: u32,
    /// Intrusive link for the injector.
    pub queue_next: UnsafeCell<Option<NonNull<Header>>>,
}

pub(crate) enum Stage<F: Future> {
    Running(F),
    Finished(super::Result<F::Output>),
    Consumed,
}

/// Built once per (F, S) pair as a const.
pub(crate) fn vtable<F: Future, S: Schedule>() -> &'static Vtable {
    &Vtable {
        poll: poll::<F, S>,
        schedule: schedule::<F, S>,
        dealloc: dealloc::<F, S>,
        try_read_output: try_read_output::<F, S>,
        drop_join_handle_slow: drop_join_handle_slow::<F, S>,
        shutdown: shutdown::<F, S>,
    }
}
```

### The state machine — one CAS per transition

```rust
// crates/eddy/src/task/state.rs

const RUNNING:        usize = 0b0001;   // a worker is polling right now
const COMPLETE:       usize = 0b0010;   // future returned Ready
const NOTIFIED:       usize = 0b0100;   // a wake arrived; queued or will be
const CANCELLED:      usize = 0b1000;
const JOIN_INTEREST:  usize = 0b0001_0000;
const JOIN_WAKER_SET: usize = 0b0010_0000;
const REF_ONE:        usize = 1 << 16;  // refcount lives in the high bits

impl State {
    /// THE most important transition in the entire runtime.
    ///
    /// The bug this prevents (and it is *the* canonical async runtime bug):
    ///
    ///   1. Worker begins polling task T, sets RUNNING
    ///   2. T's poll registers a waker with the reactor, about to return Pending
    ///   3. Reactor thread fires and calls wake() on T
    ///   4. Naive wake sees RUNNING and thinks "it's already being polled,
    ///      nothing to do" — and DROPS the wake
    ///   5. T returns Pending, is parked, and hangs forever
    ///
    /// The fix: wake() on a RUNNING task sets NOTIFIED. When the poller
    /// finishes and sees NOTIFIED, it re-queues immediately instead of parking.
    /// The wake is deferred, not lost.
    pub(super) fn transition_to_notified_by_ref(&self) -> TransitionToNotifiedByRef {
        self.fetch_update_action(|mut snapshot| {
            if snapshot.is_complete() || snapshot.is_notified() {
                // Already done, or already queued — nothing to do.
                (TransitionToNotifiedByRef::DoNothing, None)
            } else if snapshot.is_running() {
                // ← THE CRITICAL CASE. Mark it; the poller will re-queue.
                snapshot.set_notified();
                (TransitionToNotifiedByRef::DoNothing, Some(snapshot))
            } else {
                // Idle: mark notified, take a ref for the queue, submit.
                snapshot.set_notified();
                snapshot.ref_inc();
                (TransitionToNotifiedByRef::Submit, Some(snapshot))
            }
        })
    }

    /// The other half: when a poll returns Pending, check whether a wake
    /// arrived while we were running.
    pub(super) fn transition_to_idle(&self) -> TransitionToIdle {
        self.fetch_update_action(|mut snapshot| {
            assert!(snapshot.is_running());
            if snapshot.is_cancelled() {
                return (TransitionToIdle::Cancelled, None);
            }
            let mut next = snapshot;
            next.unset_running();
            if snapshot.is_notified() {
                // A wake landed during the poll. Re-queue instead of parking.
                (TransitionToIdle::OkNotified, Some(next))
            } else {
                (TransitionToIdle::Ok, Some(next))
            }
        })
    }
}
```

---

## Phase 2 — The Waker

```rust
// crates/eddy/src/task/waker.rs

/// A Waker is a hand-built fat pointer: a data pointer plus four function
/// pointers. Our data pointer is the task itself.
///
/// EACH FUNCTION MUST MAINTAIN THE REFCOUNT EXACTLY:
///
///   clone       →  +1   (a new Waker now exists)
///   wake        →  consumes  (takes self by value; must NOT leave a ref)
///   wake_by_ref →  neutral   (takes &self; must leave the ref intact)
///   drop        →  -1
///
/// Getting `wake` vs `wake_by_ref` backwards is the classic bug, and the two
/// failure modes are opposite:
///   - if wake doesn't consume  → every task ever woken leaks
///   - if wake_by_ref consumes  → tasks are freed while still queued (UAF)
///
/// Neither shows up in a simple test. Both show up under loom.
static WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref_waker, drop_waker);

unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    header.as_ref().state.ref_inc();                    // +1
    RawWaker::new(ptr, &WAKER_VTABLE)
}

unsafe fn wake_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    // CONSUMES our reference: schedule() takes ownership of it, so the task
    // stays alive while queued and the queue's pop will eventually drop it.
    RawTask::from_raw(header).wake_by_val();
}

unsafe fn wake_by_ref_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    // Does NOT consume. If this needs to queue the task, it takes its OWN
    // reference (inside transition_to_notified_by_ref), leaving ours intact.
    RawTask::from_raw(header).wake_by_ref();
}

unsafe fn drop_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    if header.as_ref().state.ref_dec() {                // -1
        (header.as_ref().vtable.dealloc)(header);
    }
}
```

### The loom test that justifies the whole approach

```rust
// tests/loom/task.rs

/// loom replaces atomics and threads with instrumented versions and
/// EXHAUSTIVELY explores every interleaving the C11 memory model permits,
/// including the weak-memory reorderings that only manifest on ARM.
///
/// This test is the difference between "it works on my machine" and "the
/// model has no interleaving that breaks the invariant".
#[test]
fn wake_during_poll_is_never_lost() {
    loom::model(|| {
        let (task, handle) = task::spawn(async { 1 });
        let waker = task.waker();

        // Reactor thread races with the worker thread.
        let th = loom::thread::spawn(move || waker.wake());
        let poll_result = task.poll_once();
        th.join().unwrap();

        // THE INVARIANT: after the race, the task is either
        //   (a) complete, or
        //   (b) queued for another poll.
        // It must NEVER be parked-and-forgotten (lost wakeup) and never
        // queued twice (double-free on the second pop).
        assert!(
            poll_result.is_ready() || task.is_queued(),
            "lost wakeup: task neither completed nor re-queued"
        );
        assert_eq!(task.queue_count(), if poll_result.is_ready() { 0 } else { 1 });
    });
}

#[test]
fn refcount_is_exact_under_concurrent_clone_drop() {
    loom::model(|| {
        let task = task::spawn(async {});
        let w1 = task.waker();
        let w2 = task.waker();

        let t1 = loom::thread::spawn(move || drop(w1.clone()));
        let t2 = loom::thread::spawn(move || drop(w2));
        t1.join().unwrap();
        t2.join().unwrap();

        // Exactly one dealloc, and not before all references are gone.
        assert_eq!(task.dealloc_count(), 0);
        drop(task);
        assert_eq!(DEALLOC_COUNT.load(Ordering::SeqCst), 1);
    });
}
```

---

## Phase 4 — The Work-Stealing Scheduler

### The Chase-Lev deque's packed head

```rust
// crates/eddy/src/scheduler/multi_thread/queue.rs

/// The head is ONE AtomicU64 packing TWO 32-bit values:
///
///     [ steal_head : 32 ][ real_head : 32 ]
///
/// A thief CASes steal_head forward to claim a range, copies the tasks out,
/// then CASes real_head to match. The owner reads both with a single load: if
/// they differ, a steal is in flight and the owner must not touch that range.
///
/// WHY ONE WORD AND NOT TWO ATOMICS:
/// With separate atomics there is a window where the owner loads a stale
/// steal_head and a fresh real_head (or vice versa), observes a range as free
/// that a thief has already claimed, and pops a task the thief is
/// simultaneously copying out. The same task then runs on two threads —
/// which for a future means poll() is called concurrently, violating the
/// central safety invariant of the entire runtime.
///
/// Packing them makes the snapshot atomic by construction.
pub(crate) struct Inner<T: 'static> {
    head: AtomicU64,
    tail: AtomicU32,      // owner writes only
    buffer: Box<[UnsafeCell<MaybeUninit<task::Notified<T>>>; LOCAL_QUEUE_CAPACITY]>,
}

const LOCAL_QUEUE_CAPACITY: usize = 256;

impl<T> Local<T> {
    /// On overflow, move the OLDER HALF to the injector and keep the new task.
    ///
    /// Moving the NEW task instead (the intuitive choice) causes a
    /// pathological ping-pong: the task goes to the global queue, gets pulled
    /// back by the same overloaded worker moments later, and overflows again.
    /// Keeping the newest local also preserves cache locality for the task
    /// that was just woken.
    pub(crate) fn push_back_or_overflow<S>(&mut self, task: Notified<T>, inject: &S)
    where S: Overflow<T> {
        loop {
            match self.push_back_inner(task) {
                Ok(()) => return,
                Err(t) => {
                    let half = LOCAL_QUEUE_CAPACITY / 2;
                    // pop_n takes from the FRONT — the oldest tasks.
                    let batch = self.pop_n(half);
                    inject.push_batch(batch);
                    // Retry; there's now room.
                    return self.push_back_inner(t).unwrap_or_else(|_| unreachable!());
                }
            }
        }
    }
}
```

### The worker loop — every clause fixes a specific failure

```rust
// crates/eddy/src/scheduler/multi_thread/worker.rs

const GLOBAL_QUEUE_INTERVAL: u32 = 61;      // prime, avoids resonance
const MAX_LIFO_POLLS_PER_TICK: u8 = 3;

impl Core {
    fn next_task(&mut self, worker: &Worker) -> Option<Notified> {
        // EVERY 61 TICKS, CHECK THE GLOBAL QUEUE FIRST.
        //
        // Without this, a worker whose tasks keep waking each other has a
        // permanently non-empty local queue and NEVER looks at the injector.
        // Tasks spawned from outside the runtime — including from
        // spawn_blocking callbacks and from other threads — starve
        // indefinitely. The symptom is "some of my tasks just never run",
        // which is nearly impossible to diagnose without knowing this exists.
        //
        // 61 is prime so it doesn't resonate with other periodic intervals
        // in the system and produce a pathological alignment.
        if self.tick % GLOBAL_QUEUE_INTERVAL == 0 {
            return worker.inject().pop().or_else(|| self.next_local_task());
        }
        self.next_local_task().or_else(|| worker.inject().pop())
    }

    fn next_local_task(&mut self) -> Option<Notified> {
        // THE LIFO SLOT.
        //
        // When task A wakes task B, B goes here rather than the queue tail, so
        // B runs next on the same worker while A's data is still in L1/L2. On
        // request/response and message-passing chains this is a large constant
        // factor — it's the difference between a channel round trip touching
        // hot cache and touching main memory.
        //
        // The cost is fairness, so it's bounded three ways:
        //   - the slot holds exactly ONE task
        //   - MAX_LIFO_POLLS_PER_TICK caps consecutive LIFO polls at 3
        //   - the slot is NOT stealable
        //
        // Without the cap, a ping-pong pair (A wakes B wakes A) monopolizes
        // the worker forever and every other task on it starves.
        if self.lifo_polls < MAX_LIFO_POLLS_PER_TICK {
            if let Some(task) = self.lifo_slot.take() {
                self.lifo_polls += 1;
                return Some(task);
            }
        }
        self.lifo_polls = 0;
        self.run_queue.pop()
    }

    /// Steal from a RANDOM victim, taking HALF.
    ///
    /// Random start: if every idle worker began at worker 0, they would all
    /// hammer the same queue, serialize on its head CAS, and mostly fail —
    /// a convoy. Random start spreads the contention.
    ///
    /// Half, not one: stealing costs a CAS and a cache-line transfer. Taking
    /// one task means paying that cost again immediately. Taking half
    /// amortizes it and gives the thief enough work to stay busy.
    fn steal_work(&mut self, worker: &Worker) -> Option<Notified> {
        // Cap concurrent searchers at half the workers. Beyond that, extra
        // thieves just add contention without finding more work.
        if !self.transition_to_searching(worker) { return None; }

        let num = worker.shared.remotes.len();
        let start = self.rand.fastrand_n(num as u32) as usize;
        for i in 0..num {
            let i = (start + i) % num;
            if i == worker.index { continue; }
            if let Some(task) = worker.shared.remotes[i].steal.steal_into(&mut self.run_queue) {
                return Some(task);
            }
        }
        // Last chance: the injector may have been filled while we searched.
        worker.inject().pop()
    }
}
```

---

## Phase 5 — The Reactor

### Generation counters and fd reuse

```rust
// crates/eddy/src/io/scheduled_io.rs

/// Packed: [ generation : 32 ][ readiness : 16 ][ shutdown : 1 ]
///
/// THE GENERATION COUNTER EXISTS FOR FD REUSE.
///
/// Sequence that corrupts a naive implementation:
///   1. Task A registers fd 7, gets slab index 3
///   2. fd 7 is closed; slab index 3 is freed
///   3. Task B opens a new socket; the kernel reuses fd 7
///   4. B registers it and gets slab index 3 back
///   5. An epoll event for the OLD fd 7, queued before the close, arrives
///   6. Without a generation check it wakes B's task with A's readiness
///
/// The result is B waking spuriously (harmless) or, worse, B's readiness
/// state being clobbered so a real event is missed (a hang). Comparing
/// generations discards the stale event.
pub(crate) struct ScheduledIo {
    readiness: AtomicUsize,
    waiters: Mutex<Waiters>,
}

struct Waiters {
    /// SEPARATE reader and writer wakers.
    ///
    /// One socket is routinely read-waited by one task and write-waited by
    /// another (that's what `split()` is for). A single waker field means one
    /// registration silently overwrites the other, and the overwritten task
    /// hangs forever. This bug looks like "my writer sometimes stops".
    reader: Option<Waker>,
    writer: Option<Waker>,
    list: LinkedList<Waiter, <Waiter as linked_list::Link>::Target>,
}
```

### The park timeout comes from the timer wheel

```rust
// crates/eddy/src/runtime/driver.rs

/// The runtime blocks in exactly ONE place: the poller's wait call. Its
/// timeout must be the next timer deadline, or every timer fires late by
/// however long the next I/O event takes to arrive — which for an idle
/// server is "never".
///
/// This is the coupling point between the I/O driver and the time driver, and
/// the reason they live behind one `Driver` rather than being independent
/// subsystems.
impl Driver {
    pub(crate) fn park(&mut self, handle: &Handle) {
        let timeout = match self.time.next_expiration() {
            // A timer is pending: wait at most until it fires.
            Some(deadline) => Some(deadline.saturating_duration_since(Instant::now())),
            // Nothing pending: block until an I/O event or an explicit unpark.
            None => None,
        };
        self.io.park_timeout(timeout);
        // Fire any timers that came due while we were blocked.
        self.time.process_at_time(Instant::now());
    }

    /// Called when tasks are already ready — poll for events without blocking.
    pub(crate) fn park_timeout_zero(&mut self) {
        self.io.park_timeout(Some(Duration::ZERO));
        self.time.process_at_time(Instant::now());
    }
}
```

### The I/O operation loop

```rust
/// EVERY async I/O operation follows this shape. The `clear_readiness` step
/// is the one people omit, and omitting it turns a spurious wakeup into a
/// 100%-CPU spin loop.
///
/// epoll (and kqueue) are permitted to report readiness that turns out to be
/// false — e.g. a packet arrives, epoll fires, and a checksum failure means
/// there's nothing to read. Without clearing the cached readiness bit, the
/// task sees "still ready", tries again, gets WouldBlock, sees "still ready",
/// and spins.
pub(crate) async fn async_io<R>(
    &self,
    interest: Interest,
    mut f: impl FnMut() -> io::Result<R>,
) -> io::Result<R> {
    loop {
        let event = self.readiness(interest).await?;
        match f() {
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Spurious. Clear the cached bit and wait for a fresh event.
                self.clear_readiness(event);
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
                // EINTR: retry immediately, readiness is still valid.
                continue;
            }
            x => return x,
        }
    }
}
```

---

## Phase 6 — The Timer Wheel

```rust
// crates/eddy/src/time/wheel/mod.rs

/// Hierarchical hashed timing wheel (Varghese & Lauck, 1987).
///
///   Level 0: 64 slots × 1 ms     → 64 ms range
///   Level 1: 64 slots × 64 ms    → ~4 s
///   Level 2: 64 slots × 4 s      → ~4 min
///   Level 3: 64 slots × 4 min    → ~4.5 hr
///   Level 4: 64 slots × 4.5 hr   → ~12 days
///   Level 5: 64 slots × 12 days  → ~2 years
///
/// WHY NOT A BINARY HEAP:
/// A heap is O(log n) insert and O(log n) cancel. The wheel is O(1) for both.
/// That sounds like a micro-optimization until you look at the actual
/// workload: a server with 100k connections sets a timeout on EVERY request
/// and cancels almost all of them when the response arrives first. The
/// dominant operation is insert-then-cancel, at full request rate, against a
/// structure holding 100k entries. O(1) vs O(log n) there is a real,
/// measurable difference — and it's why every production runtime uses a wheel.
///
/// 64 slots per level is not arbitrary:
///   - indexing becomes a shift and a mask, not a division
///   - one level's occupancy fits in a u64 bitmap, so "next non-empty slot"
///     is a single trailing_zeros() instead of scanning 64 lists
pub(crate) struct Wheel {
    elapsed: u64,
    levels: Box<[Level; NUM_LEVELS]>,
    pending: LinkedList<TimerShared, TimerShared>,
}

const NUM_LEVELS: usize = 6;
const LEVEL_MULT: usize = 64;

impl Wheel {
    /// Which level does a deadline belong in?
    ///
    /// XOR the deadline with the current time: the highest differing bit tells
    /// you the granularity of the difference. Divide by 6 (log2 of 64) to get
    /// the level. Three instructions, no loop.
    fn level_for(&self, when: u64) -> usize {
        const SLOT_MASK: u64 = (1 << 6) - 1;
        let masked = (when ^ self.elapsed) | SLOT_MASK;
        if masked >= MAX_DURATION { return NUM_LEVELS - 1; }
        let leading_zeros = masked.leading_zeros() as usize;
        let significant = 63 - leading_zeros;
        significant / NUM_LEVELS
    }

    /// CASCADING.
    ///
    /// When level 0 wraps, level 1's current slot is redistributed down into
    /// level 0. A timer set for 3 seconds starts in level 1 and migrates down
    /// as its deadline approaches.
    ///
    /// This is amortized O(1): each timer moves down at most 6 times over its
    /// entire lifetime — once per level — regardless of how long it waits.
    pub(crate) fn poll(&mut self, now: u64) -> Option<TimerHandle> {
        loop {
            if let Some(handle) = self.pending.pop_back() { return Some(handle); }
            match self.next_expiration() {
                Some(exp) if exp.deadline <= now => {
                    self.process_expiration(&exp);
                    self.set_elapsed(exp.deadline);
                }
                _ => { self.set_elapsed(now); return self.pending.pop_back(); }
            }
        }
    }

    fn process_expiration(&mut self, expiration: &Expiration) {
        while let Some(item) = self.levels[expiration.level].pop_entry_slot(expiration.slot) {
            if expiration.level == 0 {
                self.pending.push_front(item);      // fire
            } else {
                self.insert(item);                  // cascade down one level
            }
        }
    }
}

// crates/eddy/src/time/wheel/level.rs

pub(crate) struct Level {
    level: usize,
    /// Bitmap of occupied slots.
    ///
    /// `occupied.trailing_zeros()` finds the next non-empty slot in ONE
    /// instruction. Scanning 64 linked lists to find the next timer would make
    /// `next_expiration` — which runs on every park — O(64) per level, O(384)
    /// total, on the hottest path in the runtime.
    occupied: u64,
    slots: [LinkedList<TimerShared, TimerShared>; LEVEL_MULT],
}

impl Level {
    fn next_occupied_slot(&self, now: u64) -> Option<usize> {
        if self.occupied == 0 { return None; }
        let now_slot = (now / slot_range(self.level)) as usize;
        let occupied = self.occupied.rotate_right(now_slot as u32);
        let zeros = occupied.trailing_zeros() as usize;
        Some((zeros + now_slot) % LEVEL_MULT)
    }
}
```

---

## Phase 8 — Sync Primitives: the intrusive waiter

```rust
// crates/eddy/src/sync/batch_semaphore.rs

/// The waiter node lives INSIDE the future, not in a heap allocation.
///
/// A Mutex contended by 10,000 tasks would otherwise allocate 10,000 waiter
/// nodes. Here the node is part of the `Acquire` future, which is part of the
/// task allocation that already exists. Zero allocation per wait.
///
/// THIS IS WHY `Pin` EXISTS, and it is the clearest real-world demonstration
/// you will find:
///
///   1. The future contains a `Waiter`.
///   2. `poll` links that Waiter into the semaphore's list — the list now
///      holds a POINTER to memory inside the future.
///   3. The future is now self-referential-by-proxy: if it moves, the list's
///      pointer dangles, and the next `release()` writes through it.
///   4. `PhantomPinned` makes the future `!Unpin`, so `Pin<&mut Self>`
///      guarantees it cannot move between polls.
///
/// Without `Pin`, this design is unsound and you'd have to heap-allocate every
/// waiter. Rust is the only language that gives you a type-level answer here,
/// and building this is the only way to really feel why.
pub(crate) struct Waiter {
    pointers: linked_list::Pointers<Waiter>,
    waker: UnsafeCell<Option<Waker>>,
    state: AtomicUsize,
    /// Makes the containing future `!Unpin`.
    _p: PhantomPinned,
}

/// `Notify` STORES ONE PERMIT.
///
/// The bug this prevents:
///     // Task A                          // Task B
///     do_work();
///                                        notify.notify_one();   // ← no waiters yet
///     notify.notified().await;           // ← waits forever
///
/// A condvar loses this notification. `Notify` stores it as a permit, so the
/// subsequent `notified()` completes immediately. This single behavior is the
/// difference between `Notify` and a naive condvar port, and omitting it
/// produces hangs that are timing-dependent and nearly impossible to reproduce.
impl Notify {
    pub fn notify_one(&self) {
        let mut waiters = self.waiters.lock();
        if let Some(waker) = self.take_next_waiter(&mut waiters) {
            drop(waiters);
            waker.wake();
        } else {
            // No waiter — STORE the permit for the next notified().
            self.state.store(NOTIFIED, Ordering::SeqCst);
        }
    }
}
```

---

## Phase 11 — The Cooperative Budget

```rust
// crates/eddy/src/runtime/coop.rs

/// A task that always returns Ready never yields, and starves its worker:
///
///     loop { let msg = rx.recv().await; process(msg); }
///
/// If the channel always has a message, `recv()` never returns Pending, the
/// task never yields, and every other task on that worker is starved. On a
/// current-thread runtime this hangs the entire application.
///
/// The fix: every resource operation consumes from a per-task budget of 128.
/// When exhausted, the operation returns Pending and wakes itself immediately,
/// forcing one trip through the scheduler so other tasks run.
///
/// This is completely invisible to users, and is the reason a busy channel
/// doesn't hang a Tokio server. It's also the thing people are most surprised
/// to discover exists — which makes it a good console visualization.
const BUDGET: u8 = 128;

pub(crate) fn poll_proceed(cx: &mut Context<'_>) -> Poll<RestoreOnPending> {
    CURRENT.with(|cell| {
        let mut budget = cell.get();
        match budget.0 {
            Some(0) => {
                // Exhausted. Wake ourselves so we go to the BACK of the queue
                // and every other ready task gets a turn first.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Some(n) => {
                cell.set(Budget(Some(n - 1)));
                Poll::Ready(RestoreOnPending(Cell::new(budget)))
            }
            // None = unconstrained (inside block_on, or explicitly opted out)
            None => Poll::Ready(RestoreOnPending(Cell::new(Budget::unconstrained()))),
        }
    })
}
```

```rust
/// The test that proves it works. Without the budget this hangs.
#[test]
fn always_ready_channel_does_not_starve_peers() {
    let rt = Builder::new_current_thread().build().unwrap();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::unbounded_channel();
        // Keep the channel permanently non-empty.
        for _ in 0..10_000 { tx.send(()).unwrap(); }

        let progressed = Arc::new(AtomicBool::new(false));
        let p = progressed.clone();
        spawn(async move { p.store(true, Ordering::SeqCst); });

        // Drain 200 messages — more than the 128 budget, so at least one
        // forced yield must occur.
        for _ in 0..200 { rx.recv().await.unwrap(); }

        assert!(progressed.load(Ordering::SeqCst),
                "peer task never ran: cooperative budget is not working");
    });
}
```

---

## Phase 12 — io_uring: the buffer-ownership problem

```rust
// crates/eddy/src/io/uring/mod.rs

/// io_uring is COMPLETION-based, and that difference is not cosmetic — it
/// breaks a core assumption of Rust's future model.
///
/// THE PROBLEM:
///   Rust futures are cancelled by dropping them. With epoll that's free: you
///   simply stop caring that the fd is readable, and nothing bad happens.
///
///   With io_uring you have already handed the kernel a POINTER to your
///   buffer. The kernel will write into that buffer whenever it gets around
///   to it — including long after your future has been dropped and the buffer
///   freed. That is a use-after-free the borrow checker cannot see, because
///   the kernel is not a Rust reference.
///
/// THREE POSSIBLE FIXES:
///
///   1. COPY: read into a runtime-owned buffer, memcpy into the caller's.
///      Sound, keeps the familiar AsyncRead API, costs one extra copy per
///      operation — which partly defeats the point of using io_uring at all.
///
///   2. LEAK: on cancellation, leak the buffer until the CQE arrives.
///      Sound but unbounded — a cancel-heavy workload leaks without limit.
///
///   3. OWNED BUFFERS (what we implement): the API takes the buffer BY VALUE
///      and hands it back on completion. Cancellation transfers ownership to
///      the driver, which holds it until the CQE arrives and then drops it.
///      Zero copies, sound, bounded, and the API is honest about what is
///      actually happening.
///
/// This is why tokio-uring, monoio, and compio all have a DIFFERENT I/O trait
/// from Tokio. It is not gratuitous API churn — it is forced by the model, and
/// implementing it is the clearest way to understand why "just use io_uring"
/// is not a drop-in change.
pub trait AsyncReadOwned {
    fn read(&self, buf: Vec<u8>)
        -> impl Future<Output = (io::Result<usize>, Vec<u8>)>;
}

pub(crate) struct UringDriver {
    ring: IoUring,
    ops: Slab<OpState>,
    /// Operations whose future was dropped before completion.
    ///
    /// The buffer lives here until the kernel reports its CQE, then it is
    /// dropped. This BOUNDS the leak to "currently in-flight cancelled ops"
    /// rather than "forever", and it is the piece that makes cancellation
    /// sound rather than merely unlikely-to-crash.
    orphaned: Slab<OrphanedOp>,
}

impl Drop for UringOp {
    fn drop(&mut self) {
        if self.state == OpState::InFlight {
            // Transfer the buffer to the driver. It must outlive the kernel's
            // write, so we cannot simply drop it here.
            let buf = self.buf.take().expect("in-flight op without buffer");
            DRIVER.with(|d| d.orphan(self.id, buf));
            // Best-effort: ask the kernel to cancel. If it's already done,
            // the CQE arrives normally and the orphan is reaped.
            DRIVER.with(|d| d.submit_cancel(self.id));
        }
    }
}
```

---

## Frontend — the two views a terminal can't do

```tsx
// console-ui/src/components/swimlanes/TaskSwimlanes.tsx

/// Task lifecycle swimlanes — the view that makes starvation obvious.
///
/// X = time, one lane per task, segments colored:
///   gray   = idle (waiting on I/O, timer, or a channel)
///   yellow = SCHEDULED but not running (in a queue, waiting for a worker)
///   green  = running (being polled)
///
/// A long yellow stretch is the signal: that task was READY and the runtime
/// did not run it. That is starvation, and it is invisible in logs, invisible
/// in metrics, and immediately obvious here.
///
/// A long green stretch is the other signal: a single poll that took 200ms
/// is a blocking call inside an async fn.
export function TaskSwimlanes({ events, window }: Props) {
  const lanes = useMemo(() => buildLanes(events, window), [events, window]);
  const x = d3.scaleTime().domain([window.start, window.end]).range([0, width]);
  const y = d3.scaleBand<TaskId>().domain(lanes.map(l => l.id)).range([0, height]).padding(0.2);

  return (
    <svg width={width} height={height}>
      {lanes.map(lane =>
        lane.segments.map((seg, i) => (
          <rect key={i}
                x={x(seg.start)} y={y(lane.id)}
                width={Math.max(1, x(seg.end) - x(seg.start))}
                height={y.bandwidth()}
                fill={STATE_COLOR[seg.state]}
                onMouseEnter={() => setTooltip(seg)}
                onClick={() => selectTask(lane.id)} />
        ))
      )}
      {/* Long scheduled segments get an explicit marker — this is the bug */}
      {lanes.flatMap(lane => lane.segments
        .filter(s => s.state === 'scheduled' && s.duration > 50)
        .map((s, i) => (
          <text key={i} x={x(s.start)} y={y(lane.id)! - 2} fontSize={9} fill="#ef4444">
            starved {Math.round(s.duration)}ms
          </text>
        )))}
    </svg>
  );
}

const STATE_COLOR = {
  idle:      '#334155',   // slate — waiting on something external
  scheduled: '#eab308',   // yellow — READY but not running ← the problem
  running:   '#22c55e',   // green — being polled
};
```

```tsx
// console-ui/src/components/causality/WakeGraph.tsx

/// Wake causality graph — nodes are tasks, edges are "woke".
///
/// This is the async equivalent of a call graph, and NOTHING ELSE produces
/// it. In a synchronous program the stack tells you who called whom. In an
/// async program that information is destroyed the moment a task yields — the
/// stack unwinds, and the "caller" is now the executor.
///
/// This graph reconstructs it: task 7 (acceptor) woke task 12 (connection
/// handler) which woke task 3 (db pool), tracing a request's real path through
/// the concurrency structure.
///
/// Cycles are highlighted in red, because a wake cycle is almost always a
/// busy-loop: two tasks waking each other with no external event, burning a
/// core.
export function WakeGraph({ edges, tasks }: Props) {
  const { nodes, links, cycles } = useMemo(() => {
    const g = new dagre.graphlib.Graph();
    g.setGraph({ rankdir: 'LR', nodesep: 30, ranksep: 80 });
    // …
    dagre.layout(g);
    return { nodes: layoutNodes(g), links: layoutEdges(g), cycles: findCycles(edges) };
  }, [edges]);

  return (
    <svg>
      {links.map(l => (
        <path key={l.id} d={l.path}
              stroke={cycles.has(l.id) ? '#ef4444' : '#475569'}
              strokeWidth={Math.min(6, 1 + Math.log2(l.count))}   // thickness = wake count
              markerEnd="url(#arrow)" fill="none" />
      ))}
      {nodes.map(n => (
        <g key={n.id} transform={`translate(${n.x},${n.y})`} onClick={() => selectTask(n.id)}>
          <rect width={n.width} height={n.height} rx={4}
                fill={n.isRoot ? '#1e40af' : '#1e293b'}    // roots woken by I/O or timer
                stroke={cycles.hasNode(n.id) ? '#ef4444' : '#334155'} />
          <text dy={n.height / 2 + 4} dx={8} fontSize={11} fill="#e2e8f0">
            {n.name ?? `task ${n.id}`}
          </text>
        </g>
      ))}
    </svg>
  );
}
```

---

## Correctness Invariants

1. **No lost wakeups** — `loom_wake_during_poll_is_never_lost`
2. **No double-schedule** — a task is never in two queues; loom-verified
3. **Exact refcounting** — exactly one dealloc, never early; loom-verified
4. **No use-after-free** — Miri clean over all `unsafe` paths
5. **Chase-Lev correctness** — 1 producer + 2 thieves, no task lost or duplicated; loom-verified
6. **Injector liveness** — a task in the global queue is picked up within 61 polls
7. **Fairness** — the cooperative-budget test proves an always-ready channel doesn't starve peers
8. **Timer accuracy** — never early; lateness bounded by one tick + scheduling latency
9. **Timer O(1)** — insert and cancel timed at 100k entries, not just asserted asymptotically
10. **Cancel safety** — property-tested by dropping every shipped future at a random poll count
11. **`Pin` soundness** — no `!Unpin` future moves after first poll
12. **Shutdown completeness** — no thread outlives the runtime; all fds closed
13. **io_uring soundness** — no buffer freed before its CQE; verified under ASan
14. **ARM64 clean** — full suite passes under weak memory ordering

---

## Code Standards

**Rust**
- **No async runtime dependencies in `eddy`.** CI enforces this on `cargo tree`.
- Every `unsafe` block has a `// SAFETY:` comment naming its preconditions.
- **Every waker vtable function documents its refcount effect** (+1 / consume / neutral / −1). This is where the leaks and the UAFs both live.
- Every lock-free structure ships with its loom test in the same commit. "I'll add loom later" means never.
- Memory orderings are `Acquire`/`Release`/`AcqRel` with a comment explaining what they synchronize with. `SeqCst` requires a justification.
- `Pin` is never bypassed with `Pin::get_unchecked_mut` outside of `pin-project`-generated code.
- Instrumentation is feature-gated and verifiably zero-cost when off.
- Panics in user futures are caught and surfaced as `JoinError::Panic` — one bad task must not poison the runtime.

**Frontend**
- The console consumes the same event stream as the TUI; instrumentation is written once.
- Ring-buffer the event store — an async runtime emits millions of events and the browser must not accumulate them.
- D3 owns its SVG subtree; React owns everything around it.

---

## Startup

```bash
# Day one
cargo run --example spike           # hand-built waker, poll a future

cargo test                          # unit tests
RUSTFLAGS="--cfg loom" cargo test --test loom     # THE correctness tool
cargo +nightly miri test
cargo bench -- --baseline tokio

cargo run --example echo_server &
cargo run -p eddy-console          # TUI
cd console-ui && bun run dev        # web console
```

**The first milestone, and the one that proves the hard part works:**

```rust
let rt = Runtime::new()?;
rt.block_on(async {
    let (tx, rx) = oneshot::channel();
    spawn(async move {
        sleep(Duration::from_millis(100)).await;
        tx.send(42).unwrap();
    });
    assert_eq!(rx.await.unwrap(), 42);
});
```

Eleven lines, and every subsystem is exercised: task allocation, waker vtable, work-stealing scheduler, the timer wheel, a sync primitive, and the park/unpark cycle. When this passes, the runtime is real.

**Then run the echo server and open the web console.** Point 10,000 connections at it and watch the worker heatmap: queue depths rising, steal arrows firing between workers as load skews, parks and unparks tracking offered load. Work-stealing goes from a paragraph in a paper to something you can watch happen.

**Then deliberately break it.** Put a `std::thread::sleep(Duration::from_millis(200))` inside a handler and watch:

- the swimlane for that task turns solid green for 200 ms
- **every other lane on the same worker turns solid yellow** — ready, scheduled, and not running
- the poll-duration histogram spikes past the red line
- a `BlockingDetected` warning names the task and its spawn location

That single screen is the entire argument for why `spawn_blocking` exists, and it's the thing that makes an async runtime stop being magic.
