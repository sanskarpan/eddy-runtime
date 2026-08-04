# CHECKLIST.md — `eddy`: An Async Runtime from Scratch

> Priority: 🔴 blocking · 🟡 important · 🟢 enhancement · 🔵 stretch
> **Phase 1–3 (task + waker + current-thread executor) is the spine. Nothing else can be built or debugged until a single future can be spawned, polled, woken, and completed on one thread.**
> **`loom` model checking is not optional. Every lock-free structure gets a loom test in the same commit it is written.**

---

## Phase 0 — Bootstrap (14 tasks)

- [ ] 🔴 `cargo new --lib eddy`; workspace with 4 member crates per SPEC §17
- [ ] 🔴 `cargo add libc slab parking_lot pin-project-lite tracing crossbeam-utils`
- [ ] 🔴 `cargo add --dev loom criterion proptest futures tokio` — **`tokio` and `futures` are test-only oracles**
- [ ] 🔴 **Forbid runtime deps in the main crate**: CI check that `cargo tree -p eddy --edges normal` contains no `tokio`, `async-std`, `smol`, `mio`, `polling`, or `futures-executor`
- [ ] 🔴 `[target.'cfg(loom)'.dependencies] loom = "0.7"` with a `loom` cfg flag and a `sync` shim module re-exporting either `std::sync` or `loom::sync`
- [ ] 🔴 `#![deny(clippy::undocumented_unsafe_blocks)]` workspace-wide
- [ ] 🔴 CI matrix: `x86_64-linux`, `aarch64-linux` (QEMU — **weak memory ordering exposes bugs x86 hides**), `aarch64-macos`, `x86_64-windows`
- [ ] 🔴 CI jobs: `test`, `test-loom` (`RUSTFLAGS="--cfg loom"`), `miri`, `bench`
- [ ] 🔴 `Makefile`/`justfile`: `test`, `loom`, `miri`, `bench`, `bench-vs-tokio`, `console`, `console-web`
- [ ] 🔴 `util/rand.rs`: xorshift `FastRand` for steal-victim selection (no `rand` dependency)
- [ ] 🔴 `util/linked_list.rs`: intrusive doubly-linked list with `Pointers<T>` — used by waiter lists and the timer wheel
- [ ] 🔴 `cd console-ui && bun create vite . --template react-ts`
- [ ] 🔴 `bun add d3 @dagrejs/dagre recharts zustand clsx lucide-react`; `bun add -d tailwindcss postcss autoprefixer @types/d3`; `bunx shadcn@latest init` + `button card table tabs badge tooltip separator scroll-area resizable select slider`
- [ ] 🔴 Spike: hand-write a `Waker` with a `RawWakerVTable`, poll a trivial future to completion on one thread, no dependencies. **Do this first** — if the vtable refcounting is wrong, everything above it is unfixable.

---

## Phase 1 — Task Representation (28 tasks)

**Structure**
- [ ] 🔴 `Header { state: AtomicUsize, vtable: &'static Vtable, owner_id: u32, queue_next: UnsafeCell<Option<NonNull<Header>>> }`
- [ ] 🔴 `Core<F, S> { scheduler: S, stage: UnsafeCell<Stage<F>> }` with `Stage::{ Running(F), Finished(F::Output), Consumed }`
- [ ] 🔴 `Trailer { waker: UnsafeCell<Option<Waker>> }` for the `JoinHandle`'s waker
- [ ] 🔴 `#[repr(C)] Cell<F, S>` — **one allocation** holding header + core + trailer
- [ ] 🔴 `RawTask(NonNull<Header>)` — one word, type-erased, vtable reached through the header
- [ ] 🔴 `Vtable` with `poll`, `schedule`, `dealloc`, `try_read_output`, `drop_join_handle_slow`, `shutdown`
- [ ] 🔴 `vtable::<F, S>()` const fn producing a `&'static Vtable` per (future, scheduler) pair
- [ ] 🔴 **`queue_next` is the intrusive link** — the global injector is a list threaded through tasks, allocating nothing per push

**State machine**
- [ ] 🔴 Packed state: `[ refcount : 48 ][ flags : 16 ]` in one `AtomicUsize`
- [ ] 🔴 Flags: `RUNNING`, `COMPLETE`, `NOTIFIED`, `CANCELLED`, `JOIN_INTEREST`, `JOIN_WAKER_SET`
- [ ] 🔴 `transition_to_running()` — fails if already RUNNING or COMPLETE
- [ ] 🔴 `transition_to_idle()` — returns whether NOTIFIED was set while running (→ must re-queue)
- [ ] 🔴 `transition_to_complete()` — sets COMPLETE, wakes the JoinHandle waker
- [ ] 🔴 **`transition_to_notified()`**: if RUNNING, set NOTIFIED and do nothing (the poller re-queues); else set NOTIFIED and schedule
- [ ] 🔴 `ref_inc()` / `ref_dec()` returning whether the count hit zero
- [ ] 🔴 Refcount overflow check — abort rather than wrap
- [ ] 🔴 Every transition is a single `fetch_update` CAS, documented with the invariant it preserves

**Harness (the poll driver)**
- [ ] 🔴 `Harness::poll()`: transition to running → build waker → poll → transition to idle-or-complete
- [ ] 🔴 Catch panics in `poll` (`catch_unwind`), store as `JoinError::Panic`, do not poison the runtime
- [ ] 🔴 `Harness::dealloc()`: drop the stage in place, then free the allocation
- [ ] 🔴 `Harness::shutdown()`: cancel without polling, for runtime teardown

**JoinHandle**
- [ ] 🔴 `JoinHandle<T>` implementing `Future<Output = Result<T, JoinError>>`
- [ ] 🔴 Registers its waker in the trailer; woken on completion
- [ ] 🔴 `abort()` — sets CANCELLED; a not-yet-running task is dropped without polling
- [ ] 🔴 Dropping a `JoinHandle` detaches the task (it keeps running) and drops one reference
- [ ] 🔴 `JoinError::{ Panic(Box<dyn Any + Send>), Cancelled }`
- [ ] 🔴 `AbortHandle` — abort without holding the output

**Tests**
- [ ] 🔴 Test: spawn, poll to completion, output readable via JoinHandle
- [ ] 🔴 Test: allocation counter proves exactly one alloc per task and one dealloc
- [ ] 🔴 Test: panicking future → `JoinError::Panic`, runtime still usable

---

## Phase 2 — The Waker (18 tasks)

**This is the hardest `unsafe` in the project.**

- [ ] 🔴 `WAKER_VTABLE: RawWakerVTable` with the four functions
- [ ] 🔴 `clone_waker`: `ref_inc()`, return a new `RawWaker` with the same data pointer — **+1**
- [ ] 🔴 `wake_waker`: **consumes** the reference — schedule, and the schedule takes ownership
- [ ] 🔴 `wake_by_ref_waker`: **does not consume** — schedule with an explicit `ref_inc()` for the queued reference
- [ ] 🔴 `drop_waker`: `ref_dec()`, dealloc if zero — **-1**
- [ ] 🔴 **Document the +1/-1 contract on every function.** Getting `wake` vs `wake_by_ref` backwards leaks every task or frees queued ones
- [ ] 🔴 `waker_from_raw(NonNull<Header>) -> Waker`
- [ ] 🔴 `WakerRef<'a>` — a borrowed waker for the common poll path, avoiding a clone per poll
- [ ] 🔴 `noop_waker()` for tests
- [ ] 🔴 Implement `Waker::will_wake` correctly (same data pointer + same vtable) so combinators can skip redundant clones

**Tests**
- [ ] 🔴 Test: clone/drop a waker 10,000 times → refcount returns to baseline, no leak
- [ ] 🔴 Test: `wake()` on a pending task schedules exactly once
- [ ] 🔴 Test: `wake()` twice before poll schedules only once (NOTIFIED is idempotent)
- [ ] 🔴 **Test: `wake()` during poll → task is re-queued after the poll returns Pending** (the lost-wakeup case)
- [ ] 🔴 Test: `wake()` after completion is a no-op and does not resurrect the task
- [ ] 🔴 Test: waker outliving the task keeps it alive; dropping the last waker deallocs
- [ ] 🔴 **loom: wake during poll from another thread** — asserts the task ends up completed or queued, never lost, never double-queued
- [ ] 🔴 **loom: concurrent clone + drop from two threads** — refcount is exact, exactly one dealloc

---

## Phase 3 — Current-Thread Executor (16 tasks)

**Build this before the multi-thread scheduler. It proves the task/waker machinery in isolation.**

- [ ] 🔴 `CurrentThread { queue: VecDeque<Notified>, ... }` — plain deque, no atomics on the hot path
- [ ] 🔴 `block_on(fut)`: park the current thread, poll the root future, run queued tasks between polls
- [ ] 🔴 `block_on` uses a thread-parking waker (`Thread::unpark`) for the root future
- [ ] 🔴 `spawn(fut)` from inside `block_on` — supports `!Send` futures
- [ ] 🔴 Injection queue for wakes arriving from other threads while `block_on` is running
- [ ] 🔴 Fairness: check the injection queue every `GLOBAL_QUEUE_INTERVAL` polls
- [ ] 🔴 `CURRENT` thread-local holding the runtime `Handle`; `Handle::current()` with a clear panic message when outside a runtime
- [ ] 🔴 `EnterGuard` for `Handle::enter()`
- [ ] 🔴 Shutdown: drain the queue, drop all tasks at their next await point
- [ ] 🔴 `Builder::new_current_thread()`
- [ ] 🔴 **No LIFO slot** in this scheduler (nothing to gain, and it complicates fairness)
- [ ] 🔴 Test: `block_on(async { 42 })` returns 42
- [ ] 🔴 Test: spawn 1000 tasks, all complete, results correct
- [ ] 🔴 Test: nested spawn (a task spawns a task) works
- [ ] 🔴 Test: `!Send` future compiles and runs
- [ ] 🔴 Test: wake from another thread while blocked in `block_on` makes progress

---

## Phase 4 — Multi-Thread Scheduler (34 tasks)

**Chase-Lev deque**
- [ ] 🔴 `LOCAL_QUEUE_CAPACITY = 256`, fixed-size ring buffer
- [ ] 🔴 **Head is one `AtomicU64` packing `[steal_head : 32][real_head : 32]`** — two separate atomics allow a torn read where the owner hands the same task to two threads
- [ ] 🔴 `tail: AtomicU32`, written only by the owner
- [ ] 🔴 `push_back(task)` — owner only, returns `Err(task)` on full
- [ ] 🔴 `pop()` — owner only, must not race with an in-flight steal
- [ ] 🔴 `steal_into(dst)` — CAS steal_head to claim a range, copy, then CAS real_head
- [ ] 🔴 **Steal takes HALF** the victim's queue, not one task
- [ ] 🔴 `push_overflow`: on full, move the **older half** to the injector and keep the new task local — moving the new one causes ping-pong
- [ ] 🔴 Memory ordering documented per operation (Acquire/Release/AcqRel), with the reason
- [ ] 🔴 **loom: single producer + two thieves** — no task lost, no task duplicated
- [ ] 🔴 **loom: push_overflow racing with a steal**

**Injector**
- [ ] 🔴 Mutex-guarded **intrusive linked list** threaded through `Header::queue_next` — zero allocation per push
- [ ] 🔴 `push(task)`, `push_batch(iter)`, `pop()`, `pop_n(n)`
- [ ] 🔴 `is_closed` flag for shutdown
- [ ] 🔴 loom: concurrent push/pop

**Worker**
- [ ] 🔴 `Core { tick, lifo_slot, lifo_polls, run_queue, is_searching, park, rand }`
- [ ] 🔴 `Shared { remotes: Vec<Remote>, inject, idle, shutdown }`
- [ ] 🔴 Main loop: `next_task` → poll → repeat; park when nothing found
- [ ] 🔴 `next_task`: **every `GLOBAL_QUEUE_INTERVAL` (61) ticks, check the injector FIRST** — otherwise a self-replenishing local queue starves external spawns forever
- [ ] 🔴 `next_local_task`: LIFO slot, then run queue
- [ ] 🔴 **LIFO slot**: when a task wakes another on the same worker, the woken task goes here for cache locality
- [ ] 🔴 **`MAX_LIFO_POLLS_PER_TICK = 3`** — caps consecutive LIFO polls so a ping-pong pair can't monopolize the worker
- [ ] 🔴 LIFO slot is **not stealable** (it's the locality optimization; stealing it defeats the purpose)
- [ ] 🔴 `disable_lifo_slot()` builder option
- [ ] 🔴 `steal_work`: **random starting victim**, take half, try each worker once
- [ ] 🔴 Random start prevents the convoy where every idle worker hammers worker 0
- [ ] 🔴 `is_searching` flag + a searching-worker count, capped at half the workers, to avoid a steal storm
- [ ] 🔴 Park: register as idle, block on the driver, unpark on event or `unpark()`
- [ ] 🔴 **Last searching worker to find work unparks another** — keeps parallelism from collapsing to one worker
- [ ] 🔴 `IdleState` tracking parked workers as a bitmap for O(1) "who to unpark"

**Runtime & shutdown**
- [ ] 🔴 `Builder::new_multi_thread()` with `worker_threads`, `thread_name`, `thread_stack_size`, `on_thread_start/stop/park/unpark`
- [ ] 🔴 `Runtime::block_on` from outside — the calling thread drives the root future while workers run spawned tasks
- [ ] 🔴 Shutdown: signal all workers, drain all queues, shut down all tasks, join all threads
- [ ] 🔴 `Runtime::drop` blocks until shutdown completes; **no thread outlives the runtime**
- [ ] 🔴 `shutdown_timeout(dur)` for graceful shutdown with a deadline
- [ ] 🔴 Test: 100k tasks across 8 workers, all complete
- [ ] 🔴 Test: steal counts are non-zero under uneven load (the mechanism is actually exercised)

---

## Phase 5 — I/O Driver: Readiness (32 tasks)

**Platform abstraction**
- [ ] 🔴 `sys::Poller` trait: `new()`, `register(fd, token, interest)`, `reregister`, `deregister`, `wait(&mut events, timeout)`
- [ ] 🔴 `Interest { READABLE, WRITABLE }`, `Ready` bitflags with `is_readable`, `is_writable`, `is_read_closed`, `is_write_closed`, `is_error`
- [ ] 🔴 **Linux epoll**: `epoll_create1(EPOLL_CLOEXEC)`, `epoll_ctl`, `epoll_wait`
- [ ] 🔴 **Level-triggered by default.** Edge-triggered requires draining to `EWOULDBLOCK` on every wake, and one missed drain is a permanent hang
- [ ] 🔴 Edge-triggered behind a flag, with the drain requirement enforced in `AsyncRead`
- [ ] 🔴 `EPOLLRDHUP` handling for half-close detection
- [ ] 🟡 **macOS/BSD kqueue**: `kqueue()`, `kevent()` with `EVFILT_READ`/`EVFILT_WRITE`
- [ ] 🟡 kqueue reports read and write as **separate events** — must be merged into one `Ready`
- [ ] 🟡 **Windows IOCP**: `CreateIoCompletionPort`, `GetQueuedCompletionStatusEx`
- [ ] 🟡 IOCP is completion-based; emulate readiness with zero-byte `WSARecv` (this is what mio does, and it's worth documenting why)
- [ ] 🔴 **Waker fd**: `eventfd` (Linux) / `EVFILT_USER` (kqueue) / `PostQueuedCompletionStatus` (Windows) to interrupt a blocking wait

**Registration**
- [ ] 🔴 `Slab<Arc<ScheduledIo>>` — **the slab index IS the epoll token**, giving O(1) event→registration with no hashing
- [ ] 🔴 `ScheduledIo { readiness: AtomicUsize, waiters: Mutex<Waiters> }`
- [ ] 🔴 **Packed readiness: `[generation : 32][readiness : 16][shutdown : 1]`**
- [ ] 🔴 **Generation counter for fd reuse safety** — close fd 7, open a new socket that gets fd 7, and a stale in-flight event must be discarded
- [ ] 🔴 `Waiters { reader: Option<Waker>, writer: Option<Waker>, list: LinkedList<Waiter> }`
- [ ] 🔴 **Separate reader/writer wakers** — one socket can be read-waited and write-waited by different tasks; waking the wrong one is a hang
- [ ] 🔴 `Registration::new(io, interest)` — set `O_NONBLOCK`, register with the poller
- [ ] 🔴 `Registration::readiness(interest).await -> ReadyEvent`
- [ ] 🔴 `ReadyEvent::clear_ready()` — **essential**: epoll can report spuriously, and without clearing and re-waiting the task spins
- [ ] 🔴 `Registration::try_io(f)` — run a syscall, map `WouldBlock` to a readiness clear
- [ ] 🔴 `Drop` deregisters and bumps the generation

**Driver loop**
- [ ] 🔴 `Driver::park(timeout)`: wait → dispatch events → wake tasks
- [ ] 🔴 **The timeout comes from the timer wheel's next deadline** — this is the coupling point between I/O and time
- [ ] 🔴 Timeout is `Duration::ZERO` if any task is ready, `None` if nothing is pending
- [ ] 🔴 Dispatch: token → slab lookup → set readiness → take and wake the relevant wakers
- [ ] 🔴 Driver runs on whichever worker parks first; others block on a condvar
- [ ] 🔴 Test: register a socket, make it readable, assert the task is woken
- [ ] 🔴 Test: 10,000 concurrent registrations, all woken correctly
- [ ] 🔴 Test: fd reuse — close and reopen, assert no stale wake
- [ ] 🔴 Test: reader and writer on one socket woken independently
- [ ] 🔴 Test: spurious readiness → `clear_ready` → task re-waits without spinning

---

## Phase 6 — Timer Wheel (24 tasks)

- [ ] 🔴 `Wheel { elapsed: u64, levels: Box<[Level; 6]>, pending: LinkedList<TimerEntry> }`
- [ ] 🔴 `Level { occupied: u64, slots: [LinkedList<TimerEntry>; 64] }`
- [ ] 🔴 **`occupied` bitmap so "next non-empty slot" is one `trailing_zeros()`** instead of scanning 64 lists
- [ ] 🔴 64 slots per level makes indexing a shift+mask, not a division
- [ ] 🔴 `level_for(when)`: XOR deadline with elapsed, find the highest differing bit, divide by 6
- [ ] 🔴 `insert(entry)` — O(1)
- [ ] 🔴 `remove(entry)` — O(1) via the intrusive list (no search)
- [ ] 🔴 `next_expiration()` — scan levels for the earliest occupied slot
- [ ] 🔴 `advance_to(now)` — fire level 0, **cascade** higher levels down when they wrap
- [ ] 🔴 Cascading is amortized O(1): each timer moves down at most 6 times in its life
- [ ] 🔴 `TimerEntry` with intrusive `Pointers`, deadline, waker, and a state atomic
- [ ] 🔴 `TimerShared` state: `REGISTERED`, `PENDING`, `FIRED`, `CANCELLED`
- [ ] 🔴 Timer driver integrated into `Driver::park` — the wheel's next deadline is the wait timeout
- [ ] 🔴 `sleep(dur)` / `sleep_until(instant)` — `Sleep` future, resettable
- [ ] 🔴 `Sleep::reset(new_deadline)` without reallocating — the entry is removed and reinserted
- [ ] 🔴 `timeout(dur, fut)` — `Timeout` combinator returning `Result<T, Elapsed>`
- [ ] 🔴 `interval(period)` with `MissedTickBehavior::{ Burst, Delay, Skip }`
- [ ] 🟡 `DelayQueue<T>` for expiring collections (connection reaping)
- [ ] 🟡 Paused-time mode for tests: `time::pause()`, `time::advance(dur)`, auto-advance when all tasks are idle
- [ ] 🔴 Test: 100k timers inserted and cancelled — **O(1) confirmed by timing, not just asymptotics**
- [ ] 🔴 Test: timers fire in deadline order
- [ ] 🔴 Test: timer never fires early; lateness bounded by one tick + scheduling latency
- [ ] 🔴 Test: cascading across all 6 levels (a 2-year timer eventually reaches level 0)
- [ ] 🔴 Test: `sleep` in 10k concurrent tasks all complete within tolerance

---

## Phase 7 — Async I/O Types (26 tasks)

**Traits**
- [ ] 🔴 `AsyncRead::poll_read(Pin<&mut Self>, &mut Context, &mut ReadBuf) -> Poll<io::Result<()>>`
- [ ] 🔴 `AsyncWrite::poll_write` / `poll_flush` / `poll_shutdown`
- [ ] 🔴 `poll_write_vectored` + `is_write_vectored`
- [ ] 🔴 **`ReadBuf` tracking filled / initialized / remaining** — avoids zeroing buffers you're about to overwrite without exposing uninit memory to safe code
- [ ] 🔴 `AsyncReadExt` / `AsyncWriteExt`: `read`, `read_exact`, `read_to_end`, `read_to_string`, `write`, `write_all`, `flush`, `shutdown`
- [ ] 🔴 **Document cancel safety on every method.** `read_exact` is NOT cancel safe; `read` is

**Net types**
- [ ] 🔴 `TcpListener::bind` / `accept` — use `accept4(SOCK_NONBLOCK | SOCK_CLOEXEC)` where available
- [ ] 🔴 `TcpStream::connect` — nonblocking connect, wait for writable, check `SO_ERROR`
- [ ] 🔴 `TcpStream` read/write/peek, `set_nodelay`, `set_linger`, `peer_addr`, `local_addr`
- [ ] 🔴 `TcpStream::split()` (borrowed) and `into_split()` (owned)
- [ ] 🔴 `UdpSocket`: `send_to`, `recv_from`, `connect`, `send`, `recv`
- [ ] 🟡 `UnixListener` / `UnixStream` / `UnixDatagram`
- [ ] 🔴 `PollEvented<E>` wrapper generic over the raw type

**The I/O loop**
- [ ] 🔴 Every op: `readiness(interest).await` → `try_io(syscall)` → on `WouldBlock`, `clear_ready()` and loop
- [ ] 🔴 Handle `EINTR` by retrying
- [ ] 🔴 Handle partial writes in `write_all`
- [ ] 🔴 `poll_shutdown` → `shutdown(SHUT_WR)`, not `close`

**Adapters**
- [ ] 🟡 `BufReader` / `BufWriter` / `BufStream`
- [ ] 🟡 `io::copy` and `copy_bidirectional`
- [ ] 🟡 `io::empty`, `io::sink`, `io::repeat`
- [ ] 🟡 `AsyncBufRead` + `read_until`, `read_line`, `lines()`

**Tests**
- [ ] 🔴 Test: TCP echo — 1000 concurrent connections, all data correct
- [ ] 🔴 Test: large transfer (100 MB) with partial reads and writes
- [ ] 🔴 Test: half-close detected correctly
- [ ] 🔴 Test: connect to a closed port → the right error, not a hang
- [ ] 🔴 Test: `split()` — concurrent read and write on one socket
- [ ] 🔴 Test: UDP send/recv round trip

---

## Phase 8 — Synchronization Primitives (30 tasks)

**Foundation**
- [ ] 🔴 `batch_semaphore`: the primitive underneath everything else. Intrusive FIFO waiter list, multi-permit acquire
- [ ] 🔴 **Waiter node lives inside the future** (`PhantomPinned`) — zero allocation per wait, which matters when thousands of tasks contend
- [ ] 🔴 This is the clearest real demonstration of why `Pin` exists; document it as the canonical example
- [ ] 🔴 `Semaphore` public API: `acquire`, `acquire_many`, `try_acquire`, `add_permits`, `close`
- [ ] 🔴 `SemaphorePermit` / `OwnedSemaphorePermit` with `forget()`

**Locks**
- [ ] 🔴 `Mutex<T>` on a 1-permit semaphore; `lock().await -> MutexGuard`
- [ ] 🔴 `try_lock`, `get_mut`, `into_inner`, `lock_owned`
- [ ] 🔴 `RwLock<T>` — **write-preferring**, to prevent writer starvation under sustained read load
- [ ] 🔴 `read().await`, `write().await`, `try_read`, `try_write`, guard downgrade

**Notify**
- [ ] 🔴 `Notify` with `notified()`, `notify_one()`, `notify_waiters()`
- [ ] 🔴 **Stores one permit** so a `notify_one` arriving before any waiter isn't lost — this is the difference between `Notify` and a condvar, and the source of most "my task hangs" bugs when it's missing
- [ ] 🔴 `Notified` future must be pinned and `enable()`d to register before awaiting

**Channels**
- [ ] 🔴 `oneshot::channel()` — one value, `Sender::send` (not async), `Receiver` is a future
- [ ] 🔴 `oneshot::Sender::closed().await` — detect receiver drop for cancellation
- [ ] 🔴 `mpsc::channel(cap)` bounded — `send().await` awaits capacity (real backpressure, not a spin)
- [ ] 🔴 `mpsc::unbounded_channel()`
- [ ] 🔴 `Sender::reserve()` → `Permit` for cancel-safe sending
- [ ] 🔴 `Receiver::recv()` — **cancel safe**; a cancelled recv leaves the message in the channel
- [ ] 🔴 `recv_many(&mut Vec, limit)` for batching
- [ ] 🔴 Channel closes when all senders or the receiver drop
- [ ] 🟡 `broadcast::channel(cap)` — ring buffer, per-receiver cursor
- [ ] 🟡 `RecvError::Lagged(n)` when a receiver falls behind — slow receivers must not stall the sender
- [ ] 🟡 `watch::channel(init)` — latest value, `changed().await`, `borrow_and_update`

**Tests**
- [ ] 🔴 Test: Mutex under 100 concurrent tasks — mutual exclusion holds, FIFO fairness observed
- [ ] 🔴 Test: RwLock — writers not starved by continuous readers
- [ ] 🔴 Test: Notify permit — `notify_one()` before `notified().await` still wakes
- [ ] 🔴 Test: mpsc backpressure — bounded send blocks when full, resumes on recv
- [ ] 🔴 **Test: cancel `recv()` mid-flight → message not lost**
- [ ] 🔴 **loom: semaphore acquire/release with 2 waiters**
- [ ] 🔴 **loom: oneshot send racing with receiver drop**
- [ ] 🔴 **loom: Notify permit races**

---

## Phase 9 — Combinators & Cancellation (18 tasks)

- [ ] 🔴 `join!(a, b, ...)` — poll all, complete when all are Ready, `pin_project` for structural pinning
- [ ] 🔴 `try_join!` — short-circuit on first `Err`
- [ ] 🔴 `select!` proc-macro — poll branches in **random order** by default to avoid starving later branches
- [ ] 🔴 `select!` with `biased;` for deterministic priority order
- [ ] 🔴 `select!` `else` and `if` guards on branches
- [ ] 🔴 **`select!` drops the losing futures** — document that this is cancellation, and link to cancel safety
- [ ] 🔴 `race(a, b)` for same-typed futures
- [ ] 🔴 `timeout(dur, fut)` / `timeout_at(deadline, fut)`
- [ ] 🟡 `FuturesUnordered` — dynamic set with an intrusive ready-list so only woken members are polled (not O(n) per poll)
- [ ] 🟡 `JoinSet` — spawn into a set, `join_next().await`, `abort_all()`
- [ ] 🟡 `StreamExt` basics if a `Stream` trait is included
- [ ] 🔴 `poll_fn`, `pending`, `ready`, `yield_now`
- [ ] 🟡 `CancellationToken` — `cancelled().await`, `child_token()`, structured cancellation trees
- [ ] 🔴 Test: `join!` — all complete, results in order
- [ ] 🔴 Test: `select!` — loser is dropped exactly once
- [ ] 🔴 Test: `select!` branch fairness over 10,000 iterations (random ordering actually randomizes)
- [ ] 🔴 Test: `timeout` — both the completion and the elapsed path
- [ ] 🟡 Test: `FuturesUnordered` with 10k members polls only woken ones (instrument poll counts)

---

## Phase 10 — Blocking Pool (14 tasks)

- [ ] 🔴 `BlockingPool` with a dynamically-sized thread set
- [ ] 🔴 `max_blocking_threads` default 512 — **deliberately large**, since these threads are blocked on I/O, not consuming CPU
- [ ] 🔴 `keep_alive` default 10 s; idle threads exit
- [ ] 🔴 `spawn_blocking(f) -> JoinHandle<R>`
- [ ] 🔴 Job queue with a condvar; threads spawned lazily on demand
- [ ] 🔴 `block_in_place(f)` — multi-thread only; hands the current worker's queue to a replacement so blocking work can borrow non-`'static` data
- [ ] 🔴 `block_in_place` inside a current-thread runtime must panic with a clear message, not deadlock
- [ ] 🔴 Shutdown: stop accepting, wait for in-flight jobs, join threads
- [ ] 🔴 Blocking-pool tasks are not cancellable once started (document this — it surprises people)
- [ ] 🟡 `spawn_blocking` from outside the runtime → clear panic
- [ ] 🔴 Test: 1000 blocking jobs complete
- [ ] 🔴 Test: pool grows to the cap and no further
- [ ] 🔴 Test: idle threads exit after `keep_alive`
- [ ] 🔴 Test: `block_in_place` doesn't deadlock the runtime when all workers use it

---

## Phase 11 — Cooperative Budget & Fairness (12 tasks)

- [ ] 🔴 `coop::Budget(Option<u8>)` in a thread-local, `BUDGET = 128`
- [ ] 🔴 `poll_proceed(cx)` — decrement; when exhausted, `wake_by_ref()` and return Pending
- [ ] 🔴 Budget reset at the start of each task poll
- [ ] 🔴 **Every resource operation consumes budget**: channel recv, mutex lock, semaphore acquire, I/O readiness
- [ ] 🔴 `unconstrained(fut)` to opt out
- [ ] 🔴 `block_on` runs unconstrained
- [ ] 🔴 `yield_now()` — explicit cooperation point
- [ ] 🔴 `has_budget_remaining()` for library authors
- [ ] 🔴 **Test: `loop { rx.recv().await }` on an always-ready channel does not starve other tasks** — this is the canonical failure the budget exists to prevent
- [ ] 🔴 Test: budget exhaustion forces a yield after exactly 128 ops
- [ ] 🔴 Test: `unconstrained` genuinely bypasses it
- [ ] 🟡 Emit a `BudgetExhausted` event for the console

---

## Phase 12 — io_uring Backend (20 tasks)

**Linux only, feature-gated. The most conceptually interesting part of the project.**

- [ ] 🟡 Detect io_uring support at runtime; fall back to epoll
- [ ] 🟡 `IoUring::new(entries)` with SQ/CQ ring setup
- [ ] 🟡 `OpState` slab keyed by the SQE's `user_data`
- [ ] 🟡 Submit on first poll of the future; complete on CQE
- [ ] 🟡 **The buffer-ownership problem**: the kernel writes into your buffer after the future is dropped. This is a use-after-free the borrow checker cannot see
- [ ] 🟡 **Solution: owned-buffer API.** `read(buf: Vec<u8>) -> (io::Result<usize>, Vec<u8>)` — buffer in, buffer back out
- [ ] 🟡 Document the three alternatives (copy / leak / owned buffers) and why owned buffers win
- [ ] 🟡 **`orphaned: Slab<OrphanedOp>`** — a cancelled op's buffer lives here until its CQE arrives, then is dropped. Bounds the leak to in-flight cancelled ops
- [ ] 🟡 `IORING_OP_ASYNC_CANCEL` to cancel where the kernel supports it
- [ ] 🟡 Ops: `READ`, `WRITE`, `READV`, `WRITEV`, `ACCEPT`, `CONNECT`, `SEND`, `RECV`, `CLOSE`, `TIMEOUT`
- [ ] 🟡 Batched submission — one `io_uring_enter` per park, not per operation
- [ ] 🟡 `SQPOLL` mode behind a flag (kernel polls the SQ, near-zero syscalls)
- [ ] 🟡 Registered fixed buffers to skip per-op pinning
- [ ] 🟡 Registered file descriptors to skip refcounting
- [ ] 🟡 Multishot accept/recv where the kernel supports it
- [ ] 🟡 `AsyncReadOwned` / `AsyncWriteOwned` traits
- [ ] 🟡 Test: read a file, verify contents
- [ ] 🟡 **Test: drop a future mid-read → buffer is retained until the CQE, no UAF under Miri/ASan**
- [ ] 🟡 Test: 10k concurrent ops complete
- [ ] 🟡 Benchmark: echo server, io_uring vs epoll

---

## Phase 13 — Instrumentation (18 tasks)

- [ ] 🔴 `RuntimeEvent` enum with all variants from SPEC §15
- [ ] 🔴 `TaskId`, `WakeSource`, `Location` (via `#[track_caller]` at spawn)
- [ ] 🔴 Feature-gated emission: zero cost when disabled (verify in the disassembly)
- [ ] 🔴 `TaskSpawned` with name, spawn location, and parent task
- [ ] 🔴 `TaskPollStart` / `TaskPollEnd` with duration and result
- [ ] 🔴 **`TaskWoken { by: WakeSource }`** — the causality edge that makes the wake graph possible
- [ ] 🔴 `WorkerPark` / `WorkerUnpark` / `WorkerSteal` / `QueueDepth`
- [ ] 🔴 `IoRegistered` / `IoReady`
- [ ] 🔴 `TimerSet` / `TimerFired { lateness }` — **lateness measures wheel accuracy directly**
- [ ] 🔴 **`BlockingDetected`** when a poll exceeds a threshold (default 100 ms), with the task's spawn location
- [ ] 🔴 `BudgetExhausted`, `ResourceContended`
- [ ] 🔴 Per-task cumulative stats: polls, busy, idle, scheduled
- [ ] 🔴 Poll-duration histogram per task (HdrHistogram-style log buckets)
- [ ] 🔴 Runtime-level metrics: active tasks, queue depths, steal count, park count, worker busy ratio
- [ ] 🔴 Unix-socket subscriber; length-prefixed bincode frames
- [ ] 🟡 `RuntimeMetrics` handle for programmatic access
- [ ] 🟡 Task dump: capture all task states and their await-point backtraces on demand
- [ ] 🔴 Benchmark: instrumentation overhead < 5 %

---

## Phase 14 — TUI Console (16 tasks)

- [ ] 🟡 `eddy-console` binary; connects to the Unix socket
- [ ] 🟡 ratatui + crossterm; alternate screen, raw mode, restore on panic
- [ ] 🟡 Task list: ID, name, state, Total, Busy, Idle, Sched, Polls, location
- [ ] 🟡 Sortable columns (`t` total, `b` busy, `i` idle, `p` polls)
- [ ] 🟡 Filter by name/state (`/`)
- [ ] 🟡 Task detail (Enter): poll-duration histogram, scheduled-duration histogram, wake sources
- [ ] 🟡 **Warning lamps**: blocking detected, never yielded, high poll variance
- [ ] 🟡 Resource view: mutexes/semaphores with holder and waiter queue
- [ ] 🟡 Worker view: per-worker busy ratio, queue depth, park count, steal count
- [ ] 🟡 Async-op view: which tasks are waiting on which resources
- [ ] 🟡 Pause/resume the live feed (`space`)
- [ ] 🟡 Help overlay (`?`)
- [ ] 🟢 Color themes; respects `NO_COLOR`
- [ ] 🟢 Record to file and replay
- [ ] 🟡 Test: connects and renders against a synthetic event stream
- [ ] 🟡 Test: handles runtime disconnect gracefully

---

## Phase 15 — Web Console (28 tasks)

**Foundation**
- [ ] 🟡 `eddy-console-web`: bridges the Unix socket to a WebSocket
- [ ] 🟡 zustand event store with a bounded ring buffer (last N seconds)
- [ ] 🟡 App shell, connection status, time-range selector, pause/resume

**View 1 — Task Lifecycle Swimlanes ⭐**
- [ ] 🟡 D3: X = time, one lane per task
- [ ] 🟡 Segments colored gray (idle) / yellow (scheduled) / green (running)
- [ ] 🟡 **Starvation is visually obvious** — a solid yellow stretch means the task was ready and ignored
- [ ] 🟡 Hover a segment → poll duration and wake source
- [ ] 🟡 Click → task detail with spawn location
- [ ] 🟡 Zoom and pan on the time axis
- [ ] 🟢 Filter to a task subtree (spawned-by relationships)

**View 2 — Worker & Queue Heatmap ⭐**
- [ ] 🟡 D3: one row per worker, X = time, cell color = local queue depth
- [ ] 🟡 **Steal events as arrows** from victim row to thief row
- [ ] 🟡 Global queue depth as a separate strip
- [ ] 🟡 Park/unpark markers
- [ ] 🟡 A saturated worker beside idle ones is immediately visible

**View 3 — Wake Causality Graph ⭐**
- [ ] 🟡 D3 + dagre: nodes = tasks, edges = "woke"
- [ ] 🟡 Edge thickness = wake count
- [ ] 🟡 **Cycle highlighting** — a wake cycle is usually a busy-loop bug
- [ ] 🟡 Root nodes (woken by I/O or timer) distinguished from task-woken ones
- [ ] 🟡 Click a node → that task's swimlane
- [ ] 🟢 Time-scrubbing: show only wakes in the selected window

**View 4 — Poll Duration Distribution**
- [ ] 🟡 Recharts histograms per task
- [ ] 🟡 p50 / p99 / max markers
- [ ] 🟡 **Red threshold line at 100 ms: "blocking the executor"**
- [ ] 🟡 Sort tasks by p99 to surface the one bad handler
- [ ] 🟡 Click → jump to that task's spawn location

**View 5 — Runtime Metrics**
- [ ] 🟡 Recharts time series: active tasks, queue depths, steal rate, park rate, worker busy ratio
- [ ] 🟡 Timer lateness distribution (validates the wheel)

---

## Phase 16 — Macros (10 tasks)

- [ ] 🔴 `eddy-macros` proc-macro crate
- [ ] 🔴 `#[eddy::main]` — wraps `fn main` in `Runtime::new().block_on(...)`
- [ ] 🔴 `#[eddy::main(flavor = "current_thread")]`, `worker_threads = N`
- [ ] 🔴 `#[eddy::test]` — same, for tests, defaulting to current-thread
- [ ] 🔴 `#[eddy::test(start_paused = true)]` for deterministic timer tests
- [ ] 🔴 `select!` macro: random branch order, `biased`, `else`, per-branch `if` guards
- [ ] 🔴 `join!` / `try_join!` macros
- [ ] 🔴 Preserve spans so errors point at user code, not macro internals
- [ ] 🔴 `trybuild` UI tests for macro error messages
- [ ] 🟡 Test: macro-generated code compiles under `#![deny(warnings)]`

---

## Phase 17 — Correctness: loom, Miri, Differential (22 tasks)

**loom** — *the* correctness tool for this project
- [ ] 🔴 `sync` shim module re-exporting `loom::sync` under `--cfg loom`
- [ ] 🔴 loom: task state transitions (poll / wake / complete / abort interleavings)
- [ ] 🔴 loom: refcount + dealloc — exactly one dealloc, never early
- [ ] 🔴 **loom: wake during poll** — never lost, never double-queued
- [ ] 🔴 loom: Chase-Lev push/pop/steal with 1 producer + 2 thieves
- [ ] 🔴 loom: `push_overflow` racing with a steal
- [ ] 🔴 loom: injector concurrent push/pop
- [ ] 🔴 loom: semaphore acquire/release/close
- [ ] 🔴 loom: `Notify` permit races
- [ ] 🔴 loom: oneshot send / recv / drop races
- [ ] 🔴 loom: `ScheduledIo` readiness + waker registration races
- [ ] 🔴 loom: timer entry fire vs cancel race
- [ ] 🔴 **CI runs loom on every PR** (bounded preemptions to keep it under 10 minutes)

**Miri**
- [ ] 🔴 Miri over the task allocation, waker vtable, intrusive lists, `ReadBuf`
- [ ] 🔴 Stacked-borrows clean (no `-Zmiri-tag-raw-pointers` suppressions)
- [ ] 🔴 `-Zmiri-strict-provenance` clean

**Differential vs Tokio**
- [ ] 🟡 Same workload on both → same results
- [ ] 🟡 Channel semantics match exactly (capacity, close, lag)
- [ ] 🟡 Timer behavior matches within tolerance
- [ ] 🟡 `select!` cancellation semantics match

**Cancel safety & stress**
- [ ] 🔴 Property test: drop each shipped future at a random poll count → no data loss for documented cancel-safe ones
- [ ] 🔴 Stress: 10-minute soak at max concurrency, no leaks (RSS flat), no deadlocks
- [ ] 🔴 Watchdog: if no task completes for 30 s, dump all task states and fail
- [ ] 🔴 **ARM64 CI** — weak memory ordering exposes bugs x86's TSO hides

---

## Phase 18 — Benchmarks & Polish (16 tasks)

- [ ] 🟡 `criterion` suite mirroring Tokio's benches for direct comparison
- [ ] 🟡 spawn + join throughput (1M tasks)
- [ ] 🟡 ping-pong (2 tasks, 1M round trips)
- [ ] 🟡 echo server (10k connections, 64 B messages)
- [ ] 🟡 timer churn (100k concurrent sleeps)
- [ ] 🟡 channel throughput (bounded and unbounded)
- [ ] 🟡 mutex contention (1..64 tasks)
- [ ] 🟡 work-steal latency (idle worker → first task)
- [ ] 🟡 Task allocation size assertion (`size_of::<Cell<F, S>>()`)
- [ ] 🟡 Instrumentation overhead measurement
- [ ] 🟡 Results table in the README with real numbers vs Tokio
- [ ] 🟢 `docs/ARCHITECTURE.md` — the event → waker → queue → poll cycle
- [ ] 🟢 `docs/TASK.md` — allocation layout, state machine, refcounting
- [ ] 🟢 `docs/PIN.md` — why `Pin` exists, using the intrusive waiter as the worked example
- [ ] 🟢 `docs/CANCEL_SAFETY.md` — the full analysis for every shipped future
- [ ] 🟢 `examples/`: echo, chat, HTTP server, TCP proxy, graceful shutdown, `spawn_blocking` bridge

---

## Summary

| Phase | Tasks |
|---|---|
| 0. Bootstrap | 14 |
| 1. Task Representation | 28 |
| 2. The Waker | 18 |
| 3. Current-Thread Executor | 16 |
| 4. Multi-Thread Scheduler | 34 |
| 5. I/O Driver (readiness) | 32 |
| 6. Timer Wheel | 24 |
| 7. Async I/O Types | 26 |
| 8. Sync Primitives | 30 |
| 9. Combinators & Cancellation | 18 |
| 10. Blocking Pool | 14 |
| 11. Cooperative Budget | 12 |
| 12. io_uring Backend | 20 |
| 13. Instrumentation | 18 |
| 14. TUI Console | 16 |
| 15. Web Console | 28 |
| 16. Macros | 10 |
| 17. loom / Miri / Differential | 22 |
| 18. Benchmarks & Polish | 16 |
| **TOTAL** | **396** |
