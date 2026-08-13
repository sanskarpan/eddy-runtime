# CHECKLIST.md — `eddy`: An Async Runtime from Scratch

> Priority: 🔴 blocking · 🟡 important · 🟢 enhancement · 🔵 stretch
> **Phase 1–3 (task + waker + current-thread executor) is the spine. Nothing else can be built or debugged until a single future can be spawned, polled, woken, and completed on one thread.**
> **`loom` model checking is not optional. Every lock-free structure gets a loom test in the same commit it is written.**

---

## Phase 0 — Bootstrap (14 tasks)

- [x]] 🔴 `cargo new --lib eddy`; workspace with 4 member crates per SPEC §17 (`crates/eddy{,-macros,-console,-console-web}`; `cargo metadata` lists 4)
- [x] 🔴 `cargo add libc slab parking_lot pin-project-lite tracing crossbeam-utils` — all six in `crates/eddy/Cargo.toml:8`
- [x] 🔴 `cargo add --dev loom criterion proptest futures tokio` — **`tokio` and `futures` are test-only oracles**; all five in `Cargo.toml:19`
- [x] 🔴 **Forbid runtime deps in the main crate**: CI check that `cargo tree -p eddy --edges normal` contains no `tokio`, `async-std`, `smol`, `mio`, `polling`, or `futures-executor`
- [x] 🔴 `[target.'cfg(loom)'.dependencies] loom = "0.7"` with a `loom` cfg flag and a `sync` shim module re-exporting either `std::sync` or `loom::sync`
- [x] 🔴 `#![deny(clippy::undocumented_unsafe_blocks)]` workspace-wide
- [x] 🔴 CI matrix: `x86_64-linux`, `aarch64-linux` (QEMU — **weak memory ordering exposes bugs x86 hides**), `aarch64-macos`, `x86_64-windows`
- [x] 🔴 CI jobs: `test`, `test-loom` (`RUSTFLAGS="--cfg loom"`), `miri` (bench not in CI)
- [x] 🔴 `Makefile`/`justfile`: `test`, `loom`, `miri`, `bench`, `bench-vs-tokio`, `console`, `console-web`
- [x] 🔴 `util/rand.rs`: xorshift `FastRand` for steal-victim selection (no `rand` dependency)
- [x] 🔴 `util/linked_list.rs`: intrusive doubly-linked list with `Pointers<T>` — used by waiter lists and the timer wheel (SB-hardened, Miri-clean)
- [x]] 🔴 `cd console-ui && bun create vite . --template react-ts` (`console-ui/`, vite react-ts)
- [x]] 🔴 `bun add d3 @dagrejs/dagre recharts zustand clsx lucide-react`; `bun add -d tailwindcss postcss autoprefixer @types/d3`; `bunx shadcn@latest init` + `button card table tabs badge tooltip separator scroll-area resizable select slider` (`console-ui/`; 10 UI components added)
- [x] 🔴 Spike: hand-write a `Waker` with a `RawWakerVTable`, poll a trivial future to completion on one thread, no dependencies. **Do this first** — if the vtable refcounting is wrong, everything above it is unfixable.

---

## Phase 1 — Task Representation (30 tasks)

**Structure**
- [x] 🔴 `Header { state: AtomicUsize, vtable: &'static Vtable, owner_id: u32, queue_next: UnsafeCell<Option<NonNull<Header>>> }`
- [x] 🔴 `Core<F, S> { scheduler: S, stage: UnsafeCell<Stage<F>> }` with `Stage::{ Running(F), Finished(F::Output), Consumed }`
- [x] 🔴 `Trailer { waker: UnsafeCell<Option<Waker>> }` for the `JoinHandle`'s waker
- [x] 🔴 `#[repr(C)] Cell<F, S>` — **one allocation** holding header + core + trailer
- [x] 🔴 `RawTask(NonNull<Header>)` — one word, type-erased, vtable reached through the header
- [x] 🔴 `Vtable` with `poll`, `schedule`, `dealloc`, `try_read_output`, `drop_join_handle_slow`, `shutdown`
- [x] 🔴 `vtable::<F, S>()` const fn producing a `&'static Vtable` per (future, scheduler) pair
- [x] 🔴 **`queue_next` is the intrusive link** — the global injector is a list threaded through tasks, allocating nothing per push (`scheduler/multi_thread/inject.rs`) and the current-thread scheduler's injection list

**State machine**
- [x] 🔴 Packed state: `[ refcount : 48 ][ flags : 16 ]` in one `AtomicUsize`
- [x] 🔴 Flags: `RUNNING`, `COMPLETE`, `NOTIFIED`, `CANCELLED`, `JOIN_INTEREST`, `JOIN_WAKER_SET`
- [x] 🔴 `transition_to_running()` — fails if already RUNNING or COMPLETE
- [x] 🔴 `transition_to_idle()` — returns whether NOTIFIED was set while running (→ must re-queue)
- [x] 🔴 `transition_to_complete()` — sets COMPLETE, wakes the JoinHandle waker
- [x] 🔴 **`transition_to_notified()`**: if RUNNING, set NOTIFIED and do nothing (the poller re-queues); else set NOTIFIED and schedule
- [x] 🔴 `ref_inc()` / `ref_dec()` returning whether the count hit zero
- [x] 🔴 Refcount overflow check — abort rather than wrap
- [x] 🔴 Every transition is a single `fetch_update` CAS, documented with the invariant it preserves

**Harness (the poll driver)**
- [x] 🔴 `Harness::poll()`: transition to running → build waker → poll → transition to idle-or-complete
- [x] 🔴 Catch panics in `poll` (`catch_unwind`), store as `JoinError::Panic`, do not poison the runtime
- [x] 🔴 `Harness::dealloc()`: drop the stage in place, then free the allocation
- [x] 🔴 `Harness::shutdown()`: cancel without polling, for runtime teardown

**JoinHandle**
- [x] 🔴 `JoinHandle<T>` implementing `Future<Output = Result<T, JoinError>>`
- [x] 🔴 Registers its waker in the trailer; woken on completion
- [x] 🔴 `abort()` — sets CANCELLED; a not-yet-running task is dropped without polling
- [x] 🔴 Dropping a `JoinHandle` detaches the task (it keeps running) and drops one reference
- [x] 🔴 `JoinError::{ Panic(Box<dyn Any + Send>), Cancelled }`
- [x] 🔴 `AbortHandle` — abort without holding the output

**Tests**
- [x] 🔴 Test: spawn, poll to completion, output readable via JoinHandle
- [x] 🔴 Test: allocation counter proves exactly one alloc per task and one dealloc
- [x] 🔴 Test: panicking future → `JoinError::Panic`, runtime still usable

---

## Phase 2 — The Waker (18 tasks)

**This is the hardest `unsafe` in the project.**

- [x] 🔴 `WAKER_VTABLE: RawWakerVTable` with the four functions
- [x] 🔴 `clone_waker`: `ref_inc()`, return a new `RawWaker` with the same data pointer — **+1**
- [x] 🔴 `wake_waker`: **consumes** the reference — schedule, and the schedule takes ownership
- [x] 🔴 `wake_by_ref_waker`: **does not consume** — schedule with an explicit `ref_inc()` for the queued reference
- [x] 🔴 `drop_waker`: `ref_dec()`, dealloc if zero — **-1**
- [x] 🔴 **Document the +1/-1 contract on every function.** Getting `wake` vs `wake_by_ref` backwards leaks every task or frees queued ones
- [x] 🔴 `waker_from_raw(NonNull<Header>) -> Waker`
- [x] 🔴 `WakerRef<'a>` — a borrowed waker for the common poll path, avoiding a clone per poll
- [x] 🔴 `noop_waker()` for tests
- [x] 🔴 Implement `Waker::will_wake` correctly (same data pointer + same vtable) so combinators can skip redundant clones

**Tests**
- [x] 🔴 Test: clone/drop a waker 10,000 times → refcount returns to baseline, no leak
- [x] 🔴 Test: `wake()` on a pending task schedules exactly once
- [x] 🔴 Test: `wake()` twice before poll schedules only once (NOTIFIED is idempotent)
- [x] 🔴 **Test: `wake()` during poll → task is re-queued after the poll returns Pending** (the lost-wakeup case)
- [x] 🔴 Test: `wake()` after completion is a no-op and does not resurrect the task
- [x] 🔴 Test: waker outliving the task keeps it alive; dropping the last waker deallocs
- [x] 🔴 **loom: wake during poll from another thread** — asserts the task ends up completed or queued, never lost, never double-queued
- [x] 🔴 **loom: concurrent clone + drop from two threads** — refcount is exact, exactly one dealloc

---

## Phase 3 — Current-Thread Executor (16 tasks)

**Build this before the multi-thread scheduler. It proves the task/waker machinery in isolation.**

- [x] 🔴 `CurrentThread { queue: VecDeque<Notified>, ... }` — plain deque, no atomics on the hot path
- [x] 🔴 `block_on(fut)`: park the current thread, poll the root future, run queued tasks between polls
- [x] 🔴 `block_on` uses a thread-parking waker (`Thread::unpark`) for the root future
- [x] 🔴 `spawn(fut)` from inside `block_on` — supports `!Send` futures
- [x] 🔴 Injection queue for wakes arriving from other threads while `block_on` is running
- [x] 🔴 Fairness: check the injection queue every `GLOBAL_QUEUE_INTERVAL` polls
- [x] 🔴 `CURRENT` thread-local holding the runtime `Handle`; `Handle::current()` with a clear panic message when outside a runtime
- [x] 🔴 `EnterGuard` for `Handle::enter()`
- [x] 🔴 Shutdown: drain the queue, drop all tasks at their next await point
- [x] 🔴 `Builder::new_current_thread()`
- [x] 🔴 **No LIFO slot** in this scheduler (nothing to gain, and it complicates fairness)
- [x] 🔴 Test: `block_on(async { 42 })` returns 42
- [x] 🔴 Test: spawn 1000 tasks, all complete, results correct
- [x] 🔴 Test: nested spawn (a task spawns a task) works
- [x] 🔴 Test: `!Send` future compiles and runs
- [x] 🔴 Test: wake from another thread while blocked in `block_on` makes progress

---

## Phase 4 — Multi-Thread Scheduler (37 tasks)

**Chase-Lev deque**
- [x] 🔴 `LOCAL_QUEUE_CAPACITY = 256`, fixed-size ring buffer
- [x] 🔴 **Head is one `AtomicU64` packing `[steal_head : 32][real_head : 32]`** — two separate atomics allow a torn read where the owner hands the same task to two threads
- [x] 🔴 `tail: AtomicU32`, written only by the owner
- [x] 🔴 `push_back(task)` — owner only, returns `Err(task)` on full
- [x] 🔴 `pop()` — owner only, must not race with an in-flight steal
- [x] 🔴 `steal_into(dst)` — CAS steal_head to claim a range, copy, then CAS real_head
- [x] 🔴 **Steal takes HALF** the victim's queue, not one task
- [x] 🔴 `push_overflow`: on full, move the **older half** to the injector and keep the new task local — moving the new one causes ping-pong
- [x] 🔴 Memory ordering documented per operation (Acquire/Release/AcqRel), with the reason
- [x] 🔴 **loom: single producer + two thieves** — no task lost, no task duplicated
- [x] 🔴 **loom: push_overflow racing with a steal**

**Injector**
- [x] 🔴 Mutex-guarded **intrusive linked list** threaded through `Header::queue_next` — zero allocation per push
- [x] 🔴 `push(task)`, `push_batch(iter)`, `pop()`, `pop_n(n)`
- [x] 🔴 `is_closed` flag for shutdown
- [x] 🔴 loom: concurrent push/pop

**Worker**
- [x] 🔴 `Core { tick, lifo_slot, lifo_polls, run_queue, is_searching, park, rand }`
- [x] 🔴 `Shared { remotes: Vec<Remote>, inject, idle, shutdown }`
- [x] 🔴 Main loop: `next_task` → poll → repeat; park when nothing found
- [x] 🔴 `next_task`: **every `GLOBAL_QUEUE_INTERVAL` (61) ticks, check the injector FIRST** — otherwise a self-replenishing local queue starves external spawns forever
- [x] 🔴 `next_local_task`: LIFO slot, then run queue
- [x] 🔴 **LIFO slot**: when a task wakes another on the same worker, the woken task goes here for cache locality
- [x] 🔴 **`MAX_LIFO_POLLS_PER_TICK = 3`** — caps consecutive LIFO polls so a ping-pong pair can't monopolize the worker
- [x] 🔴 LIFO slot is **not stealable** (it's the locality optimization; stealing it defeats the purpose)
- [x] 🔴 `disable_lifo_slot()` builder option
- [x] 🔴 `steal_work`: **random starting victim**, take half, try each worker once
- [x] 🔴 Random start prevents the convoy where every idle worker hammers worker 0
- [x] 🔴 `is_searching` flag + a searching-worker count, capped at half the workers, to avoid a steal storm
- [x] 🔴 Park: register as idle, block on the driver, unpark on event or `unpark()`
- [x] 🔴 **Last searching worker to find work unparks another** — keeps parallelism from collapsing to one worker
- [x] 🔴 `IdleState` tracking parked workers as a bitmap for O(1) "who to unpark"

**Runtime & shutdown**
- [x] 🔴 `Builder::new_multi_thread()` with `worker_threads`, `thread_name`, `thread_stack_size`, `on_thread_start/stop/park/unpark`
- [x] 🔴 `Runtime::block_on` from outside — the calling thread drives the root future while workers run spawned tasks
- [x] 🔴 Shutdown: signal all workers, drain all queues, shut down all tasks, join all threads
- [x] 🔴 `Runtime::drop` blocks until shutdown completes; **no thread outlives the runtime**
- [x] 🔴 `shutdown_timeout(dur)` for graceful shutdown with a deadline
- [x] 🔴 Test: 100k tasks across 8 workers, all complete
- [x] 🔴 Test: steal counts are non-zero under uneven load (the mechanism is actually exercised)

---

## Phase 5 — I/O Driver: Readiness (32 tasks)

**Platform abstraction**
- [x] 🔴 `sys::Poller` trait: `new()`, `register(fd, token, interest)`, `reregister`, `deregister`, `wait(&mut events, timeout)`
- [x] 🔴 `Interest { READABLE, WRITABLE }`, `Ready` bitflags with `is_readable`, `is_writable`, `is_read_closed`, `is_write_closed`, `is_error`
- [x] 🔴 **Linux epoll**: `epoll_create1(EPOLL_CLOEXEC)`, `epoll_ctl`, `epoll_wait`
- [x] 🔴 **Level-triggered by default.** Edge-triggered requires draining to `EWOULDBLOCK` on every wake, and one missed drain is a permanent hang
- [x] 🔴 Edge-triggered behind a flag, with the drain requirement enforced in `AsyncRead`
- [x] 🔴 `EPOLLRDHUP` handling for half-close detection
- [x] 🟡 **macOS/BSD kqueue**: `kqueue()`, `kevent()` with `EVFILT_READ`/`EVFILT_WRITE`
- [x] 🟡 kqueue reports read and write as **separate events** — must be merged into one `Ready`
- [x] 🟡 **Windows IOCP**: `CreateIoCompletionPort`, `GetQueuedCompletionStatusEx` — native backend in `sys/iocp.rs`: sockets associated to a completion port with the token as the completion key; `wait` drains `GetQueuedCompletionStatusEx` into the driver's event vector; wakes are `PostQueuedCompletionStatus` sentinels; verified via `cargo check/clippy --target x86_64-pc-windows-msvc` (the driver, `PollEvented` and net types are `#[cfg(unix)]`; the io driver compiles for Windows through a platform `Fd` alias in `sys/mod.rs`)
- [x] 🟡 IOCP is completion-based; emulate readiness with zero-byte `WSARecv` (this is what mio does, and it's worth documenting why) — one outstanding zero-byte `WSARecv` probe per socket registered for reads; probes complete only when the socket becomes readable or closes, and are re-issued per completion for level-triggered semantics (mirroring epoll LT). Two quirks documented in `sys/iocp.rs`: a zero-byte receive that finds buffered data completes synchronously with no completion packet (reported through an internal pending list), and there is no completion-based write probe — writes are approximated with a one-shot `WRITABLE` plus re-reporting on every read completion
- [x] 🔴 **Waker fd**: `eventfd` (Linux) / `EVFILT_USER` (kqueue) / `PostQueuedCompletionStatus` (Windows) to interrupt a blocking wait

**Registration**
- [x] 🔴 `Slab<Arc<ScheduledIo>>` — **the slab index IS the epoll token**, giving O(1) event→registration with no hashing
- [x] 🔴 `ScheduledIo { readiness: AtomicUsize, waiters: Mutex<Waiters> }`
- [x] 🔴 **Packed readiness: `[generation : 32][readiness : 16][shutdown : 1]`**
- [x] 🔴 **Generation counter for fd reuse safety** — close fd 7, open a new socket that gets fd 7, and a stale in-flight event must be discarded
- [x] 🔴 `Waiters { reader: Option<Waker>, writer: Option<Waker>, list: LinkedList<Waiter> }`
- [x] 🔴 **Separate reader/writer wakers** — one socket can be read-waited and write-waited by different tasks; waking the wrong one is a hang
- [x] 🔴 `Registration::new(io, interest)` — set `O_NONBLOCK`, register with the poller
- [x] 🔴 `Registration::readiness(interest).await -> ReadyEvent`
- [x] 🔴 `ReadyEvent::clear_ready()` — **essential**: epoll can report spuriously, and without clearing and re-waiting the task spins
- [x] 🔴 `Registration::try_io(f)` — run a syscall, map `WouldBlock` to a readiness clear
- [x] 🔴 `Drop` deregisters and bumps the generation

**Driver loop**
- [x] 🔴 `Driver::park(timeout)`: wait → dispatch events → wake tasks
- [x] 🔴 **The timeout comes from the timer wheel's next deadline** — this is the coupling point between I/O and time
- [x] 🔴 Timeout is `Duration::ZERO` if any task is ready, `None` if nothing is pending
- [x] 🔴 Dispatch: token → slab lookup → set readiness → take and wake the relevant wakers
- [x] 🔴 Driver runs on whichever worker parks first; others block on a condvar
- [x] 🔴 Test: register a socket, make it readable, assert the task is woken
- [x] 🔴 Test: 10,000 concurrent registrations, all woken correctly
- [x] 🔴 Test: fd reuse — close and reopen, assert no stale wake
- [x] 🔴 Test: reader and writer on one socket woken independently
- [x] 🔴 Test: spurious readiness → `clear_ready` → task re-waits without spinning

---

## Phase 6 — Timer Wheel (24 tasks)

- [x] 🔴 `Wheel { elapsed: u64, levels: Box<[Level; 6]>, pending: LinkedList<TimerEntry> }`
- [x] 🔴 `Level { occupied: u64, slots: [LinkedList<TimerEntry>; 64] }`
- [x] 🔴 **`occupied` bitmap so "next non-empty slot" is one `trailing_zeros()`** instead of scanning 64 lists
- [x] 🔴 64 slots per level makes indexing a shift+mask, not a division
- [x] 🔴 `level_for(when)`: XOR deadline with elapsed, find the highest differing bit, divide by 6
- [x] 🔴 `insert(entry)` — O(1)
- [x] 🔴 `remove(entry)` — O(1) via the intrusive list (no search)
- [x] 🔴 `next_expiration()` — scan levels for the earliest occupied slot
- [x] 🔴 `advance_to(now)` — fire level 0, **cascade** higher levels down when they wrap
- [x] 🔴 Cascading is amortized O(1): each timer moves down at most 6 times in its life
- [x] 🔴 `TimerEntry` with intrusive `Pointers`, deadline, waker, and a state atomic
- [x] 🔴 `TimerShared` state: `REGISTERED`, `PENDING`, `FIRED`, `CANCELLED`
- [x] 🔴 Timer driver integrated into `Driver::park` — the wheel's next deadline is the wait timeout
- [x] 🔴 `sleep(dur)` / `sleep_until(instant)` — `Sleep` future, resettable
- [x] 🔴 `Sleep::reset(new_deadline)` without reallocating — the entry is removed and reinserted
- [x] 🔴 `timeout(dur, fut)` — `Timeout` combinator returning `Result<T, Elapsed>`
- [x] 🔴 `interval(period)` with `MissedTickBehavior::{ Burst, Delay, Skip }`
- [x] 🟡 `DelayQueue<T>` for expiring collections (connection reaping) — `time/delay_queue.rs`: `insert_at`/`remove`/`remove_if`/`next`/`poll_expired`, opaque `Key`, `Expired { key, deadline, item }`; per-entry wheel timer through a shared FireSink waker so any worker's fire wakes the poller; keys from an `AtomicU64` counter into a `HashMap`, delivered in deadline order (verified in `tests/time.rs`)
- [x] 🟡 Paused-time mode for tests: `time::pause()`, `time::advance(dur)`, auto-advance when all tasks are idle — `TimerShared` gains paused state + `now_instant()`; `arm` compares deadlines against the paused clock; `sleep`/`interval`/`timeout`/`now` build deadlines from the driver clock; parked schedulers (current-thread loop and multi-thread io driver) call `paused_advance()` instead of sleeping — jumping to the next timer deadline, or stepping 1 ms with `time::auto_advance(true)` when nothing is pending
- [x] 🔴 Test: 100k timers inserted and cancelled — **O(1) confirmed by timing, not just asymptotics**
- [x] 🔴 Test: timers fire in deadline order
- [x] 🔴 Test: timer never fires early; lateness bounded by one tick + scheduling latency
- [x] 🔴 Test: cascading across all 6 levels (a 2-year timer eventually reaches level 0)
- [x] 🔴 Test: `sleep` in 10k concurrent tasks all complete within tolerance

---

## Phase 7 — Async I/O Types (27 tasks)

**Traits**
- [x] 🔴 `AsyncRead::poll_read(Pin<&mut Self>, &mut Context, &mut ReadBuf) -> Poll<io::Result<()>>`
- [x] 🔴 `AsyncWrite::poll_write` / `poll_flush` / `poll_shutdown`
- [x] 🔴 `poll_write_vectored` + `is_write_vectored`
- [x] 🔴 **`ReadBuf` tracking filled / initialized / remaining** — avoids zeroing buffers you're about to overwrite without exposing uninit memory to safe code
- [x] 🔴 `AsyncReadExt` / `AsyncWriteExt`: `read`, `read_exact`, `read_to_end`, `read_to_string`, `write`, `write_all`, `flush`, `shutdown`
- [x] 🔴 **Document cancel safety on every method.** `read_exact` is NOT cancel safe; `read` is

**Net types**
- [x] 🔴 `TcpListener::bind` / `accept` — use `accept4(SOCK_NONBLOCK | SOCK_CLOEXEC)` where available
- [x] 🔴 `TcpStream::connect` — nonblocking connect, wait for writable, check `SO_ERROR`
- [x] 🔴 `TcpStream` read/write/peek, `set_nodelay`, `set_linger`, `peer_addr`, `local_addr`
- [x] 🔴 `TcpStream::split()` (borrowed) and `into_split()` (owned)
- [x] 🔴 `UdpSocket`: `send_to`, `recv_from`, `connect`, `send`, `recv`
- [x] 🟡 `UnixListener` / `UnixStream` / `UnixDatagram`
- [x] 🔴 `PollEvented<E>` wrapper generic over the raw type

**The I/O loop**
- [x] 🔴 Every op: `readiness(interest).await` → `try_io(syscall)` → on `WouldBlock`, `clear_ready()` and loop
- [x] 🔴 Handle `EINTR` by retrying
- [x] 🔴 Handle partial writes in `write_all`
- [x] 🔴 `poll_shutdown` → `shutdown(SHUT_WR)`, not `close`

**Adapters**
- [x] 🟡 `BufReader` / `BufWriter` / `BufStream`
- [x] 🟡 `io::copy` and `copy_bidirectional`
- [x] 🟡 `io::empty`, `io::sink`, `io::repeat`
- [x] 🟡 `AsyncBufRead` + `read_until`, `read_line`, `lines()`

**Tests**
- [x] 🔴 Test: TCP echo — 1000 concurrent connections, all data correct
- [x] 🔴 Test: large transfer (100 MB) with partial reads and writes
- [x] 🔴 Test: half-close detected correctly
- [x] 🔴 Test: connect to a closed port → the right error, not a hang
- [x] 🔴 Test: `split()` — concurrent read and write on one socket
- [x] 🔴 Test: UDP send/recv round trip

---

## Phase 8 — Synchronization Primitives (31 tasks)

**Foundation**
- [x] 🔴 `batch_semaphore`: the primitive underneath everything else. Intrusive FIFO waiter list, multi-permit acquire
- [x] 🔴 **Waiter node lives inside the future** (`PhantomPinned`) — zero allocation per wait, which matters when thousands of tasks contend
- [x] 🔴 This is the clearest real demonstration of why `Pin` exists; document it as the canonical example
- [x] 🔴 `Semaphore` public API: `acquire`, `acquire_many`, `try_acquire`, `add_permits`, `close`
- [x] 🔴 `SemaphorePermit` / `OwnedSemaphorePermit` with `forget()`

**Locks**
- [x] 🔴 `Mutex<T>` on a 1-permit semaphore; `lock().await -> MutexGuard`
- [x] 🔴 `try_lock`, `get_mut`, `into_inner`, `lock_owned`
- [x] 🔴 `RwLock<T>` — **write-preferring**, to prevent writer starvation under sustained read load
- [x] 🔴 `read().await`, `write().await`, `try_read`, `try_write`, guard downgrade

**Notify**
- [x] 🔴 `Notify` with `notified()`, `notify_one()`, `notify_waiters()`
- [x] 🔴 **Stores one permit** so a `notify_one` arriving before any waiter isn't lost — this is the difference between `Notify` and a condvar, and the source of most "my task hangs" bugs when it's missing
- [x] 🔴 `Notified` future must be pinned and `enable()`d to register before awaiting

**Channels**
- [x] 🔴 `oneshot::channel()` — one value, `Sender::send` (not async), `Receiver` is a future
- [x] 🔴 `oneshot::Sender::closed().await` — detect receiver drop for cancellation
- [x] 🔴 `mpsc::channel(cap)` bounded — `send().await` awaits capacity (real backpressure, not a spin)
- [x] 🔴 `mpsc::unbounded_channel()`
- [x] 🔴 `Sender::reserve()` → `Permit` for cancel-safe sending
- [x] 🔴 `Receiver::recv()` — **cancel safe**; a cancelled recv leaves the message in the channel
- [x] 🔴 `recv_many(&mut Vec, limit)` for batching
- [x] 🔴 Channel closes when all senders or the receiver drop
- [x] 🟡 `broadcast::channel(cap)` — ring buffer, per-receiver cursor
- [x] 🟡 `RecvError::Lagged(n)` when a receiver falls behind — slow receivers must not stall the sender
- [x] 🟡 `watch::channel(init)` — latest value, `changed().await`, `borrow_and_update`

**Tests**
- [x] 🔴 Test: Mutex under 100 concurrent tasks — mutual exclusion holds, FIFO fairness observed
- [x] 🔴 Test: RwLock — writers not starved by continuous readers
- [x] 🔴 Test: Notify permit — `notify_one()` before `notified().await` still wakes
- [x] 🔴 Test: mpsc backpressure — bounded send blocks when full, resumes on recv
- [x] 🔴 **Test: cancel `recv()` mid-flight → message not lost**
- [x] 🔴 **loom: semaphore acquire/release with 2 waiters**
- [x] 🔴 **loom: oneshot send racing with receiver drop**
- [x] 🔴 **loom: Notify permit races**

---

## Phase 9 — Combinators & Cancellation (18 tasks)

- [x] 🔴 `join!(a, b, ...)` — poll all, complete when all are Ready, `pin_project` for structural pinning
- [x] 🔴 `try_join!` — short-circuit on first `Err`
- [x] 🔴 `select!` macro — poll branches in **random order** by default to avoid starving later branches
- [x] 🔴 `select!` with `biased;` for deterministic priority order
- [x] 🔴 `select!` `else` and `if` guards on branches
- [x] 🔴 **`select!` drops the losing futures** — document that this is cancellation, and link to cancel safety
- [x] 🔴 `race(a, b)` for same-typed futures
- [x] 🔴 `timeout(dur, fut)` / `timeout_at(deadline, fut)`
- [x] 🟡 `FuturesUnordered` — dynamic set with an intrusive ready-list so only woken members are polled (not O(n) per poll)
- [x] 🟡 `JoinSet` — spawn into a set, `join_next().await`, `abort_all()`
- [x] 🟡 `StreamExt` basics if a `Stream` trait is included
- [x] 🔴 `poll_fn`, `pending`, `ready`, `yield_now`
- [x] 🟡 `CancellationToken` — `cancelled().await`, `child_token()`, structured cancellation trees
- [x] 🔴 Test: `join!` — all complete, results in order
- [x] 🔴 Test: `select!` — loser is dropped exactly once
- [x] 🔴 Test: `select!` branch fairness over 10,000 iterations (random ordering actually randomizes)
- [x] 🔴 Test: `timeout` — both the completion and the elapsed path
- [x] 🟡 Test: `FuturesUnordered` with 10k members polls only woken ones (instrument poll counts)

---

## Phase 10 — Blocking Pool (14 tasks)

- [x] 🔴 `BlockingPool` with a dynamically-sized thread set
- [x] 🔴 `max_blocking_threads` default 512 — **deliberately large**, since these threads are blocked on I/O, not consuming CPU
- [x] 🔴 `keep_alive` default 10 s; idle threads exit
- [x] 🔴 `spawn_blocking(f) -> JoinHandle<R>`
- [x] 🔴 Job queue with a condvar; threads spawned lazily on demand
- [x] 🔴 `block_in_place(f)` — multi-thread only; hands the current worker's queue to a replacement so blocking work can borrow non-`'static` data
- [x] 🔴 `block_in_place` inside a current-thread runtime must panic with a clear message, not deadlock
- [x] 🔴 Shutdown: stop accepting, wait for in-flight jobs, join threads
- [x] 🔴 Blocking-pool tasks are not cancellable once started (document this — it surprises people)
- [x] 🟡 `spawn_blocking` from outside the runtime → clear panic
- [x] 🔴 Test: 1000 blocking jobs complete
- [x] 🔴 Test: pool grows to the cap and no further
- [x] 🔴 Test: idle threads exit after `keep_alive`
- [x] 🔴 Test: `block_in_place` doesn't deadlock the runtime when all workers use it

---

## Phase 11 — Cooperative Budget & Fairness (12 tasks)

- [x] 🔴 `coop::Budget(Option<u8>)` in a thread-local, `BUDGET = 128`
- [x] 🔴 `poll_proceed(cx)` — decrement; when exhausted, `wake_by_ref()` and return Pending
- [x] 🔴 Budget reset at the start of each task poll
- [x] 🔴 **Every resource operation consumes budget**: channel recv, mutex lock, semaphore acquire, I/O readiness
- [x] 🔴 `unconstrained(fut)` to opt out
- [x] 🔴 `block_on` runs unconstrained
- [x] 🔴 `yield_now()` — explicit cooperation point
- [x] 🔴 `has_budget_remaining()` for library authors
- [x] 🔴 **Test: `loop { rx.recv().await }` on an always-ready channel does not starve other tasks** — this is the canonical failure the budget exists to prevent
- [x] 🔴 Test: budget exhaustion forces a yield after exactly 128 ops
- [x] 🔴 Test: `unconstrained` genuinely bypasses it
- [x] 🟡 Emit a `BudgetExhausted` event for the console

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

## Phase 17 — Correctness: loom, Miri, Differential (24 tasks)

**loom** — *the* correctness tool for this project
- [x] 🔴 `sync` shim module re-exporting `loom::sync` under `--cfg loom` (`crates/eddy/src/loom.rs`)
- [ ] 🔴 loom: task state transitions (poll / wake / complete / abort interleavings)
- [x] 🔴 loom: refcount + dealloc — exactly one dealloc, never early (waker.rs loom_tests `refcount_is_exact_under_concurrent_clone_drop`)
- [x] 🔴 **loom: wake during poll** — never lost, never double-queued (waker.rs loom_tests `wake_during_poll_is_never_lost`)
- [x] 🔴 loom: Chase-Lev push/pop/steal with 1 producer + 2 thieves (queue.rs loom_tests `two_thieves_do_not_duplicate_or_drop_items`)
- [x] 🔴 loom: `push_overflow` racing with a steal (queue.rs loom_tests `push_overflow_racing_a_steal_never_loses_or_duplicates_tasks`)
- [x] 🔴 loom: injector concurrent push/pop (inject.rs loom_tests `concurrent_push_pop_preserves_every_task_exactly_once`)
- [ ] 🔴 loom: semaphore acquire/release/close
- [x] 🔴 loom: `Notify` permit races (notify.rs loom_tests: registration race + registered-waiter wake)
- [x] 🔴 loom: oneshot send / recv / drop races (oneshot.rs loom_tests `send_racing_with_receiver_drop_is_safe_and_consistent`)
- [ ] 🔴 loom: `ScheduledIo` readiness + waker registration races
- [ ] 🔴 loom: timer entry fire vs cancel race
- [x] 🔴 **CI runs loom on every PR** (bounded preemptions to keep it under 10 minutes)

**Miri**
- [x] 🔴 Miri over the task allocation, waker vtable, intrusive lists, `ReadBuf` (task/waker/intrusive lists done — 35 lib tests clean; `ReadBuf`/io paths call foreign syscalls Miri cannot run, covered by the other gate jobs)
- [x] 🔴 Stacked-borrows clean (no `-Zmiri-tag-raw-pointers` suppressions) — caught and fixed the intrusive-list violation (ISSUES.md C3)
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
| 1. Task Representation | 30 |
| 2. The Waker | 18 |
| 3. Current-Thread Executor | 16 |
| 4. Multi-Thread Scheduler | 37 |
| 5. I/O Driver (readiness) | 32 |
| 6. Timer Wheel | 24 |
| 7. Async I/O Types | 27 |
| 8. Sync Primitives | 31 |
| 9. Combinators & Cancellation | 18 |
| 10. Blocking Pool | 14 |
| 11. Cooperative Budget | 12 |
| 12. io_uring Backend | 20 |
| 13. Instrumentation | 18 |
| 14. TUI Console | 16 |
| 15. Web Console | 28 |
| 16. Macros | 10 |
| 17. loom / Miri / Differential | 24 |
| 18. Benchmarks & Polish | 16 |
| **TOTAL** | **405** |
