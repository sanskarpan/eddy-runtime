# ISSUES.md — eddy bug tracker

Findings from the in-depth review of Phases 4–10 (post-merge to `main`). Every
item is verified against the source with a concrete interleaving or failure
trace. Items are fixed in order: C1 → C2 → H1+H2 → H3 → M1–M6 → L-items →
checklist/test hygiene.

**Status: all items fixed, tested, and committed in `e3ae620`** (clippy
`-D warnings` clean, `cargo fmt` applied, loom suite 17/17, full suite green).
L4 is the only exception: the proposed drain loop cannot be implemented in a
one-stream-per-call `accept()` API (see L4 below) and was reverted after
verification.

Legend: **C** = critical (memory unsafety / permanent deadlock) · **H** = high
(hang or unsoundness in realistic use) · **M** = medium (wrong semantics or
resource leak) · **L** = low (robustness / ergonomics).

| ID | Severity | Location | One-line description |
|----|----------|----------|----------------------|
| C1 | Critical | `scheduler/multi_thread/queue.rs:59,219` | Chase-Lev: owner reuses slots claimed by an in-flight steal → task loss / double-run / double-free |
| C2 | Critical | `sync/mpsc.rs:123,262` | mpsc reservation double-counted → phantom capacity slot + permanently stranded waiter |
| H1 | High | `io/driver.rs:518-557`, `worker.rs:491` | Park lost-wakeup: wake lands between work re-check and holder claim → worker blocks forever in kernel wait |
| H2 | High | `worker.rs:556-571,685-699` | `block_in_place` takeover: same lost-wakeup window → deadlock waiting for `returned` |
| H3 | High | `scheduler/current_thread.rs:38-41,49,217` | Current-thread runtime `Send`+`Sync` while `!Send` futures can be polled/dropped off-thread; timer unparks the construction thread |
| M1 | Medium | `io/ops.rs:96-110,144-147` | `copy_bidirectional` never half-closes the destination at EOF → proxy deadlock |
| M2 | Medium | `io/mod.rs:408-410` | `clear_ready` can consume a freshly re-set, same-generation event → hang |
| M3 | Medium | `cancellation.rs:85-102` | `Cancelled::poll` appends a waker per poll; vec grows without bound, stale wakers never removed |
| M4 | Medium | `future.rs:258-266` | 3-arg `try_join!` awaits both futures (uses `join2`) → no short-circuit, can hang forever |
| M5 | Medium | `worker.rs:639-711` | Panic inside `block_in_place` unwinds past the takeover hand-back → queue ownership desync (UB) |
| M6 | Medium | `worker.rs:278-302` | `block_on` from a worker thread never services tasks spawned onto the caller's queue → hang |
| L1 | Low | `time/mod.rs:30-33` | `Sleep::reset` while armed still fires at the old deadline (fires early) |
| L2 | Low | `io/driver.rs:552` | `poller.wake()` failure silently dropped → undiagnosed hang |
| L3 | Low | `worker.rs:143-156` | Baton-pass `unpark_any` only calls `thread::unpark`, which workers never consume → inert |
| L4 | Low | `io/net.rs` accept path | One connection accepted per event instead of draining |
| L5 | Low | `io/unix.rs` | `UnixDatagram::shutdown` surfaces `ENOTCONN` to callers |
| L6 | Low | `io/buf.rs` | Vectored I/O count not clamped to `IOV_MAX` |
| L7 | Low | `time/wheel.rs` | Timers can fire ~1 ms early (wheel granularity, accepted tradeoff) |
| L8 | Low | `queue.rs:82-99` | `push_overflow` busy-spins while all slots are claimed-but-uncommitted |
| L9 | Low | `future.rs:240-267` | `join!` / `try_join!` support only 2–3 args, not variadic |
| L10 | Low | `worker.rs:792-803` | `take_injected` pushes into a full local queue then re-pushes to the injector |
| L11 | Low | `CHECKLIST.md` | Phases 5 & 6 fully implemented but still marked `[ ]`; summary arithmetic stale |

---

## Critical

### C1 — Chase-Lev: owner can reuse slots claimed by an in-flight steal

**Location:** `scheduler/multi_thread/queue.rs:59` (capacity check) and
`queue.rs:219` (`commit_real_head`).

**Failure trace.** Thief A claims `[0, 2)` (steal_head → 2) and is copying slots
0–1. Thief B then claims `[2, 4)` (steal_head → 4) and copies+commits first.
B's `commit_real_head(4)` CASes `real_head` from 0 straight to 4, *skipping
over A's still-in-flight claim*. The owner now sees `real_head == 4` and treats
slots 0–1 as free:

- `push_back` passes its capacity check (`tail - real_head < CAP`) and writes a
  new task into slot 0 while A is mid-copy → A steals and runs the owner's new
  task (**double-run**) while it also stays in the owner's queue, or
- the last handle's `Inner::drop` (queue.rs:249-266) drops `[real_head, tail)`
  while A is still copying them (**double-free / use-after-free**).

The root cause is that a thief advances `real_head`, and a commit can jump the
committed head past a claim that has not completed. The capacity check at
queue.rs:59 must never admit a push whose wrap target lies inside the
claimed-but-uncommitted region `[real_head, steal_head)`.

**Fix plan.**
1. `push_back`: use `steal_head` for the capacity check —
   `tail.wrapping_sub(steal_head) as usize >= CAP` rejects exactly when the
   push slot would wrap into claimed territory.
2. `commit_real_head`: commit claims **in order** — a thief CASes `real_head`
   from its claim's start (`steal_head` observed at claim time) to its
   `claimed_end`; if another claim is still ahead, retry until the earlier
   commit lands. This guarantees `real_head` never passes an in-flight claim.
3. Keep `pop`'s `real_head != steal_head` bail-out (queue.rs:117) — it already
   refuses to race an in-flight claim.

**Tests.** The existing loom tests (queue.rs:372,411) miss this interleaving
and never run in default CI. Add a loom test: owner `push_back` racing two
thieves where the second thief commits before the first; assert every value is
produced exactly once. Also assert (in a plain test) that `Inner::drop` never
runs while a thief holds a clone.

---

### C2 — mpsc reservation double-count → phantom capacity slot, stranded waiter

**Location:** `sync/mpsc.rs:117-130` (`wake_one_sender`, line 123 increments
`reserved` when granting a `Reserve` waiter) and `sync/mpsc.rs:242-275`
(`Reserve::poll`, line 262 increments `reserved` again on the granted path).

**Failure trace.** Capacity-2 channel:

1. `r1 = sender.reserve()` → `reserved = 1` → Permit P1.
2. `P1.send("a")` → `reserved = 0`, `queue = 1`; `wake_one_sender(true)` sees
   capacity free (1 < 2), pops waiter `r2`, **`reserved = 1`** (line 123),
   sets `r2.granted`, wakes it.
3. `r2` polls: `granted == true`, but `capacity_available` is now
   `1 + 1 == 2` — **false**, because its own reservation is already counted.
   The ready path fails; `r2` is no longer in `send_waiters` (popped in step 2)
   and `registered` is still true, so it is never re-queued and never woken
   again. **`r2` is stranded forever and the channel permanently loses one
   capacity slot** (deadlock for that task, backpressure loss for the channel).
4. If `r2`'s poll *had* passed the capacity check, line 262 would add a second
   increment for the same reservation — the same phantom slot, counted twice.

A granted plain `Send` never releases a reservation either: `wake_one_sender`
with `reserve=true` can grant a `Send` waiter (its wake flag is set by the
same grant), and `Send::poll`/`Send::drop` know nothing about reservations, so
the increment at line 123 is never undone.

**Fix plan.** Count each reservation exactly once, at the grant:
1. `wake_one_sender(reserve=true)` increments `reserved` (line 123) — keep.
2. `Reserve::poll` must **not** increment when `granted` (drop line 262's
   increment; it may only re-register if capacity is still unavailable).
3. `Reserve::drop` of a granted-but-unpolled waiter releases the slot
   (`reserved -= 1`) before passing it to the next waiter.
4. `Send` grants never touch `reserved` (already the case; add the invariant
   to the docs).

**Tests.** Add: (a) reserve → drop permit while a second `reserve()` is queued
→ the second reserve completes and the channel keeps full capacity; (b) plain
`send()` granted while a reserve is queued → channel capacity is preserved and
the reserve still completes; (c) capacity-1 reserve→drop-permit→send never
loses the slot.

### C3 — Intrusive list node access violates Stacked Borrows (UB, Miri-caught)

**Location:** `util/linked_list.rs` (all writes went through `&mut` into the
node) and the shared-node callers that derived the raw pointer from a shared
reference: `time/wheel.rs:68` (`into_raw`) and `wheel.rs:152` (`remove`),
`io/driver.rs:119` (`into_raw`), `driver.rs:292/304`. The semaphore's
stack-owned waiters (`sync/semaphore.rs:107,125,138`) were already safe —
their pointers derive from `&mut`.

**Failure trace.** `into_raw`/`remove` produced the node pointer via
`NonNull::from(Arc::as_ref(..))`, which retags the node's memory
`SharedReadOnly`. The list then wrote through `&mut *node.as_ptr()`
(`linked_list.rs:69,92,151` and the neighbor updates) — an invalid `&mut`
retag over a location that only grants shared access. Latent UB on every
platform; Miri reports it as a Stacked Borrows violation on the first
insert (`time::wheel::tests::cascades_across_all_levels`).

**Fix (landed).** The links now live behind an `UnsafeCell`
(`Pointers { inner: UnsafeCell<PointersInner> }`) and every mutation goes
through the cell's interior pointer, which Stacked Borrows exempts from
shared retags — the pattern tokio's intrusive list uses. Node pointers are
derived with `Arc::as_ptr(..).cast_mut()`, which performs no retag. The
`Linked` trait dropped `pointers_mut`.

**Tests.** Miri: the full pure-module lib sweep is clean (35/35: task 27,
sync 2, time wheel 4, linked_list 1, rand 1) and CI now runs it
(`cargo miri test -p eddy --lib -- task:: sync:: time::wheel::`). Full suite
138/138 and loom 17/17 still pass; io/net/time hammer runs clean.

---

## High

### H1 — Park lost-wakeup: worker can block forever with work in its injector

**Location:** `io/driver.rs:518-544` (`park_worker`), `driver.rs:548-557`
(`unpark_worker`), `worker.rs:125-139` (`IdleState::park`), `worker.rs:480-494`
(`unpark_target`).

**Failure trace.** A worker finishes its work re-check (`has_work` empty,
worker.rs:127), then calls `park_worker`. Before it acquires the park mutex, a
task is routed to it:

- `unpark_target` calls `thread.unpark()` (worker.rs:491) — **never consumed**:
  workers block in `park_worker` (kernel wait or condvar), not in
  `thread::park()`;
- `io.unpark_worker(id)` (driver.rs:548-557) finds `holder == None` (not yet
  claimed) and `waiters == 0` (not yet a condvar sleeper) → **no-op**.

The worker then claims the holder slot and enters the kernel readiness wait
with a `None` timeout (empty timer wheel) → it never returns. Its task sits in
the global injector; with one worker (or all workers parked) nothing ever runs
→ permanent hang. Condvar sleepers self-heal via the 1 ms `wait_timeout`; the
kernel-wait holder does not.

**Fix plan.**
1. `unpark_worker`: when `holder == None && waiters == 0`, still call
   `poller.wake()` — a stale driver wake costs one spurious poller return and
   a re-check, and closes the window.
2. `park_worker` (holder path): after claiming the holder slot, re-read the
   park state and skip the kernel wait (or expect a stale wake) — simplest is
   fix 1 alone, since the poller waker fd is level-sticky.
3. Delete the misleading `thread.unpark()` in `unpark_target` (inert, and the
   comment claiming it covers the window is wrong), or keep it only where the
   target actually parks on `thread::park`.

**Tests.** Stress test: 1-worker runtime; repeatedly spawn a task from another
thread while the worker is parked, with a barrier that forces the wake into
the gap (park-then-route in a tight loop); assert every task completes within
a deadline. This test must hang on the current code.

### H2 — `block_in_place` takeover deadlock (same window as H1)

**Location:** `worker.rs:556-571` (takeover request check), `worker.rs:685-699`
(wait for `returned`).

**Failure trace.** The worker finishes `fut`, sets `takeover.requested =
true` (worker.rs:685), and unparks via `thread.unpark` + `io.unpark_worker`
(worker.rs:686-689). If the takeover thread is between its final work re-check
and claiming the driver holder, `unpark_worker` no-ops exactly as in H1 →
the takeover thread blocks in the kernel wait with `requested == true`, and
the worker waits on `takeover.condvar` for `returned` forever → deadlock.

**Fix plan.** Covered by the H1 fix (stale `poller.wake()` closes the window).
Additionally, make the takeover thread re-check `takeover.requested` *after*
`park_worker` returns and before parking again (already happens at
worker.rs:556 on every loop iteration — the H1 fix is the only missing piece).

**Tests.** Loop `block_in_place(yield_now())` from a task on a 1-worker runtime
while an external thread keeps the runtime busy; assert completion within a
deadline. Plus the H1 stress test covers the shared window.

### H3 — Current-thread runtime is unsoundly `Send`/`Sync`; timers unpark the wrong thread

**Location:** `scheduler/current_thread.rs:38-41` (`unsafe impl Send/Sync for
Inner`), `current_thread.rs:49` (timer closure unparks the construction
thread), `current_thread.rs:205-243` (`shutdown` polls/drops tasks on the
calling thread), comment at `current_thread.rs:217`.

**Problem.**
1. `Runtime` (current-thread flavor) is `Send + Sync` through `Inner`. If it is
   moved to another thread and dropped, `shutdown()` runs
   `task.run()`/`drop_reference_on_owner()` (current_thread.rs:222-241) there
   — polling and destroying `!Send` futures on a foreign thread → UB. The
   safety comment at current_thread.rs:217 ("Runtime is !Send") contradicts
   the impl.
2. `TimerShared::new` captures `owner_thread` at *construction* time
   (current_thread.rs:48-49); if `block_on` runs on a different thread,
   timer wakes unpark a dead/unparked thread → timer waits hang.

**Fix plan.**
1. Make the current-thread runtime type `!Send + !Sync` (marker fields), so it
   cannot be moved across threads at all. `Handle` stays `Send + Sync` (its
   wake path only enqueues + unparks, which is thread-safe).
2. The timer closure must unpark the thread currently in `block_on` — read the
   `unparker` slot at fire time instead of capturing the construction thread.

**Tests.** (a) Compile-fail/assertion: a current-thread `Runtime` cannot be
sent to another thread (compile-time `!Send` check via `static_assertions` or
a `fn assert_send<T: Send>()` not compiling — invert: assert it is `!Send`);
(b) runtime created on thread A, `block_on` on thread B with a timer → the
timer still fires.

---

## Medium

### M1 — `copy_bidirectional` never half-closes the destination at EOF

**Location:** `io/ops.rs:96-110` (EOF → `DirectionPoll::Done` without
`poll_shutdown`) and `io/ops.rs:144-147` (completes only when both directions
are done).

**Problem.** When the source reports EOF, the destination is never shut down
(`poll_shutdown` / `shutdown(SHUT_WR)`). In a proxy, peer A half-closes → our
side must half-close peer B so B sees EOF; without it, B's `read` hangs until
*we* close both sides, which never happens because `CopyBidirectional` itself
is waiting on the other direction → proxy deadlock.

**Fix plan.** On EOF in one direction, call `poll_shutdown` on the destination
(once, retry until `Poll::Ready`), and treat shutdown completion as that
direction's Done (so the other direction can still complete).

**Tests.** Echo/pipe pair where the left side sends data then shuts down its
write side while the right side keeps reading — assert the right side observes
EOF and the future completes.

### M2 — `clear_ready` can consume a freshly re-set same-generation event

**Location:** `io/mod.rs:401-415` (`try_io_with`), `io/driver.rs:211-228`
(`clear_ready`).

**Problem.** The syscall returns `WouldBlock`; `try_io_with` fetches an event
via `readiness()` and immediately calls `clear_ready()`. Between the fetch and
the clearing CAS, the driver can set *new* readiness bits for the same
generation (e.g. more data arrived). `clear_ready` clears whatever bits match
the event's mask regardless — the new event is consumed before its task ever
polls → the task re-parks on a readiness that was consumed → hang.

**Fix plan.** `clear_ready` must only clear when the *currently stored*
readiness matches the event's snapshot (no new bits since). If the packed
readiness differs from the event's `ready` mask, leave it; the new event will
be re-reported to the waiting task.

**Tests.** Loop: wait for readiness, read a byte, repeatedly; feed a second
byte while the first is being processed; assert the reader never hangs (this
must fail on the current code with a timing window).

### M3 — `CancellationToken` waker list grows without bound

**Location:** `cancellation.rs:85-102`.

**Problem.** Every `Cancelled::poll` pushes `cx.waker().clone()` into
`waiters: Vec<Waker>`; stale entries are only removed at `cancel()` (which
drains) — and only if cancel is ever called. A task that polls
`cancelled().await` repeatedly (e.g. re-woken by other sources) leaks one
waker per poll → unbounded memory growth.

**Fix plan.** Give each waiter a slot that is removed on `Drop` of the
`Cancelled` future: store `(id, Waker)` pairs with a monotonically increasing
per-token id, and unregister in `Drop`. Growth is then bounded by live
waiters.

**Tests.** Poll `cancelled()` 100k times on an un-cancelled token → memory
(constructor count via a counting waker or allocator probe) stays flat; then
`cancel()` wakes exactly the live waiter.

### M4 — 3-arg `try_join!` does not short-circuit

**Location:** `future.rs:258-266`.

**Problem.** The 3-arg arm is `join2(join2(a, b), c)` then `?` — it awaits all
three futures to completion before returning any error. If `a` fails fast and
`b` never completes, the macro never returns → hangs.

**Fix plan.** Implement a `TryJoin3` future (or flatten `TryJoin2` over the
`TryJoin2` result) that polls all three and returns `Err` as soon as any
future yields one.

**Tests.** `try_join!(async { Err(1) }, pending(), async { 0 })` must resolve
to `Err(1)` (wrap `pending` with `timeout` to prove it resolved without it).

### M5 — Panic in `block_in_place` unwinds past the takeover hand-back

**Location:** `worker.rs:639-711` (`block_in_place_on_worker`).

**Problem.** The loop at worker.rs:673-680 polls `fut`; if `fut` panics, the
panic propagates before the hand-back code (worker.rs:685-709) runs. The
takeover thread keeps servicing the queue (never told to return) and the
original worker continues with `WORKER_ID` cleared while the takeover thread
owns the same queue → two threads touching one `UnsafeCell` queue (UB), stale
`shared.workers[id].thread` handle, and a task that believes it is no longer a
worker. If the panic escapes the harness (root future in `block_on`), the
shutdown `thread.join().expect(...)` at worker.rs:708/331 panics.

**Fix plan.** Wrap the poll loop in `catch_unwind`: on panic, run the hand-back
(reset `requested` + wait for `returned` — or force the takeover to exit),
restore `WORKER_ID`/thread handle, then `resume_unwind`.

**Tests.** Task whose `block_in_place(fut)` future panics → `JoinError::Panic`
surfaces, `WORKER_ID` restored, runtime still spawns and completes tasks, and
shutdown joins cleanly.

### M6 — `block_on` from a worker thread never runs tasks spawned onto itself

**Location:** `worker.rs:278-302` (`MultiThread::block_on`).

**Problem.** `block_on` on a worker thread polls the root future on that
thread and parks in `thread::park()`. Spawns inside the root future see
`WORKER_ID` set and route to the *caller's* local queue (`schedule`,
worker.rs:417-445), which only `worker_loop` services — and this thread is
not in `worker_loop` → spawned tasks never run → hang.

**Fix plan.** In `block_on`, when `current_worker_id()` is `Some`, temporarily
clear `WORKER_ID` (as `block_in_place_on_worker` does) so spawns round-robin
to real workers; the root future's wakes still unpark this thread via
`thread::park()`.

**Tests.** From a task, call `Handle::block_on(async { spawn(...).await })` →
completes (must hang on current code).

---

## Low

- **L1 `Sleep::reset` fires at the old deadline** (`time/mod.rs:30-33`): reset
  only mutates `self.deadline` and marks the entry; if the entry is already
  armed in the wheel with the old deadline, the wheel fires it at the old time
  and `poll` returns `Ready` early. Fix: re-arm with the new deadline when the
  entry is armed.
- **L2 `poller.wake()` errors dropped** (`io/driver.rs:552`): a failed wake is
  invisible until a hang. Fix: log (or debug_assert) and treat failure as
  fatal-ish in debug builds.
- **L3 Baton-pass `unpark_any` is inert** (`worker.rs:143-156`): parked
  workers block in `park_worker`, never in `thread::park()`; the baton does
  nothing. Fix: route through `io.unpark_worker(id)` (clears the bit, wakes the
  driver/condvar). H1's fix is a prerequisite for the wake to be reliable.
- **L4 Accept drains one connection per event** (`io/net.rs`): level-triggered
  polling re-reports, so it is correct but slow under bursts. **Not fixed —
  reverted as unimplementable**: a drain loop was attempted and clippy's
  `never_loop` proved it cannot iterate — `accept()` returns on the first
  successful `accept(2)`, so draining would require buffering connections,
  which the one-stream-per-call API forbids. The per-connection wake cost is
  one extra poller return per connection on the same event; accepted.
- **L5 `UnixDatagram::shutdown` → `ENOTCONN`** (`io/unix.rs`): surface the
  error correctly or treat unconnected datagram shutdown as a no-op.
- **L6 iovec count unclamped**: `writev`/`readv` with more than `IOV_MAX` io
  vecs fails with `EINVAL`. Fix: clamp. Implemented in
  `TcpStream::poll_write_vectored` (`io/net.rs:652-656`) via `.take(...)` —
  this crate only exposes vectored writes on `TcpStream`.
- **L7 Timers can fire ~1 ms early** (`time/wheel.rs`): accepted wheel
  tradeoff; document it next to the wheel math.
- **L8 `push_overflow` busy-spins** (`queue.rs:82-99`): when every slot is
  claimed-but-uncommitted, `push_back`/`steal_into` both fail and the loop
  spins until a thief commits. Fix: bounded retry with a yield/backoff
  (commits are imminent).
- **L9 `join!`/`try_join!` are 2/3-arg only** (`future.rs:240-267`):
  CHECKLIST.md advertises variadic. Fix: expand arms (or document the limit).
- **L10 `take_injected` pop→push→pop** (`worker.rs:792-803`): pops from the
  injector, pushes into the local queue, and re-pushes to the injector when
  full — during that window the task is invisible to thieves. Fix: return the
  popped task directly when the local queue is full.

---

## Test coverage gaps

- **Chase-Lev loom tests never run by default** (`queue.rs:372,411`): gated
  behind `--cfg loom`, which CI does not set. The C1 interleaving is modeled
  by `owner_push_back_never_reuses_a_slot_claimed_by_an_unfinished_steal`
  (queue.rs:482). **Fixed** — `.github/workflows/ci.yml` runs the loom suite
  (`RUSTFLAGS="--cfg loom" cargo test -p eddy --lib`, 17/17 green) on every
  push and PR.
- **mpsc cancel-recv test was vacuous** (`tests/sync.rs:111-116`): **fixed** —
  replaced with a real pending-recv drop: poll a `Box::pin(receiver.recv())`
  with a noop waker, drop it, then send → the item is still delivered.
- **fd-reuse test** (`tests/io.rs`): **fixed** — the test now arms a genuine
  stale event: it makes the old registration readable and waits for the
  driver to record readiness, then frees the slab slot and reuses it (same
  index, newer generation), asserting the new registration is not woken and
  still delivers its own events. The dispatch-time generation guard
  (`driver.rs:599`) covers the remaining in-flight-race window, which is not
  deterministically constructible from user space (the kernel drops interest
  on close, so a stale event can never survive a swap at the poller level).
- **steal-count assertion** (`worker.rs:992-995`): **fixed** — the stress test
  now alternates fast/slow tasks (10× `yield_now` every other task) so the
  steal is deterministic, and it asserts completion + steal count.
- **rwlock writer-preference test is weak**: **fixed** — sustained reader loop
  with a `timeout(2s)` deadline on writer progress.
- **Missing regression tests for every C/H/M item above**: **fixed** — each fix
  ships with a test that fails on the old code (M1–M6, H1/H2/H3, C2, L1 all
  verified against the pre-fix source; M5/M6 old-code probes documented in
  session notes).

---

## Checklist hygiene (L11)

- `CHECKLIST.md` Phase 5 (I/O Driver) and Phase 6 (Timer Wheel): every task is
  implemented (sys poller, registration, driver loop, wheel, sleep/timeout)
  yet still marked `[ ]` — flip to `[x]` per item.
- The Summary table's "TOTAL 396" counts all checkboxes; per-phase entry counts
  already disagree with the phase headers (e.g. Phase 1 lists 30 entries for 28
  tasks). Fix the arithmetic or the headers.
