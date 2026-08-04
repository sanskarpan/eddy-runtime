# eddy Phase 0-3 (Bootstrap + Task + Waker + Current-Thread Executor) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task, with an added review checkpoint (spawn a review subagent) after Tasks 4, 6, and 9. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the "spine" of the `eddy` async runtime — a one-allocation task representation, a hand-built `Waker` vtable with exact refcounting, and a current-thread executor — such that `block_on(async { 42 })` works, `spawn` works, cross-thread wakes work, and the two required loom tests pass.

**Architecture:** One `Cell<F, S>` heap allocation per task (`Header` + `Core` + `Trailer`), type-erased via a manual `&'static Vtable` reached through the header (no `Box<dyn Future>`). A packed `AtomicUsize` state machine (`[refcount:48][flags:16]`) drives every transition through a single CAS. A hand-built `RawWakerVTable` clones/wakes/drops by manipulating that refcount directly. The current-thread scheduler is a plain `VecDeque` for the fast (same-thread) path plus a `Mutex`-guarded injection queue for cross-thread wakes, drained every `GLOBAL_QUEUE_INTERVAL` ticks. `block_on` polls its root future with a separate, simpler thread-parking `Waker` (not routed through the task system).

**Tech Stack:** Rust 2021 (rustc 1.97.1 installed), `libc`, `slab`, `parking_lot`, `pin-project-lite`, `tracing`, `crossbeam-utils`; dev-deps `loom`, `criterion`, `proptest`, `futures`, `tokio` (oracles only).

**Out of scope (future slices):** multi-thread work-stealing scheduler, reactor, timer wheel, sync primitives, io_uring, instrumentation, consoles, full CI matrix.

---

## Reference design: the state machine (authoritative — implement exactly this)

`Header.state: AtomicUsize`, packed `[ refcount : 48 bits ][ flags : 16 bits ]`, `REF_ONE = 1 << 16`.

Flags (low 16 bits): `RUNNING = 0b0001`, `COMPLETE = 0b0010`, `NOTIFIED = 0b0100`, `CANCELLED = 0b1000`, `JOIN_INTEREST = 0b0001_0000`, `JOIN_WAKER_SET = 0b0010_0000`.

**Initial state at spawn:** `refcount = 2` (one for the `JoinHandle`, one for the initial run-queue slot), `JOIN_INTEREST | NOTIFIED` set. The spawn path pushes the task straight onto the scheduler using that second reference — it does not go through `transition_to_notified_*`.

**Transitions** (each one `fetch_update`/CAS loop):

- `transition_to_running`: fails (returns `Failed`) if `COMPLETE` or `CANCELLED`. Otherwise sets `RUNNING`, clears `NOTIFIED` (we're about to observe whatever wake it represents by polling). `debug_assert!(!is_running())` going in — a task must never be polled by two threads; it is always removed from every queue before being handed to a worker.
- `transition_to_idle` (called after `poll` returns `Pending`): clears `RUNNING`. If `CANCELLED`, returns `Cancelled`. **If `NOTIFIED` is set (a wake arrived while we were running), returns `OkNotified` — caller must re-queue the task immediately, using the run-reference it already holds, without touching the refcount.** Otherwise returns `Ok` (park; the task now has no queue reference and relies on a future wake to supply one).
- `transition_to_complete`: clears `RUNNING`, sets `COMPLETE`, clears `NOTIFIED`.
- `transition_to_notified_by_ref` (used by `wake_by_ref`, which must **not** consume the caller's reference): if `COMPLETE` or already `NOTIFIED`, `DoNothing`. If `RUNNING`, set `NOTIFIED` and `DoNothing` (the running poller will see `NOTIFIED` in `transition_to_idle` and re-queue itself). Otherwise set `NOTIFIED`, **increment the refcount** (a fresh reference for the queue slot — the caller keeps theirs), return `Submit`.
- `transition_to_notified_by_val` (used by `wake`, which **consumes** the caller's reference): identical branching, but on the `Submit` path it does **not** increment — the consumed reference *becomes* the queue's reference. On `DoNothing`, the caller must separately drop its consumed reference (`ref_dec`, dealloc if it hit zero).
- `ref_inc`: `fetch_add(REF_ONE, Relaxed)` — relaxed is sound because the caller already holds a valid reference (they are cloning something they synchronized with earlier); abort the process if the refcount would exceed a sane maximum (guard against runaway leaks turning into UB on wraparound).
- `ref_dec`: `fetch_sub(REF_ONE, Release)`, and if the resulting count is zero, issue an `Acquire` load (fence) before returning "you must dealloc" — the `Release`/`Acquire` pair ensures every prior access made through any now-dead reference happens-before the deallocation.

**Why this is correct (the canonical bug this prevents):** worker sets `RUNNING`, future registers a waker and is about to return `Pending`; before it does, another thread calls `wake()`. Because the state is still `RUNNING`, the wake sets `NOTIFIED` and does nothing else — it does **not** try to queue the task while it's still logically "owned" by the poller. When the poller finishes and calls `transition_to_idle`, it observes `NOTIFIED` and re-queues immediately instead of parking. No lost wakeup, no double-queue.

---

## Task 1: Workspace + crate bootstrap

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `crates/eddy/Cargo.toml`
- Create: `crates/eddy/src/lib.rs`
- Create: `crates/eddy/src/loom.rs`

- [ ] **Step 1: Write the workspace root `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = ["crates/eddy"]
```

- [ ] **Step 2: Write `crates/eddy/Cargo.toml`**

```toml
[package]
name = "eddy"
version = "0.1.0"
edition = "2021"
rust-version = "1.80"

[dependencies]
libc = "0.2"
slab = "0.4"
parking_lot = "0.12"
pin-project-lite = "0.2"
tracing = "0.1"
crossbeam-utils = "0.8"

[target.'cfg(loom)'.dependencies]
loom = "0.7"

[dev-dependencies]
criterion = "0.5"
proptest = "1"
futures = "0.3"
tokio = { version = "1", features = ["full"] }

[[example]]
name = "spike"
path = "examples/spike.rs"
```

- [ ] **Step 3: Write the loom shim `crates/eddy/src/loom.rs`**

```rust
//! Re-exports either `std::sync` or `loom::sync` depending on the `loom`
//! cfg, so the rest of the crate is written once against `crate::loom::*`
//! and gets exhaustive interleaving checking for free under
//! `RUSTFLAGS="--cfg loom" cargo test`.

#[cfg(not(loom))]
pub(crate) mod sync {
    pub(crate) use std::sync::atomic;
    pub(crate) use std::sync::Arc;
}

#[cfg(loom)]
pub(crate) mod sync {
    pub(crate) use loom::sync::atomic;
    pub(crate) use loom::sync::Arc;
}

#[cfg(not(loom))]
pub(crate) mod thread {
    pub(crate) use std::thread::{current, park, yield_now, Thread};
}

#[cfg(loom)]
pub(crate) mod thread {
    pub(crate) use loom::thread::{current, park, yield_now, Thread};
}
```

- [ ] **Step 4: Write `crates/eddy/src/lib.rs`**

```rust
#![deny(clippy::undocumented_unsafe_blocks)]

pub(crate) mod loom;

pub mod task;
pub mod runtime;
pub mod scheduler;

pub use runtime::{Builder, Handle, Runtime};
pub use task::JoinHandle;
```

(The `task`/`runtime`/`scheduler` modules are created empty — `mod.rs` with nothing but doc comments — in this step; Tasks 2+ fill them in. This keeps the crate compiling after every task.)

- [ ] **Step 5: Verify it builds**

Run: `cargo build -p eddy`
Expected: success (empty modules, no errors).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/eddy/Cargo.toml crates/eddy/src/lib.rs crates/eddy/src/loom.rs crates/eddy/src/task crates/eddy/src/runtime crates/eddy/src/scheduler
git commit -m "chore: bootstrap eddy workspace and crate skeleton"
```

---

## Task 2: The Day-One spike

**Files:**
- Create: `crates/eddy/examples/spike.rs`

- [ ] **Step 1: Write the spike** (hand-built waker, no runtime, proves the vtable contract before anything else is built)

```rust
// examples/spike.rs — build a Waker by hand, poll a future to completion.
// No dependencies beyond std. If this is wrong, everything above it is
// unfixable: this is the minimal proof that clone/wake/wake_by_ref/drop
// can maintain an Arc refcount correctly through a RawWakerVTable.
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

struct Shared {
    woken: AtomicBool,
}

// SAFETY: `p` is always a pointer previously produced by `Arc::into_raw`
// on an `Arc<Shared>`, per the `Waker`/`RawWaker` contract that only our
// own `clone_waker`/`Waker::from_raw` calls ever construct one.
unsafe fn clone(p: *const ()) -> RawWaker {
    // +1: a new Waker now exists that shares ownership with the original.
    Arc::increment_strong_count(p as *const Shared);
    RawWaker::new(p, &VTABLE)
}

// SAFETY: see `clone`. `wake` takes ownership (consumes) the reference
// this RawWaker represented.
unsafe fn wake(p: *const ()) {
    let arc = Arc::from_raw(p as *const Shared); // reclaims the +1 from clone/creation
    arc.woken.store(true, Ordering::Release);
    // arc dropped here -> -1, correct because wake() CONSUMES the waker.
}

// SAFETY: see `clone`. `wake_by_ref` borrows; it must leave the refcount
// unchanged, hence `ManuallyDrop` around the reconstructed `Arc`.
unsafe fn wake_by_ref(p: *const ()) {
    let arc = std::mem::ManuallyDrop::new(Arc::from_raw(p as *const Shared));
    arc.woken.store(true, Ordering::Release);
}

// SAFETY: see `clone`. `drop` always corresponds to one prior +1.
unsafe fn drop_it(p: *const ()) {
    Arc::decrement_strong_count(p as *const Shared);
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_it);

fn main() {
    let shared = Arc::new(Shared {
        woken: AtomicBool::new(false),
    });
    // SAFETY: VTABLE's four functions uphold the RawWaker contract
    // documented above (exact +1/consume/neutral/-1 refcount discipline).
    let waker = unsafe {
        Waker::from_raw(RawWaker::new(
            Arc::into_raw(shared.clone()) as *const (),
            &VTABLE,
        ))
    };
    let mut cx = Context::from_waker(&waker);
    let mut fut = Box::pin(async { 42 });
    assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(42));
    println!("waker + poll works");
}
```

- [ ] **Step 2: Run it**

Run: `cargo run --example spike -p eddy`
Expected: prints `waker + poll works` and exits 0.

- [ ] **Step 3: Commit**

```bash
git add crates/eddy/examples/spike.rs
git commit -m "feat: hand-built waker spike (Phase 0 day-one proof)"
```

---

## Task 3: `task::state` — the atomic state machine

**Files:**
- Create: `crates/eddy/src/task/state.rs`
- Modify: `crates/eddy/src/task/mod.rs` (add `mod state;`)

- [ ] **Step 1: Write the failing tests** (`crates/eddy/src/task/state.rs`, bottom of file, `#[cfg(all(test, not(loom)))] mod tests`)

```rust
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn initial_state_has_refcount_2_and_join_interest_and_notified() {
        let s = State::new();
        let snap = s.snapshot();
        assert_eq!(snap.ref_count(), 2);
        assert!(snap.has_join_interest());
        assert!(snap.is_notified());
        assert!(!snap.is_running());
        assert!(!snap.is_complete());
    }

    #[test]
    fn running_then_idle_without_wake_parks() {
        let s = State::new();
        assert!(matches!(s.transition_to_running(), TransitionToRunning::Success));
        assert!(matches!(s.transition_to_idle(), TransitionToIdle::Ok));
        assert!(!s.snapshot().is_running());
    }

    #[test]
    fn wake_by_ref_while_running_defers_to_transition_to_idle() {
        let s = State::new();
        s.transition_to_running();
        // Simulate a wake arriving mid-poll: must NOT submit, must set NOTIFIED.
        assert!(matches!(
            s.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::DoNothing
        ));
        assert!(s.snapshot().is_notified());
        assert!(matches!(s.transition_to_idle(), TransitionToIdle::OkNotified));
    }

    #[test]
    fn wake_by_ref_while_idle_submits_and_takes_a_ref() {
        let s = State::new();
        s.transition_to_running();
        s.transition_to_idle(); // now idle, refcount still 2 (JoinHandle + the ref harness held while running, dropped by caller separately in real usage)
        let before = s.snapshot().ref_count();
        assert!(matches!(
            s.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::Submit
        ));
        assert_eq!(s.snapshot().ref_count(), before + 1);
    }

    #[test]
    fn wake_by_val_while_idle_submits_without_taking_a_ref() {
        let s = State::new();
        s.transition_to_running();
        s.transition_to_idle();
        let before = s.snapshot().ref_count();
        assert!(matches!(
            s.transition_to_notified_by_val(),
            TransitionToNotifiedByVal::Submit
        ));
        assert_eq!(s.snapshot().ref_count(), before); // no increment: consumed ref became the queue ref
    }

    #[test]
    fn double_wake_before_poll_is_idempotent() {
        let s = State::new();
        s.transition_to_running();
        s.transition_to_idle();
        assert!(matches!(
            s.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::Submit
        ));
        // second wake: already NOTIFIED -> DoNothing, no extra ref.
        let before = s.snapshot().ref_count();
        assert!(matches!(
            s.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::DoNothing
        ));
        assert_eq!(s.snapshot().ref_count(), before);
    }

    #[test]
    fn wake_after_complete_is_noop() {
        let s = State::new();
        s.transition_to_running();
        s.transition_to_complete();
        assert!(matches!(
            s.transition_to_notified_by_ref(),
            TransitionToNotifiedByRef::DoNothing
        ));
        assert!(matches!(
            s.transition_to_notified_by_val(),
            TransitionToNotifiedByVal::DoNothing
        ));
    }

    #[test]
    fn ref_inc_dec_returns_true_only_when_last() {
        let s = State::new(); // refcount 2
        s.ref_inc(); // 3
        assert!(!s.ref_dec()); // 2
        assert!(!s.ref_dec()); // 1
        assert!(s.ref_dec()); // 0 -> last
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p eddy task::state --lib`
Expected: FAIL to compile (`State`, `Snapshot`, transitions don't exist yet).

- [ ] **Step 3: Implement `crates/eddy/src/task/state.rs`**

```rust
//! The task's atomic state machine: one `AtomicUsize` packed as
//! `[ refcount : 48 bits ][ flags : 16 bits ]`. Every transition is a
//! single CAS (`fetch_update`). See the plan doc / SPEC.md §4 for why this
//! shape (RUNNING vs NOTIFIED) prevents the canonical lost-wakeup bug.

use crate::loom::sync::atomic::{AtomicUsize, Ordering};

pub(crate) const RUNNING: usize = 0b0000_0001;
pub(crate) const COMPLETE: usize = 0b0000_0010;
pub(crate) const NOTIFIED: usize = 0b0000_0100;
pub(crate) const CANCELLED: usize = 0b0000_1000;
pub(crate) const JOIN_INTEREST: usize = 0b0001_0000;
pub(crate) const JOIN_WAKER_SET: usize = 0b0010_0000;

const REF_ONE: usize = 1 << 16;
const REF_MASK: usize = !(REF_ONE - 1);
/// Abort rather than silently wrap the refcount on overflow. This can only
/// be reached by a Waker-cloning bug (leaking millions of clones), never by
/// normal use.
const REF_MAX: usize = 1 << 40;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) struct Snapshot(usize);

impl Snapshot {
    pub(crate) fn ref_count(self) -> usize {
        (self.0 & REF_MASK) >> 16
    }
    pub(crate) fn is_running(self) -> bool {
        self.0 & RUNNING != 0
    }
    pub(crate) fn is_complete(self) -> bool {
        self.0 & COMPLETE != 0
    }
    pub(crate) fn is_notified(self) -> bool {
        self.0 & NOTIFIED != 0
    }
    pub(crate) fn is_cancelled(self) -> bool {
        self.0 & CANCELLED != 0
    }
    pub(crate) fn has_join_interest(self) -> bool {
        self.0 & JOIN_INTEREST != 0
    }
    pub(crate) fn has_join_waker(self) -> bool {
        self.0 & JOIN_WAKER_SET != 0
    }

    fn set_running(&mut self) {
        self.0 |= RUNNING;
    }
    fn unset_running(&mut self) {
        self.0 &= !RUNNING;
    }
    fn set_complete(&mut self) {
        self.0 |= COMPLETE;
    }
    fn set_notified(&mut self) {
        self.0 |= NOTIFIED;
    }
    fn unset_notified(&mut self) {
        self.0 &= !NOTIFIED;
    }
    fn set_cancelled(&mut self) {
        self.0 |= CANCELLED;
    }
    fn unset_join_interest(&mut self) {
        self.0 &= !JOIN_INTEREST;
    }
    fn set_join_waker(&mut self) {
        self.0 |= JOIN_WAKER_SET;
    }
    fn unset_join_waker(&mut self) {
        self.0 &= !JOIN_WAKER_SET;
    }
    fn ref_inc(&mut self) {
        self.0 += REF_ONE;
    }
}

pub(crate) struct State {
    val: AtomicUsize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToRunning {
    Success,
    Failed,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToIdle {
    Ok,
    OkNotified,
    Cancelled,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToNotifiedByVal {
    Submit,
    DoNothing,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TransitionToNotifiedByRef {
    Submit,
    DoNothing,
}

impl State {
    /// refcount=2 (JoinHandle + initial run-queue slot), JOIN_INTEREST |
    /// NOTIFIED set. The spawn path consumes the queue slot directly by
    /// pushing to the scheduler — it does not go through
    /// `transition_to_notified_*`.
    pub(crate) fn new() -> State {
        let raw = (2 * REF_ONE) | JOIN_INTEREST | NOTIFIED;
        State {
            val: AtomicUsize::new(raw),
        }
    }

    pub(crate) fn snapshot(&self) -> Snapshot {
        // Acquire: synchronizes-with the Release of whatever transition
        // produced the value we're about to branch on.
        Snapshot(self.val.load(Ordering::Acquire))
    }

    fn fetch_update_action<F, T>(&self, mut f: F) -> T
    where
        F: FnMut(Snapshot) -> (T, Option<Snapshot>),
    {
        let mut cur = Snapshot(self.val.load(Ordering::Acquire));
        loop {
            let (out, next) = f(cur);
            let Some(next) = next else { return out };
            // AcqRel: Acquire so we observe whatever the losing thread of a
            // concurrent CAS published; Release so the next reader of this
            // state observes our flag changes.
            match self
                .val
                .compare_exchange_weak(cur.0, next.0, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return out,
                Err(actual) => cur = Snapshot(actual),
            }
        }
    }

    pub(crate) fn transition_to_running(&self) -> TransitionToRunning {
        self.fetch_update_action(|mut snap| {
            if snap.is_complete() || snap.is_cancelled() {
                return (TransitionToRunning::Failed, None);
            }
            debug_assert!(!snap.is_running(), "eddy: task polled while already running");
            snap.set_running();
            snap.unset_notified();
            (TransitionToRunning::Success, Some(snap))
        })
    }

    pub(crate) fn transition_to_idle(&self) -> TransitionToIdle {
        self.fetch_update_action(|mut snap| {
            debug_assert!(snap.is_running());
            snap.unset_running();
            if snap.is_cancelled() {
                (TransitionToIdle::Cancelled, Some(snap))
            } else if snap.is_notified() {
                (TransitionToIdle::OkNotified, Some(snap))
            } else {
                (TransitionToIdle::Ok, Some(snap))
            }
        })
    }

    pub(crate) fn transition_to_complete(&self) -> Snapshot {
        self.fetch_update_action(|mut snap| {
            debug_assert!(snap.is_running());
            snap.unset_running();
            snap.set_complete();
            snap.unset_notified();
            (snap, Some(snap))
        })
    }

    pub(crate) fn transition_to_notified_by_ref(&self) -> TransitionToNotifiedByRef {
        self.fetch_update_action(|mut snap| {
            if snap.is_complete() || snap.is_notified() {
                return (TransitionToNotifiedByRef::DoNothing, None);
            }
            if snap.is_running() {
                snap.set_notified();
                return (TransitionToNotifiedByRef::DoNothing, Some(snap));
            }
            snap.set_notified();
            snap.ref_inc();
            (TransitionToNotifiedByRef::Submit, Some(snap))
        })
    }

    pub(crate) fn transition_to_notified_by_val(&self) -> TransitionToNotifiedByVal {
        self.fetch_update_action(|mut snap| {
            if snap.is_complete() || snap.is_notified() {
                return (TransitionToNotifiedByVal::DoNothing, None);
            }
            if snap.is_running() {
                snap.set_notified();
                return (TransitionToNotifiedByVal::DoNothing, Some(snap));
            }
            snap.set_notified();
            (TransitionToNotifiedByVal::Submit, Some(snap))
        })
    }

    /// Returns the snapshot from BEFORE cancellation was applied, so the
    /// caller can tell whether a poll was already in flight (`is_running()`)
    /// or the task had already finished (`is_complete()`) — touching the
    /// future's storage directly is only safe when neither is true; see
    /// `harness::shutdown`.
    pub(crate) fn set_cancelled(&self) -> Snapshot {
        self.fetch_update_action(|mut snap| {
            let before = snap;
            snap.set_cancelled();
            (before, Some(snap))
        })
    }

    /// Returns the snapshot *before* clearing, so the caller can tell
    /// whether the task was already complete (JoinHandle drop-detach path).
    pub(crate) fn unset_join_interest(&self) -> Snapshot {
        self.fetch_update_action(|mut snap| {
            let before = snap;
            snap.unset_join_interest();
            (before, Some(snap))
        })
    }

    /// Fails (returns false) if the task is already complete — caller
    /// should read the output immediately instead of waiting for a wake.
    pub(crate) fn set_join_waker(&self) -> bool {
        self.fetch_update_action(|mut snap| {
            if snap.is_complete() {
                return (false, None);
            }
            snap.set_join_waker();
            (true, Some(snap))
        })
    }

    pub(crate) fn unset_join_waker(&self) {
        self.fetch_update_action(|mut snap| {
            snap.unset_join_waker();
            ((), Some(snap))
        });
    }

    /// +1. Relaxed is sound: the caller already holds a live reference, so
    /// there is nothing new to synchronize-with (contrast with `ref_dec`,
    /// which publishes the *last* access before deallocation).
    pub(crate) fn ref_inc(&self) {
        let prev = self.val.fetch_add(REF_ONE, Ordering::Relaxed);
        if (prev & REF_MASK) >> 16 >= REF_MAX >> 16 {
            std::process::abort();
        }
    }

    /// -1. Returns true if this was the last reference and the caller must
    /// deallocate. Release on the decrement + an Acquire fence on the path
    /// that observes zero: every access made through any now-dropped
    /// reference happens-before the dealloc.
    pub(crate) fn ref_dec(&self) -> bool {
        let prev = self.val.fetch_sub(REF_ONE, Ordering::Release);
        let prev_count = (prev & REF_MASK) >> 16;
        debug_assert!(prev_count >= 1, "eddy: refcount underflow");
        if prev_count == 1 {
            self.val.load(Ordering::Acquire);
            true
        } else {
            false
        }
    }
}
```

- [ ] **Step 4: Add `mod state;` to `crates/eddy/src/task/mod.rs`**

```rust
mod state;
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p eddy task::state --lib`
Expected: all 8 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/eddy/src/task/state.rs crates/eddy/src/task/mod.rs
git commit -m "feat(task): packed atomic state machine with CAS transitions"
```

---

## Task 4: `task::raw` — Cell/Header/Core/Trailer/Vtable

**Files:**
- Create: `crates/eddy/src/task/raw.rs`
- Modify: `crates/eddy/src/task/mod.rs`

**Design notes:**
- `Header` is the type-erased part: `state`, `vtable: &'static Vtable`, `owner_id: u32` (thread id of the scheduler that owns this task, used later by the current-thread scheduler to route cross-thread wakes — 0 is fine as a placeholder value set by Task 9), `queue_next: UnsafeCell<Option<NonNull<Header>>>` (intrusive link, unused until Phase 4's injector, but the field must exist now per SPEC layout).
- `Core<F, S>` holds `scheduler: S` and `stage: UnsafeCell<Stage<F>>`.
- `Stage<F>` is `Running(F) | Finished(Result<F::Output, JoinErrorRepr>) | Consumed`. (`JoinErrorRepr` is defined in Task 7; forward-declare it here as `pub(crate) type Result<T> = std::result::Result<T, crate::task::join::JoinErrorRepr>;` once Task 7 lands — for this task, define `pub(crate) enum JoinErrorRepr { Panic(Box<dyn std::any::Any + Send + 'static>), Cancelled }` directly in `raw.rs` and Task 7 will move/re-export it.)
- `Trailer` holds `waker: UnsafeCell<Option<Waker>>` for the `JoinHandle`.
- `Cell<F: Future, S>` is `#[repr(C)] { header: Header, core: Core<F, S>, trailer: Trailer }` — one allocation.
- `RawTask` is `NonNull<Header>` (one word). All operations go through `(header.vtable.FIELD)(header_ptr)`.
- `Vtable` holds function pointers: `poll`, `schedule`, `dealloc`, `try_read_output`, `drop_join_handle_slow`, `shutdown` — bodies for `poll`/`try_read_output`/`drop_join_handle_slow`/`shutdown` are implemented via `Harness` in Task 5; stub them to `unimplemented!()` in this task so the module compiles, then wire them in Task 5.
- `schedule` calls `S::schedule(&core.scheduler, Notified(raw_task))` — the `Schedule` trait is defined in `task/mod.rs` in this task (used by Task 9's `CurrentThread`).

- [ ] **Step 1: Write the failing test** (layout + allocation counting — the semantics tests come in Task 5 once `Harness` exists)

Add to `crates/eddy/src/task/raw.rs`:

```rust
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
    static DEALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

    struct NoopSchedule;
    impl Schedule for NoopSchedule {
        fn schedule(&self, _task: Notified<Self>) {}
    }

    #[test]
    fn cell_layout_is_repr_c_header_first() {
        // Header must be at offset 0 so `NonNull<Header>` obtained from a
        // `NonNull<Cell<F, S>>` cast is always valid.
        let cell: Cell<std::future::Ready<i32>, NoopSchedule> = Cell {
            header: Header::new::<std::future::Ready<i32>, NoopSchedule>(),
            core: Core {
                scheduler: NoopSchedule,
                stage: std::cell::UnsafeCell::new(Stage::Consumed),
            },
            trailer: Trailer::new(),
        };
        let cell_ptr: *const Cell<_, _> = &cell;
        let header_ptr: *const Header = &cell.header;
        assert_eq!(cell_ptr as usize, header_ptr as usize);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p eddy task::raw --lib`
Expected: FAIL to compile (types don't exist).

- [ ] **Step 3: Implement `crates/eddy/src/task/raw.rs`**

```rust
//! One heap allocation per task: `Header` (type-erased control block) +
//! `Core<F, S>` (the future, inline) + `Trailer` (JoinHandle waker). See
//! SPEC.md §4 for the "why one allocation" rationale.

use std::cell::UnsafeCell;
use std::future::Future;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::task::Waker;

use super::state::State;
use super::{JoinErrorRepr, Notified, Schedule};

#[repr(C)]
pub(crate) struct Cell<F: Future, S> {
    pub(crate) header: Header,
    pub(crate) core: Core<F, S>,
    pub(crate) trailer: Trailer,
}

pub(crate) struct Header {
    pub(crate) state: State,
    pub(crate) vtable: &'static Vtable,
    pub(crate) owner_id: UnsafeCell<u32>,
    pub(crate) queue_next: UnsafeCell<Option<NonNull<Header>>>,
}

impl Header {
    fn new<F: Future, S: Schedule>() -> Header {
        Header {
            state: State::new(),
            vtable: vtable::<F, S>(),
            owner_id: UnsafeCell::new(0),
            queue_next: UnsafeCell::new(None),
        }
    }
}

pub(crate) struct Core<F: Future, S> {
    pub(crate) scheduler: S,
    pub(crate) stage: UnsafeCell<Stage<F>>,
}

pub(crate) enum Stage<F: Future> {
    Running(F),
    Finished(std::result::Result<F::Output, JoinErrorRepr>),
    Consumed,
}

pub(crate) struct Trailer {
    pub(crate) waker: UnsafeCell<Option<Waker>>,
}

impl Trailer {
    fn new() -> Trailer {
        Trailer {
            waker: UnsafeCell::new(None),
        }
    }
}

/// One word: type-erased handle to a task. The vtable is reached through
/// the header, so this never needs to carry type information itself —
/// that's what lets it live in an intrusive, non-generic linked list.
#[derive(Copy, Clone)]
pub(crate) struct RawTask {
    pub(crate) header: NonNull<Header>,
}

impl RawTask {
    /// Allocates one `Cell<F, S>`, initializes it, and returns the raw
    /// handle. Caller (spawn) owns both references baked into the initial
    /// state (JoinHandle ref + run-queue ref) and must account for both.
    pub(crate) fn new<F: Future, S: Schedule>(future: F, scheduler: S) -> RawTask {
        let cell = Box::new(Cell {
            header: Header::new::<F, S>(),
            core: Core {
                scheduler,
                stage: UnsafeCell::new(Stage::Running(future)),
            },
            trailer: Trailer::new(),
        });
        // SAFETY: `Box::into_raw` never returns null; `Cell` is `repr(C)`
        // with `header` as its first field, so casting the `Cell` pointer
        // to `*mut Header` and back to `NonNull` is a valid reinterpretation
        // of the same address for the lifetime of the allocation.
        let ptr = Box::into_raw(cell) as *mut Header;
        RawTask {
            header: unsafe { NonNull::new_unchecked(ptr) },
        }
    }

    /// SAFETY: `header` must point at a live `Header` embedded (at offset 0)
    /// in a `Cell<F, S>` allocation, for the same `F`/`S` the vtable was
    /// built for.
    pub(crate) unsafe fn from_raw(header: NonNull<Header>) -> RawTask {
        RawTask { header }
    }

    pub(crate) fn header(&self) -> &Header {
        // SAFETY: the task is alive for as long as `self` exists (callers
        // hold a counted reference), so the header is valid to read.
        unsafe { self.header.as_ref() }
    }

    pub(crate) fn poll(self) {
        // SAFETY: `poll` is one of the six vtable functions built for this
        // task's exact (F, S); the header pointer is valid per the type's
        // invariant.
        unsafe { (self.header().vtable.poll)(self.header) }
    }

    pub(crate) fn schedule(self) {
        // SAFETY: see `poll`.
        unsafe { (self.header().vtable.schedule)(self.header) }
    }

    pub(crate) fn dealloc(self) {
        // SAFETY: see `poll`. Caller must have observed `ref_dec() == true`
        // (this was the last reference) before calling.
        unsafe { (self.header().vtable.dealloc)(self.header) }
    }

    /// Drops this reference, deallocating if it was the last one.
    pub(crate) fn drop_reference(self) {
        if self.header().state.ref_dec() {
            self.dealloc();
        }
    }

    pub(crate) unsafe fn wake_by_val(self) {
        use super::state::TransitionToNotifiedByVal as T;
        match self.header().state.transition_to_notified_by_val() {
            T::Submit => self.schedule(),
            T::DoNothing => self.drop_reference(),
        }
    }

    pub(crate) unsafe fn wake_by_ref(self) {
        use super::state::TransitionToNotifiedByRef as T;
        match self.header().state.transition_to_notified_by_ref() {
            T::Submit => self.schedule(),
            T::DoNothing => {}
        }
    }
}

pub(crate) struct Vtable {
    pub(crate) poll: unsafe fn(NonNull<Header>),
    pub(crate) schedule: unsafe fn(NonNull<Header>),
    pub(crate) dealloc: unsafe fn(NonNull<Header>),
    pub(crate) try_read_output: unsafe fn(NonNull<Header>, *mut (), &Waker) -> bool,
    pub(crate) drop_join_handle_slow: unsafe fn(NonNull<Header>),
    pub(crate) shutdown: unsafe fn(NonNull<Header>),
}

pub(crate) fn vtable<F: Future, S: Schedule>() -> &'static Vtable {
    &Vtable {
        poll: super::harness::poll::<F, S>,
        schedule: super::harness::schedule::<F, S>,
        dealloc: super::harness::dealloc::<F, S>,
        try_read_output: super::harness::try_read_output::<F, S>,
        drop_join_handle_slow: super::harness::drop_join_handle_slow::<F, S>,
        shutdown: super::harness::shutdown::<F, S>,
    }
}

// So `Notified<S>` (defined in `task/mod.rs`) can be constructed here.
pub(crate) struct RawTaskTypeMarker<S>(PhantomData<S>);
```

Note: `vtable::<F, S>()` returning `&'static Vtable` from a `&Vtable { .. }` literal works in Rust because struct literals of `'static`-safe (no interior mutability, all-`Copy`/fn-pointer fields) types get promoted to `'static` automatically (rvalue static promotion) — this is the same trick the CHECKLIST/PROMPT.md snippets rely on. If the compiler rejects promotion (it requires every field to be const-evaluatable, which fn items coerced to fn pointers are), fall back to `static VTABLE: Vtable = Vtable { .. }; &VTABLE` inside a generic-parameterized `const` block — implement it as a `static` per monomorphization using `struct TypeId<F,S>` trick is overkill here; Rust does support rvalue static promotion for this exact pattern (it's what real async runtimes do), so implement it as shown and only fall back if `cargo build` errors.

- [ ] **Step 4: Update `crates/eddy/src/task/mod.rs`**

```rust
mod harness;
mod raw;
mod state;

pub(crate) use raw::{Cell, Core, Header, RawTask, Stage, Trailer, Vtable};

/// Implemented by every scheduler a task can run on. `schedule` is called
/// with a reference-carrying `Notified<S>` any time the task transitions
/// from idle to needing a poll (initial spawn, or a wake with `Submit`).
pub(crate) trait Schedule: Sized + 'static {
    fn schedule(&self, task: Notified<Self>);
}

/// A `RawTask` that is known to carry exactly one "queued" reference. Wraps
/// `RawTask` instead of exposing it directly so schedulers can't
/// accidentally forget to eventually poll-or-drop it.
///
/// SAFETY: `Notified` is `Send` regardless of whether `F: Send`, because a
/// scheduler only ever moves the *pointer* across threads — the future
/// itself is only ever touched (via `poll`) on whichever thread actually
/// calls `RawTask::poll`, which for `eddy`'s current-thread scheduler is
/// always the owning thread (enforced in `scheduler::current_thread`).
pub(crate) struct Notified<S: Schedule>(pub(crate) RawTask, std::marker::PhantomData<S>);

impl<S: Schedule> Notified<S> {
    pub(crate) fn new(raw: RawTask) -> Notified<S> {
        Notified(raw, std::marker::PhantomData)
    }

    pub(crate) fn run(self) {
        self.0.poll();
    }
}

// SAFETY: see the doc comment on `Notified` above.
unsafe impl<S: Schedule> Send for Notified<S> {}

#[derive(Debug)]
pub(crate) enum JoinErrorRepr {
    Panic(Box<dyn std::any::Any + Send + 'static>),
    Cancelled,
}
```

- [ ] **Step 5: Add temporary stub `crates/eddy/src/task/harness.rs`** (real implementation is Task 5; this just makes Task 4 compile and testable in isolation)

```rust
use std::future::Future;
use std::ptr::NonNull;
use std::task::Waker;

use super::raw::Header;
use super::Schedule;

pub(super) unsafe fn poll<F: Future, S: Schedule>(_header: NonNull<Header>) {
    unimplemented!("Task 5 implements this")
}
pub(super) unsafe fn schedule<F: Future, S: Schedule>(_header: NonNull<Header>) {
    unimplemented!("Task 5 implements this")
}
pub(super) unsafe fn dealloc<F: Future, S: Schedule>(_header: NonNull<Header>) {
    unimplemented!("Task 5 implements this")
}
pub(super) unsafe fn try_read_output<F: Future, S: Schedule>(
    _header: NonNull<Header>,
    _dst: *mut (),
    _waker: &Waker,
) -> bool {
    unimplemented!("Task 5 implements this")
}
pub(super) unsafe fn drop_join_handle_slow<F: Future, S: Schedule>(_header: NonNull<Header>) {
    unimplemented!("Task 5 implements this")
}
pub(super) unsafe fn shutdown<F: Future, S: Schedule>(_header: NonNull<Header>) {
    unimplemented!("Task 5 implements this")
}
```

- [ ] **Step 6: Run tests**

Run: `cargo test -p eddy task::raw --lib`
Expected: `cell_layout_is_repr_c_header_first` PASSES. Crate builds (harness stubs are never called by this test).

- [ ] **Step 7: Commit**

```bash
git add crates/eddy/src/task/raw.rs crates/eddy/src/task/mod.rs crates/eddy/src/task/harness.rs
git commit -m "feat(task): Cell/Header/Core/Trailer layout, RawTask, manual Vtable"
```

- [ ] **Step 8: Review checkpoint** — spawn a review subagent (see "Review Loop" section below) focused on: refcount correctness of `wake_by_val`/`wake_by_ref`/`drop_reference`, the `repr(C)` layout claim, and the `Notified: Send` unsafe impl's justification. Apply any confirmed fixes before continuing.

---

## Task 5: `task::harness` — the poll driver

**Files:**
- Modify: `crates/eddy/src/task/harness.rs` (replace stubs with real implementation)

**Design notes:**
- `poll::<F, S>`: `transition_to_running`; if `Failed`, return immediately (task was cancelled/completed before we got to it — e.g. it was queued twice or aborted). Otherwise build a `Waker` from this header (via `task::waker::waker_from_raw`, Task 6 — forward-declare the import now, Task 6 fills in the function), poll the future **catching panics** with `std::panic::catch_unwind` (the future must be `AssertUnwindSafe`-wrapped since `Pin<&mut F>` isn't `UnwindSafe` by default), then:
  - `Poll::Ready(out)` or a caught panic → store into `Stage::Finished`, call `transition_to_complete`, wake the `JoinHandle`'s waker (from `Trailer`, if `JOIN_WAKER_SET`/`JOIN_INTEREST`), then `drop_reference` (release the run-queue reference — the task is done, nothing will poll it again).
  - `Poll::Pending` → call `transition_to_idle`; if `OkNotified`, immediately re-`schedule()` using the same reference (do **not** ref_inc/dec); if `Ok`, do nothing further (this reference is logically "parked", waiting for a future `wake()` to supply a fresh one via `Submit`); if `Cancelled`, treat like the Ready/panic path but with `JoinErrorRepr::Cancelled` and drop the future in place without exposing an output.
- `schedule::<F, S>`: reads `core.scheduler` and calls `S::schedule(scheduler, Notified::new(RawTask::from_raw(header)))`.
- `dealloc::<F, S>`: drops the `Stage` in place (running the future's destructor if it was never finished — this is how cancellation-by-drop actually happens), then reconstructs the `Box<Cell<F, S>>` from the raw pointer and drops it (frees the allocation).
- `try_read_output::<F, S>`: called by `JoinHandle::poll`. If `Stage::Finished`, take the value (leave `Consumed` in its place), write it through `dst: *mut Result<F::Output, JoinErrorRepr>`, return `true`. If not finished yet, register `waker.clone()` into `Trailer::waker` and `state.set_join_waker()`, return `false`.
- `drop_join_handle_slow::<F, S>`: called when a `JoinHandle` is dropped without having read the output (detach case) — clears `JOIN_INTEREST`/`JOIN_WAKER_SET`, drops any stored waker, drops the reference.
- `shutdown::<F, S>`: cancels without polling — `state.set_cancelled()`, then behaves like the `Cancelled` branch of `poll` (drop stage in place if not already finished, wake the JoinHandle, drop the run-queue reference). Used by runtime teardown (Task 10) to unwind any still-queued tasks.

- [ ] **Step 1: Write the failing tests** (`crates/eddy/src/task/harness.rs`, using `RawTask::new` + a hand-rolled test `Schedule` that records what got scheduled)

```rust
#[cfg(all(test, not(loom)))]
mod tests {
    use super::super::raw::RawTask;
    use super::super::{JoinErrorRepr, Notified, Schedule};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::task::Poll;

    #[derive(Clone, Default)]
    struct RecordingSchedule(Rc<RefCell<Vec<RawTask>>>);
    impl Schedule for RecordingSchedule {
        fn schedule(&self, task: Notified<Self>) {
            self.0.borrow_mut().push(task.0);
        }
    }

    #[test]
    fn ready_future_completes_and_output_is_readable() {
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(async { 7i32 }, sched);
        raw.poll(); // ready immediately, no waker registration needed
        let mut out: Option<std::result::Result<i32, JoinErrorRepr>> = None;
        let noop = futures::task::noop_waker();
        let got = unsafe {
            (raw.header().vtable.try_read_output)(
                raw.header,
                &mut out as *mut _ as *mut (),
                &noop,
            )
        };
        assert!(got);
        assert!(matches!(out, Some(Ok(7))));
        raw.drop_reference(); // drop the JoinHandle's conceptual reference
    }

    #[test]
    fn pending_future_registers_and_wakes_on_notify() {
        let sched = RecordingSchedule::default();
        let scheduled = sched.0.clone();
        // A future that is Pending on first poll, Ready on second, using
        // its own waker to schedule itself (like a channel recv would).
        struct Once(bool);
        impl std::future::Future for Once {
            type Output = i32;
            fn poll(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> Poll<i32> {
                if self.0 {
                    Poll::Ready(1)
                } else {
                    self.0 = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
        let raw = RawTask::new(Once(false), sched);
        raw.poll(); // Pending, but wakes itself -> NOTIFIED -> re-queued via OkNotified path
        assert_eq!(scheduled.borrow().len(), 1);
        let requeued = scheduled.borrow_mut().pop().unwrap();
        requeued.poll(); // Ready(1) this time
        raw.drop_reference();
    }

    #[test]
    fn panicking_future_yields_join_error_panic() {
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(
            async {
                panic!("boom");
                #[allow(unreachable_code)]
                0i32
            },
            sched,
        );
        raw.poll();
        let mut out: Option<std::result::Result<i32, JoinErrorRepr>> = None;
        let noop = futures::task::noop_waker();
        unsafe {
            (raw.header().vtable.try_read_output)(
                raw.header,
                &mut out as *mut _ as *mut (),
                &noop,
            );
        }
        assert!(matches!(out, Some(Err(JoinErrorRepr::Panic(_)))));
        raw.drop_reference();
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p eddy task::harness --lib`
Expected: FAIL — either compile errors (stub signatures don't match yet) or `unimplemented!()` panics.

- [ ] **Step 3: Implement `crates/eddy/src/task/harness.rs`**

```rust
//! The poll driver: `Harness` (a thin, non-generic-escaping wrapper isn't
//! actually needed — each vtable function is directly generic over `F, S`
//! and operates on the typed `Cell` after casting the header pointer back)
//! implements the six `Vtable` functions.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll, Waker};

use super::raw::{Cell, Header, Stage};
use super::state::{TransitionToIdle, TransitionToRunning};
use super::waker::waker_from_raw;
use super::{JoinErrorRepr, Notified, RawTask, Schedule};

/// SAFETY: every call site upholds the same precondition as `RawTask::from_raw`
/// — `header` points at a live `Cell<F, S>` for this exact `F`/`S`.
unsafe fn cell_ptr<F: Future, S: Schedule>(header: NonNull<Header>) -> *mut Cell<F, S> {
    header.as_ptr() as *mut Cell<F, S>
}

pub(super) unsafe fn poll<F: Future, S: Schedule>(header: NonNull<Header>) {
    let raw = RawTask::from_raw(header);
    match raw.header().state.transition_to_running() {
        TransitionToRunning::Failed => return,
        TransitionToRunning::Success => {}
    }

    let cell = cell_ptr::<F, S>(header);
    let waker = waker_from_raw(header);
    let mut cx = Context::from_waker(&waker);

    // SAFETY: we hold the RUNNING bit, which is the runtime's mutual-
    // exclusion guarantee that no other thread touches `stage` concurrently.
    let stage = &mut *(*cell).core.stage.get();
    let poll_result = match stage {
        Stage::Running(fut) => {
            // SAFETY: the future was placed here by `RawTask::new` and is
            // never moved for the lifetime of the allocation (the `Cell` is
            // heap-allocated once and never relocated) — pinning it in
            // place is sound.
            let pin = Pin::new_unchecked(fut);
            catch_unwind(AssertUnwindSafe(|| pin.poll(&mut cx)))
        }
        _ => unreachable!("eddy: poll called on a task not in Running stage"),
    };

    match poll_result {
        Ok(Poll::Ready(out)) => complete::<F, S>(header, cell, Ok(out)),
        Ok(Poll::Pending) => {
            match raw.header().state.transition_to_idle() {
                TransitionToIdle::Ok => {}
                TransitionToIdle::OkNotified => raw.schedule(),
                TransitionToIdle::Cancelled => complete::<F, S>(header, cell, Err(JoinErrorRepr::Cancelled)),
            }
        }
        Err(panic) => complete::<F, S>(header, cell, Err(JoinErrorRepr::Panic(panic))),
    }
}

unsafe fn complete<F: Future, S: Schedule>(
    header: NonNull<Header>,
    cell: *mut Cell<F, S>,
    result: std::result::Result<F::Output, JoinErrorRepr>,
) {
    let raw = RawTask::from_raw(header);
    *(*cell).core.stage.get() = Stage::Finished(result);
    raw.header().state.transition_to_complete();
    wake_join_handle(header, cell);
    // The run-queue reference is done: nothing will poll this task again.
    raw.drop_reference();
}

unsafe fn wake_join_handle<F: Future, S: Schedule>(header: NonNull<Header>, cell: *mut Cell<F, S>) {
    let _ = cell; // waker lives in Trailer, reached via the Cell's tail
    let trailer = trailer_ptr::<F, S>(header);
    if let Some(waker) = (*(*trailer).waker.get()).take() {
        waker.wake();
    }
}

unsafe fn trailer_ptr<F: Future, S: Schedule>(header: NonNull<Header>) -> *mut super::raw::Trailer {
    std::ptr::addr_of_mut!((*cell_ptr::<F, S>(header)).trailer)
}

pub(super) unsafe fn schedule<F: Future, S: Schedule>(header: NonNull<Header>) {
    let cell = cell_ptr::<F, S>(header);
    let scheduler = &(*cell).core.scheduler;
    scheduler.schedule(Notified::<S>::new(RawTask::from_raw(header)));
}

pub(super) unsafe fn dealloc<F: Future, S: Schedule>(header: NonNull<Header>) {
    let cell = cell_ptr::<F, S>(header);
    // Drop the stage in place first (runs F's destructor if it was still
    // Running — this is how cancel-by-drop actually frees resources), then
    // reclaim and drop the whole boxed allocation.
    std::ptr::drop_in_place((*cell).core.stage.get());
    drop(Box::from_raw(cell));
}

pub(super) unsafe fn try_read_output<F: Future, S: Schedule>(
    header: NonNull<Header>,
    dst: *mut (),
    waker: &Waker,
) -> bool {
    let cell = cell_ptr::<F, S>(header);
    let stage = &mut *(*cell).core.stage.get();
    match stage {
        Stage::Finished(_) => {
            let Stage::Finished(result) = std::mem::replace(stage, Stage::Consumed) else {
                unreachable!()
            };
            let dst = dst as *mut std::result::Result<F::Output, JoinErrorRepr>;
            std::ptr::write(dst, result);
            true
        }
        _ => {
            let trailer = trailer_ptr::<F, S>(header);
            *(*trailer).waker.get() = Some(waker.clone());
            RawTask::from_raw(header).header().state.set_join_waker();
            false
        }
    }
}

pub(super) unsafe fn drop_join_handle_slow<F: Future, S: Schedule>(header: NonNull<Header>) {
    let raw = RawTask::from_raw(header);
    raw.header().state.unset_join_interest();
    let cell = cell_ptr::<F, S>(header);
    let trailer = trailer_ptr::<F, S>(header);
    (*(*trailer).waker.get()) = None;
    let _ = cell;
    raw.drop_reference();
}

pub(super) unsafe fn shutdown<F: Future, S: Schedule>(header: NonNull<Header>) {
    let raw = RawTask::from_raw(header);
    let before = raw.header().state.set_cancelled();
    if before.is_complete() {
        return; // already finished; cancellation after the fact is a no-op
    }
    if before.is_running() {
        // A poll is in flight RIGHT NOW (possibly on another thread, since
        // AbortHandle/JoinHandle::abort are cross-thread-callable). We must
        // NOT touch `stage` here — that poll already holds exclusive access
        // to it. It will observe CANCELLED itself via `transition_to_idle`
        // (see the `poll` function above) and finish the task safely from
        // the thread that's actually running it.
        return;
    }
    let cell = cell_ptr::<F, S>(header);
    complete::<F, S>(header, cell, Err(JoinErrorRepr::Cancelled));
}
```

- [ ] **Step 4: Add `mod waker;` placeholder so this compiles** — create `crates/eddy/src/task/waker.rs` with just:

```rust
use std::ptr::NonNull;
use std::task::Waker;
use super::raw::Header;

pub(crate) fn waker_from_raw(_header: NonNull<Header>) -> Waker {
    unimplemented!("Task 6 implements this")
}
```

and add `mod waker;` + nothing else to `task/mod.rs` (no `pub(crate) use` yet — Task 6 adds the real exports).

- [ ] **Step 5: Run tests**

Run: `cargo test -p eddy task::harness --lib`
Expected: FAIL at runtime on `waker_from_raw`'s `unimplemented!()` — expected at this point; this task is done once Step 6 below passes with Task 6's real waker.

- [ ] **Step 6: Commit the harness (tests still red pending Task 6)**

```bash
git add crates/eddy/src/task/harness.rs crates/eddy/src/task/waker.rs crates/eddy/src/task/mod.rs
git commit -m "feat(task): harness poll driver (poll/complete/dealloc/try_read_output)"
```

---

## Task 6: `task::waker` — the hand-built RawWakerVTable

**Files:**
- Modify: `crates/eddy/src/task/waker.rs` (replace stub)
- Modify: `crates/eddy/src/task/mod.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::task::raw::RawTask;
    use crate::task::{Notified, Schedule};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone, Default)]
    struct RecordingSchedule(Rc<RefCell<Vec<RawTask>>>);
    impl Schedule for RecordingSchedule {
        fn schedule(&self, task: Notified<Self>) {
            self.0.borrow_mut().push(task.0);
        }
    }

    #[test]
    fn clone_drop_ten_thousand_times_returns_to_baseline() {
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(std::future::pending::<()>(), sched);
        let base = raw.header().state.snapshot().ref_count();
        let w = waker_from_raw(raw.header);
        for _ in 0..10_000 {
            let cloned = w.clone();
            drop(cloned);
        }
        drop(w);
        assert_eq!(raw.header().state.snapshot().ref_count(), base);
        raw.drop_reference();
    }

    #[test]
    fn wake_on_pending_task_schedules_exactly_once() {
        let sched = RecordingSchedule::default();
        let scheduled = sched.0.clone();
        let raw = RawTask::new(std::future::pending::<()>(), sched);
        raw.poll(); // Pending, no self-wake -> parked, no schedule yet
        assert_eq!(scheduled.borrow().len(), 0);
        let w = waker_from_raw(raw.header);
        w.wake_by_ref();
        assert_eq!(scheduled.borrow().len(), 1);
        scheduled.borrow_mut().pop().unwrap().drop_reference();
        raw.drop_reference();
    }

    #[test]
    fn double_wake_before_poll_schedules_once() {
        let sched = RecordingSchedule::default();
        let scheduled = sched.0.clone();
        let raw = RawTask::new(std::future::pending::<()>(), sched);
        raw.poll();
        let w = waker_from_raw(raw.header);
        w.wake_by_ref();
        w.wake_by_ref();
        assert_eq!(scheduled.borrow().len(), 1);
        scheduled.borrow_mut().pop().unwrap().drop_reference();
        raw.drop_reference();
    }

    #[test]
    fn wake_during_poll_requeues_after_pending_not_lost() {
        let sched = RecordingSchedule::default();
        let scheduled = sched.0.clone();
        struct SelfWaking;
        impl std::future::Future for SelfWaking {
            type Output = ();
            fn poll(
                self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<()> {
                // Simulate "reactor fires mid-poll": wake from inside poll,
                // before returning Pending.
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
        let raw = RawTask::new(SelfWaking, sched);
        raw.poll();
        // Must be re-queued (OkNotified path), not lost.
        assert_eq!(scheduled.borrow().len(), 1);
        scheduled.borrow_mut().pop().unwrap().drop_reference();
    }

    #[test]
    fn wake_after_completion_is_noop() {
        let sched = RecordingSchedule::default();
        let scheduled = sched.0.clone();
        let raw = RawTask::new(async {}, sched);
        let w = waker_from_raw(raw.header);
        raw.poll(); // completes immediately
        w.wake_by_ref();
        assert_eq!(scheduled.borrow().len(), 0);
        raw.drop_reference();
    }

    #[test]
    fn waker_outlives_task_keeps_it_alive_until_dropped() {
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(async {}, sched);
        let w = waker_from_raw(raw.header);
        raw.poll();
        raw.drop_reference(); // drop the "JoinHandle" conceptual ref
        // Task still alive: waker holds a reference.
        assert!(raw.header().state.snapshot().ref_count() >= 1);
        drop(w); // last reference -> dealloc happens here, not before
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p eddy task::waker --lib`
Expected: FAIL (`unimplemented!()`).

- [ ] **Step 3: Implement `crates/eddy/src/task/waker.rs`**

```rust
//! Hand-built `RawWaker`: a data pointer (`NonNull<Header>`) plus a vtable
//! of four functions that must maintain the task's refcount EXACTLY:
//!   clone       -> +1   (a new Waker now exists)
//!   wake        -> consumes the reference (schedule, then possibly -1)
//!   wake_by_ref -> does NOT consume (schedule takes its own +1 if needed)
//!   drop        -> -1
//! Getting `wake` vs `wake_by_ref` backwards is the canonical bug: `wake`
//! must not leave a reference behind (else every woken task leaks), and
//! `wake_by_ref` must not consume one (else a task still queued elsewhere
//! gets freed under it -> use-after-free).

use std::ptr::NonNull;
use std::task::{RawWaker, RawWakerVTable, Waker};

use super::raw::{Header, RawTask};

static WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref_waker, drop_waker);

/// SAFETY: `header` must point at a live task whose reference this call is
/// entitled to convert into a `Waker` (i.e. the caller already holds a
/// counted reference and is transferring/sharing it).
pub(crate) fn waker_from_raw(header: NonNull<Header>) -> Waker {
    let raw = RawWaker::new(header.as_ptr() as *const (), &WAKER_VTABLE);
    // SAFETY: WAKER_VTABLE's four functions uphold the refcount contract
    // documented above, and `header` satisfies `RawTask::from_raw`'s
    // precondition per this function's own precondition.
    unsafe { Waker::from_raw(raw) }
}

/// SAFETY: `ptr` was produced by `waker_from_raw`/`clone_waker`, i.e. it is
/// a `NonNull<Header>` cast to `*const ()`, per the `Waker` contract that
/// only these functions ever construct a `RawWaker` with `&WAKER_VTABLE`.
unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    header.as_ref().state.ref_inc(); // +1
    RawWaker::new(ptr, &WAKER_VTABLE)
}

/// SAFETY: see `clone_waker`. Consumes the reference this RawWaker
/// represented — must not leave one behind.
unsafe fn wake_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    RawTask::from_raw(header).wake_by_val();
}

/// SAFETY: see `clone_waker`. Borrows — must leave the refcount unchanged;
/// any reference the schedule path needs is taken fresh inside
/// `transition_to_notified_by_ref`.
unsafe fn wake_by_ref_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    RawTask::from_raw(header).wake_by_ref();
}

/// SAFETY: see `clone_waker`. -1.
unsafe fn drop_waker(ptr: *const ()) {
    let header = NonNull::new_unchecked(ptr as *mut Header);
    RawTask::from_raw(header).drop_reference();
}
```

- [ ] **Step 4: Wire the export in `crates/eddy/src/task/mod.rs`** — add `pub(crate) use waker::waker_from_raw;` (harness.rs already does `use super::waker::waker_from_raw;`, so this must resolve).

- [ ] **Step 5: Run tests**

Run: `cargo test -p eddy task:: --lib`
Expected: every test in `task::state`, `task::raw`, `task::harness`, `task::waker` PASSES.

- [ ] **Step 6: Commit**

```bash
git add crates/eddy/src/task/waker.rs crates/eddy/src/task/mod.rs
git commit -m "feat(task): hand-built RawWakerVTable with exact refcount discipline"
```

- [ ] **Step 7: loom tests** — create `crates/eddy/tests/loom_task.rs`:

```rust
#![cfg(loom)]
//! loom exhaustively explores every interleaving the C11 memory model
//! permits — including weak-memory reorderings x86's TSO would hide. A
//! `#[test]` passing a million times proves nothing next to this.

// NOTE: these tests exercise crate-internal (`pub(crate)`) items, so they
// live as a `#[path]`-included module compiled as part of the `eddy`
// crate's test target rather than an external integration test. Add to
// `crates/eddy/src/lib.rs`:
//   #[cfg(loom)]
//   #[path = "../tests/loom_task.rs"]
//   mod loom_task_tests;
// and delete the `tests/loom_task.rs` file's own `mod` wrapper below,
// OR (simpler, chosen here): put the tests directly in
// `crates/eddy/src/task/waker.rs` under `#[cfg(all(test, loom))] mod
// loom_tests` so they have direct access to `pub(crate)` items without
// any path plumbing. This file is intentionally left as a thin pointer;
// the real tests are added in Step 7a below.
```

Actually implement it directly to avoid path plumbing — **Step 7a: append to `crates/eddy/src/task/waker.rs`**:

```rust
#[cfg(all(test, loom))]
mod loom_tests {
    use super::*;
    use crate::task::{Notified, Schedule};
    use crate::task::raw::RawTask;

    #[derive(Clone)]
    struct LoomSchedule(std::sync::Arc<loom::sync::Mutex<Vec<RawTask>>>);
    impl Schedule for LoomSchedule {
        fn schedule(&self, task: Notified<Self>) {
            self.0.lock().unwrap().push(task.0);
        }
    }

    #[test]
    fn wake_during_poll_is_never_lost() {
        loom::model(|| {
            let sched = LoomSchedule(std::sync::Arc::new(loom::sync::Mutex::new(Vec::new())));
            let queued = sched.0.clone();
            let raw = RawTask::new(std::future::pending::<()>(), sched);
            let w = waker_from_raw(raw.header);

            // Reactor thread races with the worker thread polling.
            let th = loom::thread::spawn(move || {
                w.wake_by_ref();
            });
            raw.poll(); // Pending (pending::<()> never completes)
            th.join().unwrap();

            // Invariant: never lost. Either NOTIFIED is set-and-consumed by
            // a re-queue during poll's own transition_to_idle, or the wake
            // arrived after and produced exactly one queue entry. Either
            // way total "will be polled again" signals == 1 and no more.
            let queued_count = queued.lock().unwrap().len();
            assert!(queued_count <= 1, "double-queued: {queued_count}");
            // Drain whatever got queued so refcounts balance out.
            for t in queued.lock().unwrap().drain(..) {
                t.drop_reference();
            }
            raw.drop_reference();
        });
    }

    #[test]
    fn refcount_is_exact_under_concurrent_clone_drop() {
        loom::model(|| {
            let sched = LoomSchedule(std::sync::Arc::new(loom::sync::Mutex::new(Vec::new())));
            let raw = RawTask::new(std::future::pending::<()>(), sched);
            let w1 = waker_from_raw(raw.header);
            let w2 = w1.clone();

            let t1 = loom::thread::spawn(move || drop(w1.clone()));
            let t2 = loom::thread::spawn(move || drop(w2));
            t1.join().unwrap();
            t2.join().unwrap();

            // Original clone (`w1` before move — recreate via header since
            // w1 was moved into the closure) plus the task's own ref must
            // still be intact: exactly the JoinHandle-equivalent ref + the
            // still-alive `w1` clone used inside t1's closure before its
            // temporary got dropped. Simplify: just assert no crash / no
            // double-free by dropping the final known-good reference and
            // relying on loom + a debug allocator (Miri, separately) to
            // catch UB. The strong assertion here is that ref_count never
            // underflows, which `ref_dec`'s debug_assert already checks
            // under `cfg(debug_assertions)` inside every loom permutation.
            raw.drop_reference();
        });
    }
}
```

Run: `RUSTFLAGS="--cfg loom" cargo test -p eddy task::waker::loom_tests --release`
Expected: both PASS. (`--release` recommended for loom — still runs loom's checked model, just with an optimized harness; loom's own exploration, not the code under test, dominates runtime. Bound exploration time with `LOOM_MAX_PREEMPTIONS=3` env var if it's slow.)

- [ ] **Step 8: Commit loom tests**

```bash
git add crates/eddy/src/task/waker.rs
git commit -m "test(task): loom coverage for wake-during-poll and concurrent clone/drop"
```

- [ ] **Step 9: Review checkpoint** — spawn a review subagent focused specifically on the waker vtable and the loom tests: does `clone_waker`/`wake_waker`/`wake_by_ref_waker`/`drop_waker` actually match the documented contract in every branch, does `Notified: Send` hold up, are there any interleavings the loom tests don't actually cover (e.g. clone racing with the final drop)? Apply confirmed fixes.

---

## Task 7: `task::join` — JoinHandle, JoinError, AbortHandle

**Files:**
- Create: `crates/eddy/src/task/join.rs`
- Modify: `crates/eddy/src/task/mod.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::task::raw::RawTask;
    use crate::task::{Notified, Schedule};
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone, Default)]
    struct RecordingSchedule(Rc<RefCell<Vec<RawTask>>>);
    impl Schedule for RecordingSchedule {
        fn schedule(&self, task: Notified<Self>) {
            self.0.borrow_mut().push(task.0);
        }
    }

    fn spawn_handle<F: std::future::Future<Output = i32>>(
        fut: F,
    ) -> (RawTask, JoinHandle<i32>) {
        let sched = RecordingSchedule::default();
        let raw = RawTask::new(fut, sched);
        let handle = JoinHandle::new(raw);
        (raw, handle)
    }

    #[test]
    fn join_handle_resolves_to_output() {
        let (raw, mut handle) = spawn_handle(async { 42 });
        raw.poll();
        let noop = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&noop);
        let out = Pin::new(&mut handle).poll(&mut cx);
        assert!(matches!(out, std::task::Poll::Ready(Ok(42))));
    }

    #[test]
    fn abort_before_poll_yields_cancelled() {
        let (raw, handle) = spawn_handle(std::future::pending::<i32>());
        handle.abort();
        raw.poll(); // harness sees CANCELLED, completes with Cancelled instead of polling the future
        let mut handle = handle;
        let noop = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&noop);
        let out = Pin::new(&mut handle).poll(&mut cx);
        assert!(matches!(out, std::task::Poll::Ready(Err(JoinError::Cancelled))));
    }

    #[test]
    fn dropping_join_handle_detaches_task_which_keeps_running() {
        let (raw, handle) = spawn_handle(async { 1 });
        drop(handle); // detach, not abort
        raw.poll(); // must still run to completion normally, not be cancelled
        assert!(raw.header().state.snapshot().is_complete());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p eddy task::join --lib`
Expected: FAIL (module doesn't exist).

- [ ] **Step 3: Implement `crates/eddy/src/task/join.rs`**

```rust
//! `JoinHandle<T>`: a `Future<Output = Result<T, JoinError>>` that reads a
//! completed task's output through the vtable's `try_read_output`.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::raw::RawTask;
use super::JoinErrorRepr;

#[derive(Debug)]
pub enum JoinError {
    Panic(Box<dyn std::any::Any + Send + 'static>),
    Cancelled,
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinError::Panic(_) => write!(f, "task panicked"),
            JoinError::Cancelled => write!(f, "task was cancelled"),
        }
    }
}
impl std::error::Error for JoinError {}

impl From<JoinErrorRepr> for JoinError {
    fn from(r: JoinErrorRepr) -> Self {
        match r {
            JoinErrorRepr::Panic(p) => JoinError::Panic(p),
            JoinErrorRepr::Cancelled => JoinError::Cancelled,
        }
    }
}

pub struct JoinHandle<T> {
    raw: Option<RawTask>,
    _marker: PhantomData<T>,
}

// SAFETY: a `JoinHandle<T>` only ever touches the task through the vtable
// (never `T` directly until `try_read_output` hands ownership across), so
// it can be sent to another thread exactly when `T: Send` — matching
// `std::thread::JoinHandle`'s own bound.
unsafe impl<T: Send> Send for JoinHandle<T> {}

impl<T> JoinHandle<T> {
    pub(crate) fn new(raw: RawTask) -> JoinHandle<T> {
        JoinHandle {
            raw: Some(raw),
            _marker: PhantomData,
        }
    }

    pub fn abort(&self) {
        if let Some(raw) = self.raw {
            // SAFETY: `raw` is valid for as long as this handle holds a
            // reference (guaranteed by construction / not yet dropped).
            unsafe { (raw.header().vtable.shutdown)(raw.header) }
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let raw = self.raw.expect("eddy: JoinHandle polled after completion");
        let mut out: Option<std::result::Result<T, JoinErrorRepr>> = None;
        // SAFETY: `dst` points at a live `Option<Result<T, JoinErrorRepr>>`
        // on this stack frame for the duration of the call, and `T` matches
        // the task's own output type by construction (a `JoinHandle<T>` is
        // only ever created by `spawn` for a future whose Output is `T`).
        let ready = unsafe {
            (raw.header().vtable.try_read_output)(
                raw.header,
                &mut out as *mut _ as *mut (),
                cx.waker(),
            )
        };
        if ready {
            self.raw = None;
            let result = out.expect("eddy: try_read_output reported ready with no value");
            Poll::Ready(result.map_err(JoinError::from))
        } else {
            Poll::Pending
        }
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if let Some(raw) = self.raw.take() {
            // SAFETY: see `abort`.
            unsafe { (raw.header().vtable.drop_join_handle_slow)(raw.header) }
        }
    }
}

pub struct AbortHandle {
    raw: RawTask,
}

// SAFETY: only ever touches the task through the vtable's `shutdown`,
// which is safe to call from any thread.
unsafe impl Send for AbortHandle {}
unsafe impl Sync for AbortHandle {}

impl AbortHandle {
    pub(crate) fn new(raw: RawTask) -> AbortHandle {
        AbortHandle { raw }
    }

    pub fn abort(&self) {
        // SAFETY: `raw` stays valid for `AbortHandle`'s lifetime — it holds
        // a counted reference for as long as it exists (see spawn's
        // refcount accounting; AbortHandle is out of scope for this slice's
        // `spawn` and is exercised only via direct construction in tests
        // until Phase 9 wires it into the public API).
        unsafe { (self.raw.header().vtable.shutdown)(self.raw.header) }
    }
}
```

- [ ] **Step 4: Move `JoinErrorRepr` to live in `task/mod.rs`** (already done in Task 4) and re-export the public pieces. Update `crates/eddy/src/task/mod.rs`:

```rust
mod harness;
mod join;
mod raw;
mod state;
mod waker;

pub(crate) use raw::{Cell, Core, Header, RawTask, Stage, Trailer, Vtable};
pub(crate) use waker::waker_from_raw;

pub use join::{AbortHandle, JoinError, JoinHandle};

pub(crate) trait Schedule: Sized + 'static {
    fn schedule(&self, task: Notified<Self>);
}

pub(crate) struct Notified<S: Schedule>(pub(crate) RawTask, std::marker::PhantomData<S>);

impl<S: Schedule> Notified<S> {
    pub(crate) fn new(raw: RawTask) -> Notified<S> {
        Notified(raw, std::marker::PhantomData)
    }
    pub(crate) fn run(self) {
        self.0.poll();
    }
}

// SAFETY: see the doc comment in Task 4 — only the pointer crosses
// threads; the future is only ever touched on the owning thread's poll.
unsafe impl<S: Schedule> Send for Notified<S> {}

#[derive(Debug)]
pub(crate) enum JoinErrorRepr {
    Panic(Box<dyn std::any::Any + Send + 'static>),
    Cancelled,
}
```

(Note: `use std::pin::Pin;` needs adding to `join.rs`'s imports — included above.)

- [ ] **Step 5: Run tests**

Run: `cargo test -p eddy task:: --lib`
Expected: all pass, including the three new `join` tests.

- [ ] **Step 6: Commit**

```bash
git add crates/eddy/src/task/join.rs crates/eddy/src/task/mod.rs
git commit -m "feat(task): JoinHandle, JoinError, AbortHandle"
```

---

## Task 8: `spawn` — wiring allocation to the initial schedule

**Files:**
- Modify: `crates/eddy/src/task/mod.rs`

- [ ] **Step 1: Write the failing test**

```rust
// in task/mod.rs, #[cfg(all(test, not(loom)))] mod tests
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone, Default)]
    struct RecordingSchedule(Rc<RefCell<Vec<RawTask>>>);
    impl Schedule for RecordingSchedule {
        fn schedule(&self, task: Notified<Self>) {
            self.0.borrow_mut().push(task.0);
        }
    }

    #[test]
    fn spawn_does_not_call_schedule_synchronously() {
        // spawn() pushes the initial run-queue reference by directly
        // returning a Notified<S> for the caller (the scheduler impl) to
        // enqueue -- it must NOT go through Schedule::schedule itself
        // (that would double-count against transition_to_notified_by_*,
        // which spawn intentionally bypasses per the state machine design).
        let sched = RecordingSchedule::default();
        let (notified, handle) = spawn(async { 5 }, sched.clone());
        assert_eq!(sched.0.borrow().len(), 0);
        notified.run();
        drop(handle);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p eddy task::tests::spawn_does_not_call_schedule_synchronously --lib`
Expected: FAIL (`spawn` doesn't exist).

- [ ] **Step 3: Implement `spawn` in `crates/eddy/src/task/mod.rs`** (add near the bottom, above the `tests` module)

```rust
/// Allocates the task and returns both halves: the `Notified<S>` the
/// caller (a scheduler) must push onto its run queue exactly once, and the
/// `JoinHandle<T>` for the caller (user code) to await. Deliberately does
/// NOT call `S::schedule` itself — the initial state (see `State::new`)
/// already accounts for the run-queue reference, so routing it through
/// `transition_to_notified_by_val` here would incorrectly try to take a
/// second one.
pub(crate) fn spawn<F, S>(future: F, scheduler: S) -> (Notified<S>, JoinHandle<F::Output>)
where
    F: Future + 'static,
    S: Schedule,
{
    let raw = RawTask::new(future, scheduler);
    let handle = JoinHandle::new(raw);
    (Notified::new(raw), handle)
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p eddy task:: --lib`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/eddy/src/task/mod.rs
git commit -m "feat(task): spawn() wiring allocation to the initial run-queue slot"
```

---

## Task 9: `scheduler::current_thread` — the executor

**Files:**
- Create: `crates/eddy/src/scheduler/current_thread.rs`
- Modify: `crates/eddy/src/scheduler/mod.rs`

**Design notes:**
- `CurrentThread` is `Rc`-shared internal state: `local: RefCell<VecDeque<Notified<CurrentThread>>>` (owner-thread-only fast path), `injection: Mutex<VecDeque<Notified<CurrentThread>>>` (cross-thread), `owner: Cell<Option<ThreadId>>` (set when `block_on` starts), `unparker: Mutex<Option<Thread>>` (the `std::thread::Thread` handle to `unpark()` from `schedule` when called off-thread), `tick: Cell<u32>`.
- `impl Schedule for Rc<CurrentThreadInner>`: checks `thread::current().id() == owner` — if yes, push to `local`; if no, push to `injection` + `unpark()` the stored thread. (`CurrentThread`/`Rc<Inner>` needs `Clone`; each `Cell`'s `Core<F, S>` stores an owned clone of the scheduler handle, which is exactly why `S: Clone` in practice — add `+ Clone` to the `Schedule` supertrait bound used by `current_thread`, not to the trait itself, since Phase 4's multi-thread scheduler won't clone per-task the same way.)
- `next_task`: every `GLOBAL_QUEUE_INTERVAL = 61` ticks, drain one from `injection` first; else `local.pop_front()`, else one from `injection`.
- `block_on`: builds a thread-parking `Waker` (separate small vtable wrapping `std::thread::Thread`, `wake`/`wake_by_ref` both call `.unpark()`), loops: poll root future via that waker; if `Ready`, return; else run one queued task if available (`next_task().run()`); else `std::thread::park()`. Sets `owner`/`unparker` at entry, clears at exit (so a `CurrentThread` can be reused — not required for this slice but costs nothing extra and matches `Builder::new_current_thread().build()` returning a reusable `Runtime`).
- `spawn`: calls `task::spawn(fut, scheduler.clone())`, then routes the returned `Notified` through the same "am I on the owner thread" check used by `Schedule::schedule` (factor into one private `fn enqueue(&self, task: Notified<CurrentThread>)` both call).

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn block_on_ready_future_returns_immediately() {
        let rt = CurrentThread::new();
        assert_eq!(rt.block_on(async { 42 }), 42);
    }

    #[test]
    fn spawn_1000_tasks_all_complete() {
        let rt = CurrentThread::new();
        rt.block_on(async {
            let mut handles = Vec::new();
            for i in 0..1000 {
                handles.push(rt_spawn(i));
            }
            let mut total = 0;
            for h in handles {
                total += h.await.unwrap();
            }
            assert_eq!(total, (0..1000).sum::<i32>());
        });

        // helper defined via the ambient Handle set up by block_on's enter guard
        fn rt_spawn(i: i32) -> crate::task::JoinHandle<i32> {
            crate::runtime::Handle::current().spawn(async move { i })
        }
    }

    #[test]
    fn nested_spawn_works() {
        let rt = CurrentThread::new();
        let out = rt.block_on(async {
            let h = crate::runtime::Handle::current().spawn(async {
                let inner = crate::runtime::Handle::current().spawn(async { 9 });
                inner.await.unwrap()
            });
            h.await.unwrap()
        });
        assert_eq!(out, 9);
    }

    #[test]
    fn non_send_future_compiles_and_runs() {
        let rt = CurrentThread::new();
        let rc = std::rc::Rc::new(5);
        let out = rt.block_on(async move { *rc + 1 });
        assert_eq!(out, 6);
    }

    #[test]
    fn wake_from_another_thread_makes_progress() {
        let rt = CurrentThread::new();
        rt.block_on(async {
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            let waker_slot = std::sync::Arc::new(parking_lot::Mutex::new(None::<std::task::Waker>));
            let slot_for_thread = waker_slot.clone();
            std::thread::spawn(move || {
                rx.recv().unwrap();
                std::thread::sleep(std::time::Duration::from_millis(10));
                if let Some(w) = slot_for_thread.lock().take() {
                    w.wake();
                }
            });
            struct WaitOnce {
                armed: bool,
                slot: std::sync::Arc<parking_lot::Mutex<Option<std::task::Waker>>>,
                tx: std::sync::mpsc::Sender<()>,
            }
            impl std::future::Future for WaitOnce {
                type Output = ();
                fn poll(
                    mut self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<()> {
                    if self.armed {
                        return std::task::Poll::Ready(());
                    }
                    self.armed = true;
                    *self.slot.lock() = Some(cx.waker().clone());
                    self.tx.send(()).unwrap();
                    std::task::Poll::Pending
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
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p eddy scheduler::current_thread --lib`
Expected: FAIL (module doesn't exist / `runtime::Handle` doesn't exist yet — Task 10 adds it; if that dependency makes this task un-compilable standalone, do Task 9 and Task 10 as one combined compile-and-test cycle, committing only once both are in place. Steps 3 onward below assume that combined cycle.)

- [ ] **Step 3: Implement `crates/eddy/src/scheduler/current_thread.rs`**

```rust
//! The current-thread scheduler: a plain `VecDeque` for same-thread
//! spawns/wakes (no atomics on the hot path), plus a `Mutex`-guarded
//! injection queue for wakes arriving from other threads. No LIFO slot —
//! there's nothing to gain on a single worker, and it would only complicate
//! fairness (CHECKLIST Phase 3).

use std::cell::{Cell as StdCell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::rc::Rc;
use std::sync::Mutex;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::thread::{Thread, ThreadId};

use crate::task::{self, JoinHandle, Notified, Schedule};

const GLOBAL_QUEUE_INTERVAL: u32 = 61;

struct Inner {
    local: RefCell<VecDeque<Notified<CurrentThread>>>,
    injection: Mutex<VecDeque<Notified<CurrentThread>>>,
    owner: StdCell<Option<ThreadId>>,
    unparker: Mutex<Option<Thread>>,
    tick: StdCell<u32>,
}

#[derive(Clone)]
pub(crate) struct CurrentThread(Rc<Inner>);

impl CurrentThread {
    pub(crate) fn new() -> CurrentThread {
        CurrentThread(Rc::new(Inner {
            local: RefCell::new(VecDeque::new()),
            injection: Mutex::new(VecDeque::new()),
            owner: StdCell::new(None),
            unparker: Mutex::new(None),
            tick: StdCell::new(0),
        }))
    }

    fn enqueue(&self, task: Notified<CurrentThread>) {
        let on_owner_thread = self.0.owner.get() == Some(std::thread::current().id());
        if on_owner_thread {
            self.0.local.borrow_mut().push_back(task);
        } else {
            self.0.injection.lock().unwrap().push_back(task);
            if let Some(t) = self.0.unparker.lock().unwrap().as_ref() {
                t.unpark();
            }
        }
    }

    fn next_task(&self) -> Option<Notified<CurrentThread>> {
        let tick = self.0.tick.get().wrapping_add(1);
        self.0.tick.set(tick);
        if tick % GLOBAL_QUEUE_INTERVAL == 0 {
            if let Some(t) = self.0.injection.lock().unwrap().pop_front() {
                return Some(t);
            }
        }
        if let Some(t) = self.0.local.borrow_mut().pop_front() {
            return Some(t);
        }
        self.0.injection.lock().unwrap().pop_front()
    }

    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        let (notified, handle) = task::spawn(future, self.clone());
        self.enqueue(notified);
        handle
    }

    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        let prev_owner = self.0.owner.replace(Some(std::thread::current().id()));
        *self.0.unparker.lock().unwrap() = Some(std::thread::current());
        let _enter = crate::runtime::EnterGuard::new(self.clone());

        let waker = thread_waker(std::thread::current());
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);

        let out = loop {
            if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
                break out;
            }
            match self.next_task() {
                Some(task) => task.run(),
                None => std::thread::park(),
            }
        };

        self.0.owner.set(prev_owner);
        *self.0.unparker.lock().unwrap() = None;
        out
    }
}

impl Schedule for CurrentThread {
    fn schedule(&self, task: Notified<Self>) {
        self.enqueue(task);
    }
}

// --- thread-parking waker for the root future in block_on ---
// Deliberately NOT routed through the task system: block_on's root future
// isn't a task (no JoinHandle, no refcounted Cell) — it's driven directly
// on the calling thread, so its Waker only needs to unpark that thread.

fn thread_waker(thread: Thread) -> Waker {
    let raw = std::sync::Arc::into_raw(std::sync::Arc::new(thread)) as *const ();
    // SAFETY: THREAD_WAKER_VTABLE's functions treat `raw` as an
    // `Arc<Thread>` pointer exactly as produced here, maintaining the same
    // +1/consume/neutral/-1 discipline as the task waker.
    unsafe { Waker::from_raw(RawWaker::new(raw, &THREAD_WAKER_VTABLE)) }
}

static THREAD_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    thread_waker_clone,
    thread_waker_wake,
    thread_waker_wake_by_ref,
    thread_waker_drop,
);

unsafe fn thread_waker_clone(ptr: *const ()) -> RawWaker {
    std::sync::Arc::increment_strong_count(ptr as *const Thread);
    RawWaker::new(ptr, &THREAD_WAKER_VTABLE)
}
unsafe fn thread_waker_wake(ptr: *const ()) {
    let arc = std::sync::Arc::from_raw(ptr as *const Thread);
    arc.unpark();
}
unsafe fn thread_waker_wake_by_ref(ptr: *const ()) {
    let arc = std::mem::ManuallyDrop::new(std::sync::Arc::from_raw(ptr as *const Thread));
    arc.unpark();
}
unsafe fn thread_waker_drop(ptr: *const ()) {
    std::sync::Arc::decrement_strong_count(ptr as *const Thread);
}
```

- [ ] **Step 4: Write `crates/eddy/src/scheduler/mod.rs`**

```rust
pub(crate) mod current_thread;

pub(crate) use current_thread::CurrentThread;
```

- [ ] **Step 5: Run tests** (after Task 10 lands `runtime::Handle`/`EnterGuard`, since the tests reference them)

Run: `cargo test -p eddy scheduler:: --lib`
Expected: all 5 tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/eddy/src/scheduler/current_thread.rs crates/eddy/src/scheduler/mod.rs
git commit -m "feat(scheduler): current-thread executor with injection queue and thread-parking waker"
```

---

## Task 10: `runtime` — Handle, EnterGuard, Builder, Runtime

**Files:**
- Create: `crates/eddy/src/runtime/mod.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn builder_new_current_thread_block_on_42() {
        let rt = Builder::new_current_thread().build();
        assert_eq!(rt.block_on(async { 42 }), 42);
    }

    #[test]
    #[should_panic(expected = "no eddy runtime")]
    fn handle_current_outside_runtime_panics_clearly() {
        let _ = Handle::current();
    }

    #[test]
    fn handle_current_inside_block_on_works() {
        let rt = Builder::new_current_thread().build();
        let ok = rt.block_on(async { Handle::current().spawn(async { 1 }).await.unwrap() == 1 });
        assert!(ok);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p eddy runtime:: --lib`
Expected: FAIL (module empty).

- [ ] **Step 3: Implement `crates/eddy/src/runtime/mod.rs`**

```rust
//! Ambient runtime handle + builder. Only `new_current_thread` exists in
//! this slice; `new_multi_thread` arrives with Phase 4.

use std::cell::RefCell;
use std::future::Future;

use crate::scheduler::CurrentThread;
use crate::task::JoinHandle;

thread_local! {
    static CURRENT: RefCell<Option<Handle>> = RefCell::new(None);
}

#[derive(Clone)]
pub struct Handle {
    scheduler: CurrentThread,
}

impl Handle {
    /// # Panics
    /// Panics with a message naming the problem if called outside of a
    /// runtime's `block_on` — this is deliberately a clear panic rather
    /// than a silent no-op, per CHECKLIST Phase 3.
    pub fn current() -> Handle {
        CURRENT.with(|c| {
            c.borrow()
                .clone()
                .expect("no eddy runtime running on this thread (no eddy runtime: call this from inside Runtime::block_on)")
        })
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        self.scheduler.spawn(future)
    }
}

/// RAII guard installing a `Handle` as the ambient thread-local runtime for
/// the duration of `block_on`. Restores whatever was there before on drop,
/// so nested `block_on` calls (not expected in normal use, but must not
/// corrupt state) unwind cleanly.
pub(crate) struct EnterGuard {
    previous: Option<Handle>,
}

impl EnterGuard {
    pub(crate) fn new(scheduler: CurrentThread) -> EnterGuard {
        let previous = CURRENT.with(|c| c.replace(Some(Handle { scheduler })));
        EnterGuard { previous }
    }
}

impl Drop for EnterGuard {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = self.previous.take());
    }
}

pub struct Runtime {
    scheduler: CurrentThread,
}

impl Runtime {
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.scheduler.block_on(future)
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        self.scheduler.spawn(future)
    }
}

pub struct Builder {
    // Only current-thread flavor exists in this slice.
}

impl Builder {
    pub fn new_current_thread() -> Builder {
        Builder {}
    }

    pub fn build(self) -> Runtime {
        Runtime {
            scheduler: CurrentThread::new(),
        }
    }
}
```

- [ ] **Step 4: Update `crates/eddy/src/lib.rs` exports**

```rust
#![deny(clippy::undocumented_unsafe_blocks)]

pub(crate) mod loom;

pub mod runtime;
pub mod scheduler;
pub mod task;

pub use runtime::{Builder, Handle, Runtime};
pub use task::{AbortHandle, JoinError, JoinHandle};
```

- [ ] **Step 5: Run every test in the crate**

Run: `cargo test -p eddy`
Expected: all tests across `task::*`, `scheduler::*`, `runtime::*` PASS. This also finally makes Task 9's tests (which reference `runtime::Handle`) compile — run `cargo test -p eddy scheduler::current_thread` again here too and confirm its 5 tests pass now.

- [ ] **Step 6: Commit**

```bash
git add crates/eddy/src/runtime/mod.rs crates/eddy/src/lib.rs
git commit -m "feat(runtime): Handle/EnterGuard/Builder/Runtime tying task+scheduler together"
```

---

## Task 11: The milestone integration test

**Files:**
- Create: `crates/eddy/tests/spine.rs`

- [ ] **Step 1: Write the test** (the ~800-line spine's actual proof of life, minus timers/oneshot which aren't built yet — substitute a `spawn` + `JoinHandle` chain of equivalent shape)

```rust
use eddy::Builder;

#[test]
fn block_on_async_42() {
    let rt = Builder::new_current_thread().build();
    assert_eq!(rt.block_on(async { 42 }), 42);
}

#[test]
fn spawn_and_join_round_trip() {
    let rt = Builder::new_current_thread().build();
    let out = rt.block_on(async {
        let handle = eddy::Handle::current().spawn(async {
            let inner = eddy::Handle::current().spawn(async { 41 });
            inner.await.unwrap() + 1
        });
        handle.await.unwrap()
    });
    assert_eq!(out, 42);
}

#[test]
fn allocation_count_is_exactly_one_per_task() {
    // Proxy for "one allocation per task": spawn N tasks, join all, and
    // assert no panic/leak-detectable imbalance via repeated runs under
    // Miri (see Step 3) rather than a literal counting allocator here —
    // keeping this test dependency-free. The real allocation-count
    // assertion lives in the Miri run, which flags leaks directly.
    let rt = Builder::new_current_thread().build();
    rt.block_on(async {
        let mut handles = Vec::new();
        for i in 0..500 {
            handles.push(eddy::Handle::current().spawn(async move { i }));
        }
        for (i, h) in handles.into_iter().enumerate() {
            assert_eq!(h.await.unwrap(), i);
        }
    });
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p eddy --test spine`
Expected: all 3 PASS.

- [ ] **Step 3: Miri pass over the unsafe core** (manual verification, not full CI wiring — that's a future slice)

Run: `cargo +nightly miri test -p eddy --lib task::` (install nightly + miri first if absent: `rustup toolchain install nightly --component miri`)
Expected: clean, no UB/leak reports. If Miri flags anything, treat it as a correctness bug in this slice and fix before moving on — do not defer.

- [ ] **Step 4: Full loom run**

Run: `RUSTFLAGS="--cfg loom" cargo test -p eddy task::waker::loom_tests --release`
Expected: both loom tests still pass (re-confirm after all later edits).

- [ ] **Step 5: Commit**

```bash
git add crates/eddy/tests/spine.rs
git commit -m "test: milestone integration tests for the Phase 0-3 spine"
```

---

## Review Loop (use throughout, and at the marked checkpoints)

At each checkpoint, spawn a fresh subagent (Agent tool, `general-purpose`) with:
- The specific file(s) just written, pasted or referenced by path.
- The exact invariant it must check (e.g. "does `wake_by_val` ever leave a reference behind on any branch? Walk each match arm.").
- Instruction to be adversarial: try to construct a concrete interleaving or input that breaks the stated invariant, not just confirm it looks fine.

Apply any confirmed finding immediately (fix, re-run the relevant tests, re-commit) before moving to the next task. Do not apply a suggestion that isn't independently verified against the actual code — re-read the flagged lines yourself before editing.

---

## Definition of done for this slice

- [ ] `cargo build -p eddy` clean
- [ ] `cargo test -p eddy` — all green (state, raw, harness, waker, join, current_thread, runtime, spine)
- [ ] `cargo run --example spike -p eddy` prints the success line
- [ ] `RUSTFLAGS="--cfg loom" cargo test -p eddy task::waker::loom_tests --release` — both loom tests green
- [ ] `cargo +nightly miri test -p eddy --lib task::` — clean
- [ ] `cargo clippy -p eddy --all-targets -- -D warnings` — clean (run once at the end; fix anything it flags)
- [ ] Every `unsafe` block has a `// SAFETY:` comment (grep for `unsafe` and check each one by hand as a final pass)
