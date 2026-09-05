use std::future::{pending, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, RawWaker, RawWakerVTable, Waker};
use std::time::{Duration, Instant};

use eddy::future::{race, ready, Either};
use eddy::time::{timeout, timeout_at};
use eddy::{Builder, CancellationToken};

struct WakerCounts {
    clones: AtomicUsize,
    wakes: AtomicUsize,
    drops: AtomicUsize,
}

unsafe fn counting_clone(ptr: *const ()) -> RawWaker {
    // SAFETY: `ptr` is an `Arc<WakerCounts>` produced by `into_raw`.
    let counts = unsafe { Arc::from_raw(ptr as *const WakerCounts) };
    let cloned = Arc::clone(&counts);
    counts.clones.fetch_add(1, Ordering::SeqCst);
    std::mem::forget(counts);
    RawWaker::new(Arc::into_raw(cloned) as *const (), &COUNTING_VTABLE)
}

unsafe fn counting_wake(ptr: *const ()) {
    // SAFETY: `ptr` is an `Arc<WakerCounts>` produced by `into_raw`.
    let counts = unsafe { Arc::from_raw(ptr as *const WakerCounts) };
    counts.wakes.fetch_add(1, Ordering::SeqCst);
    std::mem::forget(counts);
}

unsafe fn counting_wake_ref(ptr: *const ()) {
    // SAFETY: `ptr` is a live `Arc<WakerCounts>` for the duration of this call.
    let counts = unsafe { &*(ptr as *const WakerCounts) };
    counts.wakes.fetch_add(1, Ordering::SeqCst);
}

unsafe fn counting_drop(ptr: *const ()) {
    // SAFETY: `ptr` is an `Arc<WakerCounts>` produced by `into_raw`.
    let counts = unsafe { Arc::from_raw(ptr as *const WakerCounts) };
    counts.drops.fetch_add(1, Ordering::SeqCst);
}

static COUNTING_VTABLE: RawWakerVTable = RawWakerVTable::new(
    counting_clone,
    counting_wake,
    counting_wake_ref,
    counting_drop,
);

fn counting_waker(counts: Arc<WakerCounts>) -> Waker {
    // SAFETY: `COUNTING_VTABLE` matches the `Arc<WakerCounts>` data pointer.
    unsafe {
        Waker::from_raw(RawWaker::new(
            Arc::into_raw(counts) as *const (),
            &COUNTING_VTABLE,
        ))
    }
}

#[test]
fn join_and_try_join_complete_in_order() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        assert_eq!(eddy::join!(ready(1), ready(2), ready(3)).await, (1, 2, 3));
        let joined: Result<(i32, i32, i32), &'static str> = eddy::try_join!(
            async { Ok::<_, &'static str>(1) },
            async { Ok::<_, &'static str>(2) },
            async { Ok::<_, &'static str>(3) },
        )
        .await;
        assert_eq!(joined, Ok((1, 2, 3)));
        assert_eq!(
            eddy::try_join!(async { Ok::<_, &'static str>(1) }, async {
                Err::<i32, _>("failed")
            },)
            .await,
            Err("failed")
        );
    });
}

#[test]
fn try_join_three_short_circuits_on_error() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        // The middle future never completes: the macro must resolve to the
        // fast error anyway. On the old code this awaited all three and hung.
        let result = timeout(
            Duration::from_millis(100),
            eddy::try_join!(
                async { Err::<i32, &'static str>("fast failure") },
                std::future::pending::<Result<i32, &'static str>>(),
                async { Ok::<_, &'static str>(0) },
            ),
        )
        .await;
        assert_eq!(result, Ok(Err("fast failure")));
        // And the success path still yields all three outputs.
        let joined = eddy::try_join!(
            async { Ok::<i32, &'static str>(1) },
            async { Ok::<i32, &'static str>(2) },
            async { Ok::<i32, &'static str>(3) },
        )
        .await;
        assert_eq!(joined, Ok((1, 2, 3)));
    });
}

#[test]
fn select_and_race_drop_losers() {
    struct Dropped(Arc<AtomicUsize>);
    impl Future for Dropped {
        type Output = i32;
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<i32> {
            std::task::Poll::Pending
        }
    }
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let dropped = Arc::new(AtomicUsize::new(0));
        assert_eq!(race(ready(9), Dropped(dropped.clone())).await, 9);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);

        let value = eddy::select! {
            value = ready(4) => value + 1,
            other = ready(8) => other + 1,
        };
        assert!(value == 5 || value == 9);
        let biased = eddy::select! {
            biased;
            value = ready(4) => value,
            other = ready(8) => other,
        };
        assert_eq!(biased, 4);
    });
}

#[test]
fn default_select_alternates_immediately_ready_branches() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut left = 0;
        let mut right = 0;
        for _ in 0..10_000 {
            match eddy::select! {
                a = ready(()) => Either::Left(a),
                b = ready(()) => Either::Right(b),
            } {
                Either::Left(()) => left += 1,
                Either::Right(()) => right += 1,
            }
        }
        assert!(left > 1_000 && right > 1_000);
        let (a, b, c, d) = eddy::join!(async { 1 }, async { 2 }, async { 3 }, async { 4 }).await;
        assert_eq!((a, b, c, d), (1, 2, 3, 4));
        let three = eddy::select! {
            a = ready(1) => a,
            b = pending::<i32>() => b,
            c = pending::<i32>() => c,
        };
        assert_eq!(three, 1);
    });
}

#[test]
fn select_else_runs_when_all_branches_disabled_by_guards() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let value = eddy::select! {
            a = ready(1), if false => a,
            b = ready(2), if false => b,
            else => 42,
        };
        assert_eq!(value, 42);
        let biased = eddy::select! {
            biased;
            a = ready(1), if false => a,
            b = ready(2), if false => b,
            else => 43
        };
        assert_eq!(biased, 43);
    });
}

#[test]
#[should_panic(expected = "all branches are disabled and there is no else branch")]
fn select_without_else_panics_when_all_branches_disabled() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let _ = eddy::select! {
            a = ready(1), if false => a,
            b = ready(2), if false => b,
        };
    });
}

#[test]
fn select_guard_expression_is_evaluated_exactly_once() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let evaluated = std::sync::Arc::new(AtomicUsize::new(0));
        let side = std::sync::Arc::clone(&evaluated);
        let value = eddy::select! {
            a = ready(1), if { side.fetch_add(1, Ordering::SeqCst); false } => a,
            b = ready(2), if true => b,
            else => 42,
        };
        assert_eq!(value, 2);
        assert_eq!(evaluated.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn select_pattern_mismatch_disables_branch_and_waits_for_other() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        // `Some(3)` fails against `Some(4)`: the first branch is disabled and
        // the second one wins even though the loser resolved first.
        let value = eddy::select! {
            Some(3) = ready(Some(4)) => 3,
            b = ready(7) => b,
            else => 0,
        };
        assert_eq!(value, 7);
    });
}

#[test]
fn select_mismatch_and_guard_together_reach_else() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let value = eddy::select! {
            Some(3) = ready(Some(4)) => 3,
            b = ready(9), if false => b,
            else => 42,
        };
        assert_eq!(value, 42);
    });
}

#[test]
fn select_else_break_exits_user_loop() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        // The first branch's pattern keeps failing once `n == 3`, disabling
        // the branch; `else => break` must exit the *user's* loop.
        let mut n = 0;
        loop {
            eddy::select! {
                Some(v) = ready(if n < 3 { Some(n) } else { None }) => n = v + 1,
                else => break,
            }
        }
        assert_eq!(n, 3);
    });
}

#[test]
fn select_biased_prefers_first_branch_each_round() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut left = 0;
        let mut right = 0;
        for _ in 0..10_000 {
            match eddy::select! {
                biased;
                _ = ready(()) => 1,
                _ = ready(()) => 2,
            } {
                1 => left += 1,
                2 => right += 1,
                _ => unreachable!(),
            }
        }
        assert_eq!(right, 0);
        assert_eq!(left, 10_000);
    });
}

#[test]
fn timeout_at_and_yield_now_work() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let deadline = Instant::now() + Duration::from_millis(5);
        assert!(timeout_at(deadline, std::future::pending::<()>())
            .await
            .is_err());
        eddy::future::yield_now().await;
    });
}

#[test]
fn cancellation_token_propagates_to_children() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        parent.cancel();
        child.cancelled().await;
        assert!(parent.is_cancelled());
        assert!(child.is_cancelled());
    });
}

#[test]
fn cancelled_polling_does_not_leak_wakers() {
    let token = CancellationToken::new();
    let counts = Arc::new(WakerCounts {
        clones: AtomicUsize::new(0),
        wakes: AtomicUsize::new(0),
        drops: AtomicUsize::new(0),
    });
    let waker = counting_waker(counts.clone());
    let mut cx = Context::from_waker(&waker);
    let mut fut = Box::pin(token.cancelled());
    for _ in 0..100_000 {
        assert!(fut.as_mut().poll(&mut cx).is_pending());
    }
    // Repeated polls with the same waker reuse the slot: a clone happens
    // once at registration. The old code appended a waker per poll.
    assert!(
        counts.clones.load(Ordering::SeqCst) <= 2,
        "each poll leaked a waker: {} clones",
        counts.clones.load(Ordering::SeqCst)
    );
    token.cancel();
    assert_eq!(
        counts.wakes.load(Ordering::SeqCst),
        1,
        "cancel must wake exactly the live waiter"
    );
    drop(fut);
}

#[test]
fn dropped_cancelled_future_is_not_woken() {
    let token = CancellationToken::new();
    let counts = Arc::new(WakerCounts {
        clones: AtomicUsize::new(0),
        wakes: AtomicUsize::new(0),
        drops: AtomicUsize::new(0),
    });
    let waker = counting_waker(counts.clone());
    let mut cx = Context::from_waker(&waker);
    let mut first = Box::pin(token.cancelled());
    let mut second = Box::pin(token.cancelled());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    drop(first);
    token.cancel();
    assert_eq!(
        counts.wakes.load(Ordering::SeqCst),
        1,
        "a dropped waiter must be unregistered before cancel"
    );
    assert!(second.as_mut().poll(&mut cx).is_ready());
}
