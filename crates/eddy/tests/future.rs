use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, RawWaker, RawWakerVTable, Waker};
use std::time::{Duration, Instant};

use eddy::future::{race, ready, select2, Either};
use eddy::time::{timeout, timeout_at};
use eddy::{Builder, CancellationToken};

struct WakerCounts {
    clones: AtomicUsize,
    wakes: AtomicUsize,
    drops: AtomicUsize,
}

unsafe fn counting_clone(ptr: *const ()) -> RawWaker {
    let counts = unsafe { Arc::from_raw(ptr as *const WakerCounts) };
    let cloned = Arc::clone(&counts);
    counts.clones.fetch_add(1, Ordering::SeqCst);
    std::mem::forget(counts);
    RawWaker::new(Arc::into_raw(cloned) as *const (), &COUNTING_VTABLE)
}

unsafe fn counting_wake(ptr: *const ()) {
    let counts = unsafe { Arc::from_raw(ptr as *const WakerCounts) };
    counts.wakes.fetch_add(1, Ordering::SeqCst);
    std::mem::forget(counts);
}

unsafe fn counting_wake_ref(ptr: *const ()) {
    let counts = unsafe { &*(ptr as *const WakerCounts) };
    counts.wakes.fetch_add(1, Ordering::SeqCst);
}

unsafe fn counting_drop(ptr: *const ()) {
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
            match select2(ready(()), ready(())).await {
                Either::Left(()) => left += 1,
                Either::Right(()) => right += 1,
            }
        }
        assert!(left > 1_000 && right > 1_000);
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
