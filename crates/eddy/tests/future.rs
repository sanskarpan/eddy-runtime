use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use eddy::future::{race, ready, select2, Either};
use eddy::time::timeout_at;
use eddy::{Builder, CancellationToken};

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
