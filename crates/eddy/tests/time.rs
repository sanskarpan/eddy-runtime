use std::future::{pending, Future};
use std::time::{Duration, Instant};

use eddy::time::{interval, sleep, timeout, Elapsed, MissedTickBehavior};
use eddy::Builder;

#[test]
fn sleep_wakes_on_current_thread_runtime() {
    let runtime = Builder::new_current_thread().build();
    let started = Instant::now();
    runtime.block_on(async {
        let mut timer = sleep(Duration::from_millis(10));
        (&mut timer).await;
        timer.reset(Instant::now() + Duration::from_millis(5));
        (&mut timer).await;
    });
    assert!(started.elapsed() >= Duration::from_millis(14));
}

#[test]
fn sleep_wakes_on_multi_thread_runtime() {
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        sleep(Duration::from_millis(10)).await;
    });
}

#[test]
fn sleep_reset_to_a_later_deadline_does_not_fire_early() {
    // L1 regression: resetting an armed sleep to a later deadline must not
    // leave the stale deadline armed in the wheel, or the wheel fires it
    // early and the sleep resolves before its new deadline.
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let sleeper = sleep(Duration::from_millis(60));
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut pinned = std::pin::pin!(sleeper);
        assert!(pinned.as_mut().poll(&mut cx).is_pending());
        pinned
            .as_mut()
            .get_mut()
            .reset(Instant::now() + Duration::from_secs(5));
        assert!(pinned.as_mut().poll(&mut cx).is_pending());
        // Wait well past the stale +60ms deadline: it must not fire.
        sleep(Duration::from_millis(250)).await;
        assert!(pinned.as_mut().poll(&mut cx).is_pending());
    });
}

#[test]
fn timeout_has_completion_and_elapsed_paths() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        assert_eq!(timeout(Duration::from_millis(50), async { 7 }).await, Ok(7));
        assert_eq!(
            timeout(Duration::from_millis(5), pending::<()>()).await,
            Err(Elapsed)
        );
    });
}

#[test]
fn interval_ticks_and_supports_missed_tick_behavior() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut ticker = interval(Duration::from_millis(5));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let first = ticker.tick().await;
        let second = ticker.tick().await;
        assert!(second > first);
    });
}
