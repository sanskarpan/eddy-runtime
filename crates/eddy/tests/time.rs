use std::future::pending;
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
