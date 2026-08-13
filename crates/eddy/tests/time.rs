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

#[test]
fn delay_queue_delivers_expired_items_in_deadline_order() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut queue = eddy::time::DelayQueue::new();
        let now = Instant::now();
        let late = queue.insert_at(now + Duration::from_millis(20), "late");
        let early = queue.insert_at(now + Duration::from_millis(10), "early");
        assert_eq!(queue.len(), 2);
        assert!(!queue.is_empty());

        let first = timeout(Duration::from_secs(1), queue.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.key(), early);
        assert_eq!(first.into_inner(), "early");

        let second = timeout(Duration::from_secs(1), queue.next())
            .await
            .unwrap()
            .unwrap();
        let (item, key, _deadline) = second.into_parts();
        assert_eq!(item, "late");
        assert_eq!(key, late);
        assert!(queue.is_empty());
    });
}

#[test]
fn delay_queue_inserts_with_past_deadline_are_delivered_immediately() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut queue = eddy::time::DelayQueue::new();
        let key = queue.insert_at(Instant::now() - Duration::from_secs(1), "due");
        let expired = timeout(Duration::from_secs(1), queue.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.key(), key);
        assert_eq!(expired.into_inner(), "due");
    });
}

#[test]
fn delay_queue_remove_before_expiry_cancels_and_returns_value() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut queue = eddy::time::DelayQueue::new();
        let key = queue.insert_at(Instant::now() + Duration::from_millis(50), "cancel-me");
        assert_eq!(queue.remove(key), Some("cancel-me"));
        assert_eq!(queue.remove(key), None);
        // The cancelled timer must never fire: next() stays pending forever.
        let result = timeout(Duration::from_millis(10), queue.next()).await;
        assert!(result.is_err(), "cancelled item must not be delivered");
    });
}

#[test]
fn delay_queue_expired_keys_cannot_be_removed() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut queue = eddy::time::DelayQueue::new();
        let key = queue.insert_at(Instant::now() + Duration::from_millis(10), "expired");
        let _ = timeout(Duration::from_secs(1), queue.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(queue.remove(key), None);
    });
}

#[test]
fn delay_queue_remove_if_only_removes_accepted_values() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut queue = eddy::time::DelayQueue::new();
        let key = queue.insert_at(Instant::now() + Duration::from_millis(50), 7);
        assert_eq!(queue.remove_if(key, |value| *value > 10), None);
        assert_eq!(queue.remove_if(key, |value| *value < 10), Some(7));
    });
}

#[test]
fn delay_queue_wakes_across_worker_threads() {
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let mut queue = eddy::time::DelayQueue::new();
        let key = queue.insert_at(Instant::now() + Duration::from_millis(10), "cross");
        let expired = timeout(Duration::from_secs(1), queue.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.key(), key);
        assert_eq!(expired.into_inner(), "cross");
    });
}

#[test]
fn delay_queue_close_ends_pending_next() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let mut queue: eddy::time::DelayQueue<()> = eddy::time::DelayQueue::new();
        queue.close();
        assert!(queue.is_closed());
        let result = timeout(Duration::from_millis(10), queue.next()).await;
        assert!(matches!(result, Ok(None)), "closed empty queue ends next()");
    });
}

#[test]
fn paused_time_sleeps_without_real_time_passing() {
    let runtime = Builder::new_current_thread().build();
    let started = Instant::now();
    runtime.block_on(async {
        eddy::time::pause();
        eddy::time::sleep(Duration::from_millis(100)).await;
    });
    assert!(
        started.elapsed() < Duration::from_millis(50),
        "paused sleep took {:?} of real time",
        started.elapsed()
    );
}

#[test]
fn paused_time_advance_fires_spawned_timers() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        eddy::time::pause();
        let task = eddy::Handle::current().spawn(async {
            eddy::time::sleep(Duration::from_millis(50)).await;
            42
        });
        eddy::time::advance(Duration::from_millis(100));
        assert_eq!(task.await.unwrap(), 42);
    });
}

#[test]
fn paused_time_now_reflects_advances() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        eddy::time::pause();
        let start = eddy::time::now();
        eddy::time::advance(Duration::from_millis(1_000));
        let end = eddy::time::now();
        assert_eq!(end - start, Duration::from_millis(1_000));
        eddy::time::resume();
        assert!(!eddy::time::pause_enabled());
    });
}

#[test]
fn paused_time_auto_advance_ticks_when_idle() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        eddy::time::pause();
        eddy::time::auto_advance(true);
        let start = eddy::time::now();
        // Poll the clock directly: no timers are armed, so progress only
        // happens through the idle auto-advance step.
        while eddy::time::now() < start + Duration::from_millis(10) {
            eddy::future::yield_now().await;
        }
    });
}

#[test]
fn paused_time_on_multi_thread_runtime() {
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    let started = Instant::now();
    runtime.block_on(async {
        eddy::time::pause();
        eddy::time::sleep(Duration::from_millis(100)).await;
        eddy::time::advance(Duration::from_millis(500));
    });
    assert!(
        started.elapsed() < Duration::from_millis(50),
        "paused multi-thread sleep took {:?} of real time",
        started.elapsed()
    );
}

#[test]
fn paused_time_delay_queue_advances_on_park() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        eddy::time::pause();
        let mut queue: eddy::time::DelayQueue<u64> = eddy::time::DelayQueue::new();
        queue.insert(eddy::time::now() + Duration::from_millis(200), 7);
        let expired = timeout(Duration::from_secs(1), queue.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.deadline(), eddy::time::now());
        assert_eq!(expired.into_inner(), 7);
    });
}
