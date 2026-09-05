#![cfg(not(loom))]

use std::future::pending;
use std::time::{Duration, Instant};

use eddy::time::sleep;
use eddy::{Builder, TaskState};

#[test]
fn watchdog_reports_stalled_tasks_before_the_test_timeout() {
    let runtime = Builder::new_current_thread().build();
    let stalled = runtime.spawn_named("watchdog-stalled", pending::<()>());

    let report = runtime.block_on(async {
        // Let the spawned task reach its stable pending await point before
        // the watchdog starts sampling the registry.
        eddy::future::yield_now().await;
        let deadline = Instant::now() + Duration::from_millis(100);
        loop {
            let snapshots = runtime.dump_tasks();
            if snapshots.iter().any(|snapshot| {
                snapshot.name.as_deref() == Some("watchdog-stalled")
                    && matches!(snapshot.state, TaskState::Queued | TaskState::Idle)
            }) {
                break snapshots;
            }
            assert!(
                Instant::now() < deadline,
                "watchdog did not observe the stalled task before its deadline"
            );
            sleep(Duration::from_millis(5)).await;
        }
    });

    assert!(report.iter().any(|snapshot| {
        snapshot.name.as_deref() == Some("watchdog-stalled")
            && matches!(snapshot.state, TaskState::Queued | TaskState::Idle)
    }));

    stalled.abort();
    runtime.block_on(async {
        assert!(matches!(stalled.await, Err(eddy::JoinError::Cancelled)));
    });
    assert!(runtime.dump_tasks().is_empty());
}
