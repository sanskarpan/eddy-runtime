#![cfg(feature = "instrumentation")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use eddy::{clear_subscriber, set_subscriber, Builder, RuntimeEvent};

static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn task_events_and_metrics_cross_worker_boundaries() {
    let _lock = TEST_LOCK.lock().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_sink = events.clone();
    set_subscriber(Arc::new(move |event| {
        events_for_sink.lock().unwrap().push(event);
    }));

    let rt = Builder::new_multi_thread().worker_threads(2).build();
    rt.block_on(async {
        let handle = eddy::Handle::current().spawn(async {
            eddy::time::sleep(Duration::from_millis(1)).await;
        });
        handle.await.unwrap();

        let semaphore = Arc::new(eddy::sync::Semaphore::new(1));
        let first = semaphore.acquire().await.unwrap();
        let semaphore_for_waiter = semaphore.clone();
        let waiter = eddy::Handle::current()
            .spawn(async move { semaphore_for_waiter.acquire().await.unwrap() });
        eddy::time::sleep(Duration::from_millis(5)).await;
        drop(first);
        drop(waiter.await.unwrap());
    });
    clear_subscriber();

    let events = events.lock().unwrap();
    assert!(events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::TaskSpawned { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::TaskPollStart { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::TaskPollEnd { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::TimerSet { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::TimerFired { .. })));
    assert!(events
        .iter()
        .any(|event| matches!(event, RuntimeEvent::ResourceContended { .. })));
    drop(events);

    let snapshot = rt.metrics().snapshot();
    assert!(snapshot.total_polls > 0);
    assert!(snapshot.scheduled_tasks > 0);
}

#[test]
fn named_spawn_is_present_in_events_and_task_dump() {
    let _lock = TEST_LOCK.lock().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_sink = events.clone();
    set_subscriber(Arc::new(move |event| {
        events_for_sink.lock().unwrap().push(event);
    }));

    let rt = Builder::new_current_thread().build();
    let handle = rt.spawn_named("named-task", std::future::pending::<()>());
    let snapshots = rt.task_snapshots();
    assert!(snapshots.iter().any(|task| {
        task.name.as_deref() == Some("named-task")
            && matches!(task.state, eddy::TaskState::Queued | eddy::TaskState::Idle)
    }));
    drop(handle);
    rt.shutdown_timeout(Duration::ZERO);
    clear_subscriber();

    assert!(events.lock().unwrap().iter().any(|event| matches!(
        event,
        RuntimeEvent::TaskSpawned {
            name: Some(name),
            ..
        } if name == "named-task"
    )));
}

#[test]
fn metrics_are_runtime_scoped_and_track_queued_tasks() {
    let _lock = TEST_LOCK.lock().unwrap();
    let first = Builder::new_current_thread().build();
    let second = Builder::new_current_thread().build();
    let handle = first.spawn(std::future::pending::<()>());

    let first_snapshot = first.metrics().snapshot();
    assert_eq!(first_snapshot.active_tasks, 1);
    assert_eq!(first_snapshot.queue_depth, 1);
    assert!(first_snapshot.worker_busy_ratio <= 1.0);
    assert_eq!(second.metrics().snapshot().active_tasks, 0);

    drop(handle);
    first.shutdown_timeout(Duration::ZERO);
    assert_eq!(first.metrics().snapshot().active_tasks, 0);
    assert_eq!(first.metrics().snapshot().queue_depth, 0);
    second.shutdown_timeout(Duration::ZERO);
}
