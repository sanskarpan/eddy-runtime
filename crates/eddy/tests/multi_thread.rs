#![cfg(not(loom))]

use eddy::Builder;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn multi_thread_runtime_spawn_and_join() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    let out = rt.block_on(async {
        let handle = eddy::Handle::current().spawn(async { 40 });
        handle.await.unwrap() + 2
    });
    assert_eq!(out, 42);
}

#[test]
fn multi_thread_nested_spawns_join_correctly() {
    let rt = Builder::new_multi_thread().worker_threads(4).build();
    let out = rt.block_on(async {
        let handle = eddy::Handle::current().spawn(async {
            let mut total = 0;
            let mut handles = Vec::new();
            for i in 0..100 {
                handles.push(eddy::Handle::current().spawn(async move { i }));
            }
            for handle in handles {
                total += handle.await.unwrap();
            }
            total
        });
        handle.await.unwrap()
    });
    assert_eq!(out, (0..100).sum::<i32>());
}

#[test]
fn thread_hooks_run_on_each_worker() {
    let started = Arc::new(AtomicUsize::new(0));
    let stopped = Arc::new(AtomicUsize::new(0));
    let parked = Arc::new(AtomicUsize::new(0));
    let started_for_hook = started.clone();
    let stopped_for_hook = stopped.clone();
    let parked_for_hook = parked.clone();
    let rt = Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("test-worker")
        .on_thread_start(move || {
            started_for_hook.fetch_add(1, Ordering::SeqCst);
        })
        .on_thread_stop(move || {
            stopped_for_hook.fetch_add(1, Ordering::SeqCst);
        })
        .on_thread_park(move || {
            parked_for_hook.fetch_add(1, Ordering::SeqCst);
        })
        .build();
    rt.block_on(async {
        eddy::Handle::current().spawn(async {}).await.unwrap();
    });
    // Give the workers a moment to park at least once.
    std::thread::sleep(Duration::from_millis(20));
    drop(rt);
    assert!(started.load(Ordering::SeqCst) >= 2);
    assert!(stopped.load(Ordering::SeqCst) >= 2);
    assert!(parked.load(Ordering::SeqCst) > 0);
}

#[test]
fn disable_lifo_slot_runtime_still_completes_work() {
    let rt = Builder::new_multi_thread()
        .worker_threads(2)
        .disable_lifo_slot()
        .build();
    let out = rt.block_on(async {
        let mut handles = Vec::new();
        for i in 0..500 {
            handles.push(eddy::Handle::current().spawn(async move { i }));
        }
        let mut total = 0;
        for handle in handles {
            total += handle.await.unwrap();
        }
        total
    });
    assert_eq!(out, (0..500).sum::<i32>());
}

#[test]
fn thread_stack_size_and_name_are_applied() {
    let name = Arc::new(std::sync::Mutex::new(String::new()));
    let name_for_hook = name.clone();
    let rt = Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("custom-worker")
        .thread_stack_size(64 * 1024)
        .on_thread_start(move || {
            *name_for_hook.lock().unwrap() =
                std::thread::current().name().unwrap_or("").to_string();
        })
        .build();
    rt.block_on(async { eddy::Handle::current().spawn(async { 1 }).await.unwrap() });
    drop(rt);
    assert!(name.lock().unwrap().starts_with("custom-worker-"));
}

#[test]
fn shutdown_timeout_drains_pending_tasks_promptly() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    let dropped = Arc::new(AtomicBool::new(false));
    let dropped_for_task = dropped.clone();
    struct Pending {
        dropped: Arc<AtomicBool>,
    }
    impl std::future::Future for Pending {
        type Output = ();
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            std::task::Poll::Pending
        }
    }
    impl Drop for Pending {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    let handle = rt.spawn(Pending {
        dropped: dropped_for_task,
    });
    drop(handle);
    let start = std::time::Instant::now();
    rt.shutdown_timeout(Duration::from_secs(5));
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(dropped.load(Ordering::SeqCst));
}

#[test]
fn task_routed_while_worker_is_parking_still_runs() {
    // H1 regression: a task routed to a worker while it is between its final
    // work re-check and claiming the driver holder must not be lost. The
    // on_thread_park hook fires exactly in that window (after the re-check,
    // before park_worker); routing a task while the hook holds the worker
    // then lets it proceed must still result in the task running. The old
    // code parked the worker in the kernel wait forever.
    let entered_park = Arc::new(AtomicBool::new(false));
    let release_park = Arc::new(AtomicBool::new(false));
    let entered_for_hook = entered_park.clone();
    let release_for_hook = release_park.clone();
    let rt = Builder::new_multi_thread()
        .worker_threads(1)
        .on_thread_park(move || {
            entered_for_hook.store(true, Ordering::Release);
            while !release_for_hook.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
        })
        .build();

    while !entered_park.load(Ordering::Acquire) {
        std::thread::yield_now();
    }
    let completed = Arc::new(AtomicBool::new(false));
    let completed_for_task = completed.clone();
    rt.spawn(async move {
        completed_for_task.store(true, Ordering::Release);
    });
    release_park.store(true, Ordering::Release);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !completed.load(Ordering::Acquire) {
        assert!(
            std::time::Instant::now() < deadline,
            "H1: task routed while the worker was parking never ran"
        );
        std::thread::yield_now();
    }
}

#[test]
fn block_in_place_handoff_stress() {
    // H2 regression guard: repeated block_in_place handoffs between a worker
    // and its takeover thread complete promptly. The deterministic H1 test
    // covers the shared lost-wakeup window; this exercises the handoff under
    // sustained cross-thread routing.
    let rt = Builder::new_multi_thread().worker_threads(1).build();
    let keep_busy = rt.spawn(async {
        loop {
            eddy::time::sleep(Duration::from_millis(1)).await;
        }
    });
    let start = std::time::Instant::now();
    let handoffs = rt.spawn(async {
        let mut count = 0usize;
        for _ in 0..200 {
            eddy::Handle::current().block_in_place(async {
                std::thread::yield_now();
            });
            count += 1;
        }
        count
    });
    assert_eq!(rt.block_on(async { handoffs.await.unwrap() }), 200);
    assert!(start.elapsed() < Duration::from_secs(30));
    drop(keep_busy);
}

#[test]
fn block_in_place_panic_still_hands_back_the_queue() {
    // M5 regression: a panicking `block_in_place` future must still hand the
    // queue back to the takeover thread before the panic propagates, or the
    // worker thread loses its identity and the takeover thread leaks.
    let rt = Builder::new_multi_thread().worker_threads(1).build();
    let boom = rt.spawn(async {
        eddy::task::block_in_place(async { panic!("boom") });
        0
    });
    let error = rt.block_on(async { boom.await.unwrap_err() });
    assert!(matches!(error, eddy::task::JoinError::Panic(_)));
    // The worker survived and still services tasks.
    let n = rt.spawn(async { 40 + 2 });
    assert_eq!(rt.block_on(async { n.await.unwrap() }), 42);
    // A later block_in_place still hands off and completes promptly.
    let handoff = rt.spawn(async {
        eddy::task::block_in_place(async { std::thread::yield_now() });
        7
    });
    assert_eq!(rt.block_on(async { handoff.await.unwrap() }), 7);
}

#[test]
fn current_thread_runtime_is_not_send_or_sync() {
    // H3 regression: a current-thread runtime must not be movable across
    // threads (its shutdown polls and destroys `!Send` futures on the drop
    // thread). This is enforced on `Runtime` itself; sharing happens through
    // the `Handle`, which stays `Send + Sync`.
    static_assertions::assert_not_impl_any!(eddy::Runtime: Send, Sync);
}

#[test]
fn current_thread_handle_is_send_and_sync() {
    static_assertions::assert_impl_all!(eddy::Handle: Send, Sync);
}
