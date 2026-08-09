#![cfg(not(loom))]

//! Phase 10: blocking pool tests — lazy thread growth, cap enforcement,
//! idle-thread expiry, shutdown draining, and `block_in_place` handoffs.

use eddy::Builder;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

#[test]
fn spawn_blocking_1000_jobs_complete() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    let total = rt.block_on(async {
        let mut handles = Vec::new();
        for i in 0..1000 {
            handles.push(eddy::task::spawn_blocking(move || i * 2));
        }
        let mut total = 0i64;
        for handle in handles {
            total += handle.await.unwrap();
        }
        total
    });
    assert_eq!(total, (0..1000).map(|i| i * 2).sum::<i64>());
}

#[test]
fn spawn_blocking_on_current_thread_runtime_works() {
    let rt = Builder::new_current_thread().build();
    let out = rt.block_on(async {
        let handle = eddy::task::spawn_blocking(|| 40 + 2);
        handle.await.unwrap()
    });
    assert_eq!(out, 42);
}

#[test]
fn pool_grows_to_cap_and_no_further() {
    let rt = Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .keep_alive(Duration::from_secs(60))
        .build();
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    rt.block_on(async {
        let mut handles = Vec::new();
        for _ in 0..8 {
            let active = active.clone();
            let peak = peak.clone();
            handles.push(eddy::task::spawn_blocking(move || {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(50));
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
    });
    assert_eq!(peak.load(Ordering::SeqCst), 2, "pool must cap at 2 threads");
}

#[test]
fn idle_threads_exit_after_keep_alive() {
    let rt = Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .keep_alive(Duration::from_millis(150))
        .build();
    let threads = Arc::new(Mutex::new(Vec::new()));
    rt.block_on({
        let threads = threads.clone();
        async move {
            let first = eddy::task::spawn_blocking(move || {
                threads.lock().unwrap().push(std::thread::current().id())
            });
            first.await.unwrap();
        }
    });
    std::thread::sleep(Duration::from_millis(600));
    rt.block_on({
        let threads = threads.clone();
        async move {
            let second = eddy::task::spawn_blocking(move || {
                threads.lock().unwrap().push(std::thread::current().id())
            });
            second.await.unwrap();
        }
    });
    let ids = threads.lock().unwrap();
    assert_eq!(ids.len(), 2);
    assert_ne!(
        ids[0], ids[1],
        "the first pool thread should have exited after keep_alive"
    );
}

fn panic_message(err: &Box<dyn std::any::Any + Send>) -> String {
    err.downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default()
}

#[test]
fn spawn_blocking_outside_runtime_panics_clearly() {
    let err = std::panic::catch_unwind(|| {
        eddy::task::spawn_blocking(|| 1);
    })
    .unwrap_err();
    let message = panic_message(&err);
    assert!(
        message.contains("no eddy runtime running"),
        "unexpected panic message: {message}"
    );
}

#[test]
fn shutdown_completes_in_flight_and_cancels_queued() {
    let rt = Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .build();
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (in_flight, queued) = rt.block_on({
        let started = started.clone();
        async move {
            let in_flight = eddy::task::spawn_blocking(move || {
                started.store(true, Ordering::Release);
                std::thread::sleep(Duration::from_millis(150));
                1
            });
            let mut queued = Vec::new();
            for i in 0..4 {
                queued.push(eddy::task::spawn_blocking(move || i));
            }
            (in_flight, queued)
        }
    });
    // Guarantee the first job actually started before shutdown, so the
    // "in-flight" and "queued" classifications are deterministic.
    while !started.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(rt);
    assert_eq!(join_elsewhere(in_flight).unwrap(), 1);
    for handle in queued {
        let err = join_elsewhere(handle);
        assert!(
            err.is_err(),
            "queued tasks must be cancelled on shutdown, got {err:?}"
        );
    }
}

fn join_elsewhere(handle: eddy::JoinHandle<i32>) -> Result<i32, eddy::JoinError> {
    let rt = Builder::new_current_thread().build();
    rt.block_on(handle)
}

#[test]
fn block_in_place_does_not_deadlock_when_all_workers_use_it() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    let started = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));
    let out = rt.block_on(async {
        let barrier = barrier.clone();
        let started = started.clone();
        let mut handles = Vec::new();
        for i in 0..2 {
            let barrier = barrier.clone();
            let started = started.clone();
            handles.push(eddy::Handle::current().spawn(async move {
                eddy::task::block_in_place(async move {
                    // All workers block simultaneously; each runs a takeover
                    // thread, so progress must not stall.
                    started.fetch_add(1, Ordering::SeqCst);
                    barrier.wait();
                    std::thread::sleep(Duration::from_millis(30));
                    i * 7
                })
            }));
        }
        let mut total = 0;
        for handle in handles {
            total += handle.await.unwrap();
        }
        total
    });
    assert_eq!(out, 7);
    assert_eq!(started.load(Ordering::SeqCst), 2);
}

#[test]
fn block_in_place_allows_spawn_blocking_inside() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    let out = rt.block_on(async {
        let handle = eddy::Handle::current().spawn(async {
            eddy::task::block_in_place(async {
                let inner = eddy::task::spawn_blocking(|| 20);
                inner.await.unwrap() + 22
            })
        });
        handle.await.unwrap()
    });
    assert_eq!(out, 42);
}

#[test]
fn block_in_place_borrows_non_static_data() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    let out = rt.block_on(async {
        let label = String::from("hello");
        // `block_in_place` futures are not required to be 'static; this
        // future borrows `label` from the enclosing scope.
        eddy::task::block_in_place(async { label.len() * 2 })
    });
    assert_eq!(out, 10);
}

#[test]
fn block_in_place_on_current_thread_runtime_panics_clearly() {
    let rt = Builder::new_current_thread().build();
    let err = rt
        .block_on(async { std::panic::catch_unwind(|| eddy::task::block_in_place(async { 1u32 })) })
        .unwrap_err();
    let message = panic_message(&err);
    assert!(
        message.contains("requires a multi-thread runtime"),
        "unexpected panic message: {message}"
    );
}

#[test]
fn block_in_place_from_block_on_thread_runs_inline() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    let out = rt.block_on(async { eddy::task::block_in_place(async { 6 * 7 }) });
    assert_eq!(out, 42);
}

#[test]
fn block_in_place_workers_keep_processing_during_handoff() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    rt.block_on(async {
        let done = done.clone();
        let mut handles = Vec::new();
        for i in 0..2 {
            let done = done.clone();
            handles.push(eddy::Handle::current().spawn(async move {
                if i == 0 {
                    eddy::task::block_in_place(async {
                        std::thread::sleep(Duration::from_millis(100));
                        done.store(true, Ordering::Release);
                    });
                } else {
                    // The other worker is blocked in place; this task must
                    // still be able to make progress.
                    while !done.load(Ordering::Acquire) {
                        std::thread::yield_now();
                    }
                }
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }
    });
}

#[test]
fn nested_spawn_blocking_from_blocking_thread_works() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    let out = rt.block_on(async {
        let handle = eddy::task::spawn_blocking(|| {
            // Inside a blocking closure the runtime handle is installed, so
            // a nested blocking task can be spawned and returned.
            eddy::Handle::current().spawn_blocking(|| 40)
        });
        let inner = handle.await.unwrap();
        inner.await.unwrap() + 2
    });
    assert_eq!(out, 42);
}

#[test]
fn drop_waits_for_in_flight_blocking_jobs() {
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started_2 = started.clone();
    let started_at = Instant::now();
    {
        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .build();
        rt.block_on(async {
            let _in_flight = eddy::task::spawn_blocking(move || {
                started_2.store(true, Ordering::Release);
                std::thread::sleep(Duration::from_millis(100));
            });
            let _queued: Vec<_> = (0..4)
                .map(|i| eddy::task::spawn_blocking(move || i))
                .collect();
        });
        while !started.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    assert!(
        started_at.elapsed() >= Duration::from_millis(90),
        "drop must wait for in-flight blocking jobs"
    );
}
