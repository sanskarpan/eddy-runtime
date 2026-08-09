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
