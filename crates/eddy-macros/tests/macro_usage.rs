//! End-to-end usage of `#[eddy::main]` and `#[eddy::test]`, compiled under
//! `#![deny(warnings)]` so macro-generated code must be warning-free.

#![deny(warnings)]

#[eddy::main]
fn main() {
    let handle = eddy::Handle::current();
    let n = handle.spawn(async { 1 + 1 }).await.unwrap();
    assert_eq!(n, 2);
}

#[eddy::test]
fn test_defaults_to_current_thread() {
    let handle = eddy::Handle::current();
    assert!(handle.is_current_thread());
    let n = handle.spawn(async { 40 + 2 }).await.unwrap();
    assert_eq!(n, 42);
}

#[eddy::test(flavor = "multi_thread", worker_threads = 2)]
fn test_multi_thread_flavor() {
    let handle = eddy::Handle::current();
    assert!(!handle.is_current_thread());
    let n = handle.spawn(async { 6 * 7 }).await.unwrap();
    assert_eq!(n, 42);
}

#[eddy::test(start_paused = true)]
fn test_start_paused_controls_time() {
    eddy::time::sleep(std::time::Duration::from_secs(3600)).await;
    assert!(eddy::time::pause_enabled());
    let elapsed = std::time::Instant::now().elapsed();
    assert!(elapsed < std::time::Duration::from_secs(1));
}

#[eddy::test]
fn select_and_join_macros_work_inside_runtime() {
    let (a, b) = eddy::join!(async { 1 }, async { 2 }).await;
    assert_eq!((a, b), (1, 2));

    let (sender, mut receiver) = eddy::sync::mpsc::unbounded_channel::<u32>();
    sender.send(7).await.unwrap();
    let out = eddy::select! {
        val = ::std::boxed::Box::pin(receiver.recv()) => val.unwrap(),
        _ = eddy::time::sleep(std::time::Duration::from_secs(60)) => panic!("timeout"),
    };
    assert_eq!(out, 7);
}
