#![cfg(not(loom))]

use std::sync::Arc;
use std::time::Duration;

use eddy::sync::{mpsc, Semaphore};
use eddy::time::timeout;
use eddy::Builder;

#[test]
fn bounded_concurrency_soak_completes_without_lost_messages_or_tasks() {
    run_bounded_soak(16, Duration::from_secs(5));
}

#[test]
#[ignore = "scheduled stress job; runs for ten minutes"]
fn ten_minute_max_concurrency_soak() {
    run_bounded_soak(6_000, Duration::from_secs(610));
}

fn run_bounded_soak(rounds: usize, deadline: Duration) {
    const WORKERS: usize = 64;
    let expected = WORKERS * rounds;

    let runtime = Builder::new_multi_thread().worker_threads(4).build();
    runtime.block_on(async {
        let handle = eddy::Handle::current();
        let permits = Arc::new(Semaphore::new(8));
        let (sender, mut receiver) = mpsc::channel::<usize>(16);

        let consumer = handle.spawn(async move {
            let mut values = Vec::with_capacity(expected);
            while let Some(value) = receiver.recv().await {
                values.push(value);
            }
            values
        });

        let mut workers = Vec::with_capacity(WORKERS);
        for worker in 0..WORKERS {
            let permits = permits.clone();
            let sender = sender.clone();
            workers.push(handle.spawn(async move {
                for round in 0..rounds {
                    let permit = permits.acquire().await.unwrap();
                    sender.send(worker * rounds + round).await.unwrap();
                    drop(permit);
                    eddy::future::yield_now().await;
                    eddy::time::sleep(Duration::from_micros(50)).await;
                }
            }));
        }
        drop(sender);

        timeout(deadline, async {
            for worker in workers {
                worker.await.unwrap();
            }
            let mut values = consumer.await.unwrap();
            values.sort_unstable();
            assert_eq!(values.len(), expected);
            assert_eq!(values, (0..expected).collect::<Vec<_>>());
        })
        .await
        .expect("bounded soak made no progress");
    });

    for _ in 0..100 {
        let tasks = runtime.dump_tasks();
        if tasks.is_empty() {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!(
        "completed soak left registered tasks behind: {:?}",
        runtime.dump_tasks()
    );
}
