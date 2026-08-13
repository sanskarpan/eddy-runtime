use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use eddy::coop::{has_budget_remaining, unconstrained};
use eddy::sync::oneshot;
use eddy::time::timeout;
use eddy::{Builder, Handle};

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    Builder::new_current_thread().build().block_on(f)
}

/// Wraps a future so every poll of the outer *task* is counted. The task is
/// the `Scheduler`'s queue entry, so its polls are exactly the scheduling
/// rounds granted to it.
struct PollTracker<F> {
    polls: Arc<AtomicU64>,
    inner: Pin<Box<F>>,
}

impl<F: std::future::Future> std::future::Future for PollTracker<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.inner.as_mut().poll(cx)
    }
}

#[test]
fn draining_a_channel_does_not_starve_other_tasks() {
    block_on(async {
        let handle = Handle::current();
        let (tx, mut rx) = eddy::sync::mpsc::unbounded_channel();
        for _ in 0..10_000 {
            tx.send(()).await.unwrap();
        }
        let (done_tx, done_rx) = oneshot::channel();
        handle.spawn(async move { while rx.recv().await.is_some() {} });
        handle.spawn(async move {
            let _ = done_tx.send(());
        });
        drop(tx);
        assert!(
            timeout(Duration::from_secs(5), done_rx).await.is_ok(),
            "the drainer monopolized the scheduler and the notifier starved"
        );
    });
}

async fn consume_budget() {
    let _ = std::future::poll_fn(eddy::coop::poll_proceed).await;
}

#[test]
fn budget_forces_a_yield_after_exactly_128_ops_per_scheduling_round() {
    block_on(async {
        let handle = Handle::current();
        let polls = Arc::new(AtomicU64::new(0));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let polls_clone = Arc::clone(&polls);
        let done_clone = Arc::clone(&done);
        handle.spawn(PollTracker {
            polls: polls_clone,
            inner: Box::pin(async move {
                for _round in 0..3 {
                    for _ in 0..128 {
                        consume_budget().await;
                    }
                    // The 129th proceed of each round suspends the task, so
                    // every round boundary costs exactly one extra task poll.
                    consume_budget().await;
                }
                done_clone.store(true, Ordering::SeqCst);
            }),
        });
        for _ in 0..10_000 {
            if done.load(Ordering::SeqCst) {
                break;
            }
            eddy::future::yield_now().await;
        }
        assert!(done.load(Ordering::SeqCst));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            4,
            "one task poll per round (128 ops) plus a final poll to finish"
        );
    });
}

#[test]
fn unconstrained_bypasses_the_budget() {
    block_on(async {
        let handle = Handle::current();
        let polls = Arc::new(AtomicU64::new(0));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let polls_clone = Arc::clone(&polls);
        let done_clone = Arc::clone(&done);
        handle.spawn(unconstrained(PollTracker {
            polls: polls_clone,
            inner: Box::pin(async move {
                for _round in 0..3 {
                    for _ in 0..128 {
                        consume_budget().await;
                    }
                    consume_budget().await;
                }
                done_clone.store(true, Ordering::SeqCst);
            }),
        }));
        for _ in 0..10_000 {
            if done.load(Ordering::SeqCst) {
                break;
            }
            eddy::future::yield_now().await;
        }
        assert!(done.load(Ordering::SeqCst));
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "unconstrained never suspends, so one task poll does all 384 iterations"
        );
    });
}

#[test]
fn budgeted_tasks_report_budget_remaining() {
    block_on(async {
        assert!(has_budget_remaining(), "block_on roots are unconstrained");
        let handle = Handle::current();
        let (tx, rx) = oneshot::channel();
        handle.spawn(async move {
            let mut seen = Vec::new();
            for _ in 0..129 {
                seen.push(has_budget_remaining());
                consume_budget().await;
            }
            // The first 128 iterations had budget; the 129th continues with
            // a fresh budget only after the task is re-polled.
            assert!(seen.iter().filter(|&&b| b).count() >= 128);
            tx.send(()).unwrap();
        });
        assert!(timeout(Duration::from_secs(5), rx).await.is_ok());
    });
}
