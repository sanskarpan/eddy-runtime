use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;

use eddy::future::FuturesUnordered;
use eddy::stream::Stream;
use eddy::sync::Notify;
use eddy::Builder;

fn block_on<F: Future>(f: F) -> F::Output {
    Builder::new_current_thread().build().block_on(f)
}

async fn make(x: usize, wait: bool) -> usize {
    if wait {
        std::future::pending::<()>().await;
    }
    x
}

#[test]
fn first_pushed_future_resolves_first() {
    block_on(async {
        let mut set = FuturesUnordered::new();
        for i in 1..=3 {
            set.push(async move { i });
        }
        let out = poll_fn(|cx| Pin::new(&mut set).poll(cx)).await;
        assert_eq!(out, 1);
    });
}

#[test]
fn pending_members_are_polled_only_once_until_woken() {
    block_on(async {
        let polls = Arc::new(AtomicUsize::new(0));
        let never = Arc::new(Notify::new());
        let mut set = FuturesUnordered::new();
        set.push(async {
            polls.fetch_add(1, Ordering::SeqCst);
            never.notified().await;
            0
        });
        let _ = poll_fn(|cx| match Pin::new(&mut set).poll(cx) {
            Poll::Ready(v) => Poll::Ready(Some(v)),
            Poll::Pending => Poll::Ready(None),
        })
        .await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        for _ in 0..3 {
            let _ = poll_fn(|cx| match Pin::new(&mut set).poll(cx) {
                Poll::Ready(v) => Poll::Ready(Some(v)),
                Poll::Pending => Poll::Ready(None),
            })
            .await;
        }
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn woken_member_resolves_on_next_poll() {
    block_on(async {
        let notify = Arc::new(Notify::new());
        let mut set = FuturesUnordered::new();
        set.push(async {
            notify.notified().await;
            7
        });
        let _ = poll_fn(|cx| match Pin::new(&mut set).poll(cx) {
            Poll::Ready(v) => Poll::Ready(Some(v)),
            Poll::Pending => Poll::Ready(None),
        })
        .await;
        notify.notify_one();
        let out = poll_fn(|cx| Pin::new(&mut set).poll(cx)).await;
        assert_eq!(out, 7);
        assert!(set.is_empty());
    });
}

#[test]
fn stream_impl_yields_all_outputs_then_ends() {
    block_on(async {
        let mut set = FuturesUnordered::new();
        for i in 1..=3 {
            set.push(async move { i });
        }
        let mut outputs = Vec::new();
        loop {
            let got = poll_fn(|cx| Pin::new(&mut set).poll_next(cx)).await;
            match got {
                Some(v) => outputs.push(v),
                None => break,
            }
        }
        assert_eq!(outputs, vec![1, 2, 3]);
    });
}

#[test]
fn await_yields_first_completed_output() {
    block_on(async {
        let mut set = FuturesUnordered::new();
        set.push(make(1, false));
        set.push(make(2, true));
        let out = poll_fn(|cx| Pin::new(&mut set).poll(cx)).await;
        assert_eq!(out, 1);
    });
}

#[test]
fn poll_cost_is_proportional_to_wakes() {
    const N: usize = 10_000;
    block_on(async {
        let mut set = FuturesUnordered::new();
        let polls = Arc::new(AtomicUsize::new(0));
        let notifies = (0..N).map(|_| Arc::new(Notify::new())).collect::<Vec<_>>();
        for (i, notify) in notifies.iter().enumerate() {
            let polls = Arc::clone(&polls);
            let notify = Arc::clone(notify);
            set.push(async move {
                polls.fetch_add(1, Ordering::SeqCst);
                notify.notified().await;
                polls.fetch_add(1, Ordering::SeqCst);
                i
            });
        }
        let pumped = poll_fn(|cx| match Pin::new(&mut set).poll(cx) {
            Poll::Ready(v) => Poll::Ready(Some(v)),
            Poll::Pending => Poll::Ready(None),
        })
        .await;
        assert_eq!(pumped, None);
        assert_eq!(polls.load(Ordering::SeqCst), N);
        notifies[N / 2].notify_one();
        let out = poll_fn(|cx| Pin::new(&mut set).poll(cx)).await;
        assert_eq!(out, N / 2);
        assert_eq!(polls.load(Ordering::SeqCst), N + 1);
    });
}
