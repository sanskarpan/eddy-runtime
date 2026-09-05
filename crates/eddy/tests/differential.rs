#![cfg(not(loom))]

use std::future::{pending, Future};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
}

#[derive(Debug, PartialEq, Eq)]
struct ChannelTrace {
    try_send_results: [bool; 3],
    blocked_send_was_pending: bool,
    values: Vec<Option<u32>>,
    receiver_closed: bool,
    pending_receive_woke_closed: bool,
    send_after_receiver_drop_failed: bool,
}

async fn eddy_channel_trace() -> ChannelTrace {
    let (sender, mut receiver) = eddy::sync::mpsc::channel(2);

    let try_send_results = [
        sender.try_send(1).is_ok(),
        sender.try_send(2).is_ok(),
        sender.try_send(3).is_ok(),
    ];
    let mut blocked_send = Box::pin(sender.send(3));
    assert_pending(blocked_send.as_mut());
    let blocked_send_was_pending = true;
    let mut values = vec![receiver.recv().await];
    let blocked_send_completed = blocked_send.await.is_ok();
    assert!(blocked_send_completed);
    values.push(receiver.recv().await);
    values.push(receiver.recv().await);

    drop(sender);
    let receiver_closed = receiver.recv().await.is_none();

    let (sender, mut receiver) = eddy::sync::mpsc::channel::<u32>(1);
    let mut pending_receive = Box::pin(receiver.recv());
    assert_pending(pending_receive.as_mut());
    drop(sender);
    let pending_receive_woke_closed = pending_receive.await.is_none();

    let (sender, receiver) = eddy::sync::mpsc::channel::<u32>(1);
    drop(receiver);
    let send_after_receiver_drop_failed = sender.send(9).await.is_err();

    ChannelTrace {
        try_send_results,
        blocked_send_was_pending,
        values,
        receiver_closed,
        pending_receive_woke_closed,
        send_after_receiver_drop_failed,
    }
}

async fn tokio_channel_trace() -> ChannelTrace {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);

    let try_send_results = [
        sender.try_send(1).is_ok(),
        sender.try_send(2).is_ok(),
        sender.try_send(3).is_ok(),
    ];
    let mut blocked_send = Box::pin(sender.send(3));
    assert_pending(blocked_send.as_mut());
    let blocked_send_was_pending = true;
    let mut values = vec![receiver.recv().await];
    let blocked_send_completed = blocked_send.await.is_ok();
    assert!(blocked_send_completed);
    values.push(receiver.recv().await);
    values.push(receiver.recv().await);

    drop(sender);
    let receiver_closed = receiver.recv().await.is_none();

    let (sender, mut receiver) = tokio::sync::mpsc::channel::<u32>(1);
    let mut pending_receive = Box::pin(receiver.recv());
    assert_pending(pending_receive.as_mut());
    drop(sender);
    let pending_receive_woke_closed = pending_receive.await.is_none();

    let (sender, receiver) = tokio::sync::mpsc::channel::<u32>(1);
    drop(receiver);
    let send_after_receiver_drop_failed = sender.send(9).await.is_err();

    ChannelTrace {
        try_send_results,
        blocked_send_was_pending,
        values,
        receiver_closed,
        pending_receive_woke_closed,
        send_after_receiver_drop_failed,
    }
}

async fn eddy_select_trace() -> (u8, Option<u32>) {
    let (sender, mut receiver) = eddy::sync::mpsc::channel(1);
    let selected_branch = eddy::select! {
        biased;
        _ = eddy::time::sleep(Duration::from_millis(1)) => 0,
        value = receiver.recv() => if value.is_some() { 1 } else { 2 },
    };
    sender.send(7).await.unwrap();
    (selected_branch, receiver.recv().await)
}

async fn tokio_select_trace() -> (u8, Option<u32>) {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let selected_branch = tokio::select! {
        biased;
        _ = tokio::time::sleep(Duration::from_millis(1)) => 0,
        value = receiver.recv() => if value.is_some() { 1 } else { 2 },
    };
    sender.send(7).await.unwrap();
    (selected_branch, receiver.recv().await)
}

#[derive(Debug, PartialEq, Eq)]
struct TimerTrace {
    zero_timeout_completed: bool,
    pending_timeout_expired: bool,
    sleep_was_not_early: bool,
    ordered_sleep_won: bool,
    interval_deadlines_advance: bool,
}

async fn eddy_timer_trace() -> TimerTrace {
    let zero_timeout_completed = eddy::time::timeout(Duration::ZERO, async { 42u8 })
        .await
        .is_ok();

    let started = Instant::now();
    eddy::time::sleep(Duration::from_millis(5)).await;
    let sleep_was_not_early = started.elapsed() >= Duration::from_millis(4);

    let pending_timeout_expired = eddy::time::timeout(Duration::from_millis(2), pending::<()>())
        .await
        .is_err();
    let ordered_sleep_won = eddy::select! {
        biased;
        _ = eddy::time::sleep(Duration::from_millis(1)) => true,
        _ = eddy::time::sleep(Duration::from_millis(10)) => false,
    };

    let mut ticker = eddy::time::interval(Duration::from_millis(1));
    let first = ticker.tick().await;
    let second = ticker.tick().await;

    TimerTrace {
        zero_timeout_completed,
        pending_timeout_expired,
        sleep_was_not_early,
        ordered_sleep_won,
        interval_deadlines_advance: second > first,
    }
}

async fn tokio_timer_trace() -> TimerTrace {
    let zero_timeout_completed = tokio::time::timeout(Duration::ZERO, async { 42u8 })
        .await
        .is_ok();

    let started = Instant::now();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let sleep_was_not_early = started.elapsed() >= Duration::from_millis(4);

    let pending_timeout_expired = tokio::time::timeout(Duration::from_millis(2), pending::<()>())
        .await
        .is_err();
    let ordered_sleep_won = tokio::select! {
        biased;
        _ = tokio::time::sleep(Duration::from_millis(1)) => true,
        _ = tokio::time::sleep(Duration::from_millis(10)) => false,
    };

    let mut ticker = tokio::time::interval(Duration::from_millis(1));
    let first = ticker.tick().await;
    let second = ticker.tick().await;

    TimerTrace {
        zero_timeout_completed,
        pending_timeout_expired,
        sleep_was_not_early,
        ordered_sleep_won,
        interval_deadlines_advance: second > first,
    }
}

#[test]
fn channel_and_select_cancellation_match_tokio() {
    let eddy_runtime = eddy::Builder::new_current_thread().build();
    let eddy_result =
        eddy_runtime.block_on(async { (eddy_channel_trace().await, eddy_select_trace().await) });

    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let tokio_result =
        tokio_runtime.block_on(async { (tokio_channel_trace().await, tokio_select_trace().await) });

    assert_eq!(eddy_result, tokio_result);
    let (channel, select) = eddy_result;
    assert_eq!(channel.try_send_results, [true, true, false]);
    assert!(channel.blocked_send_was_pending);
    assert_eq!(channel.values, [Some(1), Some(2), Some(3)]);
    assert!(channel.receiver_closed);
    assert!(channel.pending_receive_woke_closed);
    assert!(channel.send_after_receiver_drop_failed);
    assert_eq!(select, (0, Some(7)));
}

#[test]
fn timer_behavior_matches_tokio() {
    let eddy_runtime = eddy::Builder::new_current_thread().build();
    let eddy_result = eddy_runtime.block_on(eddy_timer_trace());

    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let tokio_result = tokio_runtime.block_on(tokio_timer_trace());

    assert_eq!(eddy_result, tokio_result);
    assert!(eddy_result.zero_timeout_completed);
    assert!(eddy_result.pending_timeout_expired);
    assert!(eddy_result.sleep_was_not_early);
    assert!(eddy_result.ordered_sleep_won);
    assert!(eddy_result.interval_deadlines_advance);
}
