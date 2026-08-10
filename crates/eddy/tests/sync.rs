use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use eddy::sync::{Mutex, Notify, RwLock, Semaphore};
use eddy::time::{sleep, timeout};
use eddy::Builder;

#[test]
fn mutex_serializes_one_hundred_tasks() {
    let runtime = Builder::new_multi_thread().worker_threads(4).build();
    runtime.block_on(async {
        let value = Arc::new(Mutex::new(0usize));
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let value = value.clone();
            tasks.push(eddy::Handle::current().spawn(async move {
                let mut guard = value.lock().await;
                let current = *guard;
                sleep(Duration::from_micros(10)).await;
                *guard = current + 1;
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*value.lock().await, 100);
    });
}

#[test]
fn rwlock_is_write_preferring_and_supports_downgrade() {
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let lock = Arc::new(RwLock::new(7usize));
        let read = lock.read().await;
        let writer_started = Arc::new(AtomicBool::new(false));
        let writer_started_task = writer_started.clone();
        let lock_for_writer = lock.clone();
        let writer = eddy::Handle::current().spawn(async move {
            let mut guard = lock_for_writer.write().await;
            writer_started_task.store(true, Ordering::SeqCst);
            *guard = 8;
        });
        sleep(Duration::from_millis(5)).await;
        assert!(!writer_started.load(Ordering::SeqCst));
        drop(read);
        writer.await.unwrap();
        let write = lock.write().await;
        let read = write.downgrade();
        assert_eq!(*read, 8);
        drop(read);

        // Test hygiene: with a sustained reader loop cycling, a writer must
        // still make progress (writer preference); a reader-biased lock
        // would starve it.
        let stop = Arc::new(AtomicBool::new(false));
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let reader_task = {
            let lock = lock.clone();
            let stop = stop.clone();
            let reads = reads.clone();
            eddy::Handle::current().spawn(async move {
                while !stop.load(Ordering::SeqCst) {
                    let guard = lock.read().await;
                    reads.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_micros(100)).await;
                    drop(guard);
                }
            })
        };
        sleep(Duration::from_millis(5)).await;
        let writer_progress = {
            let lock = lock.clone();
            eddy::Handle::current().spawn(async move {
                let mut guard = lock.write().await;
                *guard += 1;
            })
        };
        timeout(Duration::from_secs(2), writer_progress)
            .await
            .unwrap()
            .unwrap();
        assert!(reads.load(Ordering::SeqCst) > 0);
        stop.store(true, Ordering::SeqCst);
        reader_task.await.unwrap();
    });
}

#[test]
fn notify_preserves_a_permit_issued_before_waiting() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let notify = Notify::new();
        notify.notify_one();
        timeout(Duration::from_millis(20), notify.notified())
            .await
            .unwrap();
    });
}

#[test]
fn semaphore_waiters_and_permit_forget_work() {
    let semaphore = Semaphore::new(1);
    let permit = semaphore.try_acquire().unwrap();
    permit.forget();
    assert_eq!(semaphore.available_permits(), 0);
    semaphore.add_permits(1);
    assert_eq!(semaphore.available_permits(), 1);
}

#[test]
fn oneshot_send_and_closed_notification_work() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let (sender, receiver) = eddy::sync::oneshot::channel();
        sender.send(42).unwrap();
        assert_eq!(receiver.await.unwrap(), 42);

        let (sender, receiver) = eddy::sync::oneshot::channel::<usize>();
        let closed = sender.closed();
        drop(receiver);
        timeout(Duration::from_millis(20), closed).await.unwrap();
    });
}

#[test]
fn bounded_mpsc_backpressures_and_cancelled_recv_preserves_messages() {
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let (sender, mut receiver) = eddy::sync::mpsc::channel(1);
        sender.send(1).await.unwrap();
        let sent = Arc::new(AtomicBool::new(false));
        let sent_task = sent.clone();
        let sender_task = sender.clone();
        let task = eddy::Handle::current().spawn(async move {
            sender_task.send(2).await.unwrap();
            sent_task.store(true, Ordering::SeqCst);
        });
        sleep(Duration::from_millis(5)).await;
        assert!(!sent.load(Ordering::SeqCst));
        assert_eq!(receiver.recv().await, Some(1));
        task.await.unwrap();
        assert_eq!(receiver.recv().await, Some(2));

        // Test hygiene: a pending `recv` that is dropped (cancelled) after
        // registering a waiter must not swallow the next message or corrupt
        // the waiter list.
        let (sender, mut receiver) = eddy::sync::mpsc::channel(1);
        // Box the future so dropping `pending` really drops it (and with it
        // the `&mut receiver` borrow); `pin!` on a method call would only
        // drop the pin and keep the future alive to end of scope.
        let mut pending = Box::pin(receiver.recv());
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        drop(pending);
        sender.send(9).await.unwrap();
        assert_eq!(receiver.recv().await, Some(9));
    });
}

#[test]
fn mpsc_reserve_and_recv_many_work() {
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let (sender, mut receiver) = eddy::sync::mpsc::channel(4);
        let permit = sender.reserve().await.unwrap();
        permit.send("reserved").unwrap();
        sender.send("second").await.unwrap();
        let mut values = Vec::new();
        assert_eq!(receiver.recv_many(&mut values, 2).await, 2);
        assert_eq!(values, ["reserved", "second"]);
    });
}

#[test]
fn mpsc_reservation_is_counted_exactly_once() {
    // C2 regression: a reservation granted while another reservation is
    // queued must not be double-counted, or the granted waiter strands
    // forever and the channel permanently loses a capacity slot.
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let (sender, mut receiver) = eddy::sync::mpsc::channel(1);

        let first = sender.reserve().await.unwrap();
        // Poll `second` once so it registers as a waiter on the full channel.
        let mut second = sender.reserve();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        assert!(std::pin::Pin::new(&mut second).poll(&mut cx).is_pending());
        drop(first);
        let permit = timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap();
        permit.send("via-second").unwrap();
        assert_eq!(receiver.recv().await, Some("via-second"));

        // The channel must still have full capacity afterwards.
        timeout(Duration::from_secs(1), sender.send("after"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receiver.recv().await, Some("after"));
    });
}

#[test]
fn mpsc_dropped_granted_reserve_releases_its_slot() {
    // C2 regression: dropping a Reserve future after it was granted (but
    // before it was polled) must release the reservation instead of leaking
    // it and double-counting on the next grant.
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let (sender, mut receiver) = eddy::sync::mpsc::channel(1);

        let first = sender.reserve().await.unwrap();
        let mut second = sender.reserve();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        assert!(std::pin::Pin::new(&mut second).poll(&mut cx).is_pending());
        drop(first);
        drop(second);
        timeout(Duration::from_secs(1), sender.send("ok"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receiver.recv().await, Some("ok"));
    });
}

#[test]
fn mpsc_permit_wake_of_queued_send_does_not_leak_capacity() {
    // C2 regression: dropping a Permit must grant a queued plain Send
    // without counting a phantom reservation in its name; the channel must
    // keep full capacity afterwards.
    let runtime = Builder::new_current_thread().build();
    runtime.block_on(async {
        let (sender, mut receiver) = eddy::sync::mpsc::channel(1);

        let permit = sender.reserve().await.unwrap();
        let sender_task = sender.clone();
        let task = eddy::Handle::current().spawn(async move {
            sender_task.send("a").await.unwrap();
        });
        // Yield so the spawned send registers on the full channel before the
        // permit is dropped.
        sleep(Duration::from_millis(5)).await;
        drop(permit);

        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receiver.recv().await, Some("a"));

        // The channel must still have full capacity afterwards.
        timeout(Duration::from_secs(1), sender.send("b"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receiver.recv().await, Some("b"));
    });
}
