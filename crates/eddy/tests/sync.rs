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

        let (sender, mut receiver) = eddy::sync::mpsc::channel(1);
        assert!(timeout(Duration::from_millis(5), receiver.recv())
            .await
            .is_err());
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
