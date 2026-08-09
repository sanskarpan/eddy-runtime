#![cfg(not(loom))]

use eddy::Builder;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct DropProbe(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn block_on_async_42() {
    let rt = Builder::new_current_thread().build();
    assert_eq!(rt.block_on(async { 42 }), 42);
}

#[test]
fn spawn_and_join_round_trip() {
    let rt = Builder::new_current_thread().build();
    let out = rt.block_on(async {
        let handle = eddy::Handle::current().spawn(async {
            let inner = eddy::Handle::current().spawn(async { 41 });
            inner.await.unwrap() + 1
        });
        handle.await.unwrap()
    });
    assert_eq!(out, 42);
}

#[test]
fn many_tasks_join_in_order() {
    let rt = Builder::new_current_thread().build();
    rt.block_on(async {
        let mut handles = Vec::new();
        for i in 0..500 {
            handles.push(eddy::Handle::current().spawn(async move { i }));
        }
        for (i, h) in handles.into_iter().enumerate() {
            assert_eq!(h.await.unwrap(), i);
        }
    });
}

#[test]
fn abort_a_pending_task_yields_cancelled() {
    let rt = Builder::new_current_thread().build();
    rt.block_on(async {
        let handle = eddy::Handle::current().spawn(std::future::pending::<()>());
        handle.abort();
        let err = handle.await.unwrap_err();
        assert!(matches!(err, eddy::JoinError::Cancelled));
    });
}

#[test]
fn panicking_task_yields_panic_join_error_runtime_still_usable() {
    let rt = Builder::new_current_thread().build();
    rt.block_on(async {
        let handle = eddy::Handle::current().spawn(async {
            panic!("boom");
            #[allow(unreachable_code)]
            0
        });
        let err = handle.await.unwrap_err();
        assert!(matches!(err, eddy::JoinError::Panic(_)));
        // Runtime must still be usable after a task panic.
        let ok = eddy::Handle::current().spawn(async { 7 }).await.unwrap();
        assert_eq!(ok, 7);
    });
}

#[test]
fn detached_completed_output_is_dropped_exactly_once() {
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let slot = Arc::new(Mutex::new(None::<Waker>));
    let rt = Builder::new_current_thread().build();
    let drops_for_task = drops.clone();
    let drops_for_root = drops.clone();
    let slot_for_task = slot.clone();

    rt.block_on(async move {
        let handle = eddy::Handle::current().spawn(async move {
            let output = DropProbe(drops_for_task);
            if let Some(waker) = slot_for_task.lock().unwrap().take() {
                waker.wake();
            }
            output
        });
        drop(handle);

        std::future::poll_fn(move |cx| {
            if drops_for_root.load(std::sync::atomic::Ordering::SeqCst) == 1 {
                Poll::Ready(())
            } else {
                *slot.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;
    });

    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn runtime_drop_cancels_an_unpolled_task_and_drops_its_future_once() {
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handle;
    {
        let rt = Builder::new_current_thread().build();
        let drops_for_task = drops.clone();
        #[allow(dead_code)]
        struct PendingWithDrop(DropProbe);
        impl Future for PendingWithDrop {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                Poll::Pending
            }
        }
        handle = rt.spawn(PendingWithDrop(DropProbe(drops_for_task)));
    }
    drop(handle);
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn spawned_task_wake_from_another_thread_uses_injection_queue() {
    let rt = Builder::new_current_thread().build();
    rt.block_on(async {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let slot = Arc::new(Mutex::new(None::<Waker>));
        let slot_for_task = slot.clone();
        std::thread::spawn(move || {
            ready_rx.recv().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(10));
            if let Some(waker) = slot_for_task.lock().unwrap().take() {
                waker.wake();
            }
        });

        struct WaitOnce {
            armed: bool,
            slot: Arc<Mutex<Option<Waker>>>,
            ready_tx: std::sync::mpsc::Sender<()>,
        }

        impl Future for WaitOnce {
            type Output = ();

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                if self.armed {
                    Poll::Ready(())
                } else {
                    self.armed = true;
                    *self.slot.lock().unwrap() = Some(cx.waker().clone());
                    self.ready_tx.send(()).unwrap();
                    Poll::Pending
                }
            }
        }

        eddy::Handle::current()
            .spawn(WaitOnce {
                armed: false,
                slot,
                ready_tx,
            })
            .await
            .unwrap();
    });
}

#[test]
#[allow(clippy::async_yields_async)]
fn shutdown_drains_wakes_created_by_future_destruction() {
    struct DropWake(Arc<Mutex<Option<Waker>>>);

    impl Future for DropWake {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for DropWake {
        fn drop(&mut self) {
            if let Some(waker) = self.0.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    struct Arm {
        task_slot: Arc<Mutex<Option<Waker>>>,
        root_slot: Arc<Mutex<Option<Waker>>>,
        armed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Future for Arm {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if !self.armed.swap(true, std::sync::atomic::Ordering::Release) {
                *self.task_slot.lock().unwrap() = Some(cx.waker().clone());
                if let Some(waker) = self.root_slot.lock().unwrap().take() {
                    waker.wake();
                }
            }
            Poll::Pending
        }
    }

    let rt = Builder::new_current_thread().build();
    let b_handle = rt.block_on(async {
        let task_slot = Arc::new(Mutex::new(None::<Waker>));
        let root_slot = Arc::new(Mutex::new(None::<Waker>));
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let _a_handle = eddy::Handle::current().spawn(DropWake(task_slot.clone()));
        let b_handle = eddy::Handle::current().spawn(Arm {
            task_slot,
            root_slot: root_slot.clone(),
            armed: armed.clone(),
        });

        std::future::poll_fn(move |cx| {
            if armed.load(std::sync::atomic::Ordering::Acquire) {
                Poll::Ready(())
            } else {
                *root_slot.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;
        b_handle
    });

    drop(rt);
    let noop = futures::task::noop_waker();
    let mut cx = Context::from_waker(&noop);
    let mut b_handle = b_handle;
    assert!(matches!(
        Pin::new(&mut b_handle).poll(&mut cx),
        Poll::Ready(Err(eddy::JoinError::Cancelled))
    ));
}

#[test]
fn a_foreign_waker_can_be_dropped_after_shutdown_without_leaking_the_task() {
    struct CaptureWaker {
        task_slot: Arc<Mutex<Option<Waker>>>,
        root_slot: Arc<Mutex<Option<Waker>>>,
        captured: Arc<std::sync::atomic::AtomicBool>,
        _probe: DropProbe,
    }

    impl Future for CaptureWaker {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if !self
                .captured
                .swap(true, std::sync::atomic::Ordering::Release)
            {
                *self.task_slot.lock().unwrap() = Some(cx.waker().clone());
                if let Some(waker) = self.root_slot.lock().unwrap().take() {
                    waker.wake();
                }
            }
            Poll::Pending
        }
    }

    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let task_slot = Arc::new(Mutex::new(None::<Waker>));
    {
        let rt = Builder::new_current_thread().build();
        let root_slot = Arc::new(Mutex::new(None::<Waker>));
        let captured = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_slot_for_task = task_slot.clone();
        let root_slot_for_task = root_slot.clone();
        let captured_for_task = captured.clone();
        let captured_for_root = captured.clone();
        let drops_for_task = drops.clone();

        rt.block_on(async move {
            let _handle = eddy::Handle::current().spawn(CaptureWaker {
                task_slot: task_slot_for_task,
                root_slot: root_slot_for_task,
                captured: captured_for_task.clone(),
                _probe: DropProbe(drops_for_task),
            });
            std::future::poll_fn(move |cx| {
                if captured_for_root.load(std::sync::atomic::Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    *root_slot.lock().unwrap() = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await;
        });
    }

    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    let foreign_waker = task_slot.lock().unwrap().take().unwrap();
    std::thread::spawn(move || drop(foreign_waker))
        .join()
        .unwrap();
}

#[test]
fn detached_non_send_output_is_dropped_before_foreign_waker_cleanup() {
    struct OwnerOutput {
        owner: std::thread::ThreadId,
        drops: Arc<std::sync::atomic::AtomicUsize>,
        _not_send: std::rc::Rc<()>,
    }

    impl Drop for OwnerOutput {
        fn drop(&mut self) {
            assert_eq!(std::thread::current().id(), self.owner);
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    struct ReadyCapture {
        task_slot: Arc<Mutex<Option<Waker>>>,
        root_slot: Arc<Mutex<Option<Waker>>>,
        output: Option<OwnerOutput>,
    }

    impl Future for ReadyCapture {
        type Output = OwnerOutput;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if let Some(output) = self.output.take() {
                // Keep a task waker alive after completion so final cell
                // cleanup is exercised from a foreign thread.
                *self.task_slot.lock().unwrap() = Some(cx.waker().clone());
                if let Some(root_waker) = self.root_slot.lock().unwrap().take() {
                    root_waker.wake();
                }
                Poll::Ready(output)
            } else {
                Poll::Pending
            }
        }
    }

    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let task_slot = Arc::new(Mutex::new(None::<Waker>));
    let rt = Builder::new_current_thread().build();
    let task_slot_for_task = task_slot.clone();
    let root_slot = Arc::new(Mutex::new(None::<Waker>));
    let root_slot_for_task = root_slot.clone();
    let drops_for_task = drops.clone();
    let drops_for_root = drops.clone();

    rt.block_on(async move {
        let handle = eddy::Handle::current().spawn_local(ReadyCapture {
            task_slot: task_slot_for_task,
            root_slot: root_slot_for_task,
            output: Some(OwnerOutput {
                owner: std::thread::current().id(),
                drops: drops_for_task,
                _not_send: std::rc::Rc::new(()),
            }),
        });
        drop(handle);
        std::future::poll_fn(move |cx| {
            if drops_for_root.load(std::sync::atomic::Ordering::SeqCst) == 1 {
                Poll::Ready(())
            } else {
                *root_slot.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;
    });

    drop(rt);
    let foreign_waker = task_slot.lock().unwrap().take().unwrap();
    std::thread::spawn(move || drop(foreign_waker))
        .join()
        .unwrap();
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
}
