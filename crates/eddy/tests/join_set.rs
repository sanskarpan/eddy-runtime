use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use eddy::future::JoinSet;
use eddy::sync::Notify;
use eddy::{Builder, Handle};

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    Builder::new_current_thread().build().block_on(f)
}

#[test]
fn join_next_returns_results_in_completion_order() {
    block_on(async {
        let handle = Handle::current();
        let mut set = JoinSet::new();
        let notifies = (0..3).map(|_| Arc::new(Notify::new())).collect::<Vec<_>>();
        for (i, notify) in notifies.iter().enumerate() {
            let notify = Arc::clone(notify);
            set.spawn(&handle, async move {
                notify.notified().await;
                i
            });
        }
        notifies[2].notify_one();
        assert_eq!(set.join_next().await.unwrap().unwrap(), 2);
        notifies[0].notify_one();
        assert_eq!(set.join_next().await.unwrap().unwrap(), 0);
        notifies[1].notify_one();
        assert_eq!(set.join_next().await.unwrap().unwrap(), 1);
        assert!(set.join_next().await.is_none());
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    });
}

#[test]
fn join_next_returns_none_for_empty_set() {
    block_on(async {
        let mut set: JoinSet<i32> = JoinSet::new();
        assert!(set.join_next().await.is_none());
    });
}

#[test]
fn aborted_task_yields_cancelled_error() {
    block_on(async {
        let handle = Handle::current();
        let mut set = JoinSet::new();
        let notify = Arc::new(Notify::new());
        set.spawn(&handle, async move {
            notify.notified().await;
            1
        });
        set.abort_all();
        assert!(set.is_empty());
        assert!(set.join_next().await.is_none());
    });
}

#[test]
fn dropping_set_aborts_remaining_tasks() {
    block_on(async {
        let dropped = Arc::new(AtomicUsize::new(0));
        let handle = Handle::current();
        let mut set = JoinSet::new();
        let dropped_clone = Arc::clone(&dropped);
        set.spawn(&handle, async move {
            struct Watch(Arc<AtomicUsize>);
            impl Drop for Watch {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }
            let _watch = Watch(dropped_clone);
            std::future::pending::<()>().await;
        });
        set.spawn(&handle, async move {
            std::future::pending::<()>().await;
        });
        eddy::future::yield_now().await;
        eddy::future::yield_now().await;
        drop(set);
        for _ in 0..1000 {
            if dropped.load(Ordering::SeqCst) > 0 {
                break;
            }
            eddy::future::yield_now().await;
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    });
}
