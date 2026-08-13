//! `JoinSet`: spawn a dynamic set of tasks and gather their results as they
//! complete.

use std::future::Future;

use crate::future::unordered::FuturesUnordered;
use crate::runtime::Handle;
use crate::stream::StreamExt;
use crate::task::{JoinError, JoinHandle};

/// A collection of spawned tasks whose results arrive in completion order.
///
/// Spawned tasks run concurrently on the runtime the set was created with
/// (via [`JoinSet::spawn`]). Awaiting [`JoinSet::join_next`] returns each
/// task's output as it completes; once every task has been collected the
/// set is empty and `join_next` returns `None`. Dropping the set aborts all
/// tasks still running in it.
pub struct JoinSet<T> {
    tasks: FuturesUnordered<JoinHandle<T>>,
}

impl<T> Default for JoinSet<T> {
    fn default() -> JoinSet<T> {
        JoinSet::new()
    }
}

impl<T> JoinSet<T> {
    /// Create a new, empty `JoinSet`.
    pub fn new() -> JoinSet<T> {
        JoinSet {
            tasks: FuturesUnordered::new(),
        }
    }

    /// Spawn a task on the given runtime handle and add it to the set.
    pub fn spawn<F>(&mut self, handle: &Handle, future: F)
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.tasks.push(handle.spawn(future));
    }

    /// Spawn a potentially `!Send` task on the given current-thread runtime
    /// handle and add it to the set.
    pub fn spawn_local<F>(&mut self, handle: &Handle, future: F)
    where
        F: Future<Output = T> + 'static,
    {
        self.tasks.push(handle.spawn_local(future));
    }

    /// Waits for one of the tasks in the set to complete and returns its
    /// output, panicking with the panic message if it panicked.
    ///
    /// Returns `None` once every task spawned so far has been collected.
    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.tasks.next().await
    }

    /// Number of tasks in the set.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Whether the set currently holds no tasks.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Abort every task in the set and clear it.
    pub fn abort_all(&mut self) {
        for task in self.tasks.iter_mut() {
            task.abort();
        }
        self.tasks.clear();
    }
}

impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        for task in self.tasks.iter_mut() {
            task.abort();
        }
    }
}
