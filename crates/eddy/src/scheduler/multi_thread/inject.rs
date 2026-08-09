//! The global MPMC injector.
//!
//! A task is already a heap allocation, so the queue uses the task header's
//! `queue_next` field as its link. Pushing therefore allocates no queue node;
//! only the mutex acquisition remains on the global path.

#![allow(dead_code)]

use std::marker::PhantomData;
use std::ptr::NonNull;

use crate::loom::sync::atomic::{AtomicBool, Ordering};
use crate::loom::sync::Mutex;
use crate::task::{Header, Notified, RawTask, Schedule};

struct State {
    head: Option<NonNull<Header>>,
    tail: Option<NonNull<Header>>,
    len: usize,
}

pub(crate) struct Injector<S: Schedule> {
    state: Mutex<State>,
    closed: AtomicBool,
    _marker: PhantomData<fn() -> S>,
}

impl<S: Schedule> Injector<S> {
    pub(crate) fn new() -> Injector<S> {
        Injector {
            state: Mutex::new(State {
                head: None,
                tail: None,
                len: 0,
            }),
            closed: AtomicBool::new(false),
            _marker: PhantomData,
        }
    }

    pub(crate) fn push(&self, task: Notified<S>) {
        if self.closed.load(Ordering::Acquire) {
            drop(task);
            return;
        }
        let raw = task.into_raw();
        let mut state = self.state.lock().unwrap();
        if self.closed.load(Ordering::Acquire) {
            drop(state);
            raw.drop_reference();
            return;
        }
        // SAFETY: the task's queued reference gives us exclusive ownership of
        // its intrusive link until `pop` reconstructs the `Notified`.
        unsafe { *raw.header.as_ref().queue_next.get() = None };
        append(&mut state, raw.header);
    }

    pub(crate) fn push_batch<I>(&self, tasks: I)
    where
        I: IntoIterator<Item = Notified<S>>,
    {
        let mut state = self.state.lock().unwrap();
        for task in tasks {
            if self.closed.load(Ordering::Acquire) {
                drop(task);
                continue;
            }
            let raw = task.into_raw();
            // SAFETY: see `push`; this task is not linked anywhere else.
            unsafe { *raw.header.as_ref().queue_next.get() = None };
            append(&mut state, raw.header);
        }
    }

    pub(crate) fn pop(&self) -> Option<Notified<S>> {
        let mut state = self.state.lock().unwrap();
        let head = state.head?;
        // SAFETY: `head` is owned by this injector and has a live queue
        // reference until it is returned below.
        let next = unsafe { (*head.as_ref().queue_next.get()).take() };
        state.head = next;
        if next.is_none() {
            state.tail = None;
        }
        state.len -= 1;
        // SAFETY: the pointer came from a live `Notified<S>` pushed above, and
        // its vtable is therefore the exact scheduler type `S`.
        Some(Notified::new(unsafe { RawTask::from_raw(head) }))
    }

    pub(crate) fn pop_n(&self, limit: usize) -> Vec<Notified<S>> {
        let mut state = self.state.lock().unwrap();
        let mut tasks = Vec::with_capacity(limit.min(state.len));
        for _ in 0..limit {
            let Some(head) = state.head else { break };
            // SAFETY: see `pop`.
            let next = unsafe { (*head.as_ref().queue_next.get()).take() };
            state.head = next;
            if next.is_none() {
                state.tail = None;
            }
            state.len -= 1;
            // SAFETY: see `pop`.
            tasks.push(Notified::new(unsafe { RawTask::from_raw(head) }));
        }
        tasks
    }

    pub(crate) fn len(&self) -> usize {
        self.state.lock().unwrap().len
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
}

fn append(state: &mut State, node: NonNull<Header>) {
    if let Some(tail) = state.tail {
        // SAFETY: `tail` is a live header currently owned by this list.
        unsafe { *tail.as_ref().queue_next.get() = Some(node) };
    } else {
        state.head = Some(node);
    }
    state.tail = Some(node);
    state.len += 1;
}

impl<S: Schedule> Drop for Injector<S> {
    fn drop(&mut self) {
        self.close();
        while let Some(task) = self.pop() {
            drop(task);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::RawTask;
    use std::cell::RefCell;
    use std::rc::Rc;

    #[derive(Clone, Default)]
    struct TestSchedule(Rc<RefCell<Vec<RawTask>>>);

    // SAFETY: this scheduler is used only from the test thread.
    unsafe impl Sync for TestSchedule {}

    impl Schedule for TestSchedule {
        fn schedule(&self, task: Notified<Self>) {
            self.0.borrow_mut().push(task.into_raw());
        }
    }

    // SAFETY: the injector only moves `Notified` values around; the task's
    // scheduler is never touched by the queue itself, and these tasks are
    // never polled, only pushed and dropped.
    unsafe impl Send for Notified<TestSchedule> {}

    fn task(schedule: &TestSchedule) -> (Notified<TestSchedule>, crate::task::JoinHandle<()>) {
        crate::task::spawn(async {}, schedule.clone())
    }

    #[cfg(not(loom))]
    mod unit {
        use super::*;

        #[test]
        fn intrusive_push_pop_and_batch_preserve_all_tasks() {
            let schedule = TestSchedule::default();
            let injector = Injector::new();
            let (first, first_handle) = task(&schedule);
            let (second, second_handle) = task(&schedule);
            let (third, third_handle) = task(&schedule);
            injector.push(first);
            injector.push_batch([second, third]);
            assert_eq!(injector.len(), 3);
            let popped = injector.pop_n(2);
            assert_eq!(popped.len(), 2);
            drop(popped);
            assert!(injector.pop().is_some());
            assert_eq!(injector.len(), 0);
            drop(first_handle);
            drop(second_handle);
            drop(third_handle);
        }

        #[test]
        fn close_releases_new_tasks_instead_of_linking_them() {
            let schedule = TestSchedule::default();
            let injector = Injector::new();
            injector.close();
            let (notified, handle) = task(&schedule);
            injector.push(notified);
            assert_eq!(injector.len(), 0);
            drop(handle);
        }
    }

    #[cfg(loom)]
    mod loom_tests {
        use super::*;

        #[test]
        fn concurrent_push_pop_preserves_every_task_exactly_once() {
            loom::model(|| {
                let schedule = TestSchedule::default();
                let injector = crate::loom::sync::Arc::new(Injector::new());
                let mut producers = Vec::new();
                for _ in 0..2 {
                    let injector = injector.clone();
                    let mut tasks = Vec::new();
                    for _ in 0..2 {
                        let (notified, _handle) = task(&schedule);
                        tasks.push(notified);
                    }
                    producers.push(crate::loom::thread::spawn(move || {
                        for notified in tasks {
                            injector.push(notified);
                        }
                    }));
                }
                let consumer_injector = injector.clone();
                let consumer = crate::loom::thread::spawn(move || {
                    let mut seen = Vec::new();
                    // Race the producers, then wait for them and drain
                    // whatever arrived during the race.
                    while let Some(notified) = consumer_injector.pop() {
                        seen.push(notified.raw.header);
                    }
                    for producer in producers {
                        producer.join().unwrap();
                    }
                    while let Some(notified) = consumer_injector.pop() {
                        seen.push(notified.raw.header);
                    }
                    seen
                });
                let mut seen = consumer.join().unwrap();
                assert_eq!(injector.len(), 0);
                assert_eq!(seen.len(), 4);
                seen.sort_by_key(|header| header.as_ptr() as usize);
                seen.dedup();
                assert_eq!(seen.len(), 4, "each task must be popped exactly once");
                drop(seen);
            });
        }
    }
}
