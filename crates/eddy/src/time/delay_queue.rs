//! A heap-free delay queue: items are inserted under opaque keys and
//! delivered by deadline through the runtime's timing wheel.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Instant;

use crate::runtime::Handle;

use super::{TimerEntry, TimerShared};

/// Opaque handle to an item in a [`DelayQueue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Key(u64);

/// An item that has reached its deadline and is ready to be collected.
#[derive(Debug)]
pub struct Expired<T> {
    key: Key,
    deadline: Instant,
    item: T,
}

impl<T> Expired<T> {
    /// The key the item was inserted with.
    pub fn key(&self) -> Key {
        self.key
    }

    /// The deadline the item was scheduled for.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Split the expired item into its parts.
    pub fn into_parts(self) -> (T, Key, Instant) {
        (self.item, self.key, self.deadline)
    }

    /// Take the item, discarding its key and deadline.
    pub fn into_inner(self) -> T {
        self.item
    }
}

/// Shared wake channel: every entry's timer holds a clone of this waker, so a
/// fire from any worker wakes the task that is polling the queue.
struct FireSink {
    waker: Mutex<Option<Waker>>,
    notified: AtomicBool,
}

impl FireSink {
    fn new() -> Arc<FireSink> {
        Arc::new(FireSink {
            waker: Mutex::new(None),
            notified: AtomicBool::new(false),
        })
    }

    fn notify(&self) {
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        } else {
            self.notified.store(true, Ordering::Release);
        }
    }
}

impl Wake for FireSink {
    fn wake(self: Arc<Self>) {
        self.notify();
    }
}

struct Entry<T> {
    when: Instant,
    timer: Arc<TimerEntry>,
    value: T,
}

struct QueueState<T> {
    entries: HashMap<u64, Entry<T>>,
    expired: VecDeque<Expired<T>>,
    driver: Option<Arc<TimerShared>>,
    closed: bool,
}

/// A queue of items sorted by deadline.
///
/// Items are inserted with [`insert`](DelayQueue::insert) and delivered
/// through [`next`](DelayQueue::next) / [`poll_expired`](DelayQueue::poll_expired) in
/// deadline order — either after the real time passes, or after the runtime's
/// clock is advanced in paused-time mode.
pub struct DelayQueue<T> {
    state: Mutex<QueueState<T>>,
    next_key: AtomicU64,
    sink: Arc<FireSink>,
}

impl<T> DelayQueue<T> {
    /// Create a new, empty delay queue.
    ///
    /// The timer driver is resolved from the current runtime context lazily on
    /// the first insert, like `eddy::time::Sleep`.
    pub fn new() -> DelayQueue<T> {
        DelayQueue {
            state: Mutex::new(QueueState {
                entries: HashMap::new(),
                expired: VecDeque::new(),
                driver: None,
                closed: false,
            }),
            next_key: AtomicU64::new(0),
            sink: FireSink::new(),
        }
    }

    fn driver(&self) -> Arc<TimerShared> {
        {
            let state = self.state.lock().unwrap();
            if let Some(driver) = &state.driver {
                return driver.clone();
            }
        }
        let driver = Handle::current()
            .timer_driver()
            .unwrap_or_else(|| panic!("eddy: timers require a runtime with a timer driver"));
        self.state.lock().unwrap().driver = Some(driver.clone());
        driver
    }

    fn insert_inner(&mut self, when: Instant, value: T) -> Key {
        assert!(!self.is_closed(), "eddy: the delay queue is closed");
        let key = Key(self.next_key.fetch_add(1, Ordering::Relaxed));
        let driver = self.driver();
        let entry = Entry {
            when,
            timer: TimerEntry::new(),
            value,
        };
        let due = driver.arm(&entry.timer, when, Waker::from(self.sink.clone()));
        let mut state = self.state.lock().unwrap();
        if due {
            state.expired.push_back(Expired {
                key,
                deadline: entry.when,
                item: entry.value,
            });
        } else {
            state.entries.insert(key.0, entry);
        }
        drop(state);
        if due {
            self.sink.notify();
        }
        key
    }

    /// Insert an item to be delivered at or after `when`.
    pub fn insert(&mut self, when: Instant, value: T) -> Key {
        self.insert_inner(when, value)
    }

    /// Insert an item to be delivered at or after `when`.
    pub fn insert_at(&mut self, when: Instant, value: T) -> Key {
        self.insert_inner(when, value)
    }

    /// Remove the item with `key` before its deadline, returning the value.
    ///
    /// `None` is returned if the key was never inserted, has already expired
    /// (it will be delivered by `poll_expired`), or was removed before.
    pub fn remove(&mut self, key: Key) -> Option<T> {
        let mut state = self.state.lock().unwrap();
        let entry = state.entries.get(&key.0)?;
        if entry.timer.is_fired() {
            return None;
        }
        if let Some(driver) = &state.driver {
            driver.cancel(&entry.timer);
        }
        state.entries.remove(&key.0).map(|entry| entry.value)
    }

    /// Remove the item with `key` before its deadline if `predicate` accepts
    /// its value.
    pub fn remove_if<F>(&mut self, key: Key, predicate: F) -> Option<T>
    where
        F: FnOnce(&T) -> bool,
    {
        let mut state = self.state.lock().unwrap();
        let entry = state.entries.get(&key.0)?;
        if entry.timer.is_fired() || !predicate(&entry.value) {
            return None;
        }
        if let Some(driver) = &state.driver {
            driver.cancel(&entry.timer);
        }
        state.entries.remove(&key.0).map(|entry| entry.value)
    }

    /// Asynchronously wait for the next expired item.
    pub async fn next(&mut self) -> Option<Expired<T>> {
        std::future::poll_fn(|cx| self.poll_expired(cx)).await
    }

    /// Poll for the next expired item.
    pub fn poll_expired(&mut self, cx: &mut Context<'_>) -> Poll<Option<Expired<T>>> {
        loop {
            let item = {
                let mut state = self.state.lock().unwrap();
                let fired: Vec<u64> = state
                    .entries
                    .iter()
                    .filter(|(_, entry)| entry.timer.is_fired())
                    .map(|(key, _)| *key)
                    .collect();
                for key in fired {
                    if let Some(entry) = state.entries.remove(&key) {
                        state.expired.push_back(Expired {
                            key: Key(key),
                            deadline: entry.when,
                            item: entry.value,
                        });
                    }
                }
                state.expired.pop_front()
            };
            if let Some(item) = item {
                return Poll::Ready(Some(item));
            }

            let state = self.state.lock().unwrap();
            if state.closed && state.entries.is_empty() && state.expired.is_empty() {
                return Poll::Ready(None);
            }
            *self.sink.waker.lock().unwrap() = Some(cx.waker().clone());
            if self.sink.notified.swap(false, Ordering::AcqRel) {
                continue;
            }
            return Poll::Pending;
        }
    }

    /// Stop accepting new items. Pending items are still delivered.
    pub fn close(&mut self) {
        self.state.lock().unwrap().closed = true;
    }

    /// Whether [`close`](DelayQueue::close) was called.
    pub fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    /// The number of items currently in the queue, including expired items
    /// that have not been polled yet.
    pub fn len(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.entries.len() + state.expired.len()
    }

    /// Whether the queue holds no items.
    pub fn is_empty(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.entries.is_empty() && state.expired.is_empty()
    }

    /// The current capacity of the internal map.
    pub fn capacity(&self) -> usize {
        self.state.lock().unwrap().entries.capacity()
    }
}

impl<T> Default for DelayQueue<T> {
    fn default() -> DelayQueue<T> {
        DelayQueue::new()
    }
}

impl<T> Drop for DelayQueue<T> {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        if let Some(driver) = &state.driver {
            for entry in state.entries.values() {
                driver.cancel(&entry.timer);
            }
        }
        state.entries.clear();
        state.expired.clear();
    }
}
