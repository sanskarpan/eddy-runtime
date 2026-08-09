//! Runtime timers backed by a hierarchical timing wheel.

mod wheel;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use pin_project_lite::pin_project;

use crate::runtime::Handle;

pub(crate) use wheel::{TimerEntry, TimerShared};

/// Waits until a monotonic deadline.
pub struct Sleep {
    deadline: Instant,
    entry: Arc<TimerEntry>,
    driver: Option<Arc<TimerShared>>,
}

impl Sleep {
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Change the deadline without allocating a new timer entry.
    pub fn reset(&mut self, deadline: Instant) {
        self.deadline = deadline;
        self.entry.reset();
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.entry.is_fired() {
            return Poll::Ready(());
        }
        let driver = this.driver.get_or_insert_with(|| {
            Handle::current()
                .timer_driver()
                .unwrap_or_else(|| panic!("eddy: timers require a runtime with a timer driver"))
        });
        if driver.arm(&this.entry, this.deadline, cx.waker().clone()) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(driver) = &self.driver {
            driver.cancel(&self.entry);
        }
    }
}

/// Sleep for `duration` from the time this function is called.
pub fn sleep(duration: Duration) -> Sleep {
    sleep_until(Instant::now() + duration)
}

/// Sleep until an absolute monotonic deadline.
pub fn sleep_until(deadline: Instant) -> Sleep {
    Sleep {
        deadline,
        entry: TimerEntry::new(),
        driver: None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl std::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

pin_project! {
    pub struct Timeout<F> {
        #[pin]
        future: F,
        #[pin]
        sleep: Sleep,
    }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if let Poll::Ready(output) = this.future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        if this.sleep.as_mut().poll(cx).is_ready() {
            Poll::Ready(Err(Elapsed))
        } else {
            Poll::Pending
        }
    }
}

pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    timeout_at(Instant::now() + duration, future)
}

pub fn timeout_at<F: Future>(deadline: Instant, future: F) -> Timeout<F> {
    Timeout {
        future,
        sleep: sleep_until(deadline),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissedTickBehavior {
    Burst,
    Delay,
    Skip,
}

pub struct Interval {
    period: Duration,
    next: Instant,
    sleep: Sleep,
    missed_tick_behavior: MissedTickBehavior,
}

impl Interval {
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.missed_tick_behavior = behavior;
    }

    pub fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.missed_tick_behavior
    }

    /// Wait for the next tick and return its scheduled deadline.
    pub async fn tick(&mut self) -> Instant {
        let deadline = self.next;
        (&mut self.sleep).await;
        let now = Instant::now();
        self.next = match self.missed_tick_behavior {
            MissedTickBehavior::Burst => deadline + self.period,
            MissedTickBehavior::Delay => now + self.period,
            MissedTickBehavior::Skip => {
                let mut next = deadline + self.period;
                while next <= now {
                    next += self.period;
                }
                next
            }
        };
        self.sleep.reset(self.next);
        deadline
    }
}

pub fn interval(period: Duration) -> Interval {
    interval_at(Instant::now() + period, period)
}

pub fn interval_at(start: Instant, period: Duration) -> Interval {
    assert!(!period.is_zero(), "eddy: interval period must be non-zero");
    Interval {
        period,
        next: start,
        sleep: sleep_until(start),
        missed_tick_behavior: MissedTickBehavior::Burst,
    }
}
