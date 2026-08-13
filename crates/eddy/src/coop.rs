//! Cooperative scheduling budget.
//!
//! Each task poll is granted a budget of [`BUDGET`] resource operations
//! (channel receives, lock acquisitions, I/O readiness waits, ...). An
//! operation that finds its resource immediately ready consumes one unit;
//! when the budget runs out the operation returns `Poll::Pending` and
//! re-registers a wake, so the task is rescheduled instead of spinning on a
//! hot resource while starving everything else. The budget is reset at the
//! start of every task poll (see `Harness::poll`), so a yielded task always
//! gets a full new budget.
//!
//! The canonical failure this prevents: `loop { rx.recv().await }` over an
//! always-ready channel running forever in one scheduling round while other
//! tasks wait.
//!
//! `block_on` root futures and [`unconstrained`] wrapped futures run with no
//! budget at all (`None`), because they are the user's own driving context,
//! not a task the scheduler must keep fair.

use std::cell::Cell;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

use crate::instrument::{emit, RuntimeEvent, TaskId};

/// Operations granted to one task poll.
pub(crate) const BUDGET: u8 = 128;

thread_local! {
    static BUDGET_CELL: Cell<Option<u8>> = const { Cell::new(None) };
}

/// RAII guard arming the budget for a task poll and restoring the previous
/// value (including when the poll panics).
pub(crate) struct BudgetGuard {
    previous: Option<u8>,
}

/// Arm a fresh budget for the current task poll.
pub(crate) fn budget_guard() -> BudgetGuard {
    BudgetGuard {
        previous: BUDGET_CELL.with(|b| b.replace(Some(BUDGET))),
    }
}

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        BUDGET_CELL.with(|b| b.set(self.previous));
    }
}

/// Consume one unit of budget, yielding the task (with a registered wake) if
/// the budget is exhausted. Unconstrained contexts always proceed.
pub fn poll_proceed(cx: &mut Context<'_>) -> Poll<()> {
    match BUDGET_CELL.with(|b| b.get()) {
        None => Poll::Ready(()),
        Some(0) => {
            cx.waker().wake_by_ref();
            emit(RuntimeEvent::BudgetExhausted {
                task: TaskId::current(),
            });
            Poll::Pending
        }
        Some(remaining) => {
            BUDGET_CELL.with(|b| b.set(Some(remaining - 1)));
            Poll::Ready(())
        }
    }
}

/// Whether the calling context still has budget. Always `true` outside a
/// task (or inside [`unconstrained`]).
pub fn has_budget_remaining() -> bool {
    BUDGET_CELL.with(|b| b.get().map_or(true, |remaining| remaining > 0))
}

pin_project! {
    /// A future that polls its inner future without consuming any budget.
    ///
    /// Useful for hot loops that need to finish in one scheduling round, at
    /// the cost of potentially starving other tasks.
    pub struct Unconstrained<F> {
        #[pin]
        inner: F,
    }
}

impl<F: std::future::Future> std::future::Future for Unconstrained<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let previous = BUDGET_CELL.with(|b| b.replace(None));
        let result = this.inner.poll(cx);
        BUDGET_CELL.with(|b| b.set(previous));
        result
    }
}

/// Run `future` without a cooperative budget.
pub fn unconstrained<F: std::future::Future>(future: F) -> Unconstrained<F> {
    Unconstrained { inner: future }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;
    use std::pin::Pin;
    use std::task::Context;

    #[test]
    fn budget_consumes_exactly_128_then_yields() {
        let guard = budget_guard();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        for _ in 0..BUDGET {
            assert!(
                poll_proceed(&mut cx).is_ready(),
                "first 128 proceeds are Ready"
            );
        }
        assert!(
            poll_proceed(&mut cx).is_pending(),
            "the 129th proceed yields"
        );
        drop(guard);
        assert!(has_budget_remaining(), "no budget outside a task poll");
    }

    #[test]
    fn unconstrained_does_not_consume_budget() {
        let guard = budget_guard();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = unconstrained(poll_fn(|cx| {
            let _ = poll_proceed(cx);
            Poll::Ready(())
        }));
        use std::future::Future;
        let _ = Pin::new(&mut fut).poll(&mut cx);
        assert!(
            has_budget_remaining(),
            "the wrapper's poll must not consume the outer budget"
        );
        drop(guard);
    }
}
