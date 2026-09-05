# Eddy Architecture

Eddy has one execution path for an asynchronous operation:

```text
event -> waker -> run queue -> task poll -> event registration
```

The reactor and timer driver do not execute user futures. A readiness event or
timer expiry wakes the task's `Waker`. The waker performs the task state
transition and submits the task to the current worker, the global injector, or
the worker that owns the task. A worker claims the task, polls it once, and
then either completes it, leaves it idle waiting for the next event, or
requeues it when a wake raced with the poll.

## Runtime Flavors

`Builder::new_current_thread` uses one owner thread and a local queue. It can
run `!Send` futures. `Builder::new_multi_thread` starts workers with local
queues, a LIFO fast path, a global injector, and stealing from other workers.
The caller of `Runtime::block_on` drives the root future; worker threads drive
spawned tasks.

The scheduler checks the injector periodically before local work. This bounds
the time an externally spawned task can remain behind a self-replenishing local
queue. The cooperative budget also forces hot resource loops through the queue
instead of allowing one task to monopolize a worker.

## I/O and Timers

`io::Registration` owns the platform registration and stores readiness waiters.
The driver waits in the operating-system poller, then wakes registered tasks.
The timer driver uses the same wake path: the timing wheel selects expired
entries and wakes their task. This keeps polling policy in the scheduler and
event translation in the driver.

## Ownership Invariants

- A task is polled by at most one worker at a time.
- A task has at most one queued notification; a wake during `RUNNING` records
  `NOTIFIED` for the poller to requeue.
- The scheduler, join handle, and cloned wakers hold independent task
  references.
- A current-thread future and its destruction stay on the owner thread.

The implementation points are `crates/eddy/src/task/state.rs`,
`task/harness.rs`, `scheduler/multi_thread/worker.rs`, and `io/driver.rs`.
