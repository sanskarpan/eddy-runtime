# Eddy Tasks

Each spawned future is stored in one `Box<Cell<F, S>>`. `Cell` is `repr(C)` and
contains three regions:

```text
Header: state, vtable, ownership/instrumentation metadata, queue link
Core:   scheduler and Stage<F> (Running, Finished, or Consumed)
Trailer: join-handle waker
```

The scheduler holds a type-erased `RawTask` pointer. Its vtable knows how to
poll, schedule, read output, shut down, and deallocate the concrete cell, so
the queues do not need to be generic over every future type.

## State Machine

`Header::state` packs flags into the low bits of one atomic word and the task
reference count above them. The important transitions are:

```text
spawn -> NOTIFIED + queued reference
queue -> RUNNING + remove NOTIFIED
poll Pending -> IDLE, or NOTIFIED when a wake raced with poll
poll Ready -> COMPLETE, then output is read by JoinHandle
abort -> CANCELLED; the owner finalizes the future safely
```

`RUNNING` prevents concurrent polls. `NOTIFIED` closes the lost-wakeup window:
if a reactor or another task wakes a running task, the current poll observes
the bit and submits another queue entry before becoming idle.

## References

The initial task allocation accounts for the join handle and the initial
run-queue entry. A waker clone adds a reference. By-value `wake` consumes its
waker reference; `wake_by_ref` does not. The allocation is released only when
all scheduler, join, and waker references are gone. Current-thread destruction
is deferred to the owner when a non-owner drops the last usable reference.

The size-budget test in `task/raw.rs` guards the concrete `Cell<F, S>` layout
without making the internal scheduler or allocation layout part of the public
API.
