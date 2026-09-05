# Why `Pin` Exists

An async function is a state machine. After it has been polled, one state may
contain both a value and a reference into that value. Moving the state machine
would invalidate the reference, so polling takes `Pin<&mut Future>` rather than
`&mut Future`.

## Waiter Example

The semaphore acquire future contains its waiter node. When it cannot acquire a
permit, it puts a pointer to that node into the semaphore's intrusive FIFO
list. The node must remain at the same address until it is removed. Its poll
and drop paths therefore require the future to remain pinned:

```rust
let acquire = semaphore.acquire();
let mut acquire = std::pin::pin!(acquire);
// Polling `acquire` may link its in-future waiter into the semaphore.
```

Dropping the pinned future removes the node before its storage is destroyed.
Moving a registered future would turn the list pointer into a dangling
pointer. `PhantomPinned` makes accidental moves impossible for this waiter.
`Notify` uses the same lifetime rule for its registration state; its waiter
payload is reference counted while the future itself remains pinned.

Pinning is not a promise that a future will complete. It is an address
stability guarantee for the future's self-references and for data structures
that temporarily point into the future.
