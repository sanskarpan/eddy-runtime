# Cancellation Safety

Dropping a future is Eddy's cancellation operation. `select!`, a dropped join
set, task abort, and runtime shutdown can all drop an in-flight future. An
operation is cancel safe when dropping it at any await point does not lose or
duplicate the logical item it was processing.

## Shipped Operations

| Operation | Cancellation contract |
| --- | --- |
| `sync::mpsc::Receiver::recv` / `recv_many` | Safe. A pending receive has not removed a message. |
| `sync::mpsc::Sender::send` | Safe for the channel. The item remains owned by the send future until delivery; dropping it drops the item. Use `reserve` + `Permit` when the item must be retained across cancellation. |
| `sync::oneshot::Receiver` | Safe. A value is taken only when the receive completes. |
| `sync::broadcast::Receiver` | Safe for the cursor. A value is consumed only on `Ready`; lag is reported explicitly. |
| `sync::watch::Receiver::changed` | Safe. The version is observed only when the change is returned. |
| `sync::Notify::notified` / `Semaphore::acquire` | Safe. Drop unregisters the waiter or releases a granted permit. `Notified::enable` must be called on a pinned future before a select-style wait. |
| `sync::Mutex::lock` / `RwLock` locks | Safe. A dropped pending lock is removed from the waiter set; a completed guard releases ownership on drop. |
| `time::Sleep`, `timeout`, `interval` | Safe for the timer. Dropping cancels the timer registration; no user value is consumed. |
| `io::AsyncReadExt::read` / `read_to_end` / `read_to_string` | Safe as documented. A pending `read` consumes no bytes; completed chunks are appended before `read_to_end` continues. |
| `io::AsyncReadExt::read_exact` | Not safe. Cancellation after a partial read consumes bytes without returning the final count. |
| `io::AsyncWriteExt::write` | A pending write has not written; cancellation after `Ready` may have written a prefix. |
| `io::AsyncWriteExt::write_all` | Not safe as a whole. Cancellation can leave a partial prefix at the peer. |
| `CancellationToken::cancelled` | Safe. Registration is removed on drop and cancellation is level-triggered. |
| `JoinHandle` / `FuturesUnordered` / `JoinSet` | Dropping detaches or drops the remaining task/future according to the owning type; the user operation itself must provide its own commit semantics. |

## Designing a Select Branch

Use cancel-safe receive operations directly in a select. For a non-safe
operation, make progress explicit: retain the buffer and offset outside the
future, use a single low-level operation at a time, or finish the operation
before entering another cancellation point. The runtime cannot recover bytes
already consumed by the operating system or a peer.

The low-level contracts live beside the APIs in `crates/eddy/src/io/mod.rs`;
they are part of the behavior users must rely on, not merely implementation
notes.
