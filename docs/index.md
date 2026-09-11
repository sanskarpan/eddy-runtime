# eddy — an async runtime from scratch

`eddy` is a from-scratch async runtime in Rust: explicit task state, custom
wakers, current-thread and multi-thread (work-stealing) schedulers, readiness
I/O (epoll / kqueue / IOCP), a hierarchical timer wheel, synchronization
primitives, a Linux `io_uring` completion backend, runtime instrumentation,
and TUI + web consoles.

## Quickstart

```rust
fn main() {
    eddy::Runtime::new().block_on(async {
        let handle = eddy::spawn(async { 40 + 2 });
        assert_eq!(handle.await.unwrap(), 42);
    });
}
```

```text
cargo test -p eddy --all-targets
cargo clippy -p eddy --all-targets -- -D warnings
cargo fmt --all -- --check
RUSTFLAGS='--cfg loom' cargo test -p eddy --lib
```

## Where to look

- [Architecture](ARCHITECTURE.md) — the event → waker → queue → poll cycle.
- [Task representation](TASK.md) — allocation layout, state machine, refcounting.
- [Why `Pin` exists](PIN.md) — the intrusive waiter as the worked example.
- [Cancel safety](CANCEL_SAFETY.md) — per-future cancellation analysis.
- [Cross-platform verification](verification.md) — which gates run where
  (Linux containers, QEMU ARM, Windows, Miri, ASan, loom).
- [Roadmap](https://github.com/sanskarpan/eddy-runtime/blob/main/CHECKLIST.md) —
  all 19 phases complete; Phase 12 covers the `io_uring` backend including
  multishot accept/recv.

## Consoles

- `eddy-console` (TUI): task list, worker heatmap, wake causality, poll
  histograms — see `crates/eddy-console`.
- `eddy-console-web` + `console-ui/`: browser dashboard over a WebSocket
  bridge.
- Recorded demos live under `docs/assets/`; `docs/assets/demo.sh` drives a
  synthetic event stream through the TUI for recording.
