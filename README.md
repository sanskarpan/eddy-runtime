# eddy — async runtime from scratch

[![CI](https://github.com/sanskarpan/eddy-runtime/actions/workflows/ci.yml/badge.svg)](https://github.com/sanskarpan/eddy-runtime/actions/workflows/ci.yml)
[![Docs](https://github.com/sanskarpan/eddy-runtime/actions/workflows/docs.yml/badge.svg)](https://sanskarpan.github.io/eddy-runtime/)

`eddy` is an async runtime built from scratch, with explicit task state,
custom wakers, current- and multi-thread schedulers, readiness I/O, timers,
synchronization primitives, a Linux `io_uring` backend, and runtime
instrumentation with TUI and web consoles.

Docs: https://sanskarpan.github.io/eddy-runtime/

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

## Workspace

```text
crates/eddy            # the runtime (tasks, schedulers, I/O, timers, sync)
crates/eddy-macros     # #[eddy::main], #[eddy::test], select!/join!/try_join!
crates/eddy-console    # ratatui TUI over the instrumentation socket
crates/eddy-console-web# WebSocket bridge for the browser dashboard
console-ui/            # Vite + React dashboard (swimlanes, heatmap, wake graph)
```

## Verification

Linux-only `io_uring`, Miri, sanitizer, Windows IOCP, and ARM/QEMU checks run
in CI alongside loom model tests and the Tokio differential suite. See
[`docs/verification.md`](docs/verification.md) for the platform gates, and
[`CHECKLIST.md`](CHECKLIST.md) for the 19-phase roadmap (all complete).

## Benchmark Snapshot

These are real median results from the local macOS quick Criterion run:

```text
Command: cargo bench -p eddy --bench vs_tokio -- --quick
Host: Apple Silicon macOS development machine
Units: median wall-clock time; lower is better
```

| Workload | Eddy | Tokio | Eddy / Tokio |
| --- | ---: | ---: | ---: |
| spawn/join, 10k tasks | 11.221 ms | 3.6755 ms | 3.05x |
| ping-pong, 1M round trips | 531.54 us | 217.42 us | 2.45x |
| bounded channels | 40.286 us | 137.87 us | 0.29x |
| unbounded channels | 38.705 us | 132.52 us | 0.29x |
| mutex contention, 1 task | 35.924 us | 41.906 us | 0.86x |
| mutex contention, 2 tasks | 37.100 us | 39.356 us | 0.94x |
| mutex contention, 4 tasks | 43.795 us | 41.025 us | 1.07x |
| mutex contention, 8 tasks | 51.953 us | 45.896 us | 1.13x |
| mutex contention, 16 tasks | 76.487 us | 53.796 us | 1.42x |

The quick run was intentionally bounded and does not replace full Criterion
runs on each supported target. Instrumentation overhead for the dedicated
10k spawn/join benchmark measured `+3.1%` median in the same workspace.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). One file per commit, `track/*`
branches, CI must be green before merge.
