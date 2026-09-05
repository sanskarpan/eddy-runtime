# eddy

`eddy` is an async runtime built from scratch, with explicit task state,
custom wakers, current- and multi-thread schedulers, readiness I/O, timers,
synchronization primitives, and runtime instrumentation.

## Verification

```text
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
RUSTFLAGS='--cfg loom' cargo test -p eddy --lib
npm run build --prefix console-ui
```

Linux-only io_uring, Miri, sanitizer, Windows IOCP, and ARM/QEMU checks run in
CI. See [`docs/verification.md`](docs/verification.md) for the platform gates.

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
