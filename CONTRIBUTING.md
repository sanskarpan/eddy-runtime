# Contributing

## Workflow

- One file per commit; `track/*` branches per workstream; PRs into `main`.
- CI must be green before merge: `fmt + clippy + tests + deps`, loom, Miri,
  cross-target matrix (macOS, Windows, QEMU ARM), `io_uring`, and ASan.

## Local gates

```text
cargo fmt --all -- --check
cargo clippy -p eddy --all-targets -- -D warnings
cargo test -p eddy --all-targets
cargo test -p eddy-macros --all-targets
RUSTFLAGS='--cfg loom' cargo test -p eddy --lib
```

Linux-only checks (`io_uring`, sanitizers) run in CI and in a local
container; see `docs/verification.md` for the exact commands.

## Safety rules

- Every `unsafe` block carries a `// SAFETY:` comment (enforced by
  `clippy::undocumented_unsafe_blocks` at the workspace level).
- Every lock-free structure ships with a loom test in the same commit.
- `tokio` and `futures` are test-only oracles; they must never become
  runtime dependencies (`cargo tree -p eddy --edges normal` is gated).
