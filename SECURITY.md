# Security

## Reporting

Report suspected vulnerabilities via a private GitHub Security Advisory on
this repository. Do not open a public issue for unpatched security bugs.

## Scope notes

- `eddy` executes raw syscalls (`epoll`, `kqueue`, `io_uring`) and manages
  kernel-visible buffers; the owned-buffer and orphan-retention rules in
  `crates/eddy/src/io/uring.rs` are load-bearing for memory safety.
- The `unsafe` audit surface is gated by `clippy::undocumented_unsafe_blocks`,
  loom model tests, strict-provenance Miri, and the Linux ASan job — all
  blocking in CI (except the diagnostic `io_uring` job on shared kernels,
  which requires dedicated-kernel validation for release).
