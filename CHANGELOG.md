# Changelog

## Unreleased

- Phase 12 `io_uring`: safe multishot accept/recv streams (`AcceptMultishot`,
  `RecvMultishot`) with owned provided buffers, per-shot copies, buffer
  reuse, and cleanup/cancellation retention through the final CQE.
- Fixed `IORING_OFF_CQ_RING` / `IORING_OFF_SQES` mapping constants and the
  `CONNECT` SQE address encoding found via container testing.
- Raised the `io_uring` CI timeout to 600s; tolerate documented `ENOBUFS`
  multishot termination on shared kernels in the EOF-drain test.
- All 19 `CHECKLIST.md` phases complete (407 boxes).

## 0.1.0

- Initial workspace: task representation, wakers, current-thread and
  multi-thread schedulers, readiness I/O (epoll/kqueue/IOCP), timer wheel,
  sync primitives, combinators, blocking pool, cooperative budget,
  instrumentation, TUI + web consoles, macros, loom/Miri/differential gates,
  benchmarks, and examples.
