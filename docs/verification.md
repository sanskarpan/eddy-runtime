# Cross-Platform Verification

The native macOS build is the source of truth for kqueue behavior. Containers
on macOS run inside a Linux virtual machine, so they are useful for Linux
epoll/io_uring and Linux ARM checks, but they cannot validate kqueue or IOCP.

## Linux on macOS

Podman and Docker both provide a Linux VM on macOS. Use two vCPUs, 2-4 GiB of
RAM, and a small disk for the lowest-cost loop:

```text
podman machine init --cpus 2 --memory 2048 --disk-size 10 eddy-linux
podman machine start eddy-linux
podman run --rm -v "$PWD:/src" -w /src rust:1.87-bookworm \
  cargo test -p eddy --features io-uring io::uring::tests::
```

The equivalent Docker command is:

```text
docker run --rm -v "$PWD:/src" -w /src rust:1.87-bookworm \
  cargo test -p eddy --features io-uring io::uring::tests::
```

The ring probe treats `ENOSYS` and `EPERM` as an unavailable kernel feature;
that is a valid result for a restricted container and should fall back to the
epoll driver. Do not grant `--privileged` merely to make this probe pass.

The current operation state is single-CQE by design. Multishot accept/receive
therefore returns `Unsupported` rather than exposing a stream API that could
drop a later CQE or release its buffer too early; implementing it requires a
separate multi-completion state machine.

## ARM

On Apple Silicon, a native `aarch64` Linux VM/container is preferable to
amd64 emulation. For the weak-memory check, CI runs
`aarch64-unknown-linux-gnu` under qemu-user with the cross linker. That is
slower than a local container but validates the target ABI and memory model
without requiring a second machine.

## Windows

Linux containers cannot exercise Windows IOCP, `WSARecv`, or Windows handle
semantics. Keep the Windows job on `windows-latest` (or a Windows VM) and run
`cargo test --target x86_64-pc-windows-msvc` there. A Linux container can only
check platform-independent code and must not be used as evidence for IOCP.

## Miri and sanitizers

Miri requires the nightly toolchain and the `miri` component. It is a host
tool, not a substitute for an io_uring container: Miri cannot model epoll,
kqueue, IOCP, or the kernel writing into a mapped ring. Run the pure task,
timer, queue, and synchronization tests under Miri, and run the owned-buffer
CQE cancellation test under Linux with ASan or Valgrind where available.

CI runs the pure-module Miri selection with `MIRIFLAGS=-Zmiri-strict-provenance`.
The Linux ASan job uses nightly `-Zsanitizer=address` and only the
`dropping_a_recv_orphans_it_until_the_cqe` io_uring test. This keeps kernel
availability and sanitizer failures isolated from the portable test gate; the
io_uring test intentionally accepts an unavailable kernel feature as a valid
fallback result. It rebuilds the Linux standard library with `-Zbuild-std` and
uses an explicit target so sanitizer flags do not affect host build scripts or
procedural macros.

The repository CI already separates these checks: Linux native tests,
aarch64/qemu, Windows, loom, strict-provenance Miri, ASan, and the
feature-gated io_uring job.

## Phase 17 Tests

The cancellation, bounded soak, watchdog, and Tokio differential tests are
ordinary integration tests and run in the normal package test job:

```text
cargo test -p eddy --test cancel_safety
cargo test -p eddy --test stress --test watchdog --test differential
```

The differential test compares full-capacity `try_send`, async backpressure,
sender and receiver closure, timer ordering/expiration, interval deadlines,
and a cancelled `select!` receive against Tokio. Timer elapsed durations are
checked for no early wakeup rather than compared byte-for-byte, since host
scheduling is inherently variable.

The soak is intentionally bounded for CI. It uses 64 concurrent tasks and a
five-second progress deadline rather than claiming a ten-minute wall-clock
soak; longer RSS and deadlock runs belong in a scheduled stress job.

## Phase 18 Benchmarks

Run the complete Criterion suite with:

```text
make bench-vs-tokio
```

Criterion writes measured HTML reports under `target/criterion/report/` and
machine-readable estimates under each benchmark directory. The benchmark
harness does not include checked-in numbers: record results from the host and
toolchain that produced them. For a real instrumentation comparison, run:

```text
make bench-report BASELINE=phase18
```

The first command saves the feature-disabled measurement. The second runs the
same benchmark IDs with the `instrumentation` feature and compares against that
saved baseline. Use a different `BASELINE` name when retaining multiple host
or compiler runs. This is intentionally a measured comparison rather than a
claim that instrumentation overhead is below a threshold.

When publishing results, use this template and include the exact command,
commit, host, target, Rust version, and whether the CPU was otherwise idle:

```text
Benchmark date:
Commit:
Host / CPU:
Target:
Rust / Cargo:
Command:

| Benchmark | Eddy | Tokio | Ratio or delta | Notes |
| --- | ---: | ---: | ---: | --- |
| spawn-join | measured | measured | measured | |
| ping-pong | measured | measured | measured | |
| channels/bounded | measured | measured | measured | |
| channels/unbounded | measured | measured | measured | |
| mutex-contention | measured | measured | measured | |
| timer-churn | measured | measured | measured | |
| echo-server | measured | measured | measured | Unix only |
| work-steal-latency | measured | measured | measured | |

Instrumentation baseline:
| Mode | Median | Change |
| --- | ---: | ---: |
| feature disabled | measured | baseline |
| feature enabled | measured | measured |
```

Sources:

- https://docs.podman.io/en/latest/markdown/podman-machine.1.html
- https://docs.docker.com/desktop/features/vmm/
- https://doc.rust-lang.org/cargo/commands/cargo-miri.html
