#!/usr/bin/env python3
"""Generate a deterministic synthetic instrumentation stream for console demos.

Writes length-prefixed wire frames (see eddy-console FrameDecoder) to
docs/assets/demo.bin. Replay with:
    cargo run -p eddy-console -- --replay docs/assets/demo.bin
"""
import struct
import sys
from pathlib import Path


def u8(v):
    return struct.pack("B", v)


def u32(v):
    return struct.pack("<I", v)


def u64(v):
    return struct.pack("<Q", v)


def string(s):
    b = s.encode()
    return u32(len(b)) + b


def opt_string(s):
    return u8(0) if s is None else u8(1) + string(s)


def opt_u64(v):
    return u8(0) if v is None else u8(1) + u64(v)


def location(f, line):
    return string(f) + u32(line)


def id_list(ids):
    return u32(len(ids)) + b"".join(u64(i) for i in ids)


def frame(payload):
    return u32(len(payload)) + payload


def spawned(i, name, f, line, parent):
    return frame(u8(0) + u64(i) + opt_string(name) + location(f, line) + opt_u64(parent))


def poll_start(i, worker, at):
    return frame(u8(1) + u64(i) + u32(worker) + u64(at))


def poll_end(i, worker, dur, result):
    return frame(u8(2) + u64(i) + u32(worker) + u64(dur) + u8(result))


def woken_io(i, fd):
    return frame(u8(3) + u64(i) + u8(0) + u32(fd))


def woken_timer(i, timer):
    return frame(u8(3) + u64(i) + u8(1) + u64(timer))


def woken_task(i, other):
    return frame(u8(3) + u64(i) + u8(2) + u64(other))


def woken_channel(i, kind):
    return frame(u8(3) + u64(i) + u8(3) + string(kind))


def dropped(i, polls, busy, idle):
    return frame(u8(4) + u64(i) + u64(polls) + u64(busy) + u64(idle))


def park(worker, timeout):
    return frame(u8(6) + u32(worker) + opt_u64(timeout))


def unpark(worker, reason):
    return frame(u8(7) + u32(worker) + u8(reason))


def steal(thief, victim, count):
    return frame(u8(8) + u32(thief) + u32(victim) + u64(count))


def queue_depth(worker, local, glob, lifo):
    return frame(u8(9) + u32(worker) + u64(local) + u64(glob) + u8(1 if lifo else 0))


def io_registered(fd, interest, task):
    return frame(u8(10) + u32(fd) + string(interest) + u64(task))


def io_ready(fd, readiness, woke):
    return frame(u8(11) + u32(fd) + string(readiness) + id_list(woke))


def timer_set(i, deadline, task):
    return frame(u8(12) + u64(i) + u64(deadline) + u64(task))


def timer_fired(i, lateness):
    return frame(u8(13) + u64(i) + u64(lateness))


def timer_cancelled(i):
    return frame(u8(14) + u64(i))


def blocking(task, dur, f, line):
    return frame(u8(15) + u64(task) + u64(dur) + location(f, line))


def budget(task):
    return frame(u8(16) + u64(task))


def contended(kind, holder, waiters):
    return frame(u8(17) + string(kind) + u64(holder) + id_list(waiters))


NAMES = [
    (1, "echo-listener", "examples/echo.rs", 24),
    (2, "echo-conn-1", "examples/echo.rs", 51),
    (3, "echo-conn-2", "examples/echo.rs", 51),
    (4, "echo-conn-3", "examples/echo.rs", 51),
    (5, "chat-room", "examples/chat.rs", 33),
    (6, "http-server", "examples/http_server.rs", 41),
    (7, "timer-wheel", "src/time/wheel.rs", 112),
    (8, "blocking-pool", "src/blocking.rs", 88),
    (9, "watchdog", "src/runtime/mod.rs", 207),
    (10, "ticker", "examples/echo.rs", 67),
]


def main():
    out = []
    for i, name, f, line in NAMES:
        parent = None if i in (1, 7, 9) else 1 if i < 7 else 7
        out.append(spawned(i, name, f, line, parent))
    out.append(io_registered(7, "READABLE", 1))
    out.append(io_registered(9, "READABLE|WRITABLE", 2))
    out.append(timer_set(100, 5_000_000_000, 7))
    out.append(timer_set(101, 8_000_000_000, 10))

    now = 1_000_000_000
    for rnd in range(30):
        w = rnd % 4
        for i in range(1, 11):
            dur = (i * 137 + rnd * 7919) % 900_000 + 5_000
            out.append(poll_start(i, w, now))
            now += dur
            result = 0 if (i + rnd) % 7 == 0 else 1
            out.append(poll_end(i, (w + i) % 4, dur, result))
        out.append(woken_io(2 + rnd % 3, 9))
        out.append(woken_timer(10, 101))
        out.append(woken_task(5, 6))
        out.append(woken_channel(8, "mpsc"))
        out.append(steal((rnd + 1) % 4, rnd % 4, 3 + rnd % 12))
        out.append(queue_depth(w, rnd % 64, rnd % 17, rnd % 2 == 0))
        out.append(park((w + 2) % 4, 1_000_000))
        out.append(unpark((w + 2) % 4, 0))
        if rnd % 3 == 0:
            out.append(io_ready(9, "READABLE", [2, 3]))
        if rnd % 5 == 0:
            out.append(timer_fired(100, 12_000 + rnd))
            out.append(timer_set(100, now + 5_000_000_000, 7))
        if rnd % 4 == 0:
            out.append(budget(1 + rnd % 10))
        if rnd == 12:
            out.append(contended("mutex", 6, [2, 3, 4]))
        if rnd == 20:
            out.append(blocking(8, 142_000_000, "src/blocking.rs", 91))
    out.append(timer_cancelled(101))
    out.append(dropped(4, 312, 4_200_000, 900_000))
    out.append(dropped(9, 128, 900_000, 300_000))

    dest = Path(__file__).parent / "demo.bin"
    dest.write_bytes(b"".join(out))
    print(f"wrote {dest} ({dest.stat().st_size} bytes, {len(out)} frames)")


main()
