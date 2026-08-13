//! Platform I/O readiness abstraction.
//!
//! `Poller` is the trait every backend implements. The driver (see
//! `crate::io::driver`) is written against the trait, so it stays
//! platform-agnostic; only the syscall layer below is per-OS.
//!
//! On Windows, IOCP is completion-based rather than readiness-based, so the
//! backend (`sys/iocp.rs`) emulates readiness with outstanding zero-byte
//! `WSARecv` probes — the same approach legacy mio took — and is verified by
//! cross-compilation only.

#[cfg(target_os = "linux")]
mod epoll;
#[cfg(windows)]
mod iocp;
#[cfg(target_os = "macos")]
mod kqueue;

use std::io;
use std::time::Duration;

pub(crate) use crate::io::{Interest, Ready};

/// The platform's native socket descriptor: a file descriptor on unix, an
/// OS handle (`SOCKET`) on Windows.
#[cfg(unix)]
pub(crate) type Fd = std::os::fd::RawFd;
#[cfg(windows)]
pub(crate) type Fd = std::os::windows::io::RawSocket;

#[cfg(target_os = "linux")]
pub(crate) use self::epoll::Epoll as PollerImpl;
#[cfg(windows)]
pub(crate) use self::iocp::Iocp as PollerImpl;
#[cfg(target_os = "macos")]
pub(crate) use self::kqueue::Kqueue as PollerImpl;
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
compile_error!(
    "eddy: the I/O driver currently supports Linux (epoll), macOS (kqueue) and Windows (IOCP)"
);

/// A 64-bit event token: the high 32 bits are a generation counter, the low
/// 32 bits the slab index of the `ScheduledIo` it belongs to.
///
/// The generation rides along in the token so that a stale in-flight event
/// (an fd was deregistered, the slab slot reused by a new registration) can
/// be discarded by the dispatcher instead of misdirected to the new owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Token {
    value: u64,
}

impl Token {
    pub(crate) fn new(index: usize, generation: u32) -> Token {
        debug_assert!(index < (1 << 32));
        Token {
            value: ((generation as u64) << 32) | index as u64,
        }
    }

    pub(crate) fn index(self) -> usize {
        (self.value & 0xFFFF_FFFF) as usize
    }

    pub(crate) fn generation(self) -> u32 {
        (self.value >> 32) as u32
    }

    /// Rebuild a token from a raw kernel payload (waker fd).
    pub(crate) fn from_value(value: u64) -> Token {
        Token { value }
    }
}

/// One event delivered by `Poller::wait`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Event {
    pub(crate) token: Token,
    pub(crate) ready: Ready,
}

/// The syscall-level readiness backend.
pub(crate) trait Poller: Send + Sync {
    fn new() -> io::Result<Self>
    where
        Self: Sized;

    fn register(&self, fd: Fd, token: Token, interest: Interest) -> io::Result<()>;

    #[allow(dead_code)]
    fn reregister(&self, fd: Fd, token: Token, interest: Interest) -> io::Result<()>;

    fn deregister(&self, fd: Fd) -> io::Result<()>;

    /// Block until at least one event is ready or `timeout` elapses.
    /// `None` blocks indefinitely; the returned events are appended to
    /// `events`, which is reused across calls to avoid reallocating.
    fn wait(&self, events: &mut Vec<Event>, timeout: Option<Duration>) -> io::Result<()>;

    /// Interrupt an in-progress `wait` on another thread. This is how a
    /// remote wake reaches a worker blocked inside the kernel.
    fn wake(&self) -> io::Result<()>;
}

#[cfg(unix)]
pub(crate) fn set_nonblocking(fd: Fd) -> io::Result<()> {
    // SAFETY: `fd` is a valid open descriptor provided by the caller; fcntl
    // F_GETFL takes no other arguments.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    let flags = flags | libc::O_NONBLOCK;
    // SAFETY: as above; F_SETFL takes the integer `flags` argument.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(windows)]
pub(crate) fn set_nonblocking(fd: Fd) -> io::Result<()> {
    use windows_sys::Win32::Networking::WinSock::{ioctlsocket, FIONBIO, SOCKET, SOCKET_ERROR};

    let mut mode: u32 = 1;
    // SAFETY: `fd` is the caller's socket; `FIONBIO` only reads the mode
    // value, which sits in a live `u32`.
    let rc = unsafe { ioctlsocket(fd as SOCKET, FIONBIO as i32, &mut mode) };
    if rc == SOCKET_ERROR {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
