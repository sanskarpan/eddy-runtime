//! Windows IOCP backend — deliberately a stub.
//!
//! IOCP is completion-based: a single `CreateIoCompletionPort` plus
//! `GetQueuedCompletionStatusEx` loop delivers *completion* packets, not
//! readiness. Emulating readiness on Windows (as mio does) requires issuing
//! zero-byte `WSARecv` probes on every registration and mapping each
//! completion back to a readiness report — a fundamentally different
//! architecture than the readiness drivers on epoll/kqueue. The readiness
//! model of `crate::sys::Poller` does not map onto it without that probe
//! layer, so the stub fails construction with a clear error until a native
//! completion-based driver lands.

use std::io;
use std::os::windows::io::RawSocket;
use std::time::Duration;

use super::{Event, Interest, Poller, Ready, Token};

pub(crate) struct Iocp {
    _port: RawSocket,
}

impl Poller for Iocp {
    fn new() -> io::Result<Iocp> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "eddy: the Windows IOCP backend is not implemented; readiness on Windows requires \
             zero-byte WSARecv probe completions (see sys/iocp.rs)",
        ))
    }

    fn register(&self, _fd: RawSocket, _token: Token, _interest: Interest) -> io::Result<()> {
        unreachable!("Iocp cannot be constructed")
    }

    fn reregister(&self, _fd: RawSocket, _token: Token, _interest: Interest) -> io::Result<()> {
        unreachable!("Iocp cannot be constructed")
    }

    fn deregister(&self, _fd: RawSocket) -> io::Result<()> {
        unreachable!("Iocp cannot be constructed")
    }

    fn wait(&self, _events: &mut Vec<Event>, _timeout: Option<Duration>) -> io::Result<()> {
        unreachable!("Iocp cannot be constructed")
    }

    fn wake(&self) -> io::Result<()> {
        unreachable!("Iocp cannot be constructed")
    }
}
