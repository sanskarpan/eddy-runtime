//! Linux epoll readiness backend.
//!
//! Level-triggered by default: `epoll_wait` keeps reporting a ready fd until
//! the condition clears, so a missed drain cannot wedge a socket. Edge-
//! triggered (`EPOLLET`) is available behind `Interest::EDGE_TRIGGERED`; the
//! async-net types must then drain to `EWOULDBLOCK` on every wake or the
//! socket stops reporting. `EPOLLRDHUP` detects the remote half-close and
//! `EPOLLERR`/`EPOLLHUP` are always requested so error states never go
//! unnoticed. The waker is an `eventfd` registered with a reserved token.

use std::io;
use std::os::fd::RawFd;
use std::time::Duration;

use super::{Event, Interest, Poller, Ready, Token};

/// Reserved token of the eventfd waker.
const WAKER_TOKEN: u64 = u64::MAX;

/// Maximum events processed per `epoll_wait` call; level-triggered mode keeps
/// anything left over ready for the next call.
const MAX_EVENTS: usize = 1024;

pub(crate) struct Epoll {
    ep: RawFd,
    waker: RawFd,
}

impl Drop for Epoll {
    fn drop(&mut self) {
        // SAFETY: `ep` and `waker` are owned fds created in `Epoll::new`; this
        // is the last use of the poller, so closing them cannot race.
        unsafe {
            libc::close(self.ep);
            libc::close(self.waker);
        }
    }
}

fn last_error() -> io::Error {
    io::Error::last_os_error()
}

fn epoll_events(interest: Interest) -> u32 {
    let mut events: u32 = libc::EPOLLRDHUP as u32;
    if interest.is_readable() {
        events |= libc::EPOLLIN as u32;
    }
    if interest.is_writable() {
        events |= libc::EPOLLOUT as u32;
    }
    if interest.is_edge_triggered() {
        events |= libc::EPOLLET as u32;
    }
    events
}

fn ready_from_epoll(events: u32) -> Ready {
    let mut ready = Ready::EMPTY;
    if events & (libc::EPOLLIN | libc::EPOLLPRI) as u32 != 0 {
        ready = ready.union(Ready::READABLE);
    }
    if events & libc::EPOLLOUT as u32 != 0 {
        ready = ready.union(Ready::WRITABLE);
    }
    if events & libc::EPOLLRDHUP as u32 != 0 {
        ready = ready.union(Ready::READ_CLOSED);
    }
    if events & (libc::EPOLLHUP | libc::EPOLLERR) as u32 != 0 {
        ready = ready.union(Ready::ERROR);
    }
    ready
}

impl Epoll {
    fn ctl(&self, op: libc::c_int, fd: RawFd, token: Option<(Token, Interest)>) -> io::Result<()> {
        let mut event = libc::epoll_event { events: 0, u64: 0 };
        let event_ptr = if let Some((token, interest)) = token {
            event.events = epoll_events(interest);
            event.u64 = token.value;
            &mut event as *mut libc::epoll_event
        } else {
            std::ptr::null_mut()
        };
        // SAFETY: `ep` is a valid epoll fd; `fd` is either a registered socket
        // (this poller owns its interest) or, for `EPOLL_CTL_DEL`, a closed
        // fd that the kernel ignores. `event_ptr` points at a live local
        // `epoll_event` when an opcode needs one.
        let rc = unsafe { libc::epoll_ctl(self.ep, op, fd, event_ptr) };
        if rc == -1 {
            return Err(last_error());
        }
        Ok(())
    }
}

impl Poller for Epoll {
    fn new() -> io::Result<Epoll> {
        // SAFETY: epoll_create1 takes no pointer arguments; the returned fd
        // is stored in `self.ep` and closed in `Drop`.
        let ep = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if ep == -1 {
            return Err(last_error());
        }
        // SAFETY: eventfd takes no pointer arguments; the returned fd is
        // stored in `self.waker` and closed in `Drop`.
        let waker = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if waker == -1 {
            // SAFETY: `ep` was created in this function and its registration
            // failed, so nothing else can use it.
            unsafe { libc::close(ep) };
            return Err(last_error());
        }
        let poller = Epoll { ep, waker };
        poller.ctl(
            libc::EPOLL_CTL_ADD,
            waker,
            Some((Token::from_value(WAKER_TOKEN), Interest::READABLE)),
        )?;
        Ok(poller)
    }

    fn register(&self, fd: RawFd, token: Token, interest: Interest) -> io::Result<()> {
        self.ctl(libc::EPOLL_CTL_ADD, fd, Some((token, interest)))
    }

    fn reregister(&self, fd: RawFd, token: Token, interest: Interest) -> io::Result<()> {
        self.ctl(libc::EPOLL_CTL_MOD, fd, Some((token, interest)))
    }

    fn deregister(&self, fd: RawFd) -> io::Result<()> {
        self.ctl(libc::EPOLL_CTL_DEL, fd, None)
    }

    fn wait(&self, events: &mut Vec<Event>, timeout: Option<Duration>) -> io::Result<()> {
        let mut buf = [libc::epoll_event { events: 0, u64: 0 }; MAX_EVENTS];
        let timeout_ms = timeout
            .map(|duration| duration.as_millis().min(i32::MAX as u128) as i32)
            .unwrap_or(-1);
        // SAFETY: `buf` is a live array of `MAX_EVENTS` `epoll_event`s large
        // enough for any kernel fill; `self.ep` is a valid epoll fd.
        let n =
            unsafe { libc::epoll_wait(self.ep, buf.as_mut_ptr(), buf.len() as i32, timeout_ms) };
        if n == -1 {
            if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                return Ok(());
            }
            return Err(last_error());
        }
        for raw in buf.iter().take(n as usize) {
            if raw.u64 == WAKER_TOKEN {
                // Drain the eventfd so it does not report again (it is
                // edge-free and level-triggered would spin forever).
                let mut count = 0u64;
                // SAFETY: `count` is a live `u64` written by `read`; the
                // eventfd has non-blocking mode and is drained only by this
                // poller thread.
                unsafe {
                    libc::read(
                        self.waker,
                        &mut count as *mut u64 as *mut libc::c_void,
                        std::mem::size_of::<u64>(),
                    );
                }
                continue;
            }
            events.push(Event {
                token: Token::from_value(raw.u64),
                ready: ready_from_epoll(raw.events),
            });
        }
        Ok(())
    }

    fn wake(&self) -> io::Result<()> {
        let count = 1u64;
        // SAFETY: `count` is a live `u64`; `self.waker` is the non-blocking
        // eventfd registered at `WAKER_TOKEN`, and the 8-byte write never
        // overflows the fd's internal counter.
        let rc = unsafe {
            libc::write(
                self.waker,
                &count as *const u64 as *const libc::c_void,
                std::mem::size_of::<u64>(),
            )
        };
        if rc == -1 {
            return Err(last_error());
        }
        Ok(())
    }
}
