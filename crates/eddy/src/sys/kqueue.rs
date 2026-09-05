//! macOS/BSD kqueue readiness backend.
//!
//! Level-triggered by default (no `EV_CLEAR`): an active filter re-reports on
//! every wait until the condition clears, so a missed drain cannot wedge a
//! socket permanently. kqueue reports read and write readiness as separate
//! events; `wait` merges events for the same token into a single `Ready` so
//! the driver wakes a socket's waiters at most once per wait.

use std::io;
use std::os::fd::RawFd;
use std::time::Duration;

use super::{Event, Interest, Poller, Ready, Token};

/// Ident of the `EVFILT_USER` waker event. It lives in the EVFILT_USER
/// namespace, which is separate from the fd idents of EVFILT_READ/WRITE, so
/// it can never collide with a registered socket.
const WAKER_IDENT: libc::uintptr_t = 1;

/// Maximum events processed per `wait` call. The kernel keeps anything left
/// over ready for the next call (level-triggered).
const MAX_EVENTS: usize = 1024;

pub(crate) struct Kqueue {
    kq: RawFd,
}

impl Drop for Kqueue {
    fn drop(&mut self) {
        // SAFETY: `kq` is a valid descriptor owned exclusively by `self`,
        // created in `Kqueue::new`, and close does not require it to be open
        // (the call may return EBADF harmlessly).
        unsafe { libc::close(self.kq) };
    }
}

fn kevent_error() -> io::Error {
    io::Error::last_os_error()
}

fn build_kevent(filter: i16, fd: RawFd, token: Token, interest: Interest) -> libc::kevent {
    let mut flags = libc::EV_ADD | libc::EV_ENABLE;
    if interest.is_edge_triggered() {
        // EV_ONESHOT pairs with Registration's rearm-after-syscall protocol.
        flags |= libc::EV_CLEAR | libc::EV_ONESHOT;
    }
    libc::kevent {
        ident: fd as libc::uintptr_t,
        filter,
        flags,
        fflags: 0,
        data: 0,
        udata: token.value as *mut libc::c_void,
    }
}

impl Kqueue {
    /// Submit a changelist, discarding any immediately-active events. With a
    /// zero-length eventlist the kernel still counts matches, but the events
    /// are re-reported on the next wait because the filters are level-
    /// triggered — nothing is lost.
    fn change(&self, changes: &[libc::kevent]) -> io::Result<()> {
        // SAFETY: `self.kq` is a valid kqueue descriptor and `changes`
        // points to `changes.len()` initialized kevent structs; the zero-
        // length eventlist makes the final pointer parameters unused.
        let n = unsafe {
            libc::kevent(
                self.kq,
                changes.as_ptr(),
                changes.len() as libc::c_int,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if n < 0 {
            return Err(kevent_error());
        }
        Ok(())
    }
}

impl Poller for Kqueue {
    fn new() -> io::Result<Kqueue> {
        // SAFETY: `kqueue()` has no preconditions; the raw descriptor is
        // immediately owned by the `Kqueue` value.
        let kq = unsafe { libc::kqueue() };
        if kq < 0 {
            return Err(kevent_error());
        }
        let kqueue = Kqueue { kq };
        let waker = libc::kevent {
            ident: WAKER_IDENT,
            filter: libc::EVFILT_USER,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        kqueue.change(&[waker])?;
        Ok(kqueue)
    }

    fn register(&self, fd: RawFd, token: Token, interest: Interest) -> io::Result<()> {
        let mut changes = Vec::with_capacity(2);
        if interest.is_readable() {
            changes.push(build_kevent(libc::EVFILT_READ, fd, token, interest));
        }
        if interest.is_writable() {
            changes.push(build_kevent(libc::EVFILT_WRITE, fd, token, interest));
        }
        self.change(&changes)
    }

    fn reregister(&self, fd: RawFd, token: Token, interest: Interest) -> io::Result<()> {
        // macOS EV_ADD does not update `udata` for an existing registration,
        // so a re-registration must first remove the old entries. Deleting
        // and re-adding in one changelist is atomic from the kernel's
        // perspective.
        self.deregister(fd)?;
        self.register(fd, token, interest)
    }

    fn deregister(&self, fd: RawFd) -> io::Result<()> {
        let changes = [
            libc::kevent {
                ident: fd as libc::uintptr_t,
                filter: libc::EVFILT_READ,
                flags: libc::EV_DELETE,
                fflags: 0,
                data: 0,
                udata: std::ptr::null_mut(),
            },
            libc::kevent {
                ident: fd as libc::uintptr_t,
                filter: libc::EVFILT_WRITE,
                flags: libc::EV_DELETE,
                fflags: 0,
                data: 0,
                udata: std::ptr::null_mut(),
            },
        ];
        // EV_DELETE of a filter that was never registered returns ENOENT;
        // that is not an error worth surfacing to the caller.
        // SAFETY: `self.kq` is valid and `changes` is a live kevent array;
        // with a zero-length eventlist the output pointers are unused.
        let n = unsafe {
            libc::kevent(
                self.kq,
                changes.as_ptr(),
                changes.len() as libc::c_int,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::NotFound {
                return Err(err);
            }
        }
        Ok(())
    }

    fn wait(&self, events: &mut Vec<Event>, timeout: Option<Duration>) -> io::Result<()> {
        let mut buf = [libc::kevent {
            ident: 0,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        }; MAX_EVENTS];

        let timespec = timeout.map(|duration| {
            let secs = duration.as_secs().min(libc::time_t::MAX as u64) as libc::time_t;
            let nsecs = duration.subsec_nanos().min(999_999_999) as i64;
            libc::timespec {
                tv_sec: secs,
                tv_nsec: nsecs,
            }
        });

        // SAFETY: `self.kq` is valid, `buf` is a live array of MAX_EVENTS
        // initialized kevent structs the kernel may write, and the changelist
        // pointers are null because no changes are submitted.
        let n = unsafe {
            libc::kevent(
                self.kq,
                std::ptr::null(),
                0,
                buf.as_mut_ptr(),
                buf.len() as libc::c_int,
                timespec
                    .as_ref()
                    .map_or(std::ptr::null(), |ts| ts as *const _),
            )
        };
        if n < 0 {
            return Err(kevent_error());
        }

        for raw in buf.iter().take(n as usize) {
            if raw.ident == WAKER_IDENT && raw.filter == libc::EVFILT_USER {
                continue;
            }
            let filter = raw.filter;
            let flags = raw.flags;
            let udata = raw.udata as u64;
            let ready = match filter {
                libc::EVFILT_READ => {
                    let mut r = Ready::READABLE;
                    if flags & libc::EV_EOF != 0 {
                        r = r.union(Ready::READ_CLOSED);
                    }
                    r
                }
                libc::EVFILT_WRITE => {
                    let mut r = Ready::WRITABLE;
                    if flags & libc::EV_EOF != 0 {
                        r = r.union(Ready::WRITE_CLOSED);
                    }
                    r
                }
                _ => continue,
            };
            let token = Token::from_value(udata);
            if let Some(last) = events.last_mut() {
                if last.token == token {
                    // Same fd reported in both filters this wait: merge so
                    // the driver wakes its waiters once with a combined
                    // readiness.
                    last.ready = last.ready.union(ready);
                    continue;
                }
            }
            events.push(Event { token, ready });
        }
        Ok(())
    }

    fn wake(&self) -> io::Result<()> {
        let trigger = libc::kevent {
            ident: WAKER_IDENT,
            filter: libc::EVFILT_USER,
            flags: 0,
            fflags: libc::NOTE_TRIGGER,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        self.change(&[trigger])
    }
}
