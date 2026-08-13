//! Windows IOCP backend.
//!
//! IOCP is completion-based, not readiness-based: a `CreateIoCompletionPort`
//! plus `GetQueuedCompletionStatusEx` loop delivers *completion* packets, so
//! readiness must be emulated — the approach of legacy mio — by keeping one
//! **zero-byte `WSARecv` probe** outstanding per socket registered for reads.
//! A probe completes only when the socket becomes readable or is closed, and
//! its completion packet is mapped back to a readiness report for the driver.
//! Level-triggered semantics come from re-issuing the probe after each
//! completion, exactly like a level-triggered `epoll_wait` re-reports a ready
//! fd.
//!
//! Two Windows quirks shape the implementation:
//!
//! - A zero-byte receive that finds data already buffered completes
//!   **synchronously** (`WSARecv` returns 0) and queues *no* completion
//!   packet, so those probes are reported immediately through the pending
//!   event list instead of the completion queue.
//! - There is no completion-based write-readiness probe (a zero-byte
//!   `WSASend` finishes instantly). Writes are therefore approximated: a
//!   registration reports `WRITABLE` once immediately, and every later read
//!   probe completion re-reports both `READABLE` and `WRITABLE`, so a parked
//!   writer wakes whenever the driver hears from the socket. On Windows a
//!   connected socket is writable most of the time, which keeps this
//!   approximation functional for typical request/response flows.
//!
//! Wakes (`Poller::wake`) are posted through `PostQueuedCompletionStatus`
//! with a null `OVERLAPPED`, which no real completion ever carries.
//!
//! This backend is verified by cross-compilation only (`cargo check/clippy
//! --target x86_64-pc-windows-msvc`); it has not been executed on Windows.

use std::collections::HashMap;
use std::io;
use std::mem::MaybeUninit;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_OPERATION_ABORTED, ERROR_TIMEOUT, FALSE, HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Networking::WinSock::{
    WSAGetLastError, WSARecv, SOCKET, SOCKET_ERROR, WSABUF, WSA_IO_PENDING,
};
use windows_sys::Win32::System::IO::{
    CancelIoEx, CreateIoCompletionPort, GetQueuedCompletionStatusEx, PostQueuedCompletionStatus,
    OVERLAPPED, OVERLAPPED_ENTRY,
};

use super::{Event, Fd, Interest, Poller, Ready, Token};

/// Maximum completions drained per `GetQueuedCompletionStatusEx` call;
/// level-triggered emulation keeps anything left over for the next call.
const MAX_EVENTS: usize = 1024;

/// The zero-length buffer a probe receives into. The buffer's `len` field is
/// what makes the receive a probe: it completes only when the socket has
/// data or is closed, and consumes nothing.
fn zero_buf() -> WSABUF {
    WSABUF {
        len: 0,
        buf: std::ptr::null_mut(),
    }
}

/// The probe state of one registered socket.
///
/// `overlapped` must stay at offset zero and the `Arc` must keep the whole
/// struct pinned while the probe is outstanding: the completion handler
/// recovers the probe from the `lpOverlapped` pointer the kernel returns.
/// `transferred` and `flags` are the live `WSARecv` result slots, which the
/// Winsock docs require to remain valid until the operation completes.
#[repr(C)]
struct Probe {
    overlapped: OVERLAPPED,
    transferred: u32,
    flags: u32,
    fd: Fd,
}

impl Probe {
    fn new(fd: Fd) -> Probe {
        Probe {
            // SAFETY: `OVERLAPPED` is a `Copy` repr(C) struct; a zeroed
            // instance is a valid, unused overlapped structure.
            overlapped: unsafe { std::mem::zeroed() },
            transferred: 0,
            flags: 0,
            fd,
        }
    }
}

pub(crate) struct Iocp {
    port: HANDLE,
    /// Active probes keyed by the address of their `OVERLAPPED`, which is
    /// what the completion handler looks them up with. Removing the entry
    /// drops the probe (and frees the overlapped) only after its completion
    /// was consumed.
    probes: Mutex<HashMap<usize, Arc<Probe>>>,
    /// The current probe of each registered socket, so a stale cancelled
    /// probe is never re-armed in place of a fresh one.
    active: Mutex<HashMap<Fd, usize>>,
    /// Completion keys back to sockets: the driver's token carries the slab
    /// index, not the fd, and re-arming a probe needs the fd.
    fds: Mutex<HashMap<u64, Fd>>,
    /// Readiness reports that need no kernel round trip: synchronous probe
    /// completions and write-ready approximations.
    pending: Mutex<Vec<Event>>,
}

// SAFETY: the port handle and probe state are only touched behind the mutexes
// or by the kernel; the ownership invariants are exactly those of a shared
// poller (cf. the `Send`/`Sync` impls of the epoll backend).
unsafe impl Send for Iocp {}
// SAFETY: see `Send`; all interior mutability is mutex- or kernel-guarded.
unsafe impl Sync for Iocp {}

impl Drop for Iocp {
    fn drop(&mut self) {
        let probes = self.probes.lock().unwrap();
        for probe in probes.values() {
            // SAFETY: each fd was a registered socket at probe time;
            // cancelling an operation with its still-live overlapped is
            // always valid.
            unsafe { CancelIoEx(probe.fd as HANDLE, probe_overlapped(probe)) };
        }
        // SAFETY: the port handle is owned by this poller.
        unsafe { CloseHandle(self.port) };
    }
}

fn last_error() -> io::Error {
    io::Error::last_os_error()
}

fn probe_overlapped(probe: &Probe) -> *mut OVERLAPPED {
    // `overlapped` is the first field of `#[repr(C)] Probe`, and the caller
    // provides a live probe, so the address is valid and correctly typed.
    &probe.overlapped as *const OVERLAPPED as *mut OVERLAPPED
}

/// Whether `err` is the "operation aborted" code of a `CancelIoEx`.
fn aborted(err: u32) -> bool {
    err == ERROR_OPERATION_ABORTED
}

/// The readiness reported for a completed zero-byte receive probe. `WRITABLE`
/// rides along as the write-readiness approximation (see module docs).
fn ready_from_probe(transferred: u32, err: u32) -> Ready {
    let mut ready = Ready::READABLE.union(Ready::WRITABLE);
    if transferred == 0 {
        ready = ready.union(Ready::READ_CLOSED);
    }
    if err != 0 {
        ready = ready.union(Ready::ERROR);
    }
    ready
}

impl Iocp {
    /// Issue a zero-byte `WSARecv` probe into `probe`. Returns `true` when
    /// the operation is still pending (a completion packet will arrive), and
    /// `false` when it finished synchronously (no packet will arrive; the
    /// readiness must be reported through `pending`).
    fn issue_probe(&self, fd: Fd, probe: &Arc<Probe>) -> io::Result<bool> {
        let mut buf = zero_buf();
        let slots = Arc::as_ptr(probe).cast_mut();
        // SAFETY: `fd` is the caller's socket (kept open for the registration
        // lifetime), `buf` is a live zero-length buffer, `transferred` and
        // `flags` live inside the pinned probe, and `overlapped` points into
        // the same pinned probe for the duration of the operation.
        let rc = unsafe {
            WSARecv(
                fd as SOCKET,
                &mut buf,
                1,
                std::ptr::addr_of_mut!((*slots).transferred),
                std::ptr::addr_of_mut!((*slots).flags),
                probe_overlapped(probe),
                None,
            )
        };
        if rc == 0 {
            return Ok(false);
        }
        if rc != SOCKET_ERROR {
            return Ok(true);
        }
        // SAFETY: `WSAGetLastError` takes no arguments and returns the
        // calling thread's last socket error.
        let err = unsafe { WSAGetLastError() };
        if err == WSA_IO_PENDING {
            Ok(true)
        } else {
            Err(io::Error::from_raw_os_error(err as i32))
        }
    }

    /// Consume one completion and translate it into a readiness event,
    /// re-arming the probe for level-triggered semantics when the socket is
    /// still registered with read interest.
    fn handle_completion(&self, token: Token, probe_addr: usize, transferred: u32, err: u32) {
        let Some(probe) = self.probes.lock().unwrap().remove(&probe_addr) else {
            // A sentinel or a completion this poller no longer tracks.
            return;
        };
        let Some(fd) = self.fds.lock().unwrap().get(&token.value).copied() else {
            return;
        };
        let active = self.active.lock().unwrap();
        let still_active = active.get(&fd) == Some(&probe_addr);
        drop(active);
        if !still_active || aborted(err) {
            // The registration was cancelled or superseded while the probe
            // was in flight; drop the probe and never re-arm it.
            return;
        }
        self.pending.lock().unwrap().push(Event {
            token,
            ready: ready_from_probe(transferred, err),
        });
        self.probes
            .lock()
            .unwrap()
            .insert(probe_addr, probe.clone());
        let mut buf = zero_buf();
        let slots = Arc::as_ptr(&probe).cast_mut();
        // SAFETY: the fd is still registered (checked above) and the probe
        // was just re-inserted, so re-issuing is safe.
        let rc = unsafe {
            WSARecv(
                fd as SOCKET,
                &mut buf,
                1,
                std::ptr::addr_of_mut!((*slots).transferred),
                std::ptr::addr_of_mut!((*slots).flags),
                probe_overlapped(&probe),
                None,
            )
        };
        if rc == 0 {
            // Data is buffered again right away: report it now; the next
            // `wait` drains it before touching the kernel.
            self.pending.lock().unwrap().push(Event {
                token,
                ready: ready_from_probe(probe.transferred, 0),
            });
        } else if rc == SOCKET_ERROR
            // SAFETY: `WSAGetLastError` takes no arguments and returns the
            // calling thread's last socket error.
            && unsafe { WSAGetLastError() } != WSA_IO_PENDING
        {
            // The socket went away mid-flight; drop the probe.
            self.probes.lock().unwrap().remove(&probe_addr);
        }
    }
}

impl Poller for Iocp {
    fn new() -> io::Result<Iocp> {
        // SAFETY: the completion port is created without an associated file
        // or key; the returned handle is owned by this poller.
        let port = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, 0 as HANDLE, 0, 0) };
        if port.is_null() {
            return Err(last_error());
        }
        Ok(Iocp {
            port,
            probes: Mutex::new(HashMap::new()),
            active: Mutex::new(HashMap::new()),
            fds: Mutex::new(HashMap::new()),
            pending: Mutex::new(Vec::new()),
        })
    }

    fn register(&self, fd: Fd, token: Token, interest: Interest) -> io::Result<()> {
        // Associate the socket with the port; the token rides as the
        // completion key, so the driver can map completions back to
        // registrations without an extra lookup.
        // SAFETY: `fd` is the caller's socket handle and the port is owned by
        // this poller; `token.value` is a user-chosen completion key.
        let port =
            unsafe { CreateIoCompletionPort(fd as HANDLE, self.port, token.value as usize, 0) };
        if port.is_null() {
            return Err(last_error());
        }

        let probe = Arc::new(Probe::new(fd));
        let addr = probe_overlapped(&probe) as usize;
        self.probes.lock().unwrap().insert(addr, probe.clone());
        self.active.lock().unwrap().insert(fd, addr);
        self.fds.lock().unwrap().insert(token.value, fd);

        if interest.is_readable() {
            if !self.issue_probe(fd, &probe)? {
                // Synchronous completion: data was already buffered, no
                // completion packet is coming — report readiness directly.
                self.pending.lock().unwrap().push(Event {
                    token,
                    ready: ready_from_probe(probe.transferred, 0),
                });
            }
        } else {
            // Write-only registration: report the write approximation now;
            // later read probes keep re-reporting it (see module docs).
            self.pending.lock().unwrap().push(Event {
                token,
                ready: Ready::WRITABLE,
            });
        }
        Ok(())
    }

    fn reregister(&self, fd: Fd, token: Token, interest: Interest) -> io::Result<()> {
        self.fds.lock().unwrap().insert(token.value, fd);
        let probe_addr = self.active.lock().unwrap().get(&fd).copied().unwrap_or(0);
        if interest.is_readable() && probe_addr == 0 {
            let probe = Arc::new(Probe::new(fd));
            let addr = probe_overlapped(&probe) as usize;
            self.probes.lock().unwrap().insert(addr, probe.clone());
            self.active.lock().unwrap().insert(fd, addr);
            if !self.issue_probe(fd, &probe)? {
                self.pending.lock().unwrap().push(Event {
                    token,
                    ready: ready_from_probe(probe.transferred, 0),
                });
            }
        } else if !interest.is_readable() && probe_addr != 0 {
            // The probe's completion (or cancellation) clears it; a stale
            // completion is never re-armed because `active` no longer points
            // at it.
            let probe = self.probes.lock().unwrap().get(&probe_addr).cloned();
            self.active.lock().unwrap().remove(&fd);
            if let Some(probe) = probe {
                // SAFETY: `fd` is still open (the driver deregisters before
                // closing) and the probe's overlapped is live.
                unsafe { CancelIoEx(fd as HANDLE, probe_overlapped(&probe)) };
            }
        }
        Ok(())
    }

    fn deregister(&self, fd: Fd) -> io::Result<()> {
        let probe_addr = self.active.lock().unwrap().remove(&fd);
        self.fds
            .lock()
            .unwrap()
            .retain(|_, registered| *registered != fd);
        if let Some(addr) = probe_addr {
            if let Some(probe) = self.probes.lock().unwrap().remove(&addr) {
                // SAFETY: `fd` is still open (the driver deregisters before
                // closing); cancelling aborts the outstanding probe, whose
                // completion later drops the probe.
                unsafe { CancelIoEx(fd as HANDLE, probe_overlapped(&probe)) };
            }
        }
        Ok(())
    }

    fn wait(&self, events: &mut Vec<Event>, timeout: Option<Duration>) -> io::Result<()> {
        events.append(&mut std::mem::take(&mut *self.pending.lock().unwrap()));

        let timeout_ms = timeout
            .map(|duration| duration.as_millis().min(u32::MAX as u128) as u32)
            .unwrap_or(u32::MAX);
        let mut entries = MaybeUninit::<[OVERLAPPED_ENTRY; MAX_EVENTS]>::uninit();
        let mut count: u32 = 0;
        // SAFETY: `entries` is a live array the kernel fills, `count` a live
        // output slot, and the port is owned by this poller.
        let ok = unsafe {
            GetQueuedCompletionStatusEx(
                self.port,
                entries.as_mut_ptr() as *mut OVERLAPPED_ENTRY,
                MAX_EVENTS as u32,
                &mut count,
                timeout_ms,
                FALSE,
            )
        };
        if ok == 0 {
            let err = last_error();
            if err.raw_os_error() == Some(ERROR_TIMEOUT as i32) {
                // The wait timed out; nothing was queued.
                return Ok(());
            }
            return Err(err);
        }
        // SAFETY: the kernel wrote exactly `count` entries into the buffer.
        let entries = unsafe { entries.assume_init() };
        for entry in entries.iter().take(count as usize) {
            if entry.lpOverlapped.is_null() {
                // The wake sentinel posted by `Poller::wake`.
                continue;
            }
            let token = Token::from_value(entry.lpCompletionKey as u64);
            // SAFETY: the completion belongs to a live probe; the kernel
            // stored the completion's error status in the overlapped's
            // `Internal` field.
            let err = unsafe { (*entry.lpOverlapped).Internal } as u32 & 0xFFFF_FFFF;
            self.handle_completion(
                token,
                entry.lpOverlapped as usize,
                entry.dwNumberOfBytesTransferred,
                err,
            );
        }
        events.append(&mut std::mem::take(&mut *self.pending.lock().unwrap()));
        Ok(())
    }

    fn wake(&self) -> io::Result<()> {
        // SAFETY: the port is owned by this poller; a null overlapped marks
        // the sentinel, which `wait` skips.
        let ok = unsafe { PostQueuedCompletionStatus(self.port, 0, 0, 0 as *mut OVERLAPPED) };
        if ok == 0 {
            return Err(last_error());
        }
        Ok(())
    }
}
