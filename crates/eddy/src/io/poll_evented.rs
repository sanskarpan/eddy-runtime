//! Generic readiness wrapper for an object exposing a raw file descriptor.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use super::driver::DriverShared;
use super::net::{poll_readiness, recv_into, retry_eintr, send_from, would_block};
use super::{AsyncRead, AsyncWrite, Interest, ReadBuf, Readiness, Registration};

pub struct PollEvented<E> {
    io: E,
    registration: Arc<Registration>,
    read_waiter: Mutex<Option<Readiness>>,
    write_waiter: Mutex<Option<Readiness>>,
}

impl<E: AsRawFd> PollEvented<E> {
    pub fn new(io: E, interests: Interest) -> io::Result<PollEvented<E>> {
        let duplicate = retry_eintr(|| {
            // SAFETY: `io` supplies a live descriptor and dup creates a new
            // owned descriptor without borrowing Rust memory.
            let fd = unsafe { libc::dup(io.as_raw_fd()) };
            if fd == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(fd)
            }
        })?;
        let registration = Registration::new(owned_fd(duplicate), interests)?;
        Ok(PollEvented {
            io,
            registration: Arc::new(registration),
            read_waiter: Mutex::new(None),
            write_waiter: Mutex::new(None),
        })
    }

    pub(crate) fn with_driver(
        io: E,
        driver: Arc<DriverShared>,
        interests: Interest,
    ) -> io::Result<PollEvented<E>> {
        let duplicate = retry_eintr(|| {
            // SAFETY: `io` supplies a live descriptor and dup creates a new
            // owned descriptor without borrowing Rust memory.
            let fd = unsafe { libc::dup(io.as_raw_fd()) };
            if fd == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(fd)
            }
        })?;
        let registration = Registration::with_driver(driver, owned_fd(duplicate), interests)?;
        Ok(PollEvented {
            io,
            registration: Arc::new(registration),
            read_waiter: Mutex::new(None),
            write_waiter: Mutex::new(None),
        })
    }

    pub fn get_ref(&self) -> &E {
        &self.io
    }

    pub fn get_mut(&mut self) -> &mut E {
        &mut self.io
    }

    pub fn into_inner(self) -> E {
        self.io
    }

    pub fn registration(&self) -> &Registration {
        &self.registration
    }

    fn poll_read_inner(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        let mut waiter = self.read_waiter.lock().unwrap();
        loop {
            let event =
                match poll_readiness(&self.registration, &mut waiter, Interest::READABLE, cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(event) => event,
                };
            match self
                .registration
                .try_io_with(Interest::READABLE, || recv_into(self.as_raw_fd(), buf))
            {
                Ok(n) => {
                    *waiter = None;
                    // SAFETY: recv initialized exactly the returned byte count.
                    unsafe { buf.advance(n) };
                    drop(event);
                    return Poll::Ready(Ok(()));
                }
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => {
                    *waiter = None;
                    return Poll::Ready(Err(error));
                }
            }
        }
    }

    fn poll_write_inner(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if crate::coop::poll_proceed(cx).is_pending() {
            return Poll::Pending;
        }
        let mut waiter = self.write_waiter.lock().unwrap();
        loop {
            let event =
                match poll_readiness(&self.registration, &mut waiter, Interest::WRITABLE, cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(event) => event,
                };
            match self
                .registration
                .try_io_with(Interest::WRITABLE, || send_from(self.as_raw_fd(), buf))
            {
                Ok(n) => {
                    *waiter = None;
                    drop(event);
                    return Poll::Ready(Ok(n));
                }
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => {
                    *waiter = None;
                    return Poll::Ready(Err(error));
                }
            }
        }
    }
}

fn owned_fd(fd: RawFd) -> OwnedFd {
    // SAFETY: fd is returned by a successful dup and ownership is transferred.
    unsafe { OwnedFd::from_raw_fd(fd) }
}

impl<E: AsRawFd> AsRawFd for PollEvented<E> {
    fn as_raw_fd(&self) -> RawFd {
        self.io.as_raw_fd()
    }
}

impl<E: AsRawFd + Unpin> AsyncRead for PollEvented<E> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.get_mut().poll_read_inner(cx, buf)
    }
}

impl<E: AsRawFd + Unpin> AsyncWrite for PollEvented<E> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_write_inner(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = retry_eintr(|| {
            // SAFETY: the wrapped object supplies a valid socket descriptor.
            let result = unsafe { libc::shutdown(self.as_raw_fd(), libc::SHUT_WR) };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
        Poll::Ready(result)
    }
}
