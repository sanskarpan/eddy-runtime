//! Unix-domain stream, listener, and datagram sockets.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::{SocketAddr, UnixDatagram as StdUnixDatagram};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::net::{retry_eintr, socket_error, would_block};
use super::poll_evented::PollEvented;
use super::{AsyncRead, AsyncWrite, Interest, ReadBuf};
use crate::runtime::Handle;

fn driver() -> io::Result<Arc<super::driver::DriverShared>> {
    Handle::current().io_driver().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "eddy: Unix sockets require a multi-thread runtime",
        )
    })
}

fn unix_sockaddr(path: &Path) -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
    let bytes = path.as_os_str().as_bytes();
    // SAFETY: zeroed sockaddr_un is a valid initial state before fields are set.
    let mut address = unsafe { std::mem::zeroed::<libc::sockaddr_un>() };
    if bytes.len() >= address.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "eddy: Unix socket path is too long",
        ));
    }
    if bytes.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "eddy: Unix socket path contains NUL",
        ));
    }
    address.sun_family = libc::AF_UNIX as _;
    // SAFETY: the destination is the zeroed sun_path array and the source is
    // a live byte slice whose length was checked above.
    unsafe {
        std::ptr::copy_nonoverlapping(
            bytes.as_ptr(),
            address.sun_path.as_mut_ptr().cast::<u8>(),
            bytes.len(),
        );
    }
    Ok((
        address,
        std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
    ))
}

fn owned_fd(fd: RawFd) -> OwnedFd {
    // SAFETY: fd is returned by a successful accept syscall and ownership is
    // transferred exactly once.
    unsafe { OwnedFd::from_raw_fd(fd) }
}

fn accepted(fd: RawFd) -> io::Result<OwnedFd> {
    retry_eintr(|| {
        // SAFETY: fd is a valid nonblocking Unix listener.
        let accepted = unsafe { libc::accept(fd, std::ptr::null_mut(), std::ptr::null_mut()) };
        if accepted == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(owned_fd(accepted))
        }
    })
}

pub struct UnixListener {
    inner: PollEvented<std::os::unix::net::UnixListener>,
}

impl UnixListener {
    pub fn bind(path: impl AsRef<Path>) -> io::Result<UnixListener> {
        let listener = std::os::unix::net::UnixListener::bind(path)?;
        Ok(UnixListener {
            inner: PollEvented::new(listener, Interest::READABLE)?,
        })
    }

    pub fn from_std(listener: std::os::unix::net::UnixListener) -> io::Result<UnixListener> {
        Ok(UnixListener {
            inner: PollEvented::new(listener, Interest::READABLE)?,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.get_ref().local_addr()
    }

    pub async fn accept(&self) -> io::Result<(UnixStream, SocketAddr)> {
        loop {
            let event = self
                .inner
                .registration()
                .readiness(Interest::READABLE)
                .await;
            let result = self
                .inner
                .registration()
                .try_io_with(Interest::READABLE, || accepted(self.as_raw_fd()));
            match result {
                Ok(fd) => {
                    // SAFETY: the accepted fd is owned and converted exactly once.
                    let stream =
                        unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd.into_raw_fd()) };
                    let peer = stream.peer_addr()?;
                    let stream = UnixStream::from_std(stream)?;
                    event.clear_ready();
                    return Ok((stream, peer));
                }
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => return Err(error),
            }
        }
    }
}

impl AsRawFd for UnixListener {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

pub struct UnixStream {
    inner: PollEvented<std::os::unix::net::UnixStream>,
}

impl UnixStream {
    pub fn from_std(stream: std::os::unix::net::UnixStream) -> io::Result<UnixStream> {
        Ok(UnixStream {
            inner: PollEvented::new(stream, Interest::READABLE.add(Interest::WRITABLE))?,
        })
    }

    pub async fn connect(path: impl AsRef<Path>) -> io::Result<UnixStream> {
        let path = path.as_ref();
        let (address, length) = unix_sockaddr(path)?;
        let fd = retry_eintr(|| {
            // SAFETY: socket creates a new Unix stream descriptor.
            let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
            if fd == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(owned_fd(fd))
            }
        })?;
        crate::sys::set_nonblocking(fd.as_raw_fd())?;
        let driver = driver()?;
        // SAFETY: the owned fd is transferred exactly once into UnixStream.
        let stream = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd.into_raw_fd()) };
        let inner =
            PollEvented::with_driver(stream, driver, Interest::READABLE.add(Interest::WRITABLE))?;
        let result = retry_eintr(|| {
            // SAFETY: address is a live sockaddr_un and the descriptor is valid.
            let result = unsafe {
                libc::connect(
                    inner.as_raw_fd(),
                    &address as *const _ as *const libc::sockaddr,
                    length,
                )
            };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
        match result {
            Ok(()) => Ok(UnixStream { inner }),
            Err(error)
                if matches!(
                    error.raw_os_error(),
                    Some(libc::EINPROGRESS) | Some(libc::EALREADY) | Some(libc::EWOULDBLOCK)
                ) =>
            {
                let event = inner.registration().readiness(Interest::WRITABLE).await;
                event.clear_ready();
                socket_error(inner.as_raw_fd())?;
                Ok(UnixStream { inner })
            }
            Err(error) => Err(error),
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.get_ref().local_addr()
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.get_ref().peer_addr()
    }
}

impl AsRawFd for UnixStream {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl AsyncRead for UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // SAFETY: `inner` is structurally pinned with `self` and is never moved
        // independently of its containing UnixStream.
        unsafe { self.map_unchecked_mut(|stream| &mut stream.inner) }.poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // SAFETY: see the corresponding AsyncRead implementation.
        unsafe { self.map_unchecked_mut(|stream| &mut stream.inner) }.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // SAFETY: see the corresponding AsyncRead implementation.
        unsafe { self.map_unchecked_mut(|stream| &mut stream.inner) }.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // SAFETY: see the corresponding AsyncRead implementation.
        unsafe { self.map_unchecked_mut(|stream| &mut stream.inner) }.poll_shutdown(cx)
    }
}

pub struct UnixDatagram {
    inner: PollEvented<StdUnixDatagram>,
}

impl UnixDatagram {
    pub fn bind(path: impl AsRef<Path>) -> io::Result<UnixDatagram> {
        Ok(UnixDatagram {
            inner: PollEvented::new(
                StdUnixDatagram::bind(path)?,
                Interest::READABLE.add(Interest::WRITABLE),
            )?,
        })
    }

    pub fn unbound() -> io::Result<UnixDatagram> {
        Ok(UnixDatagram {
            inner: PollEvented::new(
                StdUnixDatagram::unbound()?,
                Interest::READABLE.add(Interest::WRITABLE),
            )?,
        })
    }

    pub fn from_std(socket: StdUnixDatagram) -> io::Result<UnixDatagram> {
        Ok(UnixDatagram {
            inner: PollEvented::new(socket, Interest::READABLE.add(Interest::WRITABLE))?,
        })
    }

    pub fn connect(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.inner.get_ref().connect(path)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.get_ref().local_addr()
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            let event = self
                .inner
                .registration()
                .readiness(Interest::READABLE)
                .await;
            let result = self
                .inner
                .registration()
                .try_io_with(Interest::READABLE, || self.inner.get_ref().recv_from(buf));
            match result {
                Ok(value) => return Ok(value),
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => return Err(error),
            }
        }
    }

    pub async fn send_to(&self, buf: &[u8], path: impl AsRef<Path>) -> io::Result<usize> {
        loop {
            let event = self
                .inner
                .registration()
                .readiness(Interest::WRITABLE)
                .await;
            let result = self
                .inner
                .registration()
                .try_io_with(Interest::WRITABLE, || {
                    self.inner.get_ref().send_to(buf, &path)
                });
            match result {
                Ok(value) => return Ok(value),
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => return Err(error),
            }
        }
    }

    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let event = self
                .inner
                .registration()
                .readiness(Interest::READABLE)
                .await;
            let result = self
                .inner
                .registration()
                .try_io_with(Interest::READABLE, || self.inner.get_ref().recv(buf));
            match result {
                Ok(value) => return Ok(value),
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => return Err(error),
            }
        }
    }

    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let event = self
                .inner
                .registration()
                .readiness(Interest::WRITABLE)
                .await;
            let result = self
                .inner
                .registration()
                .try_io_with(Interest::WRITABLE, || self.inner.get_ref().send(buf));
            match result {
                Ok(value) => return Ok(value),
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => return Err(error),
            }
        }
    }
}

impl AsRawFd for UnixDatagram {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

impl AsyncRead for UnixDatagram {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // SAFETY: `inner` is structurally pinned with `self`.
        unsafe { self.map_unchecked_mut(|socket| &mut socket.inner) }.poll_read(cx, buf)
    }
}

impl AsyncWrite for UnixDatagram {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // SAFETY: `inner` is structurally pinned with `self`.
        unsafe { self.map_unchecked_mut(|socket| &mut socket.inner) }.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // SAFETY: `inner` is structurally pinned with `self`.
        unsafe { self.map_unchecked_mut(|socket| &mut socket.inner) }.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // L5: an unconnected datagram socket reports `ENOTCONN` for
        // `shutdown(2)`; there is no write half to shut down, so treat that
        // as a successful no-op rather than surfacing a confusing error.
        // SAFETY: `inner` is structurally pinned with `self`.
        match unsafe { self.map_unchecked_mut(|socket| &mut socket.inner) }.poll_shutdown(cx) {
            Poll::Ready(Err(error)) if error.raw_os_error() == Some(libc::ENOTCONN) => {
                Poll::Ready(Ok(()))
            }
            poll => poll,
        }
    }
}
