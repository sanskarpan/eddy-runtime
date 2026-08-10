//! Readiness-backed TCP and UDP sockets.

use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use super::{AsyncRead, AsyncWrite, Interest, ReadBuf, Readiness, Registration};
use crate::runtime::Handle;
use crate::sys::set_nonblocking;

fn driver() -> io::Result<Arc<super::driver::DriverShared>> {
    Handle::current().io_driver().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "eddy: async sockets require a multi-thread runtime",
        )
    })
}

fn owned_fd(fd: RawFd) -> OwnedFd {
    // SAFETY: the caller only passes a descriptor returned by a successful
    // socket or accept syscall and transfers ownership exactly once.
    unsafe { OwnedFd::from_raw_fd(fd) }
}

fn socket_addr_parts(addr: SocketAddr) -> (libc::c_int, libc::sockaddr_storage, libc::socklen_t) {
    // SAFETY: zeroed storage is a valid initial state for either sockaddr type.
    let mut storage = unsafe { std::mem::zeroed::<libc::sockaddr_storage>() };
    let (family, length) = match addr {
        SocketAddr::V4(addr) => {
            // SAFETY: storage is aligned and large enough for sockaddr_in.
            let sin = unsafe {
                &mut *(&mut storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in)
            };
            sin.sin_family = libc::AF_INET as _;
            sin.sin_port = addr.port().to_be();
            sin.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(addr.ip().octets()),
            };
            #[cfg(any(
                target_os = "macos",
                target_os = "ios",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd"
            ))]
            {
                sin.sin_len = std::mem::size_of::<libc::sockaddr_in>() as u8;
            }
            (
                libc::AF_INET,
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
        SocketAddr::V6(addr) => {
            // SAFETY: storage is aligned and large enough for sockaddr_in6.
            let sin6 = unsafe {
                &mut *(&mut storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in6)
            };
            sin6.sin6_family = libc::AF_INET6 as _;
            sin6.sin6_port = addr.port().to_be();
            sin6.sin6_flowinfo = addr.flowinfo();
            sin6.sin6_addr = libc::in6_addr {
                s6_addr: addr.ip().octets(),
            };
            sin6.sin6_scope_id = addr.scope_id();
            #[cfg(any(
                target_os = "macos",
                target_os = "ios",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd"
            ))]
            {
                sin6.sin6_len = std::mem::size_of::<libc::sockaddr_in6>() as u8;
            }
            (
                libc::AF_INET6,
                std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            )
        }
    };
    (family, storage, length)
}

fn socket_addr_from_storage(storage: &libc::sockaddr_storage) -> io::Result<SocketAddr> {
    // SAFETY: the storage came from a socket address syscall or a constructor
    // in this module and is at least as large as sockaddr.
    let family = unsafe {
        (*(storage as *const libc::sockaddr_storage as *const libc::sockaddr)).sa_family
            as libc::c_int
    };
    match family {
        libc::AF_INET => {
            // SAFETY: the family check proves this storage contains sockaddr_in.
            let sin =
                unsafe { &*(storage as *const libc::sockaddr_storage as *const libc::sockaddr_in) };
            Ok(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(sin.sin_port),
            )))
        }
        libc::AF_INET6 => {
            // SAFETY: the family check proves this storage contains sockaddr_in6.
            let sin6 = unsafe {
                &*(storage as *const libc::sockaddr_storage as *const libc::sockaddr_in6)
            };
            Ok(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(sin6.sin6_addr.s6_addr),
                u16::from_be(sin6.sin6_port),
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "eddy: unsupported socket address family",
        )),
    }
}

fn socket_addr_for_fd(fd: RawFd, peer: bool) -> io::Result<SocketAddr> {
    // SAFETY: zeroed storage is a valid output buffer for getsockname/getpeername.
    let mut storage = unsafe { std::mem::zeroed::<libc::sockaddr_storage>() };
    let mut length = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    retry_eintr(|| {
        // SAFETY: fd is valid and the output pointers refer to live storage.
        let result = unsafe {
            if peer {
                libc::getpeername(
                    fd,
                    &mut storage as *mut _ as *mut libc::sockaddr,
                    &mut length,
                )
            } else {
                libc::getsockname(
                    fd,
                    &mut storage as *mut _ as *mut libc::sockaddr,
                    &mut length,
                )
            }
        };
        if result == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    })?;
    socket_addr_from_storage(&storage)
}

fn make_socket(addr: SocketAddr, kind: libc::c_int) -> io::Result<OwnedFd> {
    let (family, _, _) = socket_addr_parts(addr);
    let fd = retry_eintr(|| {
        // SAFETY: socket has no Rust aliasing preconditions and returns a new fd.
        let fd = unsafe { libc::socket(family, kind, 0) };
        if fd == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(fd)
        }
    })?;
    let fd = owned_fd(fd);
    set_nonblocking(fd.as_raw_fd())?;
    Ok(fd)
}

pub(crate) fn socket_error(fd: RawFd) -> io::Result<()> {
    let mut error = 0;
    let mut length = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    retry_eintr(|| {
        // SAFETY: fd is valid and the output pointers refer to live scalar storage.
        let result = unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ERROR,
                &mut error as *mut _ as *mut libc::c_void,
                &mut length,
            )
        };
        if result == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    })?;
    if error == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(error))
    }
}

pub(crate) fn would_block(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
}

pub(crate) fn retry_eintr<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    loop {
        match operation() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

pub(crate) fn poll_readiness(
    registration: &Registration,
    waiter: &mut Option<Readiness>,
    interest: Interest,
    cx: &mut Context<'_>,
) -> Poll<super::ReadyEvent> {
    if waiter.is_none() {
        *waiter = Some(registration.readiness(interest));
    }
    Pin::new(waiter.as_mut().expect("eddy: readiness waiter missing")).poll(cx)
}

pub(crate) fn recv_into(fd: RawFd, buf: &mut ReadBuf<'_>) -> io::Result<usize> {
    if buf.remaining() == 0 {
        return Ok(0);
    }
    let target = buf.unfilled_mut();
    retry_eintr(|| {
        // SAFETY: fd is valid and target is a live writable buffer.
        let result = unsafe {
            libc::recv(
                fd,
                target.as_mut_ptr().cast::<libc::c_void>(),
                target.len(),
                0,
            )
        };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    })
}

pub(crate) fn send_from(fd: RawFd, buf: &[u8]) -> io::Result<usize> {
    retry_eintr(|| {
        // SAFETY: fd is valid and buf remains alive for the duration of send.
        let result = unsafe { libc::send(fd, buf.as_ptr().cast::<libc::c_void>(), buf.len(), 0) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    })
}

fn accept_tcp(fd: RawFd) -> io::Result<OwnedFd> {
    retry_eintr(|| {
        // SAFETY: fd is a valid listener; the null address pointers discard
        // the peer address because it is queried from the accepted socket.
        let accepted = unsafe {
            #[cfg(target_os = "linux")]
            {
                libc::accept4(
                    fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                )
            }
            #[cfg(not(target_os = "linux"))]
            {
                libc::accept(fd, std::ptr::null_mut(), std::ptr::null_mut())
            }
        };
        if accepted == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(owned_fd(accepted))
        }
    })
}

#[derive(Clone)]
pub struct TcpListener {
    registration: Arc<Registration>,
}

impl TcpListener {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<TcpListener> {
        let listener = std::net::TcpListener::bind(addr)?;
        let fd = owned_fd(listener.into_raw_fd());
        Ok(TcpListener {
            registration: Arc::new(Registration::new(fd, Interest::READABLE)?),
        })
    }

    pub fn from_std(listener: std::net::TcpListener) -> io::Result<TcpListener> {
        Ok(TcpListener {
            registration: Arc::new(Registration::new(
                owned_fd(listener.into_raw_fd()),
                Interest::READABLE,
            )?),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        socket_addr_for_fd(self.registration.as_raw_fd(), false)
    }

    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        loop {
            let event = self.registration.readiness(Interest::READABLE).await;
            let accepted = self.registration.try_io_with(Interest::READABLE, || {
                accept_tcp(self.registration.as_raw_fd())
            });
            match accepted {
                Ok(fd) => {
                    let peer = socket_addr_for_fd(fd.as_raw_fd(), true)?;
                    let stream = TcpStream::from_owned_fd(fd, self.registration.driver())?;
                    event.clear_ready();
                    return Ok((stream, peer));
                }
                Err(error) if would_block(&error) => {
                    event.clear_ready();
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl AsRawFd for TcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.registration.as_raw_fd()
    }
}

pub struct TcpStream {
    registration: Arc<Registration>,
    read_waiter: Mutex<Option<Readiness>>,
    write_waiter: Mutex<Option<Readiness>>,
}

impl TcpStream {
    fn from_owned_fd(
        fd: OwnedFd,
        driver: Arc<super::driver::DriverShared>,
    ) -> io::Result<TcpStream> {
        Ok(TcpStream {
            registration: Arc::new(Registration::with_driver(
                driver,
                fd,
                Interest::READABLE.add(Interest::WRITABLE),
            )?),
            read_waiter: Mutex::new(None),
            write_waiter: Mutex::new(None),
        })
    }

    pub fn from_std(stream: std::net::TcpStream) -> io::Result<TcpStream> {
        let driver = driver()?;
        Self::from_owned_fd(owned_fd(stream.into_raw_fd()), driver)
    }

    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<TcpStream> {
        let addresses: Vec<_> = addr.to_socket_addrs()?.collect();
        let mut last_error = None;
        for address in addresses {
            match Self::connect_one(address).await {
                Ok(stream) => return Ok(stream),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "eddy: no socket addresses resolved",
            )
        }))
    }

    async fn connect_one(address: SocketAddr) -> io::Result<TcpStream> {
        let driver = driver()?;
        let (_, storage, length) = socket_addr_parts(address);
        let registration = Arc::new(Registration::with_driver(
            driver,
            make_socket(address, libc::SOCK_STREAM)?,
            Interest::READABLE.add(Interest::WRITABLE),
        )?);
        let result = retry_eintr(|| {
            // SAFETY: storage is a live sockaddr and the registration owns a
            // valid nonblocking socket descriptor.
            let result = unsafe {
                libc::connect(
                    registration.as_raw_fd(),
                    &storage as *const _ as *const libc::sockaddr,
                    length,
                )
            };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
        if result.is_ok() {
            return Ok(TcpStream {
                registration,
                read_waiter: Mutex::new(None),
                write_waiter: Mutex::new(None),
            });
        }
        let error = result.unwrap_err();
        let in_progress = matches!(
            error.raw_os_error(),
            Some(libc::EINPROGRESS) | Some(libc::EALREADY) | Some(libc::EWOULDBLOCK)
        );
        if !in_progress {
            return Err(error);
        }
        let event = registration.readiness(Interest::WRITABLE).await;
        event.clear_ready();
        socket_error(registration.as_raw_fd())?;
        Ok(TcpStream {
            registration,
            read_waiter: Mutex::new(None),
            write_waiter: Mutex::new(None),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        socket_addr_for_fd(self.registration.as_raw_fd(), false)
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        socket_addr_for_fd(self.registration.as_raw_fd(), true)
    }

    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        let value = i32::from(nodelay);
        retry_eintr(|| {
            // SAFETY: the socket and option value are valid for setsockopt.
            let result = unsafe {
                libc::setsockopt(
                    self.registration.as_raw_fd(),
                    libc::IPPROTO_TCP,
                    libc::TCP_NODELAY,
                    &value as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&value) as libc::socklen_t,
                )
            };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
    }

    pub fn set_linger(&self, linger: Option<std::time::Duration>) -> io::Result<()> {
        let value = libc::linger {
            l_onoff: i32::from(linger.is_some()),
            l_linger: linger
                .map(|duration| duration.as_secs().min(i32::MAX as u64) as i32)
                .unwrap_or(0),
        };
        retry_eintr(|| {
            // SAFETY: the socket and linger value are valid for setsockopt.
            let result = unsafe {
                libc::setsockopt(
                    self.registration.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_LINGER,
                    &value as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&value) as libc::socklen_t,
                )
            };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
    }

    /// Return bytes without consuming them from the receive queue.
    ///
    /// This operation is cancel safe: dropping the future never removes data
    /// from the socket.
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let event = self.registration.readiness(Interest::READABLE).await;
            let result = self.registration.try_io_with(Interest::READABLE, || {
                retry_eintr(|| {
                    // SAFETY: fd is valid and buf is a live writable buffer.
                    let n = unsafe {
                        libc::recv(
                            self.as_raw_fd(),
                            buf.as_mut_ptr().cast::<libc::c_void>(),
                            buf.len(),
                            libc::MSG_PEEK,
                        )
                    };
                    if n < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(n as usize)
                    }
                })
            });
            match result {
                Ok(n) => return Ok(n),
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => return Err(error),
            }
        }
    }

    pub fn split(&self) -> (TcpReadHalf<'_>, TcpWriteHalf<'_>) {
        (TcpReadHalf { stream: self }, TcpWriteHalf { stream: self })
    }

    pub fn into_split(self) -> (OwnedTcpReadHalf, OwnedTcpWriteHalf) {
        let registration = self.registration;
        (
            OwnedTcpReadHalf {
                registration: registration.clone(),
                waiter: Mutex::new(None),
            },
            OwnedTcpWriteHalf {
                registration,
                waiter: Mutex::new(None),
            },
        )
    }

    fn poll_read_inner(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
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
                Err(error) if would_block(&error) => {
                    event.clear_ready();
                }
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
                Err(error) if would_block(&error) => {
                    event.clear_ready();
                }
                Err(error) => {
                    *waiter = None;
                    return Poll::Ready(Err(error));
                }
            }
        }
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> RawFd {
        self.registration.as_raw_fd()
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.as_mut().get_mut().poll_read_inner(cx, buf)
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.as_mut().get_mut().poll_write_inner(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(retry_eintr(|| {
            // SAFETY: the registration owns a valid socket descriptor.
            let result = unsafe { libc::shutdown(self.registration.as_raw_fd(), libc::SHUT_WR) };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut waiter = self.write_waiter.lock().unwrap();
        // L6: clamp to IOV_MAX; the kernel rejects larger vectored calls
        // with EINVAL.
        let iov = bufs
            .iter()
            .take(libc::IOV_MAX as usize)
            .map(|buf| libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            })
            .collect::<Vec<_>>();
        loop {
            let event =
                match poll_readiness(&self.registration, &mut waiter, Interest::WRITABLE, cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(event) => event,
                };
            let result = retry_eintr(|| {
                // SAFETY: each iovec points into the caller's live input buffers.
                let result = unsafe {
                    libc::writev(
                        self.registration.as_raw_fd(),
                        iov.as_ptr(),
                        iov.len() as libc::c_int,
                    )
                };
                if result == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(result as usize)
                }
            });
            match result {
                Ok(result) => {
                    *waiter = None;
                    drop(event);
                    return Poll::Ready(Ok(result));
                }
                Err(error) if would_block(&error) => {
                    event.clear_ready();
                }
                Err(error) => {
                    *waiter = None;
                    return Poll::Ready(Err(error));
                }
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}

pub struct TcpReadHalf<'a> {
    stream: &'a TcpStream,
}

pub struct TcpWriteHalf<'a> {
    stream: &'a TcpStream,
}

pub struct OwnedTcpReadHalf {
    registration: Arc<Registration>,
    waiter: Mutex<Option<Readiness>>,
}

pub struct OwnedTcpWriteHalf {
    registration: Arc<Registration>,
    waiter: Mutex<Option<Readiness>>,
}

impl AsRawFd for OwnedTcpReadHalf {
    fn as_raw_fd(&self) -> RawFd {
        self.registration.as_raw_fd()
    }
}

impl AsRawFd for OwnedTcpWriteHalf {
    fn as_raw_fd(&self) -> RawFd {
        self.registration.as_raw_fd()
    }
}

impl AsyncRead for TcpReadHalf<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.get_mut().stream.poll_read_inner(cx, buf)
    }
}

impl AsyncWrite for TcpWriteHalf<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().stream.poll_write_inner(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(retry_eintr(|| {
            // SAFETY: the borrowed stream remains alive for the half's lifetime.
            let result =
                unsafe { libc::shutdown(self.stream.registration.as_raw_fd(), libc::SHUT_WR) };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }))
    }
}

impl AsyncRead for OwnedTcpReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let mut waiter = this.waiter.lock().unwrap();
        loop {
            let event =
                match poll_readiness(&this.registration, &mut waiter, Interest::READABLE, cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(event) => event,
                };
            match this
                .registration
                .try_io_with(Interest::READABLE, || recv_into(this.as_raw_fd(), buf))
            {
                Ok(n) => {
                    *waiter = None;
                    // SAFETY: recv initialized exactly the returned byte count.
                    unsafe { buf.advance(n) };
                    drop(event);
                    return Poll::Ready(Ok(()));
                }
                Err(error) if would_block(&error) => {
                    event.clear_ready();
                }
                Err(error) => {
                    *waiter = None;
                    return Poll::Ready(Err(error));
                }
            }
        }
    }
}

impl AsyncWrite for OwnedTcpWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut waiter = this.waiter.lock().unwrap();
        loop {
            let event =
                match poll_readiness(&this.registration, &mut waiter, Interest::WRITABLE, cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(event) => event,
                };
            match this
                .registration
                .try_io_with(Interest::WRITABLE, || send_from(this.as_raw_fd(), buf))
            {
                Ok(n) => {
                    *waiter = None;
                    drop(event);
                    return Poll::Ready(Ok(n));
                }
                Err(error) if would_block(&error) => {
                    event.clear_ready();
                }
                Err(error) => {
                    *waiter = None;
                    return Poll::Ready(Err(error));
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(retry_eintr(|| {
            // SAFETY: the owned registration contains a valid socket descriptor.
            let result = unsafe { libc::shutdown(self.registration.as_raw_fd(), libc::SHUT_WR) };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }))
    }
}

pub struct UdpSocket {
    registration: Arc<Registration>,
    read_waiter: Mutex<Option<Readiness>>,
    write_waiter: Mutex<Option<Readiness>>,
}

impl UdpSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<UdpSocket> {
        let addresses: Vec<_> = addr.to_socket_addrs()?.collect();
        let mut last_error = None;
        for address in addresses {
            match Self::bind_one(address) {
                Ok(socket) => return Ok(socket),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "eddy: no socket addresses resolved",
            )
        }))
    }

    fn bind_one(address: SocketAddr) -> io::Result<UdpSocket> {
        let registration = Registration::with_driver(
            driver()?,
            make_socket(address, libc::SOCK_DGRAM)?,
            Interest::READABLE.add(Interest::WRITABLE),
        )?;
        let (_, storage, length) = socket_addr_parts(address);
        retry_eintr(|| {
            // SAFETY: storage is a live sockaddr and the registration owns a valid socket.
            let result = unsafe {
                libc::bind(
                    registration.as_raw_fd(),
                    &storage as *const _ as *const libc::sockaddr,
                    length,
                )
            };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })?;
        Ok(UdpSocket {
            registration: Arc::new(registration),
            read_waiter: Mutex::new(None),
            write_waiter: Mutex::new(None),
        })
    }

    pub fn from_std(socket: std::net::UdpSocket) -> io::Result<UdpSocket> {
        Ok(UdpSocket {
            registration: Arc::new(Registration::new(
                owned_fd(socket.into_raw_fd()),
                Interest::READABLE.add(Interest::WRITABLE),
            )?),
            read_waiter: Mutex::new(None),
            write_waiter: Mutex::new(None),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        socket_addr_for_fd(self.registration.as_raw_fd(), false)
    }

    pub fn connect<A: ToSocketAddrs>(&self, address: A) -> io::Result<()> {
        let address = address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no socket address"))?;
        let (_, storage, length) = socket_addr_parts(address);
        retry_eintr(|| {
            // SAFETY: storage is a live sockaddr and the registration owns a valid socket.
            let result = unsafe {
                libc::connect(
                    self.registration.as_raw_fd(),
                    &storage as *const _ as *const libc::sockaddr,
                    length,
                )
            };
            if result == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            let event = self.registration.readiness(Interest::READABLE).await;
            // SAFETY: zeroed storage is a valid output buffer for recvfrom.
            let mut storage = unsafe { std::mem::zeroed::<libc::sockaddr_storage>() };
            let mut length = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            let result = self.registration.try_io_with(Interest::READABLE, || {
                // SAFETY: fd and output buffers are valid for the duration of recvfrom.
                retry_eintr(|| {
                    // SAFETY: fd and output buffers are valid for recvfrom.
                    let n = unsafe {
                        libc::recvfrom(
                            self.as_raw_fd(),
                            buf.as_mut_ptr().cast::<libc::c_void>(),
                            buf.len(),
                            0,
                            &mut storage as *mut _ as *mut libc::sockaddr,
                            &mut length,
                        )
                    };
                    if n == -1 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(n as usize)
                    }
                })
            });
            match result {
                Ok(n) => return Ok((n, socket_addr_from_storage(&storage)?)),
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => return Err(error),
            }
        }
    }

    pub async fn send_to<A: ToSocketAddrs>(&self, buf: &[u8], address: A) -> io::Result<usize> {
        let address = address
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no socket address"))?;
        let (_, storage, length) = socket_addr_parts(address);
        loop {
            let event = self.registration.readiness(Interest::WRITABLE).await;
            let result = self.registration.try_io_with(Interest::WRITABLE, || {
                // SAFETY: fd, input buffer, and sockaddr remain valid during sendto.
                retry_eintr(|| {
                    // SAFETY: fd, input buffer, and sockaddr remain valid for sendto.
                    let n = unsafe {
                        libc::sendto(
                            self.as_raw_fd(),
                            buf.as_ptr().cast::<libc::c_void>(),
                            buf.len(),
                            0,
                            &storage as *const _ as *const libc::sockaddr,
                            length,
                        )
                    };
                    if n == -1 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(n as usize)
                    }
                })
            });
            match result {
                Ok(n) => return Ok(n),
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => return Err(error),
            }
        }
    }

    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let event = self.registration.readiness(Interest::READABLE).await;
            match self.registration.try_io_with(Interest::READABLE, || {
                recv_into(self.as_raw_fd(), &mut ReadBuf::new(buf))
            }) {
                Ok(n) => return Ok(n),
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => return Err(error),
            }
        }
    }

    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            let event = self.registration.readiness(Interest::WRITABLE).await;
            match self
                .registration
                .try_io_with(Interest::WRITABLE, || send_from(self.as_raw_fd(), buf))
            {
                Ok(n) => return Ok(n),
                Err(error) if would_block(&error) => event.clear_ready(),
                Err(error) => return Err(error),
            }
        }
    }

    fn poll_read_inner(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
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

impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.registration.as_raw_fd()
    }
}

impl AsyncRead for UdpSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.as_mut().get_mut().poll_read_inner(cx, buf)
    }
}

impl AsyncWrite for UdpSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.as_mut().get_mut().poll_write_inner(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsRawFd for TcpReadHalf<'_> {
    fn as_raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }
}

impl AsRawFd for TcpWriteHalf<'_> {
    fn as_raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }
}
