//! Async I/O readiness.
//!
//! `Registration` ties an fd to the runtime's readiness driver: awaiting
//! `readiness(interest)` parks the task until the kernel reports the fd
//! ready. The concrete async I/O types (TcpStream, UdpSocket, ...) are built
//! on top of this.

pub mod buf;
pub(crate) mod driver;
pub mod net;
pub mod ops;
pub mod poll_evented;
#[cfg(unix)]
pub mod unix;

use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, OwnedFd};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use driver::{DriverShared, ScheduledIo};

pub use buf::{AsyncBufRead, AsyncBufReadExt, BufReader, BufStream, BufWriter, Lines};
pub use driver::{Readiness, ReadyEvent};
pub use net::{TcpListener, TcpStream, UdpSocket};
pub use ops::{copy, copy_bidirectional, empty, repeat, sink, Empty, Repeat, Sink};
pub use poll_evented::PollEvented;
#[cfg(unix)]
pub use unix::{UnixDatagram, UnixListener, UnixStream};

/// A buffer passed to [`AsyncRead::poll_read`]. It tracks bytes that have
/// been filled separately from bytes whose memory has been initialized.
pub struct ReadBuf<'a> {
    buf: &'a mut [MaybeUninit<u8>],
    filled: usize,
    initialized: usize,
}

impl<'a> ReadBuf<'a> {
    pub fn new(buf: &'a mut [u8]) -> ReadBuf<'a> {
        let ptr = buf.as_mut_ptr().cast::<MaybeUninit<u8>>();
        let len = buf.len();
        // SAFETY: `MaybeUninit<u8>` has the same layout and alignment as `u8`.
        let buf = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
        ReadBuf {
            buf,
            filled: 0,
            initialized: len,
        }
    }

    /// Create a buffer backed by uninitialized memory.
    ///
    /// # Safety
    /// The memory must not be read until the read operation has initialized it.
    pub unsafe fn uninit(buf: &'a mut [MaybeUninit<u8>]) -> ReadBuf<'a> {
        ReadBuf {
            buf,
            filled: 0,
            initialized: 0,
        }
    }

    pub fn filled(&self) -> &[u8] {
        // SAFETY: the filled prefix is initialized by construction or by the
        // unsafe `advance` contract.
        unsafe { std::slice::from_raw_parts(self.buf.as_ptr().cast::<u8>(), self.filled) }
    }

    pub fn filled_mut(&mut self) -> &mut [u8] {
        // SAFETY: the filled prefix is initialized and exclusively borrowed.
        unsafe { std::slice::from_raw_parts_mut(self.buf.as_mut_ptr().cast::<u8>(), self.filled) }
    }

    pub fn initialized(&self) -> &[u8] {
        // SAFETY: the initialized prefix is safe to read by definition.
        unsafe { std::slice::from_raw_parts(self.buf.as_ptr().cast::<u8>(), self.initialized) }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.filled
    }

    pub fn filled_len(&self) -> usize {
        self.filled
    }

    pub fn initialized_len(&self) -> usize {
        self.initialized
    }

    pub fn unfilled_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        &mut self.buf[self.filled..]
    }

    /// Mark bytes written by an external operation as initialized and filled.
    ///
    /// # Safety
    /// The first `n` bytes of the unfilled region must have been fully
    /// initialized by the operation that produced the bytes.
    pub unsafe fn advance(&mut self, n: usize) {
        assert!(n <= self.remaining(), "eddy: read advanced past buffer");
        self.filled += n;
        self.initialized = self.initialized.max(self.filled);
    }

    pub fn clear(&mut self) {
        self.filled = 0;
    }
}

/// Read bytes asynchronously into a [`ReadBuf`].
pub trait AsyncRead {
    /// This low-level operation is cancel safe: a pending poll has not read
    /// bytes yet, and a ready poll reports all bytes in `buf`.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>>;
}

/// Write bytes asynchronously.
pub trait AsyncWrite {
    /// Attempt one write. A pending operation has not written bytes; callers
    /// must account for partial writes when the operation is ready.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>>;

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let Some(buf) = bufs.iter().find(|buf| !buf.is_empty()) else {
            return Poll::Ready(Ok(0));
        };
        self.poll_write(cx, buf)
    }

    fn is_write_vectored(&self) -> bool {
        false
    }
}

#[allow(async_fn_in_trait)]
pub trait AsyncReadExt: AsyncRead + Unpin {
    /// Read once. Cancel safe: a pending read has not consumed bytes.
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read_buf = ReadBuf::new(buf);
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_read(cx, &mut read_buf)).await?;
        Ok(read_buf.filled().len())
    }

    /// Fill the buffer. Not cancel safe: cancellation may occur after a
    /// partial read and the bytes already consumed are returned only through
    /// the future's eventual success or error.
    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let mut filled = 0;
        while filled < buf.len() {
            let n = self.read(&mut buf[filled..]).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "eddy: EOF before read_exact filled the buffer",
                ));
            }
            filled += n;
        }
        Ok(())
    }

    /// Read until EOF. Cancel safe for the supplied `Vec`: completed chunks
    /// are appended before the next await point.
    async fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let start = buf.len();
        let mut chunk = [0u8; 8192];
        loop {
            let n = self.read(&mut chunk).await?;
            if n == 0 {
                return Ok(buf.len() - start);
            }
            buf.extend_from_slice(&chunk[..n]);
        }
    }

    /// Read UTF-8 until EOF. Cancel safe for the same reason as `read_to_end`.
    async fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        let mut bytes = Vec::new();
        let n = self.read_to_end(&mut bytes).await?;
        let text = String::from_utf8(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        buf.push_str(&text);
        Ok(n)
    }
}

impl<T: AsyncRead + Unpin + ?Sized> AsyncReadExt for T {}

#[allow(async_fn_in_trait)]
pub trait AsyncWriteExt: AsyncWrite + Unpin {
    /// Attempt one write. Cancellation may leave a partial write at the peer.
    async fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_write(cx, buf)).await
    }

    /// Attempt a vectored write, returning the number of bytes accepted.
    async fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_write_vectored(cx, bufs)).await
    }

    /// Write the complete buffer. Not cancel safe: cancellation may leave a
    /// prefix written without exposing the exact prefix length.
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        let mut written = 0;
        while written < buf.len() {
            let n = self.write(&buf[written..]).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "eddy: write returned zero before the buffer was complete",
                ));
            }
            written += n;
        }
        Ok(())
    }

    /// Flush pending output. Socket flush is immediate; other wrappers may
    /// perform an actual flush operation.
    async fn flush(&mut self) -> io::Result<()> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_flush(cx)).await
    }

    /// Shut down the write side of the stream.
    async fn shutdown(&mut self) -> io::Result<()> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_shutdown(cx)).await
    }
}

impl<T: AsyncWrite + Unpin + ?Sized> AsyncWriteExt for T {}

/// What an I/O object is waiting for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interest(u16);

impl Interest {
    pub const READABLE: Interest = Interest(0b0001);
    pub const WRITABLE: Interest = Interest(0b0010);

    /// Edge-triggered notifications. Level-triggered (the default) re-reports
    /// a ready fd on every wait, which tolerates missed drains; edge-triggered
    /// reports once per state change and requires draining to `WouldBlock`
    /// after every wake — a missed drain is a permanent hang.
    pub const EDGE_TRIGGERED: Interest = Interest(0b0100);

    /// Combine interests, e.g. `Interest::READABLE.add(Interest::WRITABLE)`.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, other: Interest) -> Interest {
        Interest(self.0 | other.0)
    }

    pub fn is_readable(&self) -> bool {
        self.0 & Self::READABLE.0 != 0
    }

    pub fn is_writable(&self) -> bool {
        self.0 & Self::WRITABLE.0 != 0
    }

    pub(crate) fn is_edge_triggered(self) -> bool {
        self.0 & Self::EDGE_TRIGGERED.0 != 0
    }
}

/// A set of readiness bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ready(u16);

impl Ready {
    pub const READABLE: Ready = Ready(0b0000_0001);
    pub const WRITABLE: Ready = Ready(0b0000_0010);
    pub const READ_CLOSED: Ready = Ready(0b0000_0100);
    pub const WRITE_CLOSED: Ready = Ready(0b0000_1000);
    pub const ERROR: Ready = Ready(0b0001_0000);
    pub const EMPTY: Ready = Ready(0);

    pub fn is_readable(&self) -> bool {
        self.0 & (Self::READABLE.0 | Self::READ_CLOSED.0 | Self::ERROR.0) != 0
    }

    pub fn is_writable(&self) -> bool {
        self.0 & (Self::WRITABLE.0 | Self::WRITE_CLOSED.0 | Self::ERROR.0) != 0
    }

    pub fn is_read_closed(&self) -> bool {
        self.0 & (Self::READ_CLOSED.0 | Self::ERROR.0) != 0
    }

    pub fn is_write_closed(&self) -> bool {
        self.0 & (Self::WRITE_CLOSED.0 | Self::ERROR.0) != 0
    }

    pub fn is_error(&self) -> bool {
        self.0 & Self::ERROR.0 != 0
    }

    pub fn contains(&self, other: Ready) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn union(&self, other: Ready) -> Ready {
        Ready(self.0 | other.0)
    }

    pub fn intersection(&self, other: Ready) -> Ready {
        Ready(self.0 & other.0)
    }

    pub(crate) fn bits(self) -> u16 {
        self.0
    }

    pub(crate) fn from_bits(bits: u16) -> Ready {
        Ready(bits)
    }
}

/// A handle to one fd registered with the runtime's readiness driver.
///
/// The fd must already be non-blocking; registration is ownership transfer
/// (the fd is closed when the registration drops). `Registration` is
/// `Send + Sync` and can be moved between tasks.
pub struct Registration {
    _driver: Arc<DriverShared>,
    scheduled: Arc<ScheduledIo>,
    _fd: OwnedFd,
    interests: Interest,
}

impl Registration {
    /// Register `fd` with the current runtime's driver.
    ///
    /// # Panics
    /// Panics with a clear message if no runtime is running, or if the
    /// current runtime does not yet provide an I/O driver (the current-thread
    /// flavor; a driver for it lands in a later phase).
    pub fn new(fd: OwnedFd, interests: Interest) -> io::Result<Registration> {
        let driver = crate::runtime::Handle::current()
            .io_driver()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "eddy: I/O registration requires a multi-thread runtime (the current-thread \
                 driver is not implemented yet)",
                )
            })?;
        Self::with_driver(driver, fd, interests)
    }

    pub(crate) fn with_driver(
        driver: Arc<DriverShared>,
        fd: OwnedFd,
        interests: Interest,
    ) -> io::Result<Registration> {
        let scheduled = driver.register(fd.as_raw_fd(), interests)?;
        // SAFETY: `fd` is an OwnedFd, so the raw fd is valid and owned.
        Ok(Registration {
            _driver: driver,
            scheduled,
            _fd: fd,
            interests,
        })
    }

    /// Wait until `interests` are satisfied, returning the ready state.
    pub fn readiness(&self, interests: Interest) -> Readiness {
        Readiness::new(Arc::clone(&self.scheduled), interests)
    }

    /// The registered fd, for syscalls performed after a readiness wait.
    pub fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self._fd.as_raw_fd()
    }

    /// Run a syscall; if it returns `WouldBlock`, clear the current
    /// readiness so the next `readiness().await` parks instead of spinning.
    pub fn try_io<R>(&self, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
        self.try_io_with(self.interests, f)
    }

    pub(crate) fn try_io_with<R>(
        &self,
        interest: Interest,
        f: impl FnOnce() -> io::Result<R>,
    ) -> io::Result<R> {
        match f() {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if let Some(event) = self.scheduled.readiness(interest) {
                    event.clear_ready();
                }
                Err(error)
            }
            result => result,
        }
    }

    pub(crate) fn driver(&self) -> Arc<DriverShared> {
        self._driver.clone()
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self._driver
            .deregister(&self.scheduled, self._fd.as_raw_fd());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_buf_tracks_filled_and_initialized_regions() {
        let mut bytes = [0u8; 4];
        let mut buf = ReadBuf::new(&mut bytes);
        assert_eq!(buf.filled_len(), 0);
        assert_eq!(buf.initialized_len(), 4);
        buf.unfilled_mut()[0].write(7);
        // SAFETY: the first unfilled byte was initialized immediately above.
        unsafe { buf.advance(1) };
        assert_eq!(buf.filled(), &[7]);
        assert_eq!(buf.remaining(), 3);
    }
}
