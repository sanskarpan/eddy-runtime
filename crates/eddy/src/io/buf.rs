//! Buffered I/O adapters built on the runtime's async I/O traits.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

const DEFAULT_CAPACITY: usize = 8 * 1024;

pub struct BufReader<R> {
    inner: R,
    buffer: Vec<u8>,
    pos: usize,
    cap: usize,
}

impl<R> BufReader<R> {
    pub fn new(inner: R) -> BufReader<R> {
        Self::with_capacity(DEFAULT_CAPACITY, inner)
    }

    pub fn with_capacity(capacity: usize, inner: R) -> BufReader<R> {
        assert!(capacity > 0, "eddy: buffer capacity must be non-zero");
        BufReader {
            inner,
            buffer: vec![0; capacity],
            pos: 0,
            cap: 0,
        }
    }

    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: AsyncRead + Unpin> BufReader<R> {
    fn poll_fill_buf_inner(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        if self.pos < self.cap {
            return Poll::Ready(Ok(&self.buffer[self.pos..self.cap]));
        }
        let mut read_buf = ReadBuf::new(&mut self.buffer);
        match Pin::new(&mut self.inner).poll_read(cx, &mut read_buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.pos = 0;
                self.cap = read_buf.filled_len();
                Poll::Ready(Ok(&self.buffer[..self.cap]))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for BufReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let available = match this.poll_fill_buf_inner(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(available)) => available,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
        };
        let amount = available.len().min(buf.remaining());
        for (dst, src) in buf.unfilled_mut()[..amount]
            .iter_mut()
            .zip(available[..amount].iter().copied())
        {
            dst.write(src);
        }
        this.pos += amount;
        // SAFETY: every destination byte was initialized in the loop above.
        unsafe { buf.advance(amount) };
        Poll::Ready(Ok(()))
    }
}

pub struct BufWriter<W> {
    inner: W,
    buffer: Vec<u8>,
    pos: usize,
    capacity: usize,
}

impl<W> BufWriter<W> {
    pub fn new(inner: W) -> BufWriter<W> {
        Self::with_capacity(DEFAULT_CAPACITY, inner)
    }

    pub fn with_capacity(capacity: usize, inner: W) -> BufWriter<W> {
        assert!(capacity > 0, "eddy: buffer capacity must be non-zero");
        BufWriter {
            inner,
            buffer: Vec::with_capacity(capacity),
            pos: 0,
            capacity,
        }
    }

    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    fn compact(&mut self) {
        if self.pos > 0 {
            self.buffer.copy_within(self.pos.., 0);
            let remaining = self.buffer.len() - self.pos;
            self.buffer.truncate(remaining);
            self.pos = 0;
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for BufWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        this.compact();
        if this.buffer.len() == this.capacity {
            match Pin::new(&mut *this).poll_flush(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    this.buffer.clear();
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            }
        }
        if this.buffer.is_empty() && buf.len() >= this.capacity {
            return Pin::new(&mut this.inner).poll_write(cx, buf);
        }
        let amount = buf.len().min(this.capacity - this.buffer.len());
        this.buffer.extend_from_slice(&buf[..amount]);
        Poll::Ready(Ok(amount))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while this.pos < this.buffer.len() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.buffer[this.pos..]) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "eddy: buffered writer made no progress",
                    )))
                }
                Poll::Ready(Ok(amount)) => this.pos += amount,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            }
        }
        this.buffer.clear();
        this.pos = 0;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self;
        match this.as_mut().poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                let this = this.get_mut();
                Pin::new(&mut this.inner).poll_shutdown(cx)
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }
}

pub trait AsyncBufRead: AsyncRead {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>>;

    fn consume(self: Pin<&mut Self>, amount: usize);
}

impl<R: AsyncRead + Unpin> AsyncBufRead for BufReader<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        self.get_mut().poll_fill_buf_inner(cx)
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        let this = self.get_mut();
        this.pos = (this.pos + amount).min(this.cap);
    }
}

pub struct BufStream<S> {
    inner: S,
    read_buffer: Vec<u8>,
    read_pos: usize,
    read_cap: usize,
    write_buffer: Vec<u8>,
    write_pos: usize,
    write_capacity: usize,
}

impl<S> BufStream<S> {
    pub fn new(inner: S) -> BufStream<S> {
        Self::with_capacity(DEFAULT_CAPACITY, inner)
    }

    pub fn with_capacity(capacity: usize, inner: S) -> BufStream<S> {
        assert!(capacity > 0, "eddy: buffer capacity must be non-zero");
        BufStream {
            inner,
            read_buffer: vec![0; capacity],
            read_pos: 0,
            read_cap: 0,
            write_buffer: Vec::with_capacity(capacity),
            write_pos: 0,
            write_capacity: capacity,
        }
    }

    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> BufStream<S> {
    fn poll_fill_read_buffer(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.read_pos < self.read_cap {
            return Poll::Ready(Ok(()));
        }
        let mut read_buf = ReadBuf::new(&mut self.read_buffer);
        match Pin::new(&mut self.inner).poll_read(cx, &mut read_buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.read_pos = 0;
                self.read_cap = read_buf.filled_len();
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for BufStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut *this).poll_flush(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
        }
        match this.poll_fill_read_buffer(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        let amount = (this.read_cap - this.read_pos).min(buf.remaining());
        for (dst, src) in buf.unfilled_mut()[..amount].iter_mut().zip(
            this.read_buffer[this.read_pos..this.read_pos + amount]
                .iter()
                .copied(),
        ) {
            dst.write(src);
        }
        this.read_pos += amount;
        // SAFETY: every destination byte was initialized in the loop above.
        unsafe { buf.advance(amount) };
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncBufRead for BufStream<S> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        match this.poll_fill_read_buffer(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(&this.read_buffer[this.read_pos..this.read_cap])),
        }
    }

    fn consume(self: Pin<&mut Self>, amount: usize) {
        let this = self.get_mut();
        this.read_pos = (this.read_pos + amount).min(this.read_cap);
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for BufStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.write_pos > 0 {
            this.write_buffer.copy_within(this.write_pos.., 0);
            let remaining = this.write_buffer.len() - this.write_pos;
            this.write_buffer.truncate(remaining);
            this.write_pos = 0;
        }
        if this.write_buffer.len() == this.write_capacity {
            match Pin::new(&mut *this).poll_flush(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            }
        }
        let amount = buf.len().min(this.write_capacity - this.write_buffer.len());
        this.write_buffer.extend_from_slice(&buf[..amount]);
        Poll::Ready(Ok(amount))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while this.write_pos < this.write_buffer.len() {
            match Pin::new(&mut this.inner).poll_write(cx, &this.write_buffer[this.write_pos..]) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "eddy: buffered stream made no progress",
                    )))
                }
                Poll::Ready(Ok(amount)) => this.write_pos += amount,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            }
        }
        this.write_buffer.clear();
        this.write_pos = 0;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self;
        match this.as_mut().poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                let this = this.get_mut();
                Pin::new(&mut this.inner).poll_shutdown(cx)
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }
}

pub struct Lines<R> {
    reader: R,
}

impl<R: AsyncBufRead + Unpin> Lines<R> {
    pub async fn next_line(&mut self) -> io::Result<Option<String>> {
        let mut line = String::new();
        let count = self.reader.read_line(&mut line).await?;
        if count == 0 && line.is_empty() {
            Ok(None)
        } else {
            Ok(Some(line))
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait AsyncBufReadExt: AsyncBufRead + Unpin {
    async fn read_until(&mut self, delimiter: u8, output: &mut Vec<u8>) -> io::Result<usize> {
        let start = output.len();
        let mut byte = [0u8; 1];
        loop {
            if self.read(&mut byte).await? == 0 {
                return Ok(output.len() - start);
            }
            output.push(byte[0]);
            if byte[0] == delimiter {
                return Ok(output.len() - start);
            }
        }
    }

    async fn read_line(&mut self, output: &mut String) -> io::Result<usize> {
        let mut bytes = Vec::new();
        let count = self.read_until(b'\n', &mut bytes).await?;
        let line = String::from_utf8(bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        output.push_str(&line);
        Ok(count)
    }

    fn lines(self) -> Lines<Self>
    where
        Self: Sized,
    {
        Lines { reader: self }
    }
}

impl<T: AsyncBufRead + Unpin + ?Sized> AsyncBufReadExt for T {}
