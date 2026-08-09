//! Small async I/O adapters and copy operations.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

pub async fn copy<R, W>(reader: &mut R, writer: &mut W) -> io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = [0u8; 8192];
    let mut copied = 0;
    loop {
        let amount = reader.read(&mut buffer).await?;
        if amount == 0 {
            writer.flush().await?;
            return Ok(copied);
        }
        writer.write_all(&buffer[..amount]).await?;
        copied += amount as u64;
    }
}

pub async fn copy_bidirectional<A, B>(left: &mut A, right: &mut B) -> io::Result<(u64, u64)>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    CopyBidirectional {
        left,
        right,
        left_to_right: DirectionState::new(),
        right_to_left: DirectionState::new(),
    }
    .await
}

struct DirectionState {
    buffer: Vec<u8>,
    filled: usize,
    pos: usize,
    eof: bool,
    copied: u64,
}

impl DirectionState {
    fn new() -> DirectionState {
        DirectionState {
            buffer: vec![0; 8192],
            filled: 0,
            pos: 0,
            eof: false,
            copied: 0,
        }
    }
}

enum DirectionPoll {
    Pending,
    Done,
}

fn poll_direction<S, D>(
    src: &mut S,
    dst: &mut D,
    state: &mut DirectionState,
    cx: &mut Context<'_>,
) -> Poll<io::Result<DirectionPoll>>
where
    S: AsyncRead + Unpin,
    D: AsyncWrite + Unpin,
{
    loop {
        if state.pos < state.filled {
            match Pin::new(&mut *dst).poll_write(cx, &state.buffer[state.pos..state.filled]) {
                Poll::Pending => return Poll::Ready(Ok(DirectionPoll::Pending)),
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "eddy: copy_bidirectional writer made no progress",
                    )))
                }
                Poll::Ready(Ok(amount)) => {
                    state.pos += amount;
                    state.copied += amount as u64;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            }
            continue;
        }

        if state.eof {
            return Poll::Ready(Ok(DirectionPoll::Done));
        }

        let mut read_buf = ReadBuf::new(&mut state.buffer);
        match Pin::new(&mut *src).poll_read(cx, &mut read_buf) {
            Poll::Pending => return Poll::Ready(Ok(DirectionPoll::Pending)),
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                state.filled = read_buf.filled_len();
                state.pos = 0;
                if state.filled == 0 {
                    state.eof = true;
                    return Poll::Ready(Ok(DirectionPoll::Done));
                }
            }
        }
    }
}

struct CopyBidirectional<'a, A, B> {
    left: &'a mut A,
    right: &'a mut B,
    left_to_right: DirectionState,
    right_to_left: DirectionState,
}

impl<A, B> Future for CopyBidirectional<'_, A, B>
where
    A: AsyncRead + AsyncWrite + Unpin,
    B: AsyncRead + AsyncWrite + Unpin,
{
    type Output = io::Result<(u64, u64)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let left_to_right = match poll_direction(this.left, this.right, &mut this.left_to_right, cx)
        {
            Poll::Pending => DirectionPoll::Pending,
            Poll::Ready(Ok(state)) => state,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
        };
        let right_to_left = match poll_direction(this.right, this.left, &mut this.right_to_left, cx)
        {
            Poll::Pending => DirectionPoll::Pending,
            Poll::Ready(Ok(state)) => state,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
        };
        if matches!(left_to_right, DirectionPoll::Done)
            && matches!(right_to_left, DirectionPoll::Done)
        {
            Poll::Ready(Ok((this.left_to_right.copied, this.right_to_left.copied)))
        } else {
            Poll::Pending
        }
    }
}

pub struct Empty;

pub fn empty() -> Empty {
    Empty
}

impl AsyncRead for Empty {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub struct Sink;

pub fn sink() -> Sink {
    Sink
}

impl AsyncWrite for Sink {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub struct Repeat {
    byte: u8,
}

pub fn repeat(byte: u8) -> Repeat {
    Repeat { byte }
}

impl AsyncRead for Repeat {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.as_ref().get_ref();
        let amount = buf.remaining();
        for slot in buf.unfilled_mut()[..amount].iter_mut() {
            slot.write(this.byte);
        }
        // SAFETY: every destination byte was initialized in the loop above.
        unsafe { buf.advance(amount) };
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MemoryIo {
        input: Vec<u8>,
        position: usize,
        output: Vec<u8>,
    }

    impl MemoryIo {
        fn new(input: &[u8]) -> MemoryIo {
            MemoryIo {
                input: input.to_vec(),
                position: 0,
                output: Vec::new(),
            }
        }
    }

    impl AsyncRead for MemoryIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            let amount = (this.input.len() - this.position).min(buf.remaining());
            for (dst, src) in buf.unfilled_mut()[..amount].iter_mut().zip(
                this.input[this.position..this.position + amount]
                    .iter()
                    .copied(),
            ) {
                dst.write(src);
            }
            this.position += amount;
            // SAFETY: each destination byte was initialized in the loop above.
            unsafe { buf.advance(amount) };
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for MemoryIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().output.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn copy_bidirectional_moves_both_directions_until_eof() {
        let runtime = crate::Builder::new_current_thread().build();
        runtime.block_on(async {
            let mut left = MemoryIo::new(b"left");
            let mut right = MemoryIo::new(b"right");
            let copied = copy_bidirectional(&mut left, &mut right).await.unwrap();
            assert_eq!(copied, (4, 5));
            assert_eq!(left.output, b"right");
            assert_eq!(right.output, b"left");
        });
    }
}
