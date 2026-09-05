#![cfg(not(loom))]

use std::collections::VecDeque;
use std::future::{pending, Future};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use eddy::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use eddy::sync::{broadcast, mpsc, watch, Mutex, Notify, RwLock, Semaphore};
use eddy::time::{sleep, timeout};
use eddy::{Builder, CancellationToken};
use proptest::prelude::*;

fn poll_pending<F: Future>(mut future: Pin<&mut F>, polls: u8) {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..polls {
        assert!(future.as_mut().poll(&mut cx).is_pending());
    }
}

struct ScriptedReader {
    chunks: VecDeque<Vec<u8>>,
    armed: bool,
}

impl ScriptedReader {
    fn new(chunks: &[&[u8]]) -> Self {
        Self {
            chunks: chunks.iter().map(|chunk| chunk.to_vec()).collect(),
            armed: false,
        }
    }
}

impl AsyncRead for ScriptedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.armed {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let Some(chunk) = self.chunks.pop_front() else {
            // EOF is represented by a ready read with no filled bytes.
            return Poll::Ready(Ok(()));
        };
        let len = chunk.len();
        assert!(len <= buf.remaining());
        for (slot, byte) in buf.unfilled_mut()[..len].iter_mut().zip(chunk) {
            slot.write(byte);
        }
        // SAFETY: the bytes copied immediately above are initialized.
        unsafe { buf.advance(len) };
        Poll::Ready(Ok(()))
    }
}

struct ScriptedWriter {
    output: Vec<u8>,
    armed: bool,
}

impl ScriptedWriter {
    fn new() -> Self {
        Self {
            output: Vec::new(),
            armed: false,
        }
    }
}

impl AsyncWrite for ScriptedWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if !self.armed {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.output.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn cancel_safe_futures_preserve_work_after_random_pending_polls(polls in 0u8..=8) {
        let runtime = Builder::new_current_thread().build();
        runtime.block_on(async {
            // A cancelled receive must leave the later message in the channel.
            let (sender, mut receiver) = mpsc::channel(2);
            let mut recv = Box::pin(receiver.recv());
            poll_pending(recv.as_mut(), polls);
            drop(recv);
            sender.send(7).await.unwrap();
            assert_eq!(receiver.recv().await, Some(7));

            // recv_many has the same commit point: no item is removed while it
            // is still pending.
            let (sender, mut receiver) = mpsc::channel(2);
            let mut output = Vec::new();
            let mut recv_many = Box::pin(receiver.recv_many(&mut output, 2));
            poll_pending(recv_many.as_mut(), polls);
            drop(recv_many);
            sender.send(8).await.unwrap();
            sender.send(9).await.unwrap();
            assert_eq!(receiver.recv_many(&mut output, 2).await, 2);
            assert_eq!(output, [8, 9]);

            // A cancelled reserve must release its capacity reservation.
            let (sender, mut receiver) = mpsc::channel(1);
            sender.send(1).await.unwrap();
            let mut reserve = Box::pin(sender.reserve());
            poll_pending(reserve.as_mut(), polls);
            drop(reserve);
            assert_eq!(receiver.recv().await, Some(1));
            sender.send(2).await.unwrap();
            assert_eq!(receiver.recv().await, Some(2));

            // Cancelling a borrowed oneshot receive does not close the receiver
            // or consume the value that arrives later.
            let (sender, mut receiver) = eddy::sync::oneshot::channel();
            let mut recv = Box::pin(&mut receiver);
            poll_pending(recv.as_mut(), polls);
            drop(recv);
            sender.send(10).unwrap();
            assert_eq!(receiver.await.unwrap(), 10);

            // A dropped broadcast wait unregisters only that receiver. Another
            // live receiver still gets the broadcast value.
            let (sender, mut cancelled) = broadcast::channel(2);
            let live = cancelled.clone();
            let mut recv = Box::pin(&mut cancelled);
            poll_pending(recv.as_mut(), polls);
            drop(recv);
            sender.send(11).unwrap();
            assert_eq!(live.await.unwrap(), 11);

            // A watch change future can be cancelled without advancing the
            // observed version.
            let (sender, mut receiver) = watch::channel(0);
            let mut changed = Box::pin(receiver.changed());
            poll_pending(changed.as_mut(), polls);
            drop(changed);
            sender.send(12).unwrap();
            receiver.changed().await.unwrap();
            assert_eq!(receiver.borrow_and_update(), 12);

            // Pending waiters must be removed without consuming a later permit.
            let notify = Notify::new();
            let mut notified = Box::pin(notify.notified());
            poll_pending(notified.as_mut(), polls);
            drop(notified);
            notify.notify_one();
            notify.notified().await;

            let semaphore = Semaphore::new(0);
            let mut acquire = Box::pin(semaphore.acquire());
            poll_pending(acquire.as_mut(), polls);
            drop(acquire);
            semaphore.add_permits(1);
            drop(semaphore.try_acquire().unwrap());

            let mutex = Mutex::new(());
            let guard = mutex.lock().await;
            let mut lock = Box::pin(mutex.lock());
            poll_pending(lock.as_mut(), polls);
            drop(lock);
            drop(guard);
            drop(mutex.lock().await);

            let rwlock = RwLock::new(0usize);
            let write_guard = rwlock.write().await;
            let mut read = Box::pin(rwlock.read());
            poll_pending(read.as_mut(), polls);
            drop(read);
            drop(write_guard);
            drop(rwlock.read().await);

            let token = CancellationToken::new();
            let mut cancelled = Box::pin(token.cancelled());
            poll_pending(cancelled.as_mut(), polls);
            drop(cancelled);
            token.cancel();
            token.cancelled().await;

            // Timer cancellation removes the old registration; a later timer
            // still fires normally.
            let mut timer = Box::pin(sleep(Duration::from_secs(60)));
            poll_pending(timer.as_mut(), polls);
            drop(timer);
            let mut timed = Box::pin(timeout(Duration::from_secs(60), pending::<()>()));
            poll_pending(timed.as_mut(), polls);
            drop(timed);
            sleep(Duration::ZERO).await;

            // The documented read/write adapter contracts are also exercised
            // with a deterministic readiness boundary.
            let mut reader = ScriptedReader::new(&[b"read"]);
            let mut buf = [0; 4];
            let mut read = Box::pin(reader.read(&mut buf));
            poll_pending(read.as_mut(), polls);
            drop(read);
            reader.armed = true;
            assert_eq!(reader.read(&mut buf).await.unwrap(), 4);
            assert_eq!(&buf, b"read");

            let mut reader = ScriptedReader::new(&[b"to ", b"end"]);
            let mut output = Vec::new();
            let mut read_to_end = Box::pin(reader.read_to_end(&mut output));
            poll_pending(read_to_end.as_mut(), polls);
            drop(read_to_end);
            reader.armed = true;
            assert_eq!(reader.read_to_end(&mut output).await.unwrap(), 6);
            assert_eq!(output, b"to end");

            let mut reader = ScriptedReader::new(&[b"text"]);
            let mut output = String::new();
            let mut read_to_string = Box::pin(reader.read_to_string(&mut output));
            poll_pending(read_to_string.as_mut(), polls);
            drop(read_to_string);
            reader.armed = true;
            assert_eq!(reader.read_to_string(&mut output).await.unwrap(), 4);
            assert_eq!(output, "text");

            let mut writer = ScriptedWriter::new();
            let mut write = Box::pin(writer.write(b"write"));
            poll_pending(write.as_mut(), polls);
            drop(write);
            writer.armed = true;
            assert_eq!(writer.write(b"write").await.unwrap(), 5);
            assert_eq!(writer.output, b"write");
        });
    }
}
