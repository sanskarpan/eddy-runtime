//! Streams: asynchronous sequences of values.
//!
//! A [`Stream`] is the async analogue of `Iterator`: a single `poll_next`
//! method that yields `Poll<Option<Self::Item>>`. [`StreamExt`] provides the
//! standard combinators, implemented as pinned wrapper futures/streams that
//! are cancel-safe to drop at any point.

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

/// An asynchronous sequence of values.
///
/// Like `Iterator` but driven by polling: each call to [`poll_next`] may
/// yield `Some(item)`, signal the end of the sequence with `None`, or return
/// `Pending` until the waker stored in `cx` is woken.
///
/// [`poll_next`]: Stream::poll_next
pub trait Stream {
    /// The type of items yielded by this stream.
    type Item;

    /// Attempt to pull out the next item of this stream, registering the
    /// current task for wakeup if the value is not yet available.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;

    /// Returns the bounds on the remaining length of the stream.
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl<S: ?Sized + Stream + Unpin> Stream for &mut S {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        S::poll_next(Pin::new(&mut **self.get_mut()), cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (**self).size_hint()
    }
}

/// Combinator methods for all streams.
pub trait StreamExt: Stream {
    /// Retrieve the next item of the stream, or `None` once it is exhausted.
    ///
    /// Takes the stream by reference, so the same stream can be polled from
    /// multiple `select!` branches or iterated in a `while let` loop.
    fn next(&mut self) -> Next<'_, Self> {
        Next { stream: &mut *self }
    }

    /// Transform every item with `f`, yielding a new stream.
    fn map<B, F>(self, f: F) -> Map<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> B,
    {
        Map { stream: self, f }
    }

    /// Keep only the items for which `f` returns `true`.
    fn filter<F>(self, f: F) -> Filter<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> bool,
    {
        Filter { stream: self, f }
    }

    /// Fuse the stream after its first `None`.
    fn fuse(self) -> Fuse<Self>
    where
        Self: Sized,
    {
        Fuse {
            stream: self,
            done: false,
        }
    }

    /// Accumulate a value across the whole stream, resolving with the final
    /// accumulator.
    fn fold<B, F>(self, init: B, f: F) -> Fold<Self, B, F>
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        Fold {
            stream: self,
            acc: Some(init),
            f,
        }
    }

    /// Run `f` on every item, resolving when the stream is exhausted.
    fn for_each<F>(self, f: F) -> ForEach<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item),
    {
        ForEach { stream: self, f }
    }

    /// Count how many items the stream yields.
    fn count(self) -> Count<Self>
    where
        Self: Sized,
    {
        Count {
            stream: self,
            count: 0,
        }
    }

    /// Collect all items into a target collection (e.g. `Vec`).
    fn collect<B>(self) -> Collect<Self, B>
    where
        Self: Sized,
        B: FromStream<Self::Item> + Default,
    {
        Collect {
            stream: self,
            buf: B::default(),
        }
    }
}

impl<S: Stream + ?Sized> StreamExt for S {}

/// Builds a stream from an `IntoIterator`.
pub fn iter<I>(iter: I) -> Iter<I::IntoIter>
where
    I: IntoIterator,
    I::IntoIter: Unpin,
{
    Iter {
        iter: iter.into_iter(),
    }
}

pin_project! {
    /// The future returned by [`StreamExt::next`].
    pub struct Next<'a, S: ?Sized> {
        #[pin]
        stream: &'a mut S,
    }
}

impl<S: ?Sized> Future for Next<'_, S>
where
    S: Stream + Unpin,
{
    type Output = Option<S::Item>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().stream.poll_next(cx)
    }
}

pin_project! {
    /// A stream that wraps an `Iterator`.
    pub struct Iter<I> {
        iter: I,
    }
}

impl<I: Iterator + Unpin> Stream for Iter<I> {
    type Item = I::Item;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().iter.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

pin_project! {
    /// The stream returned by [`StreamExt::map`].
    pub struct Map<St, F> {
        #[pin]
        stream: St,
        f: F,
    }
}

impl<St, F, B> Stream for Map<St, F>
where
    St: Stream,
    F: FnMut(St::Item) -> B,
{
    type Item = B;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some((this.f)(item))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pin_project! {
    /// The stream returned by [`StreamExt::filter`].
    pub struct Filter<St, F> {
        #[pin]
        stream: St,
        f: F,
    }
}

impl<St, F> Stream for Filter<St, F>
where
    St: Stream,
    F: FnMut(&St::Item) -> bool,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if (this.f)(&item) {
                        return Poll::Ready(Some(item));
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project! {
    /// The stream returned by [`StreamExt::fuse`].
    pub struct Fuse<St> {
        #[pin]
        stream: St,
        done: bool,
    }
}

impl<St: Stream> Stream for Fuse<St> {
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.done {
            return Poll::Ready(None);
        }
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                *this.done = true;
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

pin_project! {
    /// The future returned by [`StreamExt::fold`].
    pub struct Fold<St, B, F> {
        #[pin]
        stream: St,
        acc: Option<B>,
        f: F,
    }
}

impl<St, B, F> Future for Fold<St, B, F>
where
    St: Stream,
    F: FnMut(B, St::Item) -> B,
{
    type Output = B;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let acc = this.acc.take().expect("eddy: fold polled after completion");
                    let acc = (this.f)(acc, item);
                    *this.acc = Some(acc);
                }
                Poll::Ready(None) => {
                    return Poll::Ready(
                        this.acc.take().expect("eddy: fold polled after completion"),
                    );
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project! {
    /// The future returned by [`StreamExt::for_each`].
    pub struct ForEach<St, F> {
        #[pin]
        stream: St,
        f: F,
    }
}

impl<St, F> Future for ForEach<St, F>
where
    St: Stream,
    F: FnMut(St::Item),
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => (this.f)(item),
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

pin_project! {
    /// The future returned by [`StreamExt::count`].
    pub struct Count<St> {
        #[pin]
        stream: St,
        count: usize,
    }
}

impl<St: Stream> Future for Count<St> {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(_)) => *this.count += 1,
                Poll::Ready(None) => return Poll::Ready(*this.count),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Conversion of a stream's items into a target collection type.
pub trait FromStream<T> {
    /// Push one item into the collection.
    fn extend_with(&mut self, item: T);

    /// Signal that the stream is exhausted.
    fn finish(&mut self);
}

impl<T> FromStream<T> for Vec<T> {
    fn extend_with(&mut self, item: T) {
        self.push(item);
    }

    fn finish(&mut self) {}
}

pin_project! {
    /// The future returned by [`StreamExt::collect`].
    pub struct Collect<St, B> {
        #[pin]
        stream: St,
        buf: B,
    }
}

impl<St, B> Future for Collect<St, B>
where
    St: Stream,
    B: FromStream<St::Item> + Default,
{
    type Output = B;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => this.buf.extend_with(item),
                Poll::Ready(None) => {
                    this.buf.finish();
                    let buf = mem::take(&mut *this.buf);
                    return Poll::Ready(buf);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
