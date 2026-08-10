//! Future combinators and cancellation-aware utilities.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

pub use std::future::{pending, poll_fn, ready};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

pin_project! {
    pub struct Join2<A: Future, B: Future> {
        #[pin]
        a: A,
        #[pin]
        b: B,
        a_output: Option<A::Output>,
        b_output: Option<B::Output>,
    }
}

impl<A: Future, B: Future> Future for Join2<A, B> {
    type Output = (A::Output, B::Output);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if this.a_output.is_none() {
            if let Poll::Ready(output) = this.a.as_mut().poll(cx) {
                *this.a_output = Some(output);
            }
        }
        if this.b_output.is_none() {
            if let Poll::Ready(output) = this.b.as_mut().poll(cx) {
                *this.b_output = Some(output);
            }
        }
        match (this.a_output.take(), this.b_output.take()) {
            (Some(a), Some(b)) => Poll::Ready((a, b)),
            (a, b) => {
                *this.a_output = a;
                *this.b_output = b;
                Poll::Pending
            }
        }
    }
}

pub fn join2<A: Future, B: Future>(a: A, b: B) -> Join2<A, B> {
    Join2 {
        a,
        b,
        a_output: None,
        b_output: None,
    }
}

pin_project! {
    pub struct TryJoin2<A: Future, B: Future> {
        #[pin]
        a: A,
        #[pin]
        b: B,
        a_output: Option<A::Output>,
        b_output: Option<B::Output>,
    }
}

impl<A, B, TA, TB, E> Future for TryJoin2<A, B>
where
    A: Future<Output = Result<TA, E>>,
    B: Future<Output = Result<TB, E>>,
{
    type Output = Result<(TA, TB), E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if this.a_output.is_none() {
            match this.a.as_mut().poll(cx) {
                Poll::Ready(Ok(output)) => *this.a_output = Some(Ok(output)),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }
        if this.b_output.is_none() {
            match this.b.as_mut().poll(cx) {
                Poll::Ready(Ok(output)) => *this.b_output = Some(Ok(output)),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }
        match (this.a_output.take(), this.b_output.take()) {
            (Some(Ok(a)), Some(Ok(b))) => Poll::Ready(Ok((a, b))),
            (a, b) => {
                *this.a_output = a;
                *this.b_output = b;
                Poll::Pending
            }
        }
    }
}

pub fn try_join2<A, B, TA, TB, E>(a: A, b: B) -> TryJoin2<A, B>
where
    A: Future<Output = Result<TA, E>>,
    B: Future<Output = Result<TB, E>>,
{
    TryJoin2 {
        a,
        b,
        a_output: None,
        b_output: None,
    }
}

pin_project! {
    pub struct TryJoin3<A: Future, B: Future, C: Future> {
        #[pin]
        a: A,
        #[pin]
        b: B,
        #[pin]
        c: C,
        a_output: Option<A::Output>,
        b_output: Option<B::Output>,
        c_output: Option<C::Output>,
    }
}

impl<A, B, C, TA, TB, TC, E> Future for TryJoin3<A, B, C>
where
    A: Future<Output = Result<TA, E>>,
    B: Future<Output = Result<TB, E>>,
    C: Future<Output = Result<TC, E>>,
{
    type Output = Result<(TA, TB, TC), E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if this.a_output.is_none() {
            match this.a.as_mut().poll(cx) {
                Poll::Ready(Ok(output)) => *this.a_output = Some(Ok(output)),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }
        if this.b_output.is_none() {
            match this.b.as_mut().poll(cx) {
                Poll::Ready(Ok(output)) => *this.b_output = Some(Ok(output)),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }
        if this.c_output.is_none() {
            match this.c.as_mut().poll(cx) {
                Poll::Ready(Ok(output)) => *this.c_output = Some(Ok(output)),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }
        match (
            this.a_output.take(),
            this.b_output.take(),
            this.c_output.take(),
        ) {
            (Some(Ok(a)), Some(Ok(b)), Some(Ok(c))) => Poll::Ready(Ok((a, b, c))),
            (a, b, c) => {
                *this.a_output = a;
                *this.b_output = b;
                *this.c_output = c;
                Poll::Pending
            }
        }
    }
}

pub fn try_join3<A, B, C, TA, TB, TC, E>(a: A, b: B, c: C) -> TryJoin3<A, B, C>
where
    A: Future<Output = Result<TA, E>>,
    B: Future<Output = Result<TB, E>>,
    C: Future<Output = Result<TC, E>>,
{
    TryJoin3 {
        a,
        b,
        c,
        a_output: None,
        b_output: None,
        c_output: None,
    }
}

pin_project! {
pub struct Select2<A, B> {
        #[pin]
        a: A,
        #[pin]
        b: B,
        first_a: bool,
        randomize: bool,
    }
}

static SELECT_SEED: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

fn next_select_turn() -> bool {
    loop {
        let current = SELECT_SEED.load(Ordering::Relaxed);
        let mut next = current;
        next ^= next << 13;
        next ^= next >> 7;
        next ^= next << 17;
        if SELECT_SEED
            .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return next & 1 == 0;
        }
    }
}

impl<A: Future, B: Future> Future for Select2<A, B> {
    type Output = Either<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let first_a = if *this.randomize {
            let first = *this.first_a;
            *this.first_a = !*this.first_a;
            first
        } else {
            true
        };
        if first_a {
            if let Poll::Ready(output) = this.a.as_mut().poll(cx) {
                return Poll::Ready(Either::Left(output));
            }
            if let Poll::Ready(output) = this.b.as_mut().poll(cx) {
                return Poll::Ready(Either::Right(output));
            }
        } else {
            if let Poll::Ready(output) = this.b.as_mut().poll(cx) {
                return Poll::Ready(Either::Right(output));
            }
            if let Poll::Ready(output) = this.a.as_mut().poll(cx) {
                return Poll::Ready(Either::Left(output));
            }
        }
        Poll::Pending
    }
}

pub fn select2<A: Future, B: Future>(a: A, b: B) -> Select2<A, B> {
    Select2 {
        a,
        b,
        first_a: next_select_turn(),
        randomize: true,
    }
}

pub fn select2_biased<A: Future, B: Future>(a: A, b: B) -> Select2<A, B> {
    Select2 {
        a,
        b,
        first_a: true,
        randomize: false,
    }
}

pub async fn race2<A, B, T>(a: A, b: B) -> T
where
    A: Future<Output = T>,
    B: Future<Output = T>,
{
    match select2(a, b).await {
        Either::Left(value) | Either::Right(value) => value,
    }
}

pub async fn race<A, B, T>(a: A, b: B) -> T
where
    A: Future<Output = T>,
    B: Future<Output = T>,
{
    race2(a, b).await
}

pub struct YieldNow {
    yielded: bool,
}

pub fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Wait for two or three futures, resolving with all of their outputs.
///
/// L9: unlike `std::future::join!` this is not variadic — it supports only 2
/// or 3 arguments. Nest additional futures (`join!(join!(a, b, c), d)`) for
/// more, at the cost of one extra layer of awaits.
#[macro_export]
macro_rules! join {
    ($a:expr, $b:expr $(,)?) => {
        $crate::future::join2($a, $b)
    };
    ($a:expr, $b:expr, $c:expr $(,)?) => {
        async {
            let ((a, b), c) = $crate::future::join2($crate::future::join2($a, $b), $c).await;
            (a, b, c)
        }
    };
}

/// Wait for two or three `Result` futures, resolving with the first error
/// (short-circuiting) or the tuple of outputs. Supports only 2 or 3
/// arguments (L9); nest or use `select!` for larger sets.
#[macro_export]
macro_rules! try_join {
    ($a:expr, $b:expr $(,)?) => {
        $crate::future::try_join2($a, $b)
    };
    ($a:expr, $b:expr, $c:expr $(,)?) => {
        $crate::future::try_join3($a, $b, $c)
    };
}

#[macro_export]
macro_rules! select {
    (biased; $a:pat = $afut:expr => $aout:expr, $b:pat = $bfut:expr => $bout:expr $(,)?) => {{
        match $crate::future::select2_biased($afut, $bfut).await {
            $crate::future::Either::Left($a) => $aout,
            $crate::future::Either::Right($b) => $bout,
        }
    }};
    ($a:pat = $afut:expr => $aout:expr, $b:pat = $bfut:expr => $bout:expr $(,)?) => {{
        match $crate::future::select2($afut, $bfut).await {
            $crate::future::Either::Left($a) => $aout,
            $crate::future::Either::Right($b) => $bout,
        }
    }};
}
