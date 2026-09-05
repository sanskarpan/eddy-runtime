//! Future combinators and cancellation-aware utilities.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

pub use std::future::{pending, poll_fn, ready};

mod join_set;
mod unordered;

pub use join_set::JoinSet;
pub use unordered::FuturesUnordered;

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

pin_project! {
    pub struct Select2Guarded<A: Future, B: Future> {
        #[pin]
        a: A,
        #[pin]
        b: B,
        a_enabled: bool,
        b_enabled: bool,
        first_a: bool,
        randomize: bool,
    }
}

impl<A: Future, B: Future> Select2Guarded<A, B> {
    pub fn new(a: A, a_enabled: bool, b: B, b_enabled: bool, biased: bool) -> Select2Guarded<A, B> {
        Select2Guarded {
            a,
            b,
            a_enabled,
            b_enabled,
            first_a: if biased { true } else { next_select_turn() },
            randomize: !biased,
        }
    }

    /// Disables the `a` branch after its value failed the `select!` pattern.
    pub fn disable_a(self: Pin<&mut Self>) {
        let this = self.project();
        *this.a_enabled = false;
    }

    /// Disables the `b` branch after its value failed the `select!` pattern.
    pub fn disable_b(self: Pin<&mut Self>) {
        let this = self.project();
        *this.b_enabled = false;
    }
}

impl<A: Future, B: Future> Future for Select2Guarded<A, B> {
    /// `Some(Either::...)` when a branch completed, `None` when both branches
    /// are disabled (the `select!` macro then evaluates its `else` expression,
    /// or panics when there is none).
    type Output = Option<Either<A::Output, B::Output>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if !*this.a_enabled && !*this.b_enabled {
            return Poll::Ready(None);
        }
        let first_a = if *this.randomize {
            let first = *this.first_a;
            *this.first_a = !*this.first_a;
            first
        } else {
            true
        };
        if first_a {
            if *this.a_enabled {
                if let Poll::Ready(output) = this.a.as_mut().poll(cx) {
                    return Poll::Ready(Some(Either::Left(output)));
                }
            }
            if *this.b_enabled {
                if let Poll::Ready(output) = this.b.as_mut().poll(cx) {
                    return Poll::Ready(Some(Either::Right(output)));
                }
            }
        } else {
            if *this.b_enabled {
                if let Poll::Ready(output) = this.b.as_mut().poll(cx) {
                    return Poll::Ready(Some(Either::Right(output)));
                }
            }
            if *this.a_enabled {
                if let Poll::Ready(output) = this.a.as_mut().poll(cx) {
                    return Poll::Ready(Some(Either::Left(output)));
                }
            }
        }
        Poll::Pending
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

/// Wait for two or more futures, resolving with all of their outputs.
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
    ($a:expr, $b:expr, $c:expr, $d:expr $(,)?) => {
        async {
            let ((a, b), (c, d)) =
                $crate::future::join2($crate::future::join2($a, $b), $crate::future::join2($c, $d))
                    .await;
            (a, b, c, d)
        }
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr $(,)?) => {
        async {
            let ((a, b, c, d), e) = $crate::future::join2($crate::join!($a, $b, $c, $d), $e).await;
            (a, b, c, d, e)
        }
    };
    ($a:expr, $b:expr, $c:expr, $d:expr, $e:expr, $f:expr $(,)?) => {
        async {
            let ((a, b, c), (d, e, f)) =
                $crate::future::join2($crate::join!($a, $b, $c), $crate::join!($d, $e, $f)).await;
            (a, b, c, d, e, f)
        }
    };
}

/// Wait for two or more `Result` futures, resolving with the first error
/// (short-circuiting) or the tuple of outputs.
#[macro_export]
macro_rules! try_join {
    ($a:expr, $b:expr $(,)?) => {
        $crate::future::try_join2($a, $b)
    };
    ($a:expr, $b:expr, $c:expr $(,)?) => {
        $crate::future::try_join3($a, $b, $c)
    };
    ($a:expr, $b:expr, $c:expr, $d:expr $(,)?) => {
        async {
            match $crate::future::try_join2(
                $crate::future::try_join2($a, $b),
                $crate::future::try_join2($c, $d),
            )
            .await
            {
                Ok(((a, b), (c, d))) => Ok((a, b, c, d)),
                Err(e) => Err(e),
            }
        }
    };
}

/// Wait for one of the branches to complete, running the first branch whose
/// future completes (Tokio-shaped `select!`; 1–3 branches).
///
/// Each branch may carry an optional `, if <precondition>` guard: the
/// precondition is evaluated once when `select!` is entered, and a `false`
/// precondition disables its branch for the whole call (a `loop` re-entering
/// `select!` re-evaluates it). The `<async expression>` of a disabled branch
/// is still evaluated, but its future is never polled. When a branch's future
/// completes but its pattern fails to match, the branch is disabled and
/// `select!` waits on the remaining branches. If all branches are disabled,
/// the `else` expression runs; without an `else`, `select!` panics.
///
/// With `biased;`, the first branch is polled first on every round; otherwise
/// the polling order starts at a pseudo-random turn.
#[macro_export]
macro_rules! select {
    ($a:pat = $afut:expr $(, if $acond:expr)? => $aout:expr $(,)?) => {
        $crate::select!(@internal
            false;
            $a = $afut $(, if $acond)? => $aout,
            _ = $crate::future::pending::<()>(), if false => ::core::unreachable!("single-branch select second branch fired");
            ::core::panic!("all branches are disabled and there is no else branch"))
    };
    ($a:pat = $afut:expr $(, if $acond:expr)? => $aout:expr,
     else => $else:expr $(,)?) => {
        $crate::select!(@internal
            false;
            $a = $afut $(, if $acond)? => $aout,
            _ = $crate::future::pending::<()>(), if false => ::core::unreachable!("single-branch select second branch fired");
            $else)
    };
    (biased;
     $a:pat = $afut:expr $(, if $acond:expr)? => $aout:expr $(,)?) => {
        $crate::select!(@internal
            true;
            $a = $afut $(, if $acond)? => $aout,
            _ = $crate::future::pending::<()>(), if false => ::core::unreachable!("single-branch select second branch fired");
            ::core::panic!("all branches are disabled and there is no else branch"))
    };
    (biased;
     $a:pat = $afut:expr $(, if $acond:expr)? => $aout:expr,
     else => $else:expr $(,)?) => {
        $crate::select!(@internal
            true;
            $a = $afut $(, if $acond)? => $aout,
            _ = $crate::future::pending::<()>(), if false => ::core::unreachable!("single-branch select second branch fired");
            $else)
    };
    (biased;
     $a:pat = $afut:expr $(, if $acond:expr)? => $aout:expr,
     $b:pat = $bfut:expr $(, if $bcond:expr)? => $bout:expr $(,)?) => {
        $crate::select!(@internal
            true;
            $a = $afut $(, if $acond)? => $aout,
            $b = $bfut $(, if $bcond)? => $bout;
            ::core::panic!("all branches are disabled and there is no else branch"))
    };
    (biased;
     $a:pat = $afut:expr $(, if $acond:expr)? => $aout:expr,
     $b:pat = $bfut:expr $(, if $bcond:expr)? => $bout:expr,
     else => $else:expr $(,)?) => {
        $crate::select!(@internal
            true;
            $a = $afut $(, if $acond)? => $aout,
            $b = $bfut $(, if $bcond)? => $bout;
            $else)
    };
    ($a:pat = $afut:expr $(, if $acond:expr)? => $aout:expr,
     $b:pat = $bfut:expr $(, if $bcond:expr)? => $bout:expr,
     else => $else:expr $(,)?) => {
        $crate::select!(@internal
            false;
            $a = $afut $(, if $acond)? => $aout,
            $b = $bfut $(, if $bcond)? => $bout;
            $else)
    };
    ($a:pat = $afut:expr $(, if $acond:expr)? => $aout:expr,
     $b:pat = $bfut:expr $(, if $bcond:expr)? => $bout:expr,
     $c:pat = $cfut:expr $(, if $ccond:expr)? => $cout:expr $(,)?) => {
        $crate::select! {
            $a = $afut $(, if $acond)? => $aout,
            __eddy_rest = async {
                $crate::select! {
                    $b = $bfut $(, if $bcond)? => $bout,
                    $c = $cfut $(, if $ccond)? => $cout,
                }
            } => __eddy_rest,
        }
    };
    ($a:pat = $afut:expr $(, if $acond:expr)? => $aout:expr,
     $b:pat = $bfut:expr $(, if $bcond:expr)? => $bout:expr $(,)?) => {
        $crate::select!(@internal
            false;
            $a = $afut $(, if $acond)? => $aout,
            $b = $bfut $(, if $bcond)? => $bout;
            ::core::panic!("all branches are disabled and there is no else branch"))
    };
    (@internal $biased:literal;
     $a:pat = $afut:expr $(, if $acond:expr)? => $aout:expr,
     $b:pat = $bfut:expr $(, if $bcond:expr)? => $bout:expr;
     $fallback:expr) => {{
        let __eddy_a_enabled = true $(&& ($acond))?;
        let __eddy_b_enabled = true $(&& ($bcond))?;
        let __eddy_select = $crate::future::Select2Guarded::new(
            $afut,
            __eddy_a_enabled,
            $bfut,
            __eddy_b_enabled,
            $biased,
        );
        let mut __eddy_select = ::std::pin::pin!(__eddy_select);
        let __eddy_output = $crate::future::poll_fn(|__eddy_cx| {
            loop {
                match ::std::future::Future::poll(
                    __eddy_select.as_mut(),
                    __eddy_cx,
                ) {
                    ::std::task::Poll::Pending => {
                        return ::std::task::Poll::Pending;
                    }
                    ::std::task::Poll::Ready(None) => {
                        return ::std::task::Poll::Ready(None);
                    }
                    ::std::task::Poll::Ready(Some($crate::future::Either::Left(
                        __eddy_value,
                    ))) => {
                        #[allow(unused_variables)]
                        #[allow(unused_mut)]
                        match &__eddy_value {
                            $a => {}
                            _ => {
                                __eddy_select.as_mut().disable_a();
                                continue;
                            }
                        }
                        return ::std::task::Poll::Ready(Some($crate::future::Either::Left(
                            __eddy_value,
                        )));
                    }
                    ::std::task::Poll::Ready(Some($crate::future::Either::Right(
                        __eddy_value,
                    ))) => {
                        #[allow(unused_variables)]
                        #[allow(unused_mut)]
                        match &__eddy_value {
                            $b => {}
                            _ => {
                                __eddy_select.as_mut().disable_b();
                                continue;
                            }
                        }
                        return ::std::task::Poll::Ready(Some($crate::future::Either::Right(
                            __eddy_value,
                        )));
                    }
                }
            }
        })
        .await;
        match __eddy_output {
            Some($crate::future::Either::Left($a)) => $aout,
            Some($crate::future::Either::Right($b)) => $bout,
            None => $fallback,
            _ => ::core::unreachable!("select! branch value bypassed its pattern check"),
        }
    }};
}
