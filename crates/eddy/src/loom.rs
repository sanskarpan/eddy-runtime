//! Re-exports either `std::sync` or `loom::sync` depending on the `loom`
//! cfg, so the rest of the crate is written once against `crate::loom::*`
//! and gets exhaustive interleaving checking for free under
//! `RUSTFLAGS="--cfg loom" cargo test`. Extend with `Arc`/`thread`/etc. as
//! later phases (Chase-Lev deque, injector) need loom-aware versions of
//! them — only `atomic` is used so far.

#[cfg(not(loom))]
pub(crate) mod sync {
    #[allow(unused_imports)]
    pub(crate) use std::sync::atomic;
    #[allow(unused_imports)]
    pub(crate) use std::sync::{Arc, Mutex};
}

#[cfg(loom)]
pub(crate) mod sync {
    #[allow(unused_imports)]
    pub(crate) use loom::sync::atomic;
    #[allow(unused_imports)]
    pub(crate) use loom::sync::{Arc, Mutex};
}

#[cfg(not(loom))]
pub(crate) mod thread {
    #[allow(unused_imports)]
    pub(crate) use std::thread::{current, park, yield_now, Thread, ThreadId};
}

#[cfg(loom)]
pub(crate) mod thread {
    #[allow(unused_imports)]
    pub(crate) use loom::thread::{current, park, yield_now, Thread, ThreadId};
}
