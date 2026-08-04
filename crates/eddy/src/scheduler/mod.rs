//! Schedulers. Only the current-thread executor exists in this slice.

mod current_thread;

pub(crate) use current_thread::CurrentThread;
