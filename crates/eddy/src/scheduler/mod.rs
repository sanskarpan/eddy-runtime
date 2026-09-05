//! Schedulers. The current-thread executor and Phase 4 queue primitives live
//! here; the multi-thread worker loop will be layered on top of the queue.

mod current_thread;
mod multi_thread;

pub(crate) use current_thread::CurrentThread;
#[cfg(feature = "instrumentation")]
pub(crate) use multi_thread::current_worker_id;
pub(crate) use multi_thread::{MultiThread, MultiThreadHandle, MultiThreadOptions};
