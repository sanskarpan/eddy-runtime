//! Building blocks for the multi-thread work-stealing scheduler.

mod inject;
mod queue;
mod worker;

#[allow(unused_imports)]
pub(crate) use inject::Injector;
#[allow(unused_imports)]
pub(crate) use queue::{Local, LOCAL_QUEUE_CAPACITY};
#[cfg(feature = "instrumentation")]
pub(crate) use worker::current_worker_id;
pub(crate) use worker::{MultiThread, MultiThreadHandle, MultiThreadOptions};
