#![deny(clippy::undocumented_unsafe_blocks)]

pub(crate) mod blocking;
pub(crate) mod loom;
pub(crate) mod sys;
pub(crate) mod util;

pub mod cancellation;
pub mod coop;
pub mod future;
pub(crate) mod instrument;
pub mod io;
pub mod runtime;
pub mod scheduler;
pub mod stream;
pub mod sync;
pub mod task;
pub mod time;

pub use cancellation::CancellationToken;
#[cfg(feature = "instrumentation")]
pub use instrument::{
    clear_subscriber, install_unix_socket, set_subscriber, PollResult, RuntimeEvent, Subscriber,
    UnparkReason, WakeSource,
};
pub use instrument::{Location, TaskId, TaskSnapshot, TaskState};
pub use instrument::{RuntimeMetrics, RuntimeMetricsSnapshot};
pub use runtime::{Builder, Handle, Runtime};
pub use task::{noop_waker, AbortHandle, JoinError, JoinHandle};

pub use eddy_macros::{main, test};
