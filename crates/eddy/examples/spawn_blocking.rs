//! Bridge synchronous file I/O onto Eddy's dedicated blocking pool.

use eddy::{Builder, Handle};
use std::io;

fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Cargo.toml".to_owned());
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    let result = runtime.block_on(async move {
        let handle = Handle::current();
        let read = handle.spawn_blocking(move || std::fs::read_to_string(&path));
        read.await
            .expect("blocking file task panicked")
            .map(|contents| (contents.len(), contents.lines().count()))
    });

    match result {
        Ok((bytes, lines)) => println!("read {bytes} bytes across {lines} line(s)"),
        Err(error) => report_error(error),
    }
}

fn report_error(error: io::Error) {
    eprintln!("blocking read failed: {error}");
}
