#![cfg(unix)]

use std::io::Write;
use std::os::unix::net::UnixStream as StdUnixStream;

use eddy::io::{AsyncReadExt, PollEvented};
use eddy::{io::Interest, Builder};

#[test]
fn poll_evented_registers_a_duplicate_without_double_closing_the_wrapped_io() {
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let (left, mut right) = StdUnixStream::pair().unwrap();
        let mut wrapped =
            PollEvented::new(left, Interest::READABLE.add(Interest::WRITABLE)).unwrap();
        right.write_all(b"wrapped").unwrap();
        let mut buffer = [0u8; 7];
        wrapped.read_exact(&mut buffer).await.unwrap();
        assert_eq!(&buffer, b"wrapped");
        let _original = wrapped.into_inner();
    });
}
