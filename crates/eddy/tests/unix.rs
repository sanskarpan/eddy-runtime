#![cfg(unix)]

use std::path::PathBuf;

use eddy::io::{AsyncReadExt, AsyncWriteExt, UnixDatagram, UnixListener, UnixStream};
use eddy::Builder;

fn path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("eddy-{}-{name}", std::process::id()))
}

#[test]
fn unix_stream_connect_accept_and_echo() {
    let socket_path = path("stream");
    let _ = std::fs::remove_file(&socket_path);
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = eddy::Handle::current().spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4];
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(b"pong").await.unwrap();
        });
        let mut client = UnixStream::connect(&socket_path).await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut response = [0u8; 4];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"pong");
        server.await.unwrap();
    });
    let _ = std::fs::remove_file(socket_path);
}

#[test]
fn unix_datagram_send_to_and_receive_from() {
    let first = path("datagram-a");
    let second = path("datagram-b");
    let _ = std::fs::remove_file(&first);
    let _ = std::fs::remove_file(&second);
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let receiver = UnixDatagram::bind(&first).unwrap();
        let sender = UnixDatagram::bind(&second).unwrap();
        sender.send_to(b"unix", &first).await.unwrap();
        let mut buffer = [0u8; 8];
        let (size, address) = receiver.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..size], b"unix");
        assert_eq!(address.as_pathname(), Some(second.as_path()));
    });
    let _ = std::fs::remove_file(first);
    let _ = std::fs::remove_file(second);
}
