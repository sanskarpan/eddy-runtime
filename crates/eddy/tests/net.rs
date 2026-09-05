#![cfg(unix)]

use eddy::io::{AsyncReadExt, AsyncWriteExt, TcpListener, TcpStream, UdpSocket};
use eddy::Builder;
use std::time::Duration;

#[test]
fn tcp_accept_connect_and_echo() {
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = eddy::Handle::current().spawn(async move {
            let (mut stream, peer) = listener.accept().await.unwrap();
            assert_eq!(peer.ip(), address.ip());
            let mut request = [0u8; 5];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"hello");
            stream.write_all(b"world").await.unwrap();
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        let mut response = [0u8; 5];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"world");
        server.await.unwrap();
    });
}

#[test]
fn udp_send_to_and_receive_from_round_trip() {
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receiver_address = receiver.local_addr().unwrap();
        sender.send_to(b"datagram", receiver_address).await.unwrap();

        let mut buffer = [0u8; 32];
        let (size, sender_address) = receiver.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..size], b"datagram");
        assert_eq!(sender_address, sender.local_addr().unwrap());
    });
}

#[test]
fn owned_tcp_split_supports_concurrent_directions() {
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = eddy::Handle::current().spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 4];
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(b"pong").await.unwrap();
        });

        let stream = TcpStream::connect(address).await.unwrap();
        let (mut read, mut write) = stream.into_split();
        write.write_all(b"ping").await.unwrap();
        let mut response = [0u8; 4];
        read.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"pong");
        server.await.unwrap();
    });
}

#[test]
fn tcp_large_transfer_and_half_close_reach_eof() {
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = eddy::Handle::current().spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            received
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.set_nodelay(true).unwrap();
        client.set_linger(Some(Duration::from_secs(1))).unwrap();
        let payload = vec![0x5au8; 100 * 1024 * 1024];
        client.write_all(&payload).await.unwrap();
        client.shutdown().await.unwrap();
        assert_eq!(server.await.unwrap(), payload);
    });
}

#[test]
fn tcp_peek_does_not_consume_data_and_closed_port_returns_error() {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = std_listener.local_addr().unwrap();
    drop(std_listener);

    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let error = match TcpStream::connect(address).await {
            Ok(_) => panic!("connect unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::TimedOut
        ));

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = eddy::Handle::current().spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"peek").await.unwrap();
        });
        let client = TcpStream::connect(address).await.unwrap();
        let mut peeked = [0u8; 4];
        assert_eq!(client.peek(&mut peeked).await.unwrap(), 4);
        assert_eq!(&peeked, b"peek");
        let mut received = [0u8; 4];
        let mut client = client;
        client.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"peek");
        server.await.unwrap();
    });
}

#[test]
fn tcp_echo_handles_many_concurrent_connections() {
    #[cfg(target_os = "macos")]
    const CONNECTIONS: usize = 64;
    #[cfg(not(target_os = "macos"))]
    const CONNECTIONS: usize = 1_000;
    let runtime = Builder::new_multi_thread().worker_threads(4).build();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = eddy::Handle::current().spawn(async move {
            let handle = eddy::Handle::current();
            let mut workers = Vec::with_capacity(CONNECTIONS);
            for _ in 0..CONNECTIONS {
                let (stream, _) = listener.accept().await.unwrap();
                workers.push(handle.spawn(async move {
                    let mut stream = stream;
                    let mut request = [0u8; 4];
                    stream.read_exact(&mut request).await.unwrap();
                    stream.write_all(&request).await.unwrap();
                }));
            }
            for worker in workers {
                worker.await.unwrap();
            }
        });

        let mut clients = Vec::with_capacity(CONNECTIONS);
        for _ in 0..CONNECTIONS {
            clients.push(eddy::Handle::current().spawn(async move {
                let mut stream = TcpStream::connect(address).await.unwrap();
                stream.write_all(b"ping").await.unwrap();
                let mut response = [0u8; 4];
                stream.read_exact(&mut response).await.unwrap();
                assert_eq!(&response, b"ping");
            }));
            eddy::future::yield_now().await;
        }
        for client in clients {
            client.await.unwrap();
        }
        server.await.unwrap();
    });
}
