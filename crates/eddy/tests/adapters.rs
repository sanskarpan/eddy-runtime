use eddy::io::{
    copy, empty, repeat, sink, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufStream,
    BufWriter, TcpListener, TcpStream,
};
use eddy::Builder;

#[test]
fn buffered_reader_and_writer_preserve_lines_and_flush_on_shutdown() {
    let runtime = Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = eddy::Handle::current().spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"first\nsecond\n").await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let stream = TcpStream::connect(address).await.unwrap();
        let reader = BufReader::new(stream);
        let mut lines = reader.lines();
        assert_eq!(lines.next_line().await.unwrap().as_deref(), Some("first\n"));
        assert_eq!(
            lines.next_line().await.unwrap().as_deref(),
            Some("second\n")
        );
        assert_eq!(lines.next_line().await.unwrap(), None);
        server.await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = eddy::Handle::current().spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            received
        });
        let stream = TcpStream::connect(address).await.unwrap();
        let mut writer = BufWriter::new(stream);
        writer.write_all(b"buffered").await.unwrap();
        writer.shutdown().await.unwrap();
        assert_eq!(server.await.unwrap(), b"buffered");
    });
}

#[test]
fn buf_stream_and_basic_io_adapters_work() {
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
        let mut stream = BufStream::new(stream);
        stream.write_all(b"ping").await.unwrap();
        stream.flush().await.unwrap();
        let mut response = [0u8; 4];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"pong");
        server.await.unwrap();

        let mut source = empty();
        let mut destination = sink();
        assert_eq!(copy(&mut source, &mut destination).await.unwrap(), 0);
        let mut repeated = repeat(b'x');
        let mut bytes = [0u8; 4];
        repeated.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"xxxx");
    });
}
