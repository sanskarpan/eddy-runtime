//! Minimal TCP echo server using Eddy's readiness-backed sockets.

#[cfg(unix)]
use eddy::io::{AsyncReadExt, AsyncWriteExt, TcpListener};

#[cfg(unix)]
fn main() {
    let runtime = eddy::Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:8080").expect("could not bind echo server");
        println!(
            "echo server listening on {}",
            listener.local_addr().unwrap()
        );
        let handle = eddy::Handle::current();

        loop {
            let (mut stream, peer) = listener.accept().await.expect("accept failed");
            handle.spawn(async move {
                let mut buffer = [0u8; 8 * 1024];
                loop {
                    let read = stream.read(&mut buffer).await?;
                    if read == 0 {
                        break;
                    }
                    stream.write_all(&buffer[..read]).await?;
                }
                Ok::<(), std::io::Error>(())
            });
            println!("accepted {peer}");
        }
    });
}

#[cfg(not(unix))]
fn main() {
    eprintln!("the echo example requires Eddy's Unix networking API");
}
