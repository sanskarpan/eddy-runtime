//! Minimal HTTP/1.1 server with no framework or external dependencies.

#[cfg(unix)]
use eddy::io::{AsyncReadExt, AsyncWriteExt, TcpListener, TcpStream};
#[cfg(unix)]
use std::io;

#[cfg(unix)]
fn main() {
    let runtime = eddy::Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        run().await.expect("HTTP server failed");
    });
}

#[cfg(not(unix))]
fn main() {
    eprintln!("the HTTP example requires Eddy's Unix networking API");
}

#[cfg(unix)]
async fn run() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    println!("HTTP server listening on {}", listener.local_addr()?);
    let handle = eddy::Handle::current();

    loop {
        let (stream, peer) = listener.accept().await?;
        handle.spawn(async move {
            if let Err(error) = serve(stream).await {
                eprintln!("{peer}: {error}");
            }
        });
    }
}

#[cfg(unix)]
async fn serve(mut stream: TcpStream) -> io::Result<()> {
    let mut request = Vec::with_capacity(1024);
    let mut buffer = [0u8; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buffer[..read]);
        if request.len() > 8 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP request headers exceed 8 KiB",
            ));
        }
    }

    let request = String::from_utf8_lossy(&request);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");
    let (status, body) = match path {
        "/" => ("200 OK", "hello from eddy\n"),
        _ => ("404 Not Found", "not found\n"),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}
