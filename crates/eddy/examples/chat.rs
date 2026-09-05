//! Small line-oriented chat server using Eddy's broadcast channel.

#[cfg(unix)]
use eddy::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, TcpListener, TcpStream};
#[cfg(unix)]
use eddy::sync::broadcast::{self, Receiver, RecvError, Sender};
#[cfg(unix)]
use std::io;
#[cfg(unix)]
use std::net::SocketAddr;

#[cfg(unix)]
fn main() {
    let runtime = eddy::Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        run().await.expect("chat server failed");
    });
}

#[cfg(not(unix))]
fn main() {
    eprintln!("the chat example requires Eddy's Unix networking API");
}

#[cfg(unix)]
async fn run() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7000")?;
    let address = listener.local_addr()?;
    let (messages, announcements) = broadcast::channel::<String>(256);
    let handle = eddy::Handle::current();

    println!("chat server listening on {address}");
    loop {
        let (stream, peer) = listener.accept().await?;
        let messages = messages.clone();
        let incoming = announcements.clone();
        handle.spawn(async move {
            if let Err(error) = serve_client(stream, peer, messages, incoming).await {
                eprintln!("{peer}: {error}");
            }
        });
    }
}

#[cfg(unix)]
async fn serve_client(
    stream: TcpStream,
    peer: SocketAddr,
    messages: Sender<String>,
    mut incoming: Receiver<String>,
) -> io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let writer = eddy::Handle::current().spawn(async move {
        loop {
            match (&mut incoming).await {
                Ok(message) => {
                    write_half.write_all(message.as_bytes()).await?;
                    write_half.write_all(b"\n").await?;
                }
                Err(RecvError::Lagged(skipped)) => {
                    eprintln!("{peer}: skipped {skipped} chat message(s)");
                }
                Err(RecvError::Closed) => break,
            }
        }
        Ok::<(), io::Error>(())
    });

    let joined = format!("{peer} joined");
    let _ = messages.send(joined);
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }
        let message = line.trim_end().to_owned();
        if !message.is_empty() {
            let _ = messages.send(format!("{peer}: {message}"));
        }
    }
    let _ = messages.send(format!("{peer} left"));
    writer.abort();
    let _ = writer.await;
    Ok(())
}
