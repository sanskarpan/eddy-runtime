//! Graceful TCP server shutdown driven by Enter on stdin.

#[cfg(unix)]
use eddy::future::JoinSet;
#[cfg(unix)]
use eddy::io::{AsyncReadExt, AsyncWriteExt, TcpListener, TcpStream};
#[cfg(unix)]
use eddy::sync::oneshot;
#[cfg(unix)]
use eddy::CancellationToken;
#[cfg(unix)]
use std::io;
#[cfg(unix)]
use std::time::Duration;

#[cfg(unix)]
fn main() {
    let runtime = eddy::Builder::new_multi_thread().worker_threads(2).build();
    let result = runtime.block_on(run());
    if let Err(error) = result {
        eprintln!("graceful server failed: {error}");
    }
    runtime.shutdown_timeout(Duration::from_secs(2));
}

#[cfg(not(unix))]
fn main() {
    eprintln!("the graceful shutdown example requires Eddy's Unix networking API");
}

#[cfg(unix)]
async fn run() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7001")?;
    println!(
        "graceful server listening on {}; press Enter to stop",
        listener.local_addr()?
    );

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
        let _ = shutdown_tx.send(());
    });

    let token = CancellationToken::new();
    let handle = eddy::Handle::current();
    let mut workers = JoinSet::new();
    loop {
        let stop = eddy::select! {
            result = listener.accept() => {
                let (stream, peer) = result?;
                workers.spawn(&handle, serve(stream, token.child_token()));
                println!("accepted {peer}");
                false
            },
            _ = &mut shutdown_rx => true,
        };
        if stop {
            break;
        }
    }

    println!("stopping accepts and draining connections");
    token.cancel();
    while let Some(result) = workers.join_next().await {
        let _ = result;
    }
    Ok(())
}

#[cfg(unix)]
async fn serve(mut stream: TcpStream, token: CancellationToken) -> io::Result<()> {
    let mut buffer = [0u8; 8 * 1024];
    loop {
        let read = eddy::select! {
            result = stream.read(&mut buffer) => result?,
            _ = token.cancelled() => return Ok(()),
        };
        if read == 0 {
            return Ok(());
        }
        stream.write_all(&buffer[..read]).await?;
    }
}
