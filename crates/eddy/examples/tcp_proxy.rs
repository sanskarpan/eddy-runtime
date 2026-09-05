//! Bidirectional TCP proxy. Set EDDY_PROXY_TARGET to the upstream address.

#[cfg(unix)]
use eddy::io::{copy_bidirectional, TcpListener, TcpStream};
#[cfg(unix)]
use std::io;

#[cfg(unix)]
fn main() {
    let runtime = eddy::Builder::new_multi_thread().worker_threads(2).build();
    runtime.block_on(async {
        run().await.expect("TCP proxy failed");
    });
}

#[cfg(not(unix))]
fn main() {
    eprintln!("the TCP proxy example requires Eddy's Unix networking API");
}

#[cfg(unix)]
async fn run() -> io::Result<()> {
    let target = std::env::var("EDDY_PROXY_TARGET").unwrap_or_else(|_| "127.0.0.1:9001".to_owned());
    let listener = TcpListener::bind("127.0.0.1:9000")?;
    println!(
        "TCP proxy listening on {} and forwarding to {target}",
        listener.local_addr()?
    );
    let handle = eddy::Handle::current();

    loop {
        let (client, peer) = listener.accept().await?;
        let target = target.clone();
        handle.spawn(async move {
            if let Err(error) = proxy(client, &target).await {
                eprintln!("{peer}: {error}");
            }
        });
    }
}

#[cfg(unix)]
async fn proxy(mut client: TcpStream, target: &str) -> io::Result<()> {
    let mut upstream = TcpStream::connect(target).await?;
    let (client_to_upstream, upstream_to_client) =
        copy_bidirectional(&mut client, &mut upstream).await?;
    println!(
        "proxy connection copied {client_to_upstream} bytes upstream and {upstream_to_client} bytes downstream"
    );
    Ok(())
}
