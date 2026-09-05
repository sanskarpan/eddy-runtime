use std::net::SocketAddr;
use std::path::PathBuf;

fn main() {
    let mut args = std::env::args_os().skip(1);
    let mut config = eddy_console_web::BridgeConfig::default();

    while let Some(argument) = args.next() {
        match argument.to_str() {
            Some("--socket") => {
                config.socket_path = args
                    .next()
                    .map(PathBuf::from)
                    .unwrap_or_else(eddy_console_web::default_socket);
            }
            Some("--bind") => {
                let value = args.next().unwrap_or_default();
                config.bind_addr = value
                    .to_str()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_else(|| {
                        eprintln!("eddy-console-web: --bind expects a socket address");
                        std::process::exit(2);
                    });
            }
            Some("--port") => {
                let value = args.next().unwrap_or_default();
                let port = value
                    .to_str()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_else(|| {
                        eprintln!("eddy-console-web: --port expects a number");
                        std::process::exit(2);
                    });
                config.bind_addr = SocketAddr::new(config.bind_addr.ip(), port);
            }
            Some("--help") | Some("-h") => {
                println!("Usage: eddy-console-web [--socket PATH] [--bind ADDR] [--port PORT]");
                return;
            }
            Some(argument) => {
                eprintln!("eddy-console-web: unknown argument {argument}");
                std::process::exit(2);
            }
            None => {
                eprintln!("eddy-console-web: arguments must be valid UTF-8");
                std::process::exit(2);
            }
        }
    }

    if let Err(error) = eddy_console_web::run(config) {
        eprintln!("eddy-console-web: {error}");
        std::process::exit(1);
    }
}
