use std::path::PathBuf;

fn main() {
    let mut args = std::env::args_os().skip(1);
    let mut socket = eddy_console::default_socket();
    let mut record = None;
    let mut replay = None;
    let mut positional_socket = false;
    while let Some(argument) = args.next() {
        match argument.to_str() {
            Some("--socket") => match args.next() {
                Some(path) => socket = PathBuf::from(path),
                None => usage_error("--socket expects a path"),
            },
            Some("--record") => match args.next() {
                Some(path) => record = Some(PathBuf::from(path)),
                None => usage_error("--record expects a path"),
            },
            Some("--replay") => match args.next() {
                Some(path) => replay = Some(PathBuf::from(path)),
                None => usage_error("--replay expects a path"),
            },
            Some("--help") | Some("-h") => {
                println!("Usage: eddy-console [--socket PATH] [--record PATH] [--replay PATH]");
                return;
            }
            Some(path) if !path.starts_with('-') && !positional_socket => {
                socket = PathBuf::from(path);
                positional_socket = true;
            }
            _ => usage_error("unknown argument"),
        }
    }

    if let Err(error) = eddy_console::run_with_options(eddy_console::ConsoleOptions {
        socket,
        record,
        replay,
    }) {
        eprintln!("eddy-console: {error}");
        std::process::exit(1);
    }
}

fn usage_error(message: &str) -> ! {
    eprintln!("eddy-console: {message}");
    eprintln!("Usage: eddy-console [--socket PATH] [--record PATH] [--replay PATH]");
    std::process::exit(2);
}
