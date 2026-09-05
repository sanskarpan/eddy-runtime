//! WebSocket bridge between the `eddy` runtime and the `console-ui` React app.
//!
//! Each browser connection gets its own Unix-socket subscription. The runtime
//! socket already broadcasts length-prefixed frames to every subscriber, so
//! this keeps clients independent and prevents one slow browser from holding
//! up the bridge or another browser.

use std::fmt::Write as _;
use std::io::{self, Read};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};

use eddy_console::{Event, FrameDecoder, PollResult, UnparkReason, WakeSource};
use tungstenite::{accept, Message, WebSocket};

const DEFAULT_BIND: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 9001);
const MAX_HTTP_ERROR: &str = "websocket bridge error";

/// Configuration for the web bridge listener.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BridgeConfig {
    pub socket_path: PathBuf,
    pub bind_addr: SocketAddr,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            socket_path: default_socket(),
            bind_addr: std::env::var("EDDY_WEB_ADDR")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_BIND),
        }
    }
}

/// Return the runtime instrumentation socket used by both consoles.
pub fn default_socket() -> PathBuf {
    std::env::var_os("EDDY_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/eddy-instrumentation.sock"))
}

/// Listen for browser WebSocket connections and bridge them to the runtime.
pub fn run(config: BridgeConfig) -> io::Result<()> {
    let listener = TcpListener::bind(config.bind_addr)?;
    eprintln!(
        "eddy-console-web listening on ws://{}/ws (socket: {})",
        config.bind_addr,
        config.socket_path.display()
    );

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let socket_path = config.socket_path.clone();
                std::thread::Builder::new()
                    .name("eddy-console-web-client".to_string())
                    .spawn(move || {
                        if let Err(error) = serve_client(stream, &socket_path) {
                            if error.kind() != io::ErrorKind::BrokenPipe {
                                eprintln!("eddy-console-web client: {error}");
                            }
                        }
                    })
                    .map_err(io::Error::other)?;
            }
            Err(error) => eprintln!("eddy-console-web accept: {error}"),
        }
    }
    Ok(())
}

/// Serve one WebSocket client until the runtime socket closes or either side
/// reports an error.
pub fn serve_client(stream: TcpStream, socket_path: impl AsRef<Path>) -> io::Result<()> {
    let mut websocket = accept(stream).map_err(|error| io::Error::other(error.to_string()))?;
    let mut unix = match std::os::unix::net::UnixStream::connect(socket_path.as_ref()) {
        Ok(stream) => stream,
        Err(error) => {
            send_status(
                &mut websocket,
                "error",
                &format!("could not connect to runtime socket: {error}"),
            )?;
            return Err(error);
        }
    };
    let mut decoder = FrameDecoder::new();
    let mut bytes = [0_u8; 8192];

    loop {
        match unix.read(&mut bytes) {
            Ok(0) => {
                let reason = match decoder.finish() {
                    Ok(()) => "runtime disconnected".to_string(),
                    Err(error) => format!("runtime disconnected with protocol error: {error}"),
                };
                send_status(&mut websocket, "disconnected", &reason)?;
                return Ok(());
            }
            Ok(count) => match decoder.push(&bytes[..count]) {
                Ok(events) => {
                    for event in events {
                        websocket
                            .send(Message::Text(event_to_json(&event).into()))
                            .map_err(tungstenite_error)?;
                    }
                }
                Err(error) => {
                    let reason = format!("protocol error: {error}");
                    send_status(&mut websocket, "error", &reason)?;
                    return Err(io::Error::new(io::ErrorKind::InvalidData, reason));
                }
            },
            Err(error) => {
                let reason = format!("socket error: {error}");
                send_status(&mut websocket, "error", &reason)?;
                return Err(error);
            }
        }
    }
}

fn send_status<S>(websocket: &mut WebSocket<S>, state: &str, reason: &str) -> io::Result<()>
where
    S: io::Read + io::Write,
{
    let mut message = String::from("{\"type\":\"bridge_status\",\"state\":");
    json_string(&mut message, state);
    message.push_str(",\"reason\":");
    json_string(&mut message, reason);
    message.push('}');
    websocket
        .send(Message::Text(message.into()))
        .map_err(tungstenite_error)
}

fn tungstenite_error(error: tungstenite::Error) -> io::Error {
    io::Error::other(format!("{MAX_HTTP_ERROR}: {error}"))
}

fn json_string(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                let _ = write!(output, "\\u{:04x}", character as u32);
            }
            character => output.push(character),
        }
    }
    output.push('"');
}

fn optional_string(output: &mut String, value: Option<&str>) {
    match value {
        Some(value) => json_string(output, value),
        None => output.push_str("null"),
    }
}

fn optional_u64(output: &mut String, value: Option<u64>) {
    match value {
        Some(value) => {
            let _ = write!(output, "{value}");
        }
        None => output.push_str("null"),
    }
}

fn location_json(output: &mut String, file: &str, line: u32) {
    output.push_str("{\"file\":");
    json_string(output, file);
    let _ = write!(output, ",\"line\":{line}}}");
}

/// Serialize the shared console event model into the browser wire format.
/// Keeping this format explicit avoids adding serde to the runtime crate.
pub fn event_to_json(event: &Event) -> String {
    let mut output = String::new();
    match event {
        Event::TaskSpawned {
            id,
            name,
            location,
            parent,
        } => {
            let _ = write!(output, "{{\"type\":\"task_spawned\",\"id\":{id},\"name\":");
            optional_string(&mut output, name.as_deref());
            output.push_str(",\"location\":");
            location_json(&mut output, &location.file, location.line);
            output.push_str(",\"parent\":");
            optional_u64(&mut output, *parent);
            output.push('}');
        }
        Event::TaskPollStart { id, worker, at_ns } => {
            let _ = write!(
                output,
                "{{\"type\":\"task_poll_start\",\"id\":{id},\"worker\":{worker},\"at_ns\":{at_ns}}}"
            );
        }
        Event::TaskPollEnd {
            id,
            worker,
            duration_ns,
            result,
        } => {
            let result = match result {
                PollResult::Ready => "ready",
                PollResult::Pending => "pending",
                PollResult::Panicked => "panicked",
            };
            let _ = write!(
                output,
                "{{\"type\":\"task_poll_end\",\"id\":{id},\"worker\":{worker},\"duration_ns\":{duration_ns},\"result\":\"{result}\"}}"
            );
        }
        Event::TaskWoken { id, by } => {
            let _ = write!(output, "{{\"type\":\"task_woken\",\"id\":{id},\"by\":");
            wake_source_json(&mut output, by);
            output.push('}');
        }
        Event::TaskDropped {
            id,
            total_polls,
            total_busy_ns,
            total_idle_ns,
        } => {
            let _ = write!(
                output,
                "{{\"type\":\"task_dropped\",\"id\":{id},\"total_polls\":{total_polls},\"total_busy_ns\":{total_busy_ns},\"total_idle_ns\":{total_idle_ns}}}"
            );
        }
        Event::TaskAborted { id } => {
            let _ = write!(output, "{{\"type\":\"task_aborted\",\"id\":{id}}}");
        }
        Event::WorkerPark { worker, timeout_ns } => {
            let _ = write!(
                output,
                "{{\"type\":\"worker_park\",\"worker\":{worker},\"timeout_ns\":"
            );
            optional_u64(&mut output, *timeout_ns);
            output.push('}');
        }
        Event::WorkerUnpark { worker, reason } => {
            let _ = write!(
                output,
                "{{\"type\":\"worker_unpark\",\"worker\":{worker},\"reason\":"
            );
            json_string(&mut output, unpark_reason_name(*reason));
            output.push('}');
        }
        Event::WorkerSteal {
            thief,
            victim,
            count,
        } => {
            let _ = write!(output, "{{\"type\":\"worker_steal\",\"thief\":{thief},\"victim\":{victim},\"count\":{count}}}");
        }
        Event::QueueDepth {
            worker,
            local,
            global,
            lifo,
        } => {
            let _ = write!(output, "{{\"type\":\"queue_depth\",\"worker\":{worker},\"local\":{local},\"global\":{global},\"lifo\":{lifo}}}");
        }
        Event::IoRegistered { fd, interest, task } => {
            let _ = write!(
                output,
                "{{\"type\":\"io_registered\",\"fd\":{fd},\"interest\":"
            );
            json_string(&mut output, interest);
            let _ = write!(output, ",\"task\":{task}}}");
        }
        Event::IoReady {
            fd,
            readiness,
            woke,
        } => {
            let _ = write!(output, "{{\"type\":\"io_ready\",\"fd\":{fd},\"readiness\":");
            json_string(&mut output, readiness);
            output.push_str(",\"woke\":[");
            for (index, task) in woke.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                let _ = write!(output, "{task}");
            }
            output.push_str("]}");
        }
        Event::TimerSet {
            id,
            deadline_ns,
            task,
        } => {
            let _ = write!(output, "{{\"type\":\"timer_set\",\"id\":{id},\"deadline_ns\":{deadline_ns},\"task\":{task}}}");
        }
        Event::TimerFired { id, lateness_ns } => {
            let _ = write!(
                output,
                "{{\"type\":\"timer_fired\",\"id\":{id},\"lateness_ns\":{lateness_ns}}}"
            );
        }
        Event::TimerCancelled { id } => {
            let _ = write!(output, "{{\"type\":\"timer_cancelled\",\"id\":{id}}}");
        }
        Event::BlockingDetected {
            task,
            poll_duration_ns,
            location,
        } => {
            let _ = write!(output, "{{\"type\":\"blocking_detected\",\"task\":{task},\"poll_duration_ns\":{poll_duration_ns},\"location\":");
            location_json(&mut output, &location.file, location.line);
            output.push('}');
        }
        Event::BudgetExhausted { task } => {
            let _ = write!(output, "{{\"type\":\"budget_exhausted\",\"task\":{task}}}");
        }
        Event::ResourceContended {
            kind,
            holder,
            waiters,
        } => {
            let _ = write!(output, "{{\"type\":\"resource_contended\",\"kind\":");
            json_string(&mut output, kind);
            let _ = write!(output, ",\"holder\":{holder},\"waiters\":[");
            for (index, waiter) in waiters.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                let _ = write!(output, "{waiter}");
            }
            output.push_str("]}");
        }
    }
    output
}

fn wake_source_json(output: &mut String, source: &WakeSource) {
    match source {
        WakeSource::Io { fd } => {
            let _ = write!(output, "{{\"kind\":\"io\",\"fd\":{fd}}}");
        }
        WakeSource::Timer { id } => {
            let _ = write!(output, "{{\"kind\":\"timer\",\"id\":{id}}}");
        }
        WakeSource::Task(id) => {
            let _ = write!(output, "{{\"kind\":\"task\",\"id\":{id}}}");
        }
        WakeSource::Channel { kind } => {
            output.push_str("{\"kind\":\"channel\",\"channel\":");
            json_string(output, kind);
            output.push('}');
        }
        WakeSource::External => output.push_str("{\"kind\":\"external\"}"),
    }
}

fn unpark_reason_name(reason: UnparkReason) -> &'static str {
    match reason {
        UnparkReason::Task => "task",
        UnparkReason::Shutdown => "shutdown",
        UnparkReason::Steal => "steal",
        UnparkReason::Event => "event",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::net::UnixListener;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tungstenite::client::connect;

    fn socket_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "eddy-console-web-{}-{nonce}.sock",
            std::process::id()
        ))
    }

    #[test]
    fn event_json_escapes_text_and_preserves_wire_fields() {
        let json = event_to_json(&Event::TaskSpawned {
            id: 7,
            name: Some("worker\"task".to_string()),
            location: eddy_console::Location {
                file: "src/main.rs".to_string(),
                line: 42,
            },
            parent: Some(2),
        });
        assert_eq!(
            json,
            r#"{"type":"task_spawned","id":7,"name":"worker\"task","location":{"file":"src/main.rs","line":42},"parent":2}"#
        );
    }

    #[test]
    fn websocket_client_receives_a_unix_socket_event() {
        let unix_path = socket_path();
        let unix_listener = UnixListener::bind(&unix_path).unwrap();
        let unix_thread = std::thread::spawn(move || {
            let (mut stream, _) = unix_listener.accept().unwrap();
            let payload = [16_u8, 9, 0, 0, 0, 0, 0, 0, 0];
            stream
                .write_all(&(payload.len() as u32).to_le_bytes())
                .unwrap();
            stream.write_all(&payload).unwrap();
        });

        let tcp_listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = tcp_listener.local_addr().unwrap();
        let bridge_path = unix_path.clone();
        let bridge_thread = std::thread::spawn(move || {
            let (stream, _) = tcp_listener.accept().unwrap();
            serve_client(stream, bridge_path).unwrap();
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let (mut websocket, _) = loop {
            match connect(format!("ws://{address}/ws")) {
                Ok(value) => break value,
                Err(error) if std::time::Instant::now() < deadline => {
                    let _ = error;
                    std::thread::sleep(Duration::from_millis(2));
                }
                Err(error) => panic!("could not connect to bridge: {error}"),
            }
        };
        let message = websocket.read().unwrap();
        assert_eq!(
            message,
            Message::Text(r#"{"type":"budget_exhausted","task":9}"#.into())
        );

        bridge_thread.join().unwrap();
        unix_thread.join().unwrap();
        let _ = std::fs::remove_file(unix_path);
    }
}
