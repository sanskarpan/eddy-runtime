use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::future::Future;
use std::hint::black_box;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::time::Duration;

#[cfg(not(test))]
const SPAWN_TASKS: &[usize] = &[10_000, 100_000, 1_000_000];
#[cfg(test)]
const SPAWN_TASKS: &[usize] = &[1_000, 10_000];
#[cfg(not(test))]
const PING_PONG_ROUNDS: usize = 1_000_000;
#[cfg(test)]
const PING_PONG_ROUNDS: usize = 1_000;
#[cfg(not(test))]
const ECHO_CONNECTIONS: usize = 10_000;
#[cfg(test)]
const ECHO_CONNECTIONS: usize = 100;
const ECHO_MESSAGE: [u8; 64] = [b'e'; 64];
#[cfg(not(test))]
const TIMER_COUNT: usize = 100_000;
#[cfg(test)]
const TIMER_COUNT: usize = 1_000;
#[cfg(not(test))]
const CHANNEL_MESSAGES: usize = 100_000;
#[cfg(test)]
const CHANNEL_MESSAGES: usize = 1_000;
#[cfg(not(test))]
const MUTEX_ITERATIONS: usize = 1_000;
#[cfg(test)]
const MUTEX_ITERATIONS: usize = 10;

fn eddy_current() -> eddy::Runtime {
    eddy::Builder::new_current_thread().build()
}

fn tokio_current() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Tokio runtime construction failed")
}

fn eddy_multi() -> eddy::Runtime {
    eddy::Builder::new_multi_thread().worker_threads(2).build()
}

fn tokio_multi() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("Tokio runtime construction failed")
}

fn run_eddy_spawn_join(tasks: usize) {
    let runtime = eddy_current();
    runtime.block_on(async move {
        let handle = eddy::Handle::current();
        let mut set = eddy::future::JoinSet::new();
        for task in 0..tasks {
            set.spawn(&handle, async move { task });
        }

        let mut sum = 0usize;
        while let Some(result) = set.join_next().await {
            sum += result.expect("Eddy task failed");
        }
        black_box(sum);
    });
}

fn run_tokio_spawn_join(tasks: usize) {
    let runtime = tokio_current();
    runtime.block_on(async move {
        let mut set = tokio::task::JoinSet::new();
        for task in 0..tasks {
            set.spawn(async move { task });
        }

        let mut sum = 0usize;
        while let Some(result) = set.join_next().await {
            sum += result.expect("Tokio task failed");
        }
        black_box(sum);
    });
}

fn spawn_join_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("spawn-join");
    group.sample_size(10);
    for &tasks in SPAWN_TASKS {
        group.throughput(Throughput::Elements(tasks as u64));
        group.bench_with_input(BenchmarkId::new("eddy", tasks), &tasks, |b, &tasks| {
            b.iter(|| run_eddy_spawn_join(tasks));
        });
        group.bench_with_input(BenchmarkId::new("tokio", tasks), &tasks, |b, &tasks| {
            b.iter(|| run_tokio_spawn_join(tasks));
        });
    }
    group.finish();
}

fn run_eddy_ping_pong(rounds: usize) {
    let runtime = eddy_multi();
    runtime.block_on(async move {
        let handle = eddy::Handle::current();
        let (ping_tx, mut ping_rx) = eddy::sync::mpsc::channel(1);
        let (pong_tx, mut pong_rx) = eddy::sync::mpsc::channel(1);
        ping_tx.try_send(0usize).expect("initial ping send failed");

        let pong = handle.spawn(async move {
            for _ in 0..rounds {
                let value = ping_rx.recv().await.expect("ping channel closed");
                pong_tx.send(value + 1).await.expect("pong send failed");
            }
        });
        let ping = handle.spawn(async move {
            for round in 0..rounds {
                let value = pong_rx.recv().await.expect("pong channel closed");
                if round + 1 < rounds {
                    ping_tx.send(value + 1).await.expect("ping send failed");
                }
            }
        });

        pong.await.expect("Eddy pong task failed");
        ping.await.expect("Eddy ping task failed");
    });
}

fn run_tokio_ping_pong(rounds: usize) {
    let runtime = tokio_multi();
    runtime.block_on(async move {
        let (ping_tx, mut ping_rx) = tokio::sync::mpsc::channel(1);
        let (pong_tx, mut pong_rx) = tokio::sync::mpsc::channel(1);
        ping_tx.try_send(0usize).expect("initial ping send failed");

        let pong = tokio::spawn(async move {
            for _ in 0..rounds {
                let value = ping_rx.recv().await.expect("ping channel closed");
                pong_tx.send(value + 1).await.expect("pong send failed");
            }
        });
        let ping = tokio::spawn(async move {
            for round in 0..rounds {
                let value = pong_rx.recv().await.expect("pong channel closed");
                if round + 1 < rounds {
                    ping_tx.send(value + 1).await.expect("ping send failed");
                }
            }
        });

        pong.await.expect("Tokio pong task failed");
        ping.await.expect("Tokio ping task failed");
    });
}

fn ping_pong(c: &mut Criterion) {
    let mut group = c.benchmark_group("ping-pong");
    group.sample_size(10);
    group.throughput(Throughput::Elements((PING_PONG_ROUNDS * 2) as u64));
    group.bench_function("eddy/1m-round-trips", |b| {
        b.iter(|| run_eddy_ping_pong(PING_PONG_ROUNDS));
    });
    group.bench_function("tokio/1m-round-trips", |b| {
        b.iter(|| run_tokio_ping_pong(PING_PONG_ROUNDS));
    });
    group.finish();
}

fn run_eddy_channel(messages: usize, bounded: bool) {
    let runtime = eddy_current();
    runtime.block_on(async move {
        let handle = eddy::Handle::current();
        let (producer, mut consumer) = if bounded {
            let (producer, consumer) = eddy::sync::mpsc::channel(1024);
            (producer, consumer)
        } else {
            let (producer, consumer) = eddy::sync::mpsc::unbounded_channel();
            (producer, consumer)
        };
        let sender = handle.spawn(async move {
            for value in 0..messages {
                producer.send(value).await.expect("Eddy send failed");
            }
        });

        let mut sum = 0usize;
        for _ in 0..messages {
            sum += consumer.recv().await.expect("Eddy receive failed");
        }
        sender.await.expect("Eddy producer failed");
        black_box(sum);
    });
}

fn run_tokio_channel(messages: usize, bounded: bool) {
    let runtime = tokio_current();
    runtime.block_on(async move {
        if bounded {
            let (producer, mut consumer) = tokio::sync::mpsc::channel(1024);
            let sender = tokio::spawn(async move {
                for value in 0..messages {
                    producer.send(value).await.expect("Tokio send failed");
                }
            });
            let mut sum = 0usize;
            for _ in 0..messages {
                sum += consumer.recv().await.expect("Tokio receive failed");
            }
            sender.await.expect("Tokio producer failed");
            black_box(sum);
        } else {
            let (producer, mut consumer) = tokio::sync::mpsc::unbounded_channel();
            let sender = tokio::spawn(async move {
                for value in 0..messages {
                    producer.send(value).expect("Tokio send failed");
                }
            });
            let mut sum = 0usize;
            for _ in 0..messages {
                sum += consumer.recv().await.expect("Tokio receive failed");
            }
            sender.await.expect("Tokio producer failed");
            black_box(sum);
        }
    });
}

fn channel_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("channels");
    group.sample_size(10);
    group.throughput(Throughput::Elements(CHANNEL_MESSAGES as u64));
    for bounded in [true, false] {
        let kind = if bounded { "bounded" } else { "unbounded" };
        group.bench_function(format!("eddy/{kind}"), |b| {
            b.iter(|| run_eddy_channel(CHANNEL_MESSAGES, bounded));
        });
        group.bench_function(format!("tokio/{kind}"), |b| {
            b.iter(|| run_tokio_channel(CHANNEL_MESSAGES, bounded));
        });
    }
    group.finish();
}

fn run_eddy_mutex(tasks: usize) {
    let runtime = eddy_multi();
    runtime.block_on(async move {
        let shared = Arc::new(eddy::sync::Mutex::new(0usize));
        let handle = eddy::Handle::current();
        let mut joins = Vec::with_capacity(tasks);
        for _ in 0..tasks {
            let shared = Arc::clone(&shared);
            joins.push(handle.spawn(async move {
                for _ in 0..MUTEX_ITERATIONS {
                    let mut guard = shared.lock().await;
                    *guard += 1;
                }
            }));
        }
        for join in joins {
            join.await.expect("Eddy mutex task failed");
        }
        let value = Arc::try_unwrap(shared)
            .unwrap_or_else(|_| panic!("Eddy mutex still has references"))
            .into_inner();
        assert_eq!(value, tasks * MUTEX_ITERATIONS);
    });
}

fn run_tokio_mutex(tasks: usize) {
    let runtime = tokio_multi();
    runtime.block_on(async move {
        let shared = Arc::new(tokio::sync::Mutex::new(0usize));
        let mut joins = Vec::with_capacity(tasks);
        for _ in 0..tasks {
            let shared = Arc::clone(&shared);
            joins.push(tokio::spawn(async move {
                for _ in 0..MUTEX_ITERATIONS {
                    let mut guard = shared.lock().await;
                    *guard += 1;
                }
            }));
        }
        for join in joins {
            join.await.expect("Tokio mutex task failed");
        }
        let value = Arc::try_unwrap(shared)
            .unwrap_or_else(|_| panic!("Tokio mutex still has references"))
            .into_inner();
        assert_eq!(value, tasks * MUTEX_ITERATIONS);
    });
}

fn mutex_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("mutex-contention");
    group.sample_size(10);
    for tasks in [1usize, 2, 4, 8, 16, 32, 64] {
        group.throughput(Throughput::Elements((tasks * MUTEX_ITERATIONS) as u64));
        group.bench_with_input(BenchmarkId::new("eddy", tasks), &tasks, |b, &tasks| {
            b.iter(|| run_eddy_mutex(tasks));
        });
        group.bench_with_input(BenchmarkId::new("tokio", tasks), &tasks, |b, &tasks| {
            b.iter(|| run_tokio_mutex(tasks));
        });
    }
    group.finish();
}

fn run_eddy_timer_churn(count: usize) {
    let runtime = eddy_current();
    runtime.block_on(async move {
        let deadline = eddy::time::now() + Duration::from_secs(3_600);
        let mut sleeps = (0..count)
            .map(|_| eddy::time::sleep_until(deadline))
            .collect::<Vec<_>>();
        let waker = eddy::noop_waker();
        let mut context = Context::from_waker(&waker);
        for sleep in &mut sleeps {
            assert!(Pin::new(sleep).poll(&mut context).is_pending());
        }
        black_box(sleeps.len());
        drop(sleeps);
    });
}

fn run_tokio_timer_churn(count: usize) {
    let runtime = tokio_current();
    runtime.block_on(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3_600);
        let mut sleeps = (0..count)
            .map(|_| Box::pin(tokio::time::sleep_until(deadline)))
            .collect::<Vec<_>>();
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        for sleep in &mut sleeps {
            assert!(sleep.as_mut().poll(&mut context).is_pending());
        }
        black_box(sleeps.len());
        drop(sleeps);
    });
}

fn timer_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("timer-churn");
    group.sample_size(10);
    group.throughput(Throughput::Elements(TIMER_COUNT as u64));
    group.bench_function("eddy/100k-concurrent-sleeps", |b| {
        b.iter(|| run_eddy_timer_churn(TIMER_COUNT));
    });
    group.bench_function("tokio/100k-concurrent-sleeps", |b| {
        b.iter(|| run_tokio_timer_churn(TIMER_COUNT));
    });
    group.finish();
}

#[cfg(unix)]
fn run_eddy_echo(connections: usize) {
    use eddy::io::{AsyncReadExt, AsyncWriteExt, TcpListener, TcpStream};

    let runtime = eddy_multi();
    runtime.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Eddy listener bind failed");
        let address = listener.local_addr().expect("Eddy listener address failed");
        let handle = eddy::Handle::current();
        let server = handle.spawn(async move {
            for _ in 0..connections {
                let (mut stream, _) = listener.accept().await.expect("Eddy accept failed");
                let mut message = [0u8; 64];
                stream
                    .read_exact(&mut message)
                    .await
                    .expect("Eddy read failed");
                stream.write_all(&message).await.expect("Eddy write failed");
            }
        });
        let client = handle.spawn(async move {
            for _ in 0..connections {
                let mut stream = TcpStream::connect(address)
                    .await
                    .expect("Eddy connect failed");
                stream
                    .write_all(&ECHO_MESSAGE)
                    .await
                    .expect("Eddy client write failed");
                let mut response = [0u8; 64];
                stream
                    .read_exact(&mut response)
                    .await
                    .expect("Eddy client read failed");
                assert_eq!(response, ECHO_MESSAGE);
            }
        });
        server.await.expect("Eddy server task failed");
        client.await.expect("Eddy client task failed");
    });
}

#[cfg(unix)]
fn run_tokio_echo(connections: usize) {
    use tokio::io::{AsyncReadExt as TokioReadExt, AsyncWriteExt as TokioWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    let runtime = tokio_multi();
    runtime.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Tokio listener bind failed");
        let address = listener
            .local_addr()
            .expect("Tokio listener address failed");
        let server = tokio::spawn(async move {
            for _ in 0..connections {
                let (mut stream, _) = listener.accept().await.expect("Tokio accept failed");
                let mut message = [0u8; 64];
                stream
                    .read_exact(&mut message)
                    .await
                    .expect("Tokio read failed");
                stream
                    .write_all(&message)
                    .await
                    .expect("Tokio write failed");
            }
        });
        let client = tokio::spawn(async move {
            for _ in 0..connections {
                let mut stream = TcpStream::connect(address)
                    .await
                    .expect("Tokio connect failed");
                stream
                    .write_all(&ECHO_MESSAGE)
                    .await
                    .expect("Tokio client write failed");
                let mut response = [0u8; 64];
                stream
                    .read_exact(&mut response)
                    .await
                    .expect("Tokio client read failed");
                assert_eq!(response, ECHO_MESSAGE);
            }
        });
        server.await.expect("Tokio server task failed");
        client.await.expect("Tokio client task failed");
    });
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
fn run_uring_echo(connections: usize) {
    use eddy::io::IoUring;
    use std::net::TcpListener;
    use std::os::fd::{AsRawFd, IntoRawFd};
    use std::task::{Context, Poll};

    let Some(ring) = IoUring::new_or_fallback(256).expect("io_uring probe failed") else {
        return;
    };
    let listener = TcpListener::bind("127.0.0.1:0").expect("io_uring listener bind failed");
    let address = listener
        .local_addr()
        .expect("io_uring listener address failed");
    let waker = eddy::noop_waker();
    let mut cx = Context::from_waker(&waker);

    for _ in 0..connections {
        // The ring owns this descriptor after the close operation is queued.
        // `connect` itself is asynchronous, so no blocking connect is needed.
        // SAFETY: the syscall has no Rust pointers; its return value is checked
        // before the descriptor is used.
        let client_fd = unsafe {
            libc::socket(
                libc::AF_INET,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            )
        };
        assert!(client_fd >= 0, "io_uring client socket failed");

        let mut accept = Box::pin(ring.accept(listener.as_raw_fd()));
        let mut connect = Box::pin(ring.connect(client_fd, address));
        assert!(matches!(accept.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(connect.as_mut().poll(&mut cx), Poll::Pending));

        let accepted = loop {
            ring.submit_and_wait()
                .expect("io_uring connect/accept failed");
            let accepted = match accept.as_mut().poll(&mut cx) {
                Poll::Ready(result) => Some(result.expect("io_uring accept failed")),
                Poll::Pending => None,
            };
            let connected = match connect.as_mut().poll(&mut cx) {
                Poll::Ready(result) => {
                    result.expect("io_uring connect failed");
                    true
                }
                Poll::Pending => false,
            };
            if connected && accepted.is_some() {
                break accepted.expect("accepted descriptor disappeared");
            }
        };
        let accepted_fd = accepted.into_raw_fd();

        let mut send = Box::pin(ring.send(client_fd, ECHO_MESSAGE.to_vec()));
        let mut recv = Box::pin(ring.recv(accepted_fd, vec![0_u8; ECHO_MESSAGE.len()]));
        assert!(matches!(send.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(recv.as_mut().poll(&mut cx), Poll::Pending));
        let mut sent = None;
        let mut received = None;
        while sent.is_none() || received.is_none() {
            ring.submit_and_wait().expect("io_uring send/recv failed");
            if sent.is_none() {
                if let Poll::Ready(value) = send.as_mut().poll(&mut cx) {
                    sent = Some(value);
                }
            }
            if received.is_none() {
                if let Poll::Ready(value) = recv.as_mut().poll(&mut cx) {
                    received = Some(value);
                }
            }
        }
        let (send_result, sent_buffer) = sent.expect("io_uring send result disappeared");
        let (recv_result, received_buffer) = received.expect("io_uring receive result disappeared");
        assert_eq!(
            send_result.expect("io_uring send failed"),
            ECHO_MESSAGE.len()
        );
        assert_eq!(
            recv_result.expect("io_uring receive failed"),
            ECHO_MESSAGE.len()
        );
        assert_eq!(sent_buffer, ECHO_MESSAGE);
        assert_eq!(received_buffer, ECHO_MESSAGE);

        let mut close_client = Box::pin(ring.close(client_fd));
        let mut close_accepted = Box::pin(ring.close(accepted_fd));
        assert!(matches!(close_client.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(
            close_accepted.as_mut().poll(&mut cx),
            Poll::Pending
        ));
        let mut client_closed = false;
        let mut accepted_closed = false;
        while !client_closed || !accepted_closed {
            ring.submit_and_wait().expect("io_uring close failed");
            if !client_closed {
                client_closed = close_client.as_mut().poll(&mut cx).is_ready();
            }
            if !accepted_closed {
                accepted_closed = close_accepted.as_mut().poll(&mut cx).is_ready();
            }
        }
    }
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
fn io_uring_vs_epoll(c: &mut Criterion) {
    let Ok(Some(_ring)) = eddy::io::IoUring::new_or_fallback(256) else {
        return;
    };
    let mut group = c.benchmark_group("io-uring-vs-epoll");
    group.sample_size(10);
    group.throughput(Throughput::Elements(ECHO_CONNECTIONS as u64));
    group.bench_function("eddy-epoll/10k-echo", |b| {
        b.iter(|| run_eddy_echo(ECHO_CONNECTIONS));
    });
    group.bench_function("io-uring/10k-echo", |b| {
        b.iter(|| run_uring_echo(ECHO_CONNECTIONS));
    });
    group.finish();
}

#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
fn io_uring_vs_epoll(c: &mut Criterion) {
    let _ = c;
}

#[cfg(unix)]
fn echo_server(c: &mut Criterion) {
    let mut group = c.benchmark_group("echo-server");
    group.sample_size(10);
    group.throughput(Throughput::Elements(ECHO_CONNECTIONS as u64));
    group.bench_function("eddy/10k-connections-64b", |b| {
        b.iter(|| run_eddy_echo(ECHO_CONNECTIONS));
    });
    group.bench_function("tokio/10k-connections-64b", |b| {
        b.iter(|| run_tokio_echo(ECHO_CONNECTIONS));
    });
    group.finish();
}

fn echo_server_benchmark(c: &mut Criterion) {
    #[cfg(unix)]
    echo_server(c);
    #[cfg(not(unix))]
    let _ = c;
}

fn run_eddy_work_steal() {
    let runtime = eddy::Builder::new_multi_thread()
        .worker_threads(2)
        .disable_lifo_slot()
        .build();
    runtime.block_on(async {
        let handle = eddy::Handle::current();
        let producer = handle.clone();
        let value = handle
            .spawn(async move {
                let mut children = Vec::with_capacity(32);
                for _ in 0..32 {
                    children.push(producer.spawn(async { 1usize }));
                }
                let mut sum = 0usize;
                for child in children {
                    sum += child.await.expect("Eddy stolen task failed");
                }
                sum
            })
            .await
            .expect("Eddy producer failed");
        black_box(value);
    });
}

fn run_tokio_work_steal() {
    let runtime = tokio_multi();
    runtime.block_on(async {
        let value = tokio::spawn(async {
            let mut children = Vec::with_capacity(32);
            for _ in 0..32 {
                children.push(tokio::spawn(async { 1usize }));
            }
            let mut sum = 0usize;
            for child in children {
                sum += child.await.expect("Tokio stolen task failed");
            }
            sum
        })
        .await
        .expect("Tokio producer failed");
        black_box(value);
    });
}

fn work_steal_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("work-steal-latency");
    group.sample_size(20);
    group.bench_function("eddy/idle-worker-to-first-task", |b| {
        b.iter(run_eddy_work_steal);
    });
    group.bench_function("tokio/idle-worker-to-first-task", |b| {
        b.iter(run_tokio_work_steal);
    });
    group.finish();
}

fn instrumentation_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("instrumentation-overhead");
    group.sample_size(10);
    group.throughput(Throughput::Elements(10_000));
    #[cfg(feature = "instrumentation")]
    eddy::set_subscriber(Arc::new(|_event| {}));
    group.bench_function("eddy/spawn-join-10k", |b| {
        b.iter(|| run_eddy_spawn_join(10_000));
    });
    #[cfg(feature = "instrumentation")]
    eddy::clear_subscriber();
    group.finish();
}

criterion_group!(
    benches,
    spawn_join_throughput,
    ping_pong,
    channel_throughput,
    mutex_contention,
    timer_churn,
    work_steal_latency,
    instrumentation_overhead,
    echo_server_benchmark,
    io_uring_vs_epoll,
);
criterion_main!(benches);
