//! Phase 5: I/O readiness driver integration tests.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use eddy::io::{
    copy_bidirectional, AsyncReadExt, AsyncWriteExt, Interest, Registration, TcpListener, TcpStream,
};
use eddy::time::timeout;
use eddy::Builder;

fn socketpair() -> (OwnedFd, OwnedFd) {
    let mut fds = [0i32; 2];
    let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
    assert_eq!(rc, 0, "socketpair failed: {}", io::Error::last_os_error());
    // SAFETY: on success the kernel handed us two owned descriptors.
    unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
}

fn write_byte(fd: &OwnedFd) {
    write_byte_raw(fd.as_raw_fd());
}

fn write_byte_raw(fd: std::os::fd::RawFd) {
    let byte = [0x5au8; 1];
    // SAFETY: `fd` is a valid open socket and `byte` is a live buffer.
    let n = unsafe { libc::write(fd, byte.as_ptr() as *const libc::c_void, byte.len()) };
    assert_eq!(n, 1, "write failed: {}", io::Error::last_os_error());
}

fn read_byte(fd: std::os::fd::RawFd) {
    let mut byte = [0u8; 1];
    // SAFETY: `fd` is a valid open socket and `byte` is a live buffer.
    let n = unsafe { libc::read(fd, byte.as_mut_ptr() as *mut libc::c_void, byte.len()) };
    assert_eq!(n, 1, "read failed: {}", io::Error::last_os_error());
}

/// Raise the soft fd limit to make room for the 10k-registration test.
fn ensure_fd_limit(needed: u64) {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid pointer to writable memory.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
        0
    );
    if limit.rlim_cur < needed {
        let target = needed.min(limit.rlim_max);
        assert!(
            target >= needed,
            "cannot raise RLIMIT_NOFILE to {needed} (hard limit {})",
            limit.rlim_max
        );
        let new_limit = libc::rlimit {
            rlim_cur: target,
            rlim_max: limit.rlim_max,
        };
        // SAFETY: raising the soft limit up to the hard limit is always
        // permitted; `new_limit` is a valid rlimit struct.
        assert_eq!(
            unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &new_limit) },
            0
        );
    }
}

#[test]
fn register_socket_make_it_readable_task_wakes() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    rt.block_on(async {
        let (read, write) = socketpair();
        let reg = Arc::new(Registration::new(read, Interest::READABLE).unwrap());
        let task_reg = reg.clone();
        let handle = eddy::Handle::current().spawn(async move {
            let event = task_reg.readiness(Interest::READABLE).await;
            event.ready()
        });
        write_byte(&write);
        let ready = handle.await.unwrap();
        assert!(ready.is_readable());
    });
}

#[test]
fn ten_thousand_concurrent_registrations_all_wake() {
    ensure_fd_limit(10_100);
    let rt = Builder::new_multi_thread().worker_threads(4).build();
    rt.block_on(async {
        let mut handles = Vec::with_capacity(10_000);
        let mut write_ends = Vec::with_capacity(5_000);
        for _ in 0..5_000 {
            let (read, write) = socketpair();
            let read = Arc::new(Registration::new(read, Interest::READABLE).unwrap());
            let write = Arc::new(Registration::new(write, Interest::WRITABLE).unwrap());
            let read_reg = read.clone();
            let write_reg = write.clone();
            // Both registrations live on one task; the write end is
            // immediately writable (fast path), the read end waits for the
            // byte written below.
            handles.push(eddy::Handle::current().spawn(async move {
                let readable = read_reg.readiness(Interest::READABLE).await.ready();
                let writable = write_reg.readiness(Interest::WRITABLE).await.ready();
                (readable, writable)
            }));
            // Keep the write-end registrations for the bulk writes.
            write_ends.push(write);
        }
        for write in &write_ends {
            write_byte_raw(write.as_raw_fd());
        }
        drop(write_ends);
        for handle in handles {
            let (readable, writable) = handle.await.unwrap();
            assert!(readable.is_readable());
            assert!(writable.is_writable());
        }
    });
}

#[test]
fn fd_reuse_does_not_deliver_stale_events() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    rt.block_on(async {
        let (read1, write1) = socketpair();
        let reg1 = Registration::new(read1, Interest::READABLE).unwrap();
        // Arm a genuine stale event: make the old registration readable and
        // let the driver record readiness for its generation before the slot
        // is freed (the poller returns events at ~1 ms granularity).
        write_byte(&write1);
        std::thread::sleep(Duration::from_millis(100));

        // Free the slab slot, then reuse it with a fresh fd. `slab.insert`
        // hands the new registration the same index (0) with a newer
        // generation; its readiness must start from zero, never from the
        // bits the driver recorded for the old registration.
        drop(reg1);
        let (read2, write2) = socketpair();

        let reg = Arc::new(Registration::new(read2, Interest::READABLE).unwrap());
        let task_reg = reg.clone();
        let woken = Arc::new(AtomicBool::new(false));
        let woken_for_task = woken.clone();
        let handle = eddy::Handle::current().spawn(async move {
            let _event = task_reg.readiness(Interest::READABLE).await;
            woken_for_task.store(true, Ordering::SeqCst);
        });
        // A stale event recorded for the old generation must not wake the new
        // registration on the reused slot.
        std::thread::sleep(Duration::from_millis(150));
        assert!(!woken.load(Ordering::SeqCst), "stale event leaked through");
        write_byte(&write2);
        handle.await.unwrap();
        assert!(woken.load(Ordering::SeqCst));
    });
}

#[test]
fn reader_and_writer_on_one_socket_wake_independently() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    let (read, write) = socketpair();

    rt.block_on(async {
        let reg =
            Arc::new(Registration::new(read, Interest::READABLE.add(Interest::WRITABLE)).unwrap());
        let reader_reg = reg.clone();
        let writer_reg = reg.clone();
        let reader_done = Arc::new(AtomicBool::new(false));
        let writer_done = Arc::new(AtomicBool::new(false));
        let reader_flag = reader_done.clone();
        let writer_flag = writer_done.clone();

        let reader = eddy::Handle::current().spawn(async move {
            let event = reader_reg.readiness(Interest::READABLE).await;
            event.clear_ready();
            reader_flag.store(true, Ordering::SeqCst);
        });
        let writer = eddy::Handle::current().spawn(async move {
            let _event = writer_reg.readiness(Interest::WRITABLE).await;
            writer_flag.store(true, Ordering::SeqCst);
        });

        // Give both tasks time to park on their wakers.
        std::thread::sleep(Duration::from_millis(50));
        // The write end is immediately writable. That event must not make
        // the reader future lose its registration or complete early.
        assert!(!reader_done.load(Ordering::SeqCst));
        write_byte(&write);
        reader.await.unwrap();

        assert!(reader_done.load(Ordering::SeqCst));
        drop(writer);
    });
}

#[test]
fn spurious_readiness_is_cleared_and_rewait_parks() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    rt.block_on(async {
        let (read, write) = socketpair();
        let reg = Arc::new(Registration::new(read, Interest::READABLE).unwrap());
        let task_reg = reg.clone();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_for_task = completed.clone();
        let handle = eddy::Handle::current().spawn(async move {
            let first = task_reg.readiness(Interest::READABLE).await;
            // Drain the byte BEFORE clearing: a level-triggered poller is
            // free to re-report an fd that is still readable, so clearing
            // first leaves a window where the driver legitimately re-records
            // the event and the re-wait below fires without a new byte.
            read_byte(task_reg.as_raw_fd());
            first.clear_ready();
            let second = task_reg.readiness(Interest::READABLE).await;
            second.clear_ready();
            completed_for_task.store(true, Ordering::SeqCst);
        });

        // Let the readiness future register before producing the first byte.
        std::thread::sleep(Duration::from_millis(50));
        write_byte(&write);

        // The re-wait after clear_ready + drain must park, not spin on the
        // stale bits.
        std::thread::sleep(Duration::from_millis(150));
        assert!(
            !completed.load(Ordering::SeqCst),
            "stale readiness caused a spin instead of a park"
        );

        // A real event releases the task.
        write_byte(&write);
        handle.await.unwrap();
        assert!(completed.load(Ordering::SeqCst));
    });
}

#[test]
fn registration_on_current_thread_runtime_is_unsupported_clearly() {
    let rt = Builder::new_current_thread().build();
    let error = rt.block_on(async {
        let (read, _write) = socketpair();
        match Registration::new(read, Interest::READABLE) {
            Ok(_) => panic!("expected an unsupported error"),
            Err(error) => error,
        }
    });
    assert!(
        error.to_string().contains("multi-thread runtime"),
        "unexpected error: {error}"
    );
}

#[test]
fn two_tasks_on_one_registration_both_are_woken() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    rt.block_on(async {
        let (read, write) = socketpair();
        let reg = Arc::new(Registration::new(read, Interest::READABLE).unwrap());
        let done = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let task_reg = reg.clone();
            let done_for_task = done.clone();
            handles.push(eddy::Handle::current().spawn(async move {
                let _event = task_reg.readiness(Interest::READABLE).await;
                done_for_task.fetch_add(1, Ordering::SeqCst);
            }));
        }
        // Let both tasks park: the first takes the reader slot, the second
        // the overflow list.
        std::thread::sleep(Duration::from_millis(50));
        write_byte(&write);
        for handle in handles {
            handle.await.unwrap();
        }
        assert_eq!(done.load(Ordering::SeqCst), 2);
    });
}

#[test]
fn copy_bidirectional_half_closes_destination_on_eof() {
    let rt = Builder::new_multi_thread().worker_threads(2).build();
    rt.block_on(async {
        let listener_left = TcpListener::bind("127.0.0.1:0").unwrap();
        let left_addr = listener_left.local_addr().unwrap();
        let listener_right = TcpListener::bind("127.0.0.1:0").unwrap();
        let right_addr = listener_right.local_addr().unwrap();

        // Left peer: send a message, then half-close its write side so `left`
        // observes EOF while the connection stays open.
        let left_server = eddy::Handle::current().spawn(async move {
            let (mut stream, _) = listener_left.accept().await.unwrap();
            stream.write_all(b"hello").await.unwrap();
            stream.shutdown().await.unwrap();
        });

        // Right peer: keep reading until it observes EOF from the copy. This
        // only happens if `right`'s write side is shut down at the end of the
        // copy; the old code never half-closed the destination and hung here.
        let right_server = eddy::Handle::current().spawn(async move {
            let (stream, _) = listener_right.accept().await.unwrap();
            let (mut read, _write) = stream.into_split();
            let mut buf = Vec::new();
            read.read_to_end(&mut buf).await.unwrap();
            buf
        });

        let mut left = TcpStream::connect(left_addr).await.unwrap();
        let mut right = TcpStream::connect(right_addr).await.unwrap();
        let result = timeout(
            Duration::from_secs(5),
            copy_bidirectional(&mut left, &mut right),
        )
        .await;
        match result {
            Ok(Ok((to_right, to_left))) => {
                assert_eq!(to_right, 5);
                assert_eq!(to_left, 0);
            }
            Ok(Err(error)) => panic!("copy_bidirectional failed: {error}"),
            Err(_) => panic!("copy_bidirectional hung: destination never half-closed"),
        }
        left_server.await.unwrap();
        assert_eq!(right_server.await.unwrap(), b"hello");
    });
}
