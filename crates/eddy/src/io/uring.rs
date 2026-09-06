//! A small, dependency-free Linux `io_uring` backend.
//!
//! The readiness driver remains the portable default. This module exposes
//! completion operations behind the `io-uring` feature so an executor can
//! choose it when `io_uring_setup` succeeds and fall back to epoll otherwise.
//! The ring is deliberately raw: keeping this code here avoids adding a
//! runtime dependency and makes the buffer lifetime rules visible.
//!
//! A submitted operation owns its buffer until its CQE is reaped. Borrowed
//! buffers are not accepted because dropping a future cannot cancel a kernel
//! DMA/write safely. Copying, leaking, and retaining an owned buffer are the
//! three usual choices; retaining an owned buffer in the operation/orphan
//! slabs wins because it is safe and bounds cancelled memory to operations
//! still in flight. `IORING_OP_ASYNC_CANCEL` is best effort, but cancellation
//! never releases the buffer before the target CQE arrives.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::mem::size_of;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use slab::Slab;

const IORING_OFF_SQ_RING: libc::off_t = 0;
const IORING_OFF_CQ_RING: libc::off_t = 0x8000_0000;
const IORING_OFF_SQES: libc::off_t = 0x1_0000_0000;
const IORING_FEAT_SINGLE_MMAP: u32 = 1 << 0;
const IORING_SETUP_SQPOLL: u32 = 1 << 1;
const IORING_SQ_NEED_WAKEUP: u32 = 1 << 0;
const IORING_ENTER_GETEVENTS: u32 = 1;
const IORING_ENTER_SQ_WAKEUP: u32 = 1 << 1;
const IORING_REGISTER_BUFFERS: u32 = 0;
const IORING_UNREGISTER_BUFFERS: u32 = 1;
const IORING_REGISTER_FILES: u32 = 2;
const IORING_UNREGISTER_FILES: u32 = 3;
const IORING_OP_ASYNC_CANCEL: u8 = 14;
const IORING_OP_TIMEOUT: u8 = 11;
const IORING_OP_READV: u8 = 1;
const IORING_OP_WRITEV: u8 = 2;
const IORING_OP_ACCEPT: u8 = 13;
const IORING_OP_CONNECT: u8 = 16;
const IORING_OP_CLOSE: u8 = 19;
const IORING_OP_READ: u8 = 22;
const IORING_OP_WRITE: u8 = 23;
const IORING_OP_SEND: u8 = 26;
const IORING_OP_RECV: u8 = 27;

/// Configuration used when creating an [`IoUring`].
///
/// SQPOLL is opt-in because it creates a kernel thread and can require
/// privileges or a suitable `RLIMIT_MEMLOCK`. A failed SQPOLL setup is
/// returned to the caller; it is never silently downgraded to a normal ring.
#[derive(Clone, Copy, Debug)]
pub struct IoUringBuilder {
    entries: u32,
    sqpoll: bool,
    sq_thread_cpu: Option<u32>,
    sq_thread_idle: Option<Duration>,
}

#[repr(C)]
#[derive(Default)]
struct IoUringSqringOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    flags: u32,
    dropped: u32,
    array: u32,
    resv1: u32,
    resv2: u64,
}

#[repr(C)]
#[derive(Default)]
struct IoUringCqringOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    overflow: u32,
    cqes: u32,
    resv: [u64; 2],
}

#[repr(C)]
#[derive(Default)]
struct IoUringParams {
    sq_entries: u32,
    cq_entries: u32,
    flags: u32,
    sq_thread_cpu: u32,
    sq_thread_idle: u32,
    features: u32,
    wq_fd: u32,
    resv: [u32; 3],
    sq_off: IoUringSqringOffsets,
    cq_off: IoUringCqringOffsets,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IoUringSqe {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: u64,
    addr: u64,
    len: u32,
    rw_flags: u32,
    user_data: u64,
    buf_index: u16,
    personality: u16,
    splice_fd_in: i32,
    pad2: [u64; 2],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IoUringCqe {
    user_data: u64,
    res: i32,
    flags: u32,
}

struct MmapRegion {
    ptr: NonNull<u8>,
    len: usize,
    unmap: bool,
}

impl MmapRegion {
    fn map(fd: RawFd, len: usize, offset: libc::off_t) -> io::Result<MmapRegion> {
        // SAFETY: the kernel validates the ring fd, length, and offset. A
        // successful mapping is owned by this value until Drop.
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                offset,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: mmap returned a non-MAP_FAILED pointer.
        Ok(MmapRegion {
            ptr: unsafe { NonNull::new_unchecked(ptr.cast()) },
            len,
            unmap: true,
        })
    }

    unsafe fn at<T>(&self, offset: u32) -> *mut T {
        // SAFETY: callers pass offsets supplied by the kernel and only use the
        // matching ring type for that offset.
        unsafe { self.ptr.as_ptr().add(offset as usize).cast() }
    }
}

impl Drop for MmapRegion {
    fn drop(&mut self) {
        if self.unmap {
            // SAFETY: this is the exact pointer/length returned by mmap.
            unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
        }
    }
}

enum OpResource {
    Buffer(Vec<u8>),
    Vectored {
        buffers: Vec<Vec<u8>>,
        iovecs: Vec<libc::iovec>,
    },
    Address {
        bytes: Vec<u8>,
        length: libc::socklen_t,
    },
    FixedBuffer {
        registration: Arc<RegisteredBuffersInner>,
        index: u16,
        length: u32,
    },
    FixedFileBuffer {
        registration: Arc<RegisteredFilesInner>,
        index: u32,
        buffer: Vec<u8>,
    },
    Accept,
    Close,
}

// SAFETY: the only raw pointers are iovec bases pointing into the Vec buffers
// held by this same value. Moving the resource does not move those allocations,
// and all access after submission is serialized by the ring state mutex.
unsafe impl Send for OpResource {}

impl OpResource {
    fn addr_len(&self) -> io::Result<(u64, u32)> {
        match self {
            OpResource::Buffer(buffer) => Ok((
                buffer.as_ptr() as u64,
                u32::try_from(buffer.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "eddy: io_uring buffer is too large",
                    )
                })?,
            )),
            OpResource::Vectored { iovecs, .. } => Ok((
                iovecs.as_ptr() as u64,
                u32::try_from(iovecs.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "eddy: too many io_uring vectors",
                    )
                })?,
            )),
            OpResource::Address { bytes, length } => Ok((
                bytes.as_ptr() as u64,
                u32::try_from(*length).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "eddy: socket address is too large",
                    )
                })?,
            )),
            OpResource::FixedBuffer {
                registration,
                index,
                length,
            } => Ok((
                registration.buffers[*index as usize].as_ptr() as u64,
                *length,
            )),
            OpResource::FixedFileBuffer { buffer, .. } => Ok((
                buffer.as_ptr() as u64,
                u32::try_from(buffer.len()).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "eddy: fixed-file buffer is too large",
                    )
                })?,
            )),
            OpResource::Accept | OpResource::Close => Ok((0, 0)),
        }
    }

    fn buffer_index(&self) -> u16 {
        match self {
            OpResource::FixedBuffer { index, .. } => *index,
            _ => 0,
        }
    }

    fn fixed_file_index(&self) -> Option<RawFd> {
        match self {
            OpResource::FixedFileBuffer { index, .. } => Some(*index as RawFd),
            _ => None,
        }
    }
}

struct RegisteredBuffersInner {
    ring_fd: OwnedFd,
    buffers: Vec<Vec<u8>>,
}

impl Drop for RegisteredBuffersInner {
    fn drop(&mut self) {
        // The duplicate ring fd keeps the ring alive even when an operation
        // is orphaned after its public registration handle was dropped.
        // SAFETY: this fd was duplicated from a live io_uring fd and the
        // registration belongs to this ring instance.
        unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd.as_raw_fd(),
                IORING_UNREGISTER_BUFFERS,
                ptr::null::<libc::c_void>(),
                0,
            );
        }
    }
}

struct RegisteredFilesInner {
    ring_fd: OwnedFd,
    files: Vec<OwnedFd>,
}

impl Drop for RegisteredFilesInner {
    fn drop(&mut self) {
        // SAFETY: the duplicate ring fd and registration are owned by this
        // value and remain live until the unregister call completes.
        unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                self.ring_fd.as_raw_fd(),
                IORING_UNREGISTER_FILES,
                ptr::null::<libc::c_void>(),
                0,
            );
        }
    }
}

/// An owned set of buffers registered with an io_uring instance.
///
/// A fixed-buffer operation consumes this value and returns it in its output.
/// This prevents callers from mutating or unregistering a buffer while the
/// kernel can still access it, including after cancellation.
pub struct RegisteredBuffers {
    ring: IoUring,
    inner: Arc<RegisteredBuffersInner>,
}

impl RegisteredBuffers {
    pub fn len(&self) -> usize {
        self.inner.buffers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.buffers.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&[u8]> {
        self.inner.buffers.get(index).map(Vec::as_slice)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        Arc::get_mut(&mut self.inner)
            .and_then(|inner| inner.buffers.get_mut(index).map(Vec::as_mut_slice))
    }

    pub fn read_fixed(self, fd: RawFd, index: usize, length: usize) -> FixedBufferOwned {
        FixedBufferOwned {
            ring: self.ring.clone(),
            fd,
            index,
            length,
            registration: Some(self),
            key: None,
            opcode: IORING_OP_READ,
        }
    }

    pub fn write_fixed(self, fd: RawFd, index: usize, length: usize) -> FixedBufferOwned {
        FixedBufferOwned {
            ring: self.ring.clone(),
            fd,
            index,
            length,
            registration: Some(self),
            key: None,
            opcode: IORING_OP_WRITE,
        }
    }
}

/// An owned set of file descriptors registered with an io_uring instance.
///
/// The original descriptors remain owned by this value until the registration
/// is dropped. A fixed-file operation consumes and returns the registration so
/// the kernel never observes a reused descriptor slot.
pub struct RegisteredFiles {
    ring: IoUring,
    inner: Arc<RegisteredFilesInner>,
}

impl RegisteredFiles {
    pub fn len(&self) -> usize {
        self.inner.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.files.is_empty()
    }

    pub fn as_raw_fd(&self, index: usize) -> Option<RawFd> {
        self.inner.files.get(index).map(AsRawFd::as_raw_fd)
    }

    pub fn read_fixed(self, index: usize, buffer: Vec<u8>) -> FixedFileOwned {
        FixedFileOwned {
            ring: self.ring.clone(),
            index,
            buffer: Some(buffer),
            registration: Some(self),
            key: None,
            opcode: IORING_OP_READ,
        }
    }

    pub fn write_fixed(self, index: usize, buffer: Vec<u8>) -> FixedFileOwned {
        FixedFileOwned {
            ring: self.ring.clone(),
            index,
            buffer: Some(buffer),
            registration: Some(self),
            key: None,
            opcode: IORING_OP_WRITE,
        }
    }
}

struct OpState {
    resource: OpResource,
    result: Option<io::Result<usize>>,
    waker: Option<Waker>,
    user_data: u64,
}

struct OrphanedOp {
    resource: OpResource,
    user_data: u64,
}

struct RingState {
    ops: Slab<OpState>,
    ops_by_user_data: HashMap<u64, usize>,
    orphaned: Slab<OrphanedOp>,
    orphaned_by_user_data: HashMap<u64, usize>,
    cancel_targets: HashMap<u64, u64>,
    next_user_data: u64,
    sq_pending: u32,
}

struct Inner {
    fd: RawFd,
    params: IoUringParams,
    sq_ring: MmapRegion,
    cq_ring: MmapRegion,
    sqes: MmapRegion,
    state: Mutex<RingState>,
}

// SAFETY: all mutable ring cursors and operation state are protected by the
// mutex. The kernel owns the mapped pages, not Rust references into them.
unsafe impl Send for Inner {}
// SAFETY: sharing the ring only exposes mutex-protected operation state and
// kernel-owned mappings.
unsafe impl Sync for Inner {}

/// A shared Linux completion ring.
#[derive(Clone)]
pub struct IoUring {
    inner: Arc<Inner>,
}

impl IoUring {
    pub fn builder(entries: u32) -> IoUringBuilder {
        IoUringBuilder {
            entries,
            sqpoll: false,
            sq_thread_cpu: None,
            sq_thread_idle: None,
        }
    }

    /// Probe and create a ring. `Unsupported` means the caller should use the
    /// normal epoll driver; seccomp and older kernels commonly return ENOSYS
    /// or EPERM here.
    pub fn new(entries: u32) -> io::Result<IoUring> {
        Self::builder(entries).build()
    }

    fn new_with_config(config: IoUringBuilder) -> io::Result<IoUring> {
        let entries = config.entries;
        if entries == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "eddy: io_uring entries must be non-zero",
            ));
        }
        if !config.sqpoll && (config.sq_thread_cpu.is_some() || config.sq_thread_idle.is_some()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "eddy: SQPOLL CPU and idle settings require SQPOLL",
            ));
        }
        let mut params = IoUringParams::default();
        if config.sqpoll {
            params.flags |= IORING_SETUP_SQPOLL;
            params.sq_thread_cpu = config.sq_thread_cpu.unwrap_or(0);
            if let Some(idle) = config.sq_thread_idle {
                let idle = idle.as_millis();
                params.sq_thread_idle = u32::try_from(idle).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "eddy: SQPOLL idle duration exceeds the kernel ABI",
                    )
                })?;
            }
        }
        // SAFETY: params points to writable, correctly-sized kernel ABI
        // storage and entries is supplied by the caller.
        let fd = unsafe { libc::syscall(libc::SYS_io_uring_setup, entries, &mut params) } as RawFd;
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let sq_size = params.sq_off.array as usize + params.sq_entries as usize * size_of::<u32>();
        let cq_size =
            params.cq_off.cqes as usize + params.cq_entries as usize * size_of::<IoUringCqe>();
        let (sq_ring, cq_ring) = if params.features & IORING_FEAT_SINGLE_MMAP != 0 {
            let region = match MmapRegion::map(fd, sq_size.max(cq_size), IORING_OFF_SQ_RING) {
                Ok(region) => region,
                Err(error) => {
                    // SAFETY: fd was returned by io_uring_setup above.
                    unsafe { libc::close(fd) };
                    return Err(error);
                }
            };
            // The two fields intentionally alias the one kernel mapping.
            let cq = MmapRegion {
                ptr: region.ptr,
                len: region.len,
                unmap: false,
            };
            (region, cq)
        } else {
            let sq = match MmapRegion::map(fd, sq_size, IORING_OFF_SQ_RING) {
                Ok(region) => region,
                Err(error) => {
                    // SAFETY: fd was returned by io_uring_setup above.
                    unsafe { libc::close(fd) };
                    return Err(error);
                }
            };
            let cq = match MmapRegion::map(fd, cq_size, IORING_OFF_CQ_RING) {
                Ok(region) => region,
                Err(error) => {
                    drop(sq);
                    // SAFETY: fd was returned by io_uring_setup above.
                    unsafe { libc::close(fd) };
                    return Err(error);
                }
            };
            (sq, cq)
        };
        let sqes = match MmapRegion::map(
            fd,
            params.sq_entries as usize * size_of::<IoUringSqe>(),
            IORING_OFF_SQES,
        ) {
            Ok(region) => region,
            Err(error) => {
                drop(cq_ring);
                drop(sq_ring);
                // SAFETY: fd was returned by io_uring_setup above.
                unsafe { libc::close(fd) };
                return Err(error);
            }
        };
        Ok(IoUring {
            inner: Arc::new(Inner {
                fd,
                params,
                sq_ring,
                cq_ring,
                sqes,
                state: Mutex::new(RingState {
                    ops: Slab::new(),
                    ops_by_user_data: HashMap::new(),
                    orphaned: Slab::new(),
                    orphaned_by_user_data: HashMap::new(),
                    cancel_targets: HashMap::new(),
                    next_user_data: 1,
                    sq_pending: 0,
                }),
            }),
        })
    }

    /// Whether this kernel can create a ring without retaining it.
    pub fn is_supported() -> bool {
        Self::probe(2).unwrap_or(false)
    }

    /// Whether this ring was created with the explicit SQPOLL option.
    pub fn is_sqpoll(&self) -> bool {
        self.inner.params.flags & IORING_SETUP_SQPOLL != 0
    }

    /// Probe the kernel without exposing a ring to the caller.
    ///
    /// `Ok(false)` means the completion backend is unavailable and the caller
    /// should use the readiness/epoll backend. Other errors describe a real
    /// setup failure and are returned to the caller.
    pub fn probe(entries: u32) -> io::Result<bool> {
        match Self::new(entries) {
            Ok(ring) => {
                drop(ring);
                Ok(true)
            }
            Err(error) if is_unavailable(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Create a ring when available, otherwise return `None` so the caller
    /// can continue with its normal readiness/epoll driver.
    pub fn new_or_fallback(entries: u32) -> io::Result<Option<IoUring>> {
        match Self::new(entries) {
            Ok(ring) => Ok(Some(ring)),
            Err(error) if is_unavailable(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Create an owned-buffer read. The operation is submitted on its first
    /// poll; the owning executor must call [`IoUring::poll_completions`] from
    /// its park loop to deliver the CQE and wake the future.
    pub fn read(&self, fd: RawFd, buffer: Vec<u8>) -> ReadOwned {
        self.read_at(fd, buffer, 0)
    }

    /// Create an owned-buffer read at an explicit file offset.
    ///
    /// For sockets and other non-seekable descriptors the kernel ignores the
    /// offset, as it does for the raw `read` operation.
    pub fn read_at(&self, fd: RawFd, buffer: Vec<u8>, offset: u64) -> ReadOwned {
        ReadOwned {
            ring: self.clone(),
            fd,
            offset,
            buffer: Some(buffer),
            key: None,
        }
    }

    /// Create an owned-buffer write with the same cancellation guarantees as
    /// [`IoUring::read`]. The returned buffer is not released until its CQE.
    pub fn write(&self, fd: RawFd, buffer: Vec<u8>) -> WriteOwned {
        self.write_at(fd, buffer, 0)
    }

    /// Create an owned-buffer write at an explicit file offset.
    pub fn write_at(&self, fd: RawFd, buffer: Vec<u8>, offset: u64) -> WriteOwned {
        WriteOwned {
            ring: self.clone(),
            fd,
            offset,
            buffer: Some(buffer),
            key: None,
        }
    }

    /// Create an owned-buffer vectored read. Each vector remains alive until
    /// the CQE, including when the future is cancelled.
    pub fn readv(&self, fd: RawFd, buffers: Vec<Vec<u8>>) -> ReadvOwned {
        self.readv_at(fd, buffers, 0)
    }

    /// Create an owned-buffer vectored read at an explicit file offset.
    pub fn readv_at(&self, fd: RawFd, buffers: Vec<Vec<u8>>, offset: u64) -> ReadvOwned {
        ReadvOwned {
            ring: self.clone(),
            fd,
            offset,
            resource: Some(vectored_resource(buffers)),
            key: None,
        }
    }

    /// Create an owned-buffer vectored write.
    pub fn writev(&self, fd: RawFd, buffers: Vec<Vec<u8>>) -> WritevOwned {
        self.writev_at(fd, buffers, 0)
    }

    /// Create an owned-buffer vectored write at an explicit file offset.
    pub fn writev_at(&self, fd: RawFd, buffers: Vec<Vec<u8>>, offset: u64) -> WritevOwned {
        WritevOwned {
            ring: self.clone(),
            fd,
            offset,
            resource: Some(vectored_resource(buffers)),
            key: None,
        }
    }

    /// Register owned buffers for fixed-buffer operations.
    pub fn register_buffers(&self, buffers: Vec<Vec<u8>>) -> io::Result<RegisteredBuffers> {
        if buffers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "eddy: at least one io_uring buffer is required",
            ));
        }
        if buffers.len() > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "eddy: too many io_uring registered buffers",
            ));
        }
        for buffer in &buffers {
            if buffer.len() > u32::MAX as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "eddy: registered io_uring buffer is too large",
                ));
            }
        }
        let ring_fd = duplicate_fd(self.inner.fd)?;
        let iovecs = buffers
            .iter()
            .map(|buffer| libc::iovec {
                iov_base: buffer.as_ptr().cast_mut().cast(),
                iov_len: buffer.len(),
            })
            .collect::<Vec<_>>();
        register_resources(
            self.inner.fd,
            IORING_REGISTER_BUFFERS,
            iovecs.as_ptr().cast(),
            iovecs.len() as u32,
        )?;
        Ok(RegisteredBuffers {
            ring: self.clone(),
            inner: Arc::new(RegisteredBuffersInner { ring_fd, buffers }),
        })
    }

    /// Register owned file descriptors for fixed-file operations.
    pub fn register_files(&self, files: Vec<OwnedFd>) -> io::Result<RegisteredFiles> {
        if files.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "eddy: at least one io_uring file is required",
            ));
        }
        if files.len() > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "eddy: too many io_uring registered files",
            ));
        }
        let ring_fd = duplicate_fd(self.inner.fd)?;
        let raw_fds = files.iter().map(AsRawFd::as_raw_fd).collect::<Vec<_>>();
        register_resources(
            self.inner.fd,
            IORING_REGISTER_FILES,
            raw_fds.as_ptr().cast(),
            raw_fds.len() as u32,
        )?;
        Ok(RegisteredFiles {
            ring: self.clone(),
            inner: Arc::new(RegisteredFilesInner { ring_fd, files }),
        })
    }

    /// Create an asynchronous accept. A descriptor returned by the kernel is
    /// wrapped in `OwnedFd` only after its CQE has been received.
    pub fn accept(&self, fd: RawFd) -> AcceptOwned {
        AcceptOwned {
            ring: self.clone(),
            fd,
            key: None,
        }
    }

    /// Create an asynchronous connect to a socket address.
    pub fn connect(&self, fd: RawFd, address: SocketAddr) -> ConnectOwned {
        let (bytes, length) = socket_addr_bytes(address);
        ConnectOwned {
            ring: self.clone(),
            fd,
            address: Some(OpResource::Address { bytes, length }),
            key: None,
        }
    }

    /// Create an owned-buffer socket send.
    pub fn send(&self, fd: RawFd, buffer: Vec<u8>) -> SendOwned {
        SendOwned {
            ring: self.clone(),
            fd,
            buffer: Some(buffer),
            key: None,
        }
    }

    /// Create an owned-buffer socket receive.
    pub fn recv(&self, fd: RawFd, buffer: Vec<u8>) -> RecvOwned {
        RecvOwned {
            ring: self.clone(),
            fd,
            buffer: Some(buffer),
            key: None,
        }
    }

    /// Report that multishot accept is not supported by this ring state.
    ///
    /// This method deliberately does not submit an SQE. Multishot CQEs reuse
    /// one `user_data` value, while this backend's operation table treats one
    /// CQE as terminal and releases the operation after that CQE. Returning an
    /// explicit error is safer than exposing a future that would lose events
    /// or close a descriptor belonging to a later shot.
    pub fn accept_multishot(&self, _fd: RawFd) -> io::Result<()> {
        Err(multishot_unsupported("accept"))
    }

    /// Report that multishot receive is not supported by this ring state.
    ///
    /// The supplied buffer is borrowed because this method returns
    /// immediately and never gives the kernel access to it. A real multishot
    /// receive API also needs provided-buffer or equivalent per-shot
    /// ownership, which this backend does not implement.
    pub fn recv_multishot(&self, _fd: RawFd, _buffer: &mut [u8]) -> io::Result<()> {
        Err(multishot_unsupported("receive"))
    }

    /// Create an asynchronous close for a descriptor owned by the caller.
    /// The caller must not close or reuse `fd` until this future completes.
    pub fn close(&self, fd: RawFd) -> CloseOwned {
        CloseOwned {
            ring: self.clone(),
            fd,
            key: None,
        }
    }

    /// Create a timeout operation. Expiration is reported as `Ok(())`; kernel
    /// cancellation or setup failures are returned as errors.
    pub fn timeout(&self, duration: Duration) -> TimeoutOwned {
        TimeoutOwned {
            ring: self.clone(),
            buffer: Some(timeout_buffer(duration)),
            key: None,
        }
    }

    fn start_op(
        &self,
        opcode: u8,
        fd: RawFd,
        resource: OpResource,
        offset: u64,
        length: u32,
        rw_flags: u32,
        waker: Waker,
    ) -> Result<usize, (io::Error, OpResource)> {
        let mut state = self.inner.state.lock().unwrap();
        let user_data = match next_user_data(&mut state) {
            Ok(user_data) => user_data,
            Err(error) => return Err((error, resource)),
        };
        let key = state.ops.insert(OpState {
            resource,
            result: None,
            waker: Some(waker),
            user_data,
        });
        state.ops_by_user_data.insert(user_data, key);
        let (addr, resource_length) = match state.ops[key].resource.addr_len() {
            Ok(value) => value,
            Err(error) => {
                state.ops_by_user_data.remove(&user_data);
                let op = state.ops.remove(key);
                return Err((error, op.resource));
            }
        };
        let fixed_file = state.ops[key].resource.fixed_file_index();
        let buffer_index = state.ops[key].resource.buffer_index();
        if let Err(error) = queue_sqe_locked(
            &self.inner,
            &mut state,
            IoUringSqe {
                opcode,
                flags: u8::from(fixed_file.is_some()),
                ioprio: 0,
                fd: fixed_file.unwrap_or(fd),
                off: offset,
                addr,
                len: if length == 0 { resource_length } else { length },
                rw_flags,
                user_data,
                buf_index: buffer_index,
                personality: 0,
                splice_fd_in: 0,
                pad2: [0; 2],
            },
        ) {
            state.ops_by_user_data.remove(&user_data);
            let op = state.ops.remove(key);
            return Err((error, op.resource));
        }
        Ok(key)
    }

    /// Submit all SQEs currently queued. Call once per park iteration to
    /// batch operations rather than entering the kernel per future.
    pub fn submit(&self) -> io::Result<usize> {
        submit_inner(&self.inner)
    }

    /// Submit queued operations and block until at least one CQE is available.
    pub fn submit_and_wait(&self) -> io::Result<()> {
        self.submit()?;
        // SAFETY: waiting on this live ring with no signal mask is the normal
        // io_uring_enter ABI path.
        let result = unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                self.inner.fd,
                0,
                1,
                IORING_ENTER_GETEVENTS,
                ptr::null::<libc::c_void>(),
                0,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        self.reap_completions();
        Ok(())
    }

    /// Reap all currently available CQEs and wake their futures.
    pub fn poll_completions(&self) -> usize {
        self.reap_completions()
    }

    fn reap_completions(&self) -> usize {
        reap_completions_inner(&self.inner)
    }

    fn take_result(&self, key: usize) -> Option<(io::Result<usize>, OpResource)> {
        let mut state = self.inner.state.lock().unwrap();
        let result_is_ready = state.ops.get(key)?.result.is_some();
        if !result_is_ready {
            return None;
        }
        let user_data = state.ops.get(key)?.user_data;
        let mut op = state.ops.remove(key);
        state.ops_by_user_data.remove(&user_data);
        Some((
            op.result.take().expect("ready operation has a result"),
            op.resource,
        ))
    }

    fn take_buffer_result(&self, key: usize) -> Option<(io::Result<usize>, Vec<u8>)> {
        let (result, resource) = self.take_result(key)?;
        match resource {
            OpResource::Buffer(buffer) => Some((result, buffer)),
            _ => unreachable!("eddy: non-buffer operation used as a buffer operation"),
        }
    }

    fn take_vectored_result(&self, key: usize) -> Option<(io::Result<usize>, Vec<Vec<u8>>)> {
        let (result, resource) = self.take_result(key)?;
        match resource {
            OpResource::Vectored { buffers, .. } => Some((result, buffers)),
            _ => unreachable!("eddy: non-vectored operation used as a vectored operation"),
        }
    }

    fn take_fixed_buffer_result(
        &self,
        key: usize,
    ) -> Option<(io::Result<usize>, Arc<RegisteredBuffersInner>)> {
        let (result, resource) = self.take_result(key)?;
        match resource {
            OpResource::FixedBuffer { registration, .. } => Some((result, registration)),
            _ => unreachable!("eddy: non-fixed-buffer operation used as a fixed-buffer operation"),
        }
    }

    fn take_fixed_file_result(
        &self,
        key: usize,
    ) -> Option<(io::Result<usize>, Vec<u8>, Arc<RegisteredFilesInner>)> {
        let (result, resource) = self.take_result(key)?;
        match resource {
            OpResource::FixedFileBuffer {
                registration,
                buffer,
                ..
            } => Some((result, buffer, registration)),
            _ => unreachable!("eddy: non-fixed-file operation used as a fixed-file operation"),
        }
    }

    fn orphan(&self, key: usize) {
        let mut state = self.inner.state.lock().unwrap();
        let Some(is_complete) = state.ops.get(key).map(|op| op.result.is_some()) else {
            return;
        };
        let mut op = state.ops.remove(key);
        state.ops_by_user_data.remove(&op.user_data);
        if is_complete {
            if matches!(op.resource, OpResource::Accept) {
                if let Some(Ok(fd)) = op.result.take() {
                    // A completed ACCEPT can still be cancelled at the future
                    // level before it is polled, so close its returned fd.
                    // SAFETY: the successful CQE transferred this fd here.
                    unsafe { libc::close(fd as RawFd) };
                }
            }
            // The CQE has already arrived, so dropping the retained resource is safe.
            return;
        }
        let user_data = op.user_data;
        let orphan_key = state.orphaned.insert(OrphanedOp {
            resource: op.resource,
            user_data,
        });
        state.orphaned_by_user_data.insert(user_data, orphan_key);
        drop(state);

        // Cancellation is only an optimization. The orphan remains until the
        // target CQE arrives when the kernel cannot cancel this operation.
        self.submit_cancel(user_data);
    }

    fn submit_cancel(&self, target_user_data: u64) {
        let mut state = self.inner.state.lock().unwrap();
        let Ok(cancel_user_data) = next_user_data(&mut state) else {
            return;
        };
        let Ok(()) = queue_sqe_locked(
            &self.inner,
            &mut state,
            IoUringSqe {
                opcode: IORING_OP_ASYNC_CANCEL,
                flags: 0,
                ioprio: 0,
                fd: -1,
                off: 0,
                addr: target_user_data,
                len: 0,
                rw_flags: 0,
                user_data: cancel_user_data,
                buf_index: 0,
                personality: 0,
                splice_fd_in: 0,
                pad2: [0; 2],
            },
        ) else {
            return;
        };
        state
            .cancel_targets
            .insert(cancel_user_data, target_user_data);
        drop(state);
        if self.submit().is_err() {
            self.inner
                .state
                .lock()
                .unwrap()
                .cancel_targets
                .remove(&cancel_user_data);
        }
    }
}

impl IoUringBuilder {
    /// Enable or disable kernel-side submission queue polling.
    pub fn sqpoll(mut self, enabled: bool) -> Self {
        self.sqpoll = enabled;
        self
    }

    /// Pin the SQPOLL thread to a Linux CPU number.
    pub fn sq_thread_cpu(mut self, cpu: u32) -> Self {
        self.sq_thread_cpu = Some(cpu);
        self
    }

    /// Set how long the SQPOLL thread remains active without work.
    pub fn sq_thread_idle(mut self, idle: Duration) -> Self {
        self.sq_thread_idle = Some(idle);
        self
    }

    pub fn build(self) -> io::Result<IoUring> {
        IoUring::new_with_config(self)
    }
}

fn is_unavailable(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ENOSYS | libc::EPERM | libc::EACCES | libc::ENODEV)
    )
}

fn multishot_unsupported(operation: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "eddy: io_uring multishot {operation} is unsupported by the single-CQE operation state"
        ),
    )
}

fn duplicate_fd(fd: RawFd) -> io::Result<OwnedFd> {
    // SAFETY: fcntl does not retain a Rust pointer and duplicates this live fd.
    let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fcntl returned a new owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
}

fn register_resources(
    fd: RawFd,
    opcode: u32,
    arg: *const libc::c_void,
    count: u32,
) -> io::Result<()> {
    // SAFETY: `arg` points to the ABI array for the duration of the syscall,
    // and fd is the live ring descriptor.
    let result = unsafe { libc::syscall(libc::SYS_io_uring_register, fd, opcode, arg, count) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn vectored_resource(buffers: Vec<Vec<u8>>) -> OpResource {
    let iovecs = buffers
        .iter()
        .map(|buffer| libc::iovec {
            iov_base: buffer.as_ptr().cast_mut().cast(),
            iov_len: buffer.len(),
        })
        .collect();
    OpResource::Vectored { buffers, iovecs }
}

fn socket_addr_bytes(address: SocketAddr) -> (Vec<u8>, libc::socklen_t) {
    // SAFETY: zeroed storage is a valid initial state for either IPv4 or IPv6
    // socket address, and both types fit in sockaddr_storage.
    let mut storage = unsafe { std::mem::zeroed::<libc::sockaddr_storage>() };
    let length = match address {
        SocketAddr::V4(address) => {
            // SAFETY: sockaddr_storage is aligned and large enough for sockaddr_in.
            let sin = unsafe {
                &mut *(&mut storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in)
            };
            sin.sin_family = libc::AF_INET as _;
            sin.sin_port = address.port().to_be();
            sin.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(address.ip().octets()),
            };
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(address) => {
            // SAFETY: sockaddr_storage is aligned and large enough for sockaddr_in6.
            let sin6 = unsafe {
                &mut *(&mut storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in6)
            };
            sin6.sin6_family = libc::AF_INET6 as _;
            sin6.sin6_port = address.port().to_be();
            sin6.sin6_flowinfo = address.flowinfo();
            sin6.sin6_addr = libc::in6_addr {
                s6_addr: address.ip().octets(),
            };
            sin6.sin6_scope_id = address.scope_id();
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    };
    // SAFETY: `storage` is initialized above and `length` is the size of the
    // active sockaddr variant.
    let bytes = unsafe {
        std::slice::from_raw_parts(
            (&storage as *const libc::sockaddr_storage).cast::<u8>(),
            length as usize,
        )
    }
    .to_vec();
    (bytes, length)
}

fn next_user_data(state: &mut RingState) -> io::Result<u64> {
    let candidate = state.next_user_data;
    state.next_user_data = state.next_user_data.wrapping_add(1).max(1);
    if candidate != 0
        && !state.ops_by_user_data.contains_key(&candidate)
        && !state.orphaned_by_user_data.contains_key(&candidate)
    {
        Ok(candidate)
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "eddy: io_uring user_data space exhausted",
        ))
    }
}

unsafe fn load_ring_u32(pointer: *mut u32, ordering: Ordering) -> u32 {
    // SAFETY: ring offsets are u32-aligned kernel ABI fields in shared mmap.
    unsafe { (&*pointer.cast::<AtomicU32>()).load(ordering) }
}

unsafe fn store_ring_u32(pointer: *mut u32, value: u32, ordering: Ordering) {
    // SAFETY: ring offsets are u32-aligned kernel ABI fields in shared mmap.
    unsafe { (&*pointer.cast::<AtomicU32>()).store(value, ordering) };
}

fn queue_sqe_locked(inner: &Inner, state: &mut RingState, sqe_value: IoUringSqe) -> io::Result<()> {
    // SAFETY: these offsets are supplied by the kernel in `IoUringParams`.
    let tail = unsafe { inner.sq_ring.at::<u32>(inner.params.sq_off.tail) };
    let head = unsafe { inner.sq_ring.at::<u32>(inner.params.sq_off.head) };
    // SAFETY: these pointers are kernel-provided ring cursors.
    let (tail_value, head_value, mask) = unsafe {
        (
            load_ring_u32(tail, Ordering::Relaxed),
            load_ring_u32(head, Ordering::Acquire),
            load_ring_u32(
                inner.sq_ring.at::<u32>(inner.params.sq_off.ring_mask),
                Ordering::Relaxed,
            ),
        )
    };
    if tail_value.wrapping_sub(head_value) >= inner.params.sq_entries {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "eddy: io_uring submission queue is full",
        ));
    }
    let index = tail_value & mask;
    // SAFETY: the SQE mapping is sized from the kernel's SQ entry count and
    // `index` is masked to a valid slot.
    let sqe = unsafe {
        inner
            .sqes
            .ptr
            .as_ptr()
            .add(index as usize * size_of::<IoUringSqe>())
            .cast::<IoUringSqe>()
    };
    // SAFETY: the SQE is the slot selected by the ring mask and remains
    // owned by userspace until the release-store of the SQ tail.
    unsafe {
        ptr::write(sqe, sqe_value);
        ptr::write_volatile(
            inner
                .sq_ring
                .at::<u32>(inner.params.sq_off.array + index * size_of::<u32>() as u32),
            index,
        );
        store_ring_u32(tail, tail_value.wrapping_add(1), Ordering::Release);
    }
    state.sq_pending += 1;
    Ok(())
}

fn submit_inner(inner: &Inner) -> io::Result<usize> {
    let mut state = inner.state.lock().unwrap();
    let pending = state.sq_pending;
    if pending == 0 {
        return Ok(0);
    }
    if inner.params.flags & IORING_SETUP_SQPOLL != 0 {
        // SQPOLL consumes SQEs directly from the shared ring. An enter call
        // is needed only after the kernel thread has gone idle.
        let flags = unsafe {
            load_ring_u32(
                inner.sq_ring.at::<u32>(inner.params.sq_off.flags),
                Ordering::Acquire,
            )
        };
        if flags & IORING_SQ_NEED_WAKEUP != 0 {
            loop {
                // SAFETY: this is the documented SQPOLL wakeup ABI for the
                // live ring, with no userspace SQEs passed to enter.
                let result = unsafe {
                    libc::syscall(
                        libc::SYS_io_uring_enter,
                        inner.fd,
                        0,
                        0,
                        IORING_ENTER_SQ_WAKEUP,
                        ptr::null::<libc::c_void>(),
                        0,
                    )
                };
                if result >= 0 {
                    break;
                }
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(error);
            }
        }
        state.sq_pending = 0;
        return Ok(pending as usize);
    }
    loop {
        // SAFETY: the ring fd and arguments are valid; SQEs were published
        // while holding the same state mutex.
        let submitted = unsafe {
            libc::syscall(
                libc::SYS_io_uring_enter,
                inner.fd,
                pending,
                0,
                0,
                ptr::null::<libc::c_void>(),
                0,
            )
        };
        if submitted >= 0 {
            state.sq_pending -= submitted as u32;
            return Ok(submitted as usize);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(error);
    }
}

fn reap_completions_inner(inner: &Inner) -> usize {
    let mut wake = Vec::new();
    let mut state = inner.state.lock().unwrap();
    // SAFETY: these offsets are supplied by the kernel in `IoUringParams`.
    let head = unsafe { inner.cq_ring.at::<u32>(inner.params.cq_off.head) };
    let tail = unsafe { inner.cq_ring.at::<u32>(inner.params.cq_off.tail) };
    // SAFETY: the ring mask is a kernel-provided u32 field in the CQ mapping.
    let mask = unsafe {
        load_ring_u32(
            inner.cq_ring.at::<u32>(inner.params.cq_off.ring_mask),
            Ordering::Relaxed,
        )
    };
    // SAFETY: head/tail are kernel-owned cursors and CQE slots are initialized
    // before the kernel advances tail.
    let (mut current, available) = unsafe {
        let current = load_ring_u32(head, Ordering::Relaxed);
        let tail_value = load_ring_u32(tail, Ordering::Acquire);
        (current, tail_value.wrapping_sub(current))
    };
    for _ in 0..available {
        // SAFETY: the CQE mapping is sized from the kernel's CQ entry count
        // and `current` is masked to a valid slot.
        let cqe = unsafe {
            inner.cq_ring.at::<IoUringCqe>(
                inner.params.cq_off.cqes + (current & mask) * size_of::<IoUringCqe>() as u32,
            )
        };
        // SAFETY: this CQE lies within the mapped CQ ring.
        let cqe = unsafe { ptr::read_volatile(cqe) };
        #[cfg(test)]
        eprintln!(
            "io_uring cqe user_data={} res={} flags={} cancel={} orphan={} op={}",
            cqe.user_data,
            cqe.res,
            cqe.flags,
            state.cancel_targets.contains_key(&cqe.user_data),
            state.orphaned_by_user_data.contains_key(&cqe.user_data),
            state.ops_by_user_data.contains_key(&cqe.user_data),
        );
        if let Some(target_user_data) = state.cancel_targets.remove(&cqe.user_data) {
            if cqe.res == 0 {
                if let Some(key) = state.orphaned_by_user_data.remove(&target_user_data) {
                    state.orphaned.remove(key);
                } else if let Some(key) = state.ops_by_user_data.remove(&target_user_data) {
                    state.ops.remove(key);
                }
            }
        } else if let Some(&key) = state.ops_by_user_data.get(&cqe.user_data) {
            if let Some(op) = state.ops.get_mut(key) {
                op.result = Some(if cqe.res < 0 {
                    Err(io::Error::from_raw_os_error(-(cqe.res as i64) as i32))
                } else {
                    Ok(cqe.res as usize)
                });
                if let Some(waker) = op.waker.take() {
                    wake.push(waker);
                }
            }
        } else if let Some(&key) = state.orphaned_by_user_data.get(&cqe.user_data) {
            state.orphaned_by_user_data.remove(&cqe.user_data);
            let orphan = state.orphaned.remove(key);
            if matches!(orphan.resource, OpResource::Accept) && cqe.res >= 0 {
                // An accepted descriptor belongs to the cancelled operation;
                // do not leak it merely because its future was dropped.
                // SAFETY: a successful ACCEPT CQE contains a live owned fd.
                unsafe { libc::close(cqe.res) };
            }
        }
        current = current.wrapping_add(1);
    }
    // SAFETY: publishing consumed CQEs after reading their contents.
    unsafe { store_ring_u32(head, current, Ordering::Release) };
    drop(state);
    for waker in wake {
        waker.wake();
    }
    available as usize
}

impl Drop for Inner {
    fn drop(&mut self) {
        // A future normally submits a best-effort cancel when it is dropped.
        // This final pass covers the case where the future owned the last ring
        // handle, and keeps every kernel-visible buffer alive until its CQE.
        let _ = submit_inner(self);
        let targets = {
            let mut state = self.state.lock().unwrap();
            let targets = state
                .ops
                .iter()
                .map(|(_, op)| op.user_data)
                .chain(state.orphaned.iter().map(|(_, op)| op.user_data))
                .collect::<Vec<_>>();
            for target in &targets {
                let Ok(cancel_user_data) = next_user_data(&mut state) else {
                    break;
                };
                if queue_sqe_locked(
                    self,
                    &mut state,
                    IoUringSqe {
                        opcode: IORING_OP_ASYNC_CANCEL,
                        flags: 0,
                        ioprio: 0,
                        fd: -1,
                        off: 0,
                        addr: *target,
                        len: 0,
                        rw_flags: 0,
                        user_data: cancel_user_data,
                        buf_index: 0,
                        personality: 0,
                        splice_fd_in: 0,
                        pad2: [0; 2],
                    },
                )
                .is_ok()
                {
                    state.cancel_targets.insert(cancel_user_data, *target);
                }
            }
            targets
        };
        let _ = submit_inner(self);
        while !targets.is_empty() {
            let _ = reap_completions_inner(self);
            let remaining = {
                let mut state = self.state.lock().unwrap();
                let completed = state
                    .ops
                    .iter()
                    .filter_map(|(key, op)| op.result.is_some().then_some((key, op.user_data)))
                    .collect::<Vec<_>>();
                for (key, user_data) in completed {
                    state.ops_by_user_data.remove(&user_data);
                    state.ops.remove(key);
                }
                state.ops.len() + state.orphaned.len()
            };
            if remaining == 0 {
                break;
            }
            // SAFETY: this ring remains open and all operation buffers remain
            // owned by `state` while the kernel drains the cancellation CQEs.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_io_uring_enter,
                    self.fd,
                    0,
                    1,
                    IORING_ENTER_GETEVENTS,
                    ptr::null::<libc::c_void>(),
                    0,
                )
            };
            if result < 0 && io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
        // SAFETY: no cloned IoUring remains when Arc invokes this destructor.
        unsafe { libc::close(self.fd) };
    }
}

/// Owned-buffer read future. Dropping it moves the operation into the ring's
/// orphan state; its buffer is released only after the CQE arrives.
pub struct ReadOwned {
    ring: IoUring,
    fd: RawFd,
    offset: u64,
    buffer: Option<Vec<u8>>,
    key: Option<usize>,
}

impl Future for ReadOwned {
    type Output = (io::Result<usize>, Vec<u8>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, buffer)) = this.ring.take_buffer_result(key) {
                this.key = None;
                return Poll::Ready((result, buffer));
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }
        let buffer = this
            .buffer
            .take()
            .expect("read future polled after completion");
        let length = match u32::try_from(buffer.len()) {
            Ok(length) => length,
            Err(error) => {
                return Poll::Ready((
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("eddy: read buffer is too large: {error}"),
                    )),
                    buffer,
                ));
            }
        };
        match this.ring.start_op(
            IORING_OP_READ,
            this.fd,
            OpResource::Buffer(buffer),
            this.offset,
            length,
            0,
            cx.waker().clone(),
        ) {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready((Err(error), Vec::new()));
                }
                Poll::Pending
            }
            Err((error, OpResource::Buffer(buffer))) => Poll::Ready((Err(error), buffer)),
            Err((_error, _)) => unreachable!("eddy: read operation lost its buffer"),
        }
    }
}

impl Drop for ReadOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

/// Owned-buffer write future. The buffer is returned with the completion
/// result, including after a partial write.
pub struct WriteOwned {
    ring: IoUring,
    fd: RawFd,
    offset: u64,
    buffer: Option<Vec<u8>>,
    key: Option<usize>,
}

impl Future for WriteOwned {
    type Output = (io::Result<usize>, Vec<u8>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, buffer)) = this.ring.take_buffer_result(key) {
                this.key = None;
                return Poll::Ready((result, buffer));
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }
        let buffer = this
            .buffer
            .take()
            .expect("write future polled after completion");
        let length = match u32::try_from(buffer.len()) {
            Ok(length) => length,
            Err(error) => {
                return Poll::Ready((
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("eddy: write buffer is too large: {error}"),
                    )),
                    buffer,
                ));
            }
        };
        match this.ring.start_op(
            IORING_OP_WRITE,
            this.fd,
            OpResource::Buffer(buffer),
            this.offset,
            length,
            0,
            cx.waker().clone(),
        ) {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready((Err(error), Vec::new()));
                }
                Poll::Pending
            }
            Err((error, OpResource::Buffer(buffer))) => Poll::Ready((Err(error), buffer)),
            Err((_error, _)) => unreachable!("eddy: write operation lost its buffer"),
        }
    }
}

impl Drop for WriteOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

/// Owned-buffer vectored read future.
pub struct ReadvOwned {
    ring: IoUring,
    fd: RawFd,
    offset: u64,
    resource: Option<OpResource>,
    key: Option<usize>,
}

impl Future for ReadvOwned {
    type Output = (io::Result<usize>, Vec<Vec<u8>>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, buffers)) = this.ring.take_vectored_result(key) {
                this.key = None;
                return Poll::Ready((result, buffers));
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }
        let resource = this
            .resource
            .take()
            .expect("readv future polled after completion");
        match this.ring.start_op(
            IORING_OP_READV,
            this.fd,
            resource,
            this.offset,
            0,
            0,
            cx.waker().clone(),
        ) {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready((Err(error), Vec::new()));
                }
                Poll::Pending
            }
            Err((error, OpResource::Vectored { buffers, .. })) => {
                Poll::Ready((Err(error), buffers))
            }
            Err((_error, _)) => unreachable!("eddy: readv operation lost its vectors"),
        }
    }
}

impl Drop for ReadvOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

/// Owned-buffer vectored write future.
pub struct WritevOwned {
    ring: IoUring,
    fd: RawFd,
    offset: u64,
    resource: Option<OpResource>,
    key: Option<usize>,
}

impl Future for WritevOwned {
    type Output = (io::Result<usize>, Vec<Vec<u8>>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, buffers)) = this.ring.take_vectored_result(key) {
                this.key = None;
                return Poll::Ready((result, buffers));
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }
        let resource = this
            .resource
            .take()
            .expect("writev future polled after completion");
        match this.ring.start_op(
            IORING_OP_WRITEV,
            this.fd,
            resource,
            this.offset,
            0,
            0,
            cx.waker().clone(),
        ) {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready((Err(error), Vec::new()));
                }
                Poll::Pending
            }
            Err((error, OpResource::Vectored { buffers, .. })) => {
                Poll::Ready((Err(error), buffers))
            }
            Err((_error, _)) => unreachable!("eddy: writev operation lost its vectors"),
        }
    }
}

impl Drop for WritevOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

/// Asynchronous accept future.
pub struct AcceptOwned {
    ring: IoUring,
    fd: RawFd,
    key: Option<usize>,
}

impl Future for AcceptOwned {
    type Output = io::Result<OwnedFd>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, resource)) = this.ring.take_result(key) {
                this.key = None;
                debug_assert!(matches!(resource, OpResource::Accept));
                return Poll::Ready(result.map(|fd| {
                    // SAFETY: a successful ACCEPT CQE transfers ownership of
                    // this descriptor to the future.
                    unsafe { OwnedFd::from_raw_fd(fd as RawFd) }
                }));
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }
        match this.ring.start_op(
            IORING_OP_ACCEPT,
            this.fd,
            OpResource::Accept,
            0,
            0,
            (libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC) as u32,
            cx.waker().clone(),
        ) {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Pending
            }
            Err((error, _)) => Poll::Ready(Err(error)),
        }
    }
}

impl Drop for AcceptOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

/// Asynchronous connect future.
pub struct ConnectOwned {
    ring: IoUring,
    fd: RawFd,
    address: Option<OpResource>,
    key: Option<usize>,
}

impl Future for ConnectOwned {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, resource)) = this.ring.take_result(key) {
                this.key = None;
                debug_assert!(matches!(resource, OpResource::Address { .. }));
                return Poll::Ready(result.map(|_| ()));
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }
        let resource = this
            .address
            .take()
            .expect("connect future polled after completion");
        match this.ring.start_op(
            IORING_OP_CONNECT,
            this.fd,
            resource,
            0,
            0,
            0,
            cx.waker().clone(),
        ) {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Pending
            }
            Err((error, _)) => Poll::Ready(Err(error)),
        }
    }
}

impl Drop for ConnectOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

/// Owned-buffer socket send future.
pub struct SendOwned {
    ring: IoUring,
    fd: RawFd,
    buffer: Option<Vec<u8>>,
    key: Option<usize>,
}

impl Future for SendOwned {
    type Output = (io::Result<usize>, Vec<u8>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, buffer)) = this.ring.take_buffer_result(key) {
                this.key = None;
                return Poll::Ready((result, buffer));
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }
        let buffer = this
            .buffer
            .take()
            .expect("send future polled after completion");
        let length = match u32::try_from(buffer.len()) {
            Ok(length) => length,
            Err(error) => {
                return Poll::Ready((
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("eddy: send buffer is too large: {error}"),
                    )),
                    buffer,
                ));
            }
        };
        match this.ring.start_op(
            IORING_OP_SEND,
            this.fd,
            OpResource::Buffer(buffer),
            0,
            length,
            0,
            cx.waker().clone(),
        ) {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready((Err(error), Vec::new()));
                }
                Poll::Pending
            }
            Err((error, OpResource::Buffer(buffer))) => Poll::Ready((Err(error), buffer)),
            Err((_error, _)) => unreachable!("eddy: send operation lost its buffer"),
        }
    }
}

impl Drop for SendOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

/// Owned-buffer socket receive future.
pub struct RecvOwned {
    ring: IoUring,
    fd: RawFd,
    buffer: Option<Vec<u8>>,
    key: Option<usize>,
}

impl Future for RecvOwned {
    type Output = (io::Result<usize>, Vec<u8>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, buffer)) = this.ring.take_buffer_result(key) {
                this.key = None;
                return Poll::Ready((result, buffer));
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }
        let buffer = this
            .buffer
            .take()
            .expect("recv future polled after completion");
        let length = match u32::try_from(buffer.len()) {
            Ok(length) => length,
            Err(error) => {
                return Poll::Ready((
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("eddy: recv buffer is too large: {error}"),
                    )),
                    buffer,
                ));
            }
        };
        match this.ring.start_op(
            IORING_OP_RECV,
            this.fd,
            OpResource::Buffer(buffer),
            0,
            length,
            0,
            cx.waker().clone(),
        ) {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready((Err(error), Vec::new()));
                }
                Poll::Pending
            }
            Err((error, OpResource::Buffer(buffer))) => Poll::Ready((Err(error), buffer)),
            Err((_error, _)) => unreachable!("eddy: recv operation lost its buffer"),
        }
    }
}

impl Drop for RecvOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

/// A fixed-buffer read or write. The registration is returned when the CQE is
/// observed. `None` is returned only when submission failed or the future was
/// cancelled after the kernel acquired the registration; the ring retains it
/// until the operation's CQE in both cases.
pub struct FixedBufferOwned {
    ring: IoUring,
    fd: RawFd,
    index: usize,
    length: usize,
    registration: Option<RegisteredBuffers>,
    key: Option<usize>,
    opcode: u8,
}

impl Future for FixedBufferOwned {
    type Output = (io::Result<usize>, Option<RegisteredBuffers>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, inner)) = this.ring.take_fixed_buffer_result(key) {
                this.key = None;
                return Poll::Ready((
                    result,
                    Some(RegisteredBuffers {
                        ring: this.ring.clone(),
                        inner,
                    }),
                ));
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }

        let registration = this
            .registration
            .take()
            .expect("fixed-buffer future polled after completion");
        let Some(buffer) = registration.inner.buffers.get(this.index) else {
            return Poll::Ready((
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "eddy: fixed-buffer index is out of range",
                )),
                Some(registration),
            ));
        };
        let Ok(index) = u16::try_from(this.index) else {
            return Poll::Ready((
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "eddy: fixed-buffer index exceeds the io_uring ABI",
                )),
                Some(registration),
            ));
        };
        if this.length > buffer.len() || this.length > u32::MAX as usize {
            return Poll::Ready((
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "eddy: fixed-buffer operation exceeds the registered buffer",
                )),
                Some(registration),
            ));
        }
        let resource = OpResource::FixedBuffer {
            registration: registration.inner.clone(),
            index,
            length: this.length as u32,
        };
        match this.ring.start_op(
            this.opcode,
            this.fd,
            resource,
            0,
            this.length as u32,
            0,
            cx.waker().clone(),
        ) {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready((Err(error), None));
                }
                Poll::Pending
            }
            Err((error, _)) => Poll::Ready((Err(error), Some(registration))),
        }
    }
}

impl Drop for FixedBufferOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

/// A fixed-file read or write using an owned operation buffer.
pub struct FixedFileOwned {
    ring: IoUring,
    index: usize,
    buffer: Option<Vec<u8>>,
    registration: Option<RegisteredFiles>,
    key: Option<usize>,
    opcode: u8,
}

impl Future for FixedFileOwned {
    type Output = (io::Result<usize>, Vec<u8>, Option<RegisteredFiles>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, buffer, inner)) = this.ring.take_fixed_file_result(key) {
                this.key = None;
                return Poll::Ready((
                    result,
                    buffer,
                    Some(RegisteredFiles {
                        ring: this.ring.clone(),
                        inner,
                    }),
                ));
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }

        let registration = this
            .registration
            .take()
            .expect("fixed-file future polled after completion");
        let buffer = this
            .buffer
            .take()
            .expect("fixed-file future polled after completion");
        let Some(_) = registration.inner.files.get(this.index) else {
            return Poll::Ready((
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "eddy: fixed-file index is out of range",
                )),
                buffer,
                Some(registration),
            ));
        };
        let Ok(index) = i32::try_from(this.index) else {
            return Poll::Ready((
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "eddy: fixed-file index exceeds the io_uring ABI",
                )),
                buffer,
                Some(registration),
            ));
        };
        let resource = OpResource::FixedFileBuffer {
            registration: registration.inner.clone(),
            index: index as u32,
            buffer,
        };
        match this
            .ring
            .start_op(this.opcode, -1, resource, 0, 0, 0, cx.waker().clone())
        {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready((Err(error), Vec::new(), None));
                }
                Poll::Pending
            }
            Err((error, OpResource::FixedFileBuffer { buffer, .. })) => {
                Poll::Ready((Err(error), buffer, Some(registration)))
            }
            Err((_error, _)) => unreachable!("eddy: fixed-file operation lost its buffer"),
        }
    }
}

impl Drop for FixedFileOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

/// Asynchronous close future. The descriptor must remain valid until its CQE.
pub struct CloseOwned {
    ring: IoUring,
    fd: RawFd,
    key: Option<usize>,
}

impl Future for CloseOwned {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, resource)) = this.ring.take_result(key) {
                this.key = None;
                debug_assert!(matches!(resource, OpResource::Close));
                return Poll::Ready(result.map(|_| ()));
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }
        match this.ring.start_op(
            IORING_OP_CLOSE,
            this.fd,
            OpResource::Close,
            0,
            0,
            0,
            cx.waker().clone(),
        ) {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Pending
            }
            Err((error, _)) => Poll::Ready(Err(error)),
        }
    }
}

impl Drop for CloseOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

/// Owned timeout future. The kernel timespec remains in the ring's operation
/// state until the timeout CQE arrives, including when this future is dropped.
pub struct TimeoutOwned {
    ring: IoUring,
    buffer: Option<Vec<u8>>,
    key: Option<usize>,
}

impl Future for TimeoutOwned {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(key) = this.key {
            if let Some((result, buffer)) = this.ring.take_buffer_result(key) {
                this.key = None;
                drop(buffer);
                return Poll::Ready(match result {
                    Ok(_) => Ok(()),
                    Err(error) if error.raw_os_error() == Some(libc::ETIME) => Ok(()),
                    Err(error) => Err(error),
                });
            }
            if let Some(op) = this.ring.inner.state.lock().unwrap().ops.get_mut(key) {
                op.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }

        let buffer = this
            .buffer
            .take()
            .expect("timeout future polled after completion");
        match this.ring.start_op(
            IORING_OP_TIMEOUT,
            -1,
            OpResource::Buffer(buffer),
            0,
            1,
            0,
            cx.waker().clone(),
        ) {
            Ok(key) => {
                this.key = Some(key);
                if let Err(error) = this.ring.submit() {
                    this.ring.orphan(key);
                    this.key = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Pending
            }
            Err((error, _)) => Poll::Ready(Err(error)),
        }
    }
}

impl Drop for TimeoutOwned {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.ring.orphan(key);
        }
    }
}

fn timeout_buffer(duration: Duration) -> Vec<u8> {
    // `__kernel_timespec` is two native-endian 64-bit integers. Write bytes
    // explicitly so the Vec<u8> does not need typed-pointer alignment.
    let mut buffer = vec![0_u8; 16];
    buffer[..8].copy_from_slice(&duration.as_secs().min(i64::MAX as u64).to_ne_bytes());
    buffer[8..].copy_from_slice(&i64::from(duration.subsec_nanos()).to_ne_bytes());
    buffer
}

/// Trait for completion-based reads whose buffer ownership is explicit.
pub trait AsyncReadOwned {
    fn read_owned(&self, fd: RawFd, buffer: Vec<u8>) -> ReadOwned;
}

impl AsyncReadOwned for IoUring {
    fn read_owned(&self, fd: RawFd, buffer: Vec<u8>) -> ReadOwned {
        self.read(fd, buffer)
    }
}

/// Trait for completion-based writes whose buffer ownership is explicit.
pub trait AsyncWriteOwned {
    fn write_owned(&self, fd: RawFd, buffer: Vec<u8>) -> WriteOwned;
}

impl AsyncWriteOwned for IoUring {
    fn write_owned(&self, fd: RawFd, buffer: Vec<u8>) -> WriteOwned {
        self.write(fd, buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
    use std::task::Context;

    #[test]
    fn capability_probe_has_a_readiness_fallback() {
        assert!(matches!(
            IoUring::new_or_fallback(0),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
        let probe = IoUring::probe(2).unwrap();
        assert_eq!(probe, IoUring::new_or_fallback(2).unwrap().is_some());
    }

    #[test]
    fn multishot_operations_report_explicit_unsupported() {
        let Ok(ring) = IoUring::new(2) else {
            return;
        };
        let accept_error = ring.accept_multishot(-1).unwrap_err();
        assert_eq!(accept_error.kind(), io::ErrorKind::Unsupported);
        let mut buffer = [0_u8; 1];
        let recv_error = ring.recv_multishot(-1, &mut buffer).unwrap_err();
        assert_eq!(recv_error.kind(), io::ErrorKind::Unsupported);
    }

    #[test]
    #[ignore = "SQPOLL is unreliable on shared CI kernels"]
    fn sqpoll_is_explicit_and_uses_the_kernel_wakeup_path() {
        let Ok(ring) = IoUring::builder(8).sqpoll(true).build() else {
            return;
        };
        assert!(ring.is_sqpoll());
        let path = std::env::temp_dir().join(format!(
            "eddy-uring-sqpoll-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(&path, b"sqpoll").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let mut read = Box::pin(ring.read(file.as_raw_fd(), vec![0; 6]));
        let waker = crate::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(read.as_mut().poll(&mut cx), Poll::Pending));
        ring.submit().unwrap();
        let completed = (0..5_000).find_map(|_| {
            ring.poll_completions();
            if let Poll::Ready(value) = read.as_mut().poll(&mut cx) {
                Some(value)
            } else {
                std::thread::sleep(Duration::from_millis(1));
                None
            }
        });
        let (result, buffer) = completed.expect("SQPOLL CQE was not delivered");
        assert_eq!(result.unwrap(), 6);
        assert_eq!(buffer, b"sqpoll");
        drop(file);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn registered_buffers_are_returned_after_fixed_io() {
        let Ok(ring) = IoUring::new(8) else {
            return;
        };
        let path =
            std::env::temp_dir().join(format!("eddy-uring-fixed-buffer-{}", std::process::id()));
        std::fs::write(&path, b"fixed io").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let buffers = match ring.register_buffers(vec![vec![0; 8]]) {
            Ok(buffers) => buffers,
            Err(error) if is_unavailable(&error) || error.raw_os_error() == Some(libc::EINVAL) => {
                return
            }
            Err(error) => panic!("register_buffers failed: {error}"),
        };
        let mut read = Box::pin(buffers.read_fixed(file.as_raw_fd(), 0, 8));
        let waker = crate::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(read.as_mut().poll(&mut cx), Poll::Pending));
        ring.submit_and_wait().unwrap();
        let (result, buffers) = match read.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("fixed-buffer CQE was not delivered"),
        };
        assert_eq!(result.unwrap(), 8);
        assert_eq!(buffers.unwrap().get(0).unwrap(), b"fixed io");
        drop(file);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn registered_files_keep_file_ownership_until_fixed_io_completes() {
        let Ok(ring) = IoUring::new(8) else {
            return;
        };
        let path =
            std::env::temp_dir().join(format!("eddy-uring-fixed-file-{}", std::process::id()));
        std::fs::write(&path, b"fixed file").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let files = match ring.register_files(vec![file.into()]) {
            Ok(files) => files,
            Err(error) if is_unavailable(&error) || error.raw_os_error() == Some(libc::EINVAL) => {
                return
            }
            Err(error) => panic!("register_files failed: {error}"),
        };
        let mut read = Box::pin(files.read_fixed(0, vec![0; 10]));
        let waker = crate::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(read.as_mut().poll(&mut cx), Poll::Pending));
        ring.submit_and_wait().unwrap();
        let (result, buffer, files) = match read.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("fixed-file CQE was not delivered"),
        };
        assert_eq!(result.unwrap(), 10);
        assert_eq!(buffer, b"fixed file");
        assert_eq!(files.unwrap().len(), 1);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn owned_read_keeps_the_buffer_until_the_cqe() {
        let Ok(ring) = IoUring::new(8) else {
            return;
        };
        let path = std::env::temp_dir().join(format!("eddy-uring-{}", std::process::id()));
        std::fs::write(&path, b"io_uring").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let mut read = Box::pin(ring.read(file.as_raw_fd(), vec![0; 8]));
        let waker = crate::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(read.as_mut().poll(&mut cx), Poll::Pending));
        ring.submit_and_wait().unwrap();
        let (result, buffer) = match read.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("CQE was not delivered"),
        };
        assert_eq!(result.unwrap(), 8);
        assert_eq!(&buffer, b"io_uring");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn owned_write_returns_the_buffer_and_writes_at_an_offset() {
        let Ok(ring) = IoUring::new(8) else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "eddy-uring-write-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut write = Box::pin(ring.write_at(file.as_raw_fd(), b"uring".to_vec(), 2));
        let waker = crate::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(write.as_mut().poll(&mut cx), Poll::Pending));
        ring.submit_and_wait().unwrap();
        let (result, buffer) = match write.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("CQE was not delivered"),
        };
        assert_eq!(result.unwrap(), 5);
        assert_eq!(buffer, b"uring");
        drop(file);
        assert_eq!(std::fs::read(&path).unwrap(), b"\0\0uring");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn owned_vectored_read_and_write_retain_all_vectors_until_the_cqe() {
        let Ok(ring) = IoUring::new(8) else {
            return;
        };
        let path = std::env::temp_dir().join(format!("eddy-uring-vectored-{}", std::process::id()));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let waker = crate::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut write = Box::pin(ring.writev(
            file.as_raw_fd(),
            vec![b"vector ".to_vec(), b"write".to_vec()],
        ));
        assert!(matches!(write.as_mut().poll(&mut cx), Poll::Pending));
        ring.submit_and_wait().unwrap();
        let (result, buffers) = match write.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("vectored write CQE was not delivered"),
        };
        assert_eq!(result.unwrap(), 12);
        assert_eq!(buffers, vec![b"vector ".to_vec(), b"write".to_vec()]);

        let mut read = Box::pin(ring.readv(file.as_raw_fd(), vec![vec![0; 7], vec![0; 5]]));
        assert!(matches!(read.as_mut().poll(&mut cx), Poll::Pending));
        ring.submit_and_wait().unwrap();
        let (result, buffers) = match read.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("vectored read CQE was not delivered"),
        };
        assert_eq!(result.unwrap(), 12);
        assert_eq!(buffers, vec![b"vector ".to_vec(), b"write".to_vec()]);
        drop(file);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn socket_accept_connect_send_recv_and_close_complete() {
        let Ok(ring) = IoUring::new(16) else {
            return;
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        // SAFETY: socket returns a new descriptor or -1, which is checked.
        let client_fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
        assert!(client_fd >= 0);
        let waker = crate::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut accept = Box::pin(ring.accept(listener.as_raw_fd()));
        let mut connect = Box::pin(ring.connect(client_fd, address));
        assert!(matches!(accept.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(connect.as_mut().poll(&mut cx), Poll::Pending));

        let mut accepted = None;
        let mut connected = None;
        for _ in 0..16 {
            if accepted.is_some() && connected.is_some() {
                break;
            }
            ring.submit_and_wait().unwrap();
            if accepted.is_none() {
                accepted = match accept.as_mut().poll(&mut cx) {
                    Poll::Ready(result) => Some(result.unwrap()),
                    Poll::Pending => None,
                };
            }
            if connected.is_none() {
                connected = match connect.as_mut().poll(&mut cx) {
                    Poll::Ready(result) => Some(result.unwrap()),
                    Poll::Pending => None,
                };
            }
        }
        assert!(accepted.is_some() && connected.is_some());
        let accepted = accepted.unwrap();
        let connected = connected.unwrap();
        assert_eq!(connected, ());

        let accepted_fd = accepted.into_raw_fd();
        let mut send = Box::pin(ring.send(client_fd, b"socket io_uring".to_vec()));
        let mut recv = Box::pin(ring.recv(accepted_fd, vec![0; 15]));
        assert!(matches!(send.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(recv.as_mut().poll(&mut cx), Poll::Pending));
        ring.submit_and_wait().unwrap();
        let (send_result, sent) = match send.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("socket send CQE was not delivered"),
        };
        let (recv_result, received) = match recv.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => {
                ring.submit_and_wait().unwrap();
                match recv.as_mut().poll(&mut cx) {
                    Poll::Ready(value) => value,
                    Poll::Pending => panic!("socket recv CQE was not delivered"),
                }
            }
        };
        assert_eq!(send_result.unwrap(), 15);
        assert_eq!(sent, b"socket io_uring");
        assert_eq!(recv_result.unwrap(), 15);
        assert_eq!(&received, b"socket io_uring");

        let mut close_client = Box::pin(ring.close(client_fd));
        let mut close_accepted = Box::pin(ring.close(accepted_fd));
        assert!(matches!(close_client.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(
            close_accepted.as_mut().poll(&mut cx),
            Poll::Pending
        ));
        ring.submit_and_wait().unwrap();
        let mut client_closed = false;
        let mut accepted_closed = false;
        for _ in 0..4 {
            if !client_closed {
                client_closed = close_client.as_mut().poll(&mut cx).is_ready();
            }
            if !accepted_closed {
                accepted_closed = close_accepted.as_mut().poll(&mut cx).is_ready();
            }
            if client_closed && accepted_closed {
                break;
            }
            if !client_closed || !accepted_closed {
                ring.submit_and_wait().unwrap();
            }
        }
        assert!(client_closed && accepted_closed);
    }

    #[test]
    fn timeout_reports_expiration_as_success() {
        let Ok(ring) = IoUring::new(8) else {
            return;
        };
        let mut timeout = Box::pin(ring.timeout(Duration::from_millis(1)));
        let waker = crate::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(timeout.as_mut().poll(&mut cx), Poll::Pending));
        ring.submit_and_wait().unwrap();
        assert!(matches!(
            timeout.as_mut().poll(&mut cx),
            Poll::Ready(Ok(()))
        ));
    }

    #[test]
    fn dropping_a_read_orphans_it_until_the_cqe() {
        let Ok(ring) = IoUring::new(8) else {
            return;
        };
        let path = std::env::temp_dir().join(format!("eddy-uring-orphan-{}", std::process::id()));
        std::fs::write(&path, [7u8]).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let mut read = Box::pin(ring.read(file.as_raw_fd(), vec![0; 1]));
        let waker = crate::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(read.as_mut().poll(&mut cx), Poll::Pending));
        ring.submit().unwrap();
        drop(read);
        assert_eq!(ring.inner.state.lock().unwrap().orphaned.len(), 1);

        ring.submit_and_wait().unwrap();
        ring.poll_completions();
        if !ring.inner.state.lock().unwrap().orphaned.is_empty() {
            ring.submit_and_wait().unwrap();
            ring.poll_completions();
        }
        assert_eq!(ring.inner.state.lock().unwrap().orphaned.len(), 0);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn ten_thousand_concurrent_operations_complete() {
        const OPERATIONS: usize = 10_000;
        let Ok(ring) = IoUring::new(256) else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "eddy-uring-10k-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        std::fs::write(&path, b"x").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let mut operations = (0..OPERATIONS)
            .map(|_| Box::pin(ring.read(file.as_raw_fd(), vec![0_u8; 1])))
            .collect::<Vec<_>>();
        let mut completed = vec![false; OPERATIONS];
        let waker = crate::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        for operation in &mut operations {
            assert!(matches!(operation.as_mut().poll(&mut cx), Poll::Pending));
        }
        while completed.iter().any(|done| !done) {
            ring.submit_and_wait().unwrap();
            for (index, operation) in operations.iter_mut().enumerate() {
                if completed[index] {
                    continue;
                }
                let Poll::Ready((result, buffer)) = operation.as_mut().poll(&mut cx) else {
                    continue;
                };
                assert_eq!(result.unwrap(), 1);
                assert_eq!(buffer, b"x");
                completed[index] = true;
            }
        }
        drop(operations);
        drop(file);
        std::fs::remove_file(path).unwrap();
    }
}
