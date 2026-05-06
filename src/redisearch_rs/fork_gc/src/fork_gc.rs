/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    io::{self, Read, Write},
    marker::PhantomData,
    mem::ManuallyDrop,
    os::fd::FromRawFd,
    time::Duration,
};

use crate::reader::Reader;
use crate::util::read_with_timeout;
use crate::writer::Writer;

/// Poll timeout used by the parent-side pipe reader (3 minutes).
const POLL_TIMEOUT: Duration = Duration::from_mins(3);

/// Safe wrapper around [`ffi::ForkGC`].
#[repr(transparent)]
pub struct ForkGC(ffi::ForkGC);

impl ForkGC {
    /// Borrow a `ForkGC` from a raw pointer provided by the C layer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must point to a valid, initialised [`ffi::ForkGC`].
    /// 2. For the chosen lifetime `'a`, no other reference to the same
    ///    struct may exist.
    pub unsafe fn from_ptr_mut<'a>(ptr: *mut ffi::ForkGC) -> &'a mut Self {
        // SAFETY: `#[repr(transparent)]` guarantees `*mut ffi::ForkGC` and
        // `*mut ForkGC` have identical layout. Validity and aliasing are
        // the caller's responsibility per (1) and (2).
        unsafe { &mut *ptr.cast::<ForkGC>() }
    }

    /// Return a writable handle to the GC pipe.
    ///
    /// The returned [`Writer`] wraps a [`ForkGCPipeWriter`] that holds a
    /// `PhantomData<&'a mut ForkGC>`, so it statically borrows `self` —
    /// preventing two concurrent writers and ensuring the writer cannot
    /// outlive the `ForkGC` it came from. The concrete inner writer type is
    /// hidden behind `impl Write + '_`.
    pub fn writer(&mut self) -> Writer<impl Write + '_> {
        Writer::from_writer(ForkGCPipeWriter {
            // SAFETY: `pipe_write_fd` is an open writable fd maintained by
            // the C side's Fork GC state machine.
            pipe_writer: ManuallyDrop::new(unsafe {
                io::PipeWriter::from_raw_fd(self.0.pipe_write_fd)
            }),
            _borrow: PhantomData,
        })
    }

    /// Return a readable handle to the GC pipe.
    ///
    /// Each call to [`Reader::recv_fixed`] delegates to
    /// [`read_with_timeout`] with a 3-minute poll timeout and retries on
    /// `EINTR`. On timeout or pipe error the error is surfaced.
    ///
    /// When `pipe_read_fd` is negative — tests deliberately set it to
    /// `-1` to simulate pipe failure — the returned reader yields
    /// `EBADF` on every read instead of constructing an `io::PipeReader`,
    /// which would otherwise trip a std-internal `fd != -1` precondition.
    pub fn reader(&mut self) -> Reader<impl Read + '_> {
        self.reader_with_timeout(POLL_TIMEOUT)
    }

    /// Like [`reader`] but with a caller-supplied poll timeout.
    ///
    /// Intended for tests where the 3-minute default would be impractical.
    pub fn reader_with_timeout(&mut self, timeout: Duration) -> Reader<impl Read + '_> {
        // Existing C++ unit tests (`FGCTestTag.testPipeErrorDuringGC` and
        // `FGCTestTag.testPipeErrorDuringApply`) set `pipe_read_fd` to `-1`
        // to simulate pipe failure. `io::PipeReader` doesn't allow an fd of
        // `-1` to be used and panics instead. Work around this for now. When
        // all of Fork GC (including the tests) are ported over to Rust, use a
        // better approach to simulate pipe failure.
        let fd = self.0.pipe_read_fd;
        let pipe_reader = (fd >= 0).then(|| {
            // SAFETY: `fd` is non-negative (checked above) and refers to
            // an open readable fd maintained by the C side's Fork GC
            // state machine.
            ManuallyDrop::new(unsafe { io::PipeReader::from_raw_fd(fd) })
        });

        Reader::from_reader(ForkGCPipeReader {
            pipe_reader,
            timeout,
            _borrow: PhantomData,
        })
    }
}

/// [`Write`] adapter over `pipe_write_fd`. Holds a
/// `ManuallyDrop<io::PipeWriter>` so dropping the writer does not close
/// the caller's fd, and a `PhantomData<&'a mut ForkGC>` makes the type
/// system behave as if the writer borrows the `ForkGC` for its entire lifetime.
struct ForkGCPipeWriter<'a> {
    pipe_writer: ManuallyDrop<io::PipeWriter>,
    _borrow: PhantomData<&'a mut ForkGC>,
}

impl Write for ForkGCPipeWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.pipe_writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.pipe_writer.flush()
    }
}

/// [`Read`] adapter over `pipe_read_fd`. Holds an
/// `Option<ManuallyDrop<io::PipeReader>>`: `Some` for a live pipe (the
/// `ManuallyDrop` keeps `Drop` from closing the caller's fd), `None` to
/// surface `EBADF` on each read. The `PhantomData<&'a mut ForkGC>` makes
/// the type system behave as if the reader borrows `ForkGC` for its entire
/// lifetime.
struct ForkGCPipeReader<'a> {
    pipe_reader: Option<ManuallyDrop<io::PipeReader>>,
    timeout: Duration,
    _borrow: PhantomData<&'a mut ForkGC>,
}

impl Read for ForkGCPipeReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.pipe_reader {
            Some(pr) => read_with_timeout(&mut **pr, buf, self.timeout),
            None => Err(io::Error::from_raw_os_error(libc::EBADF)),
        }
    }
}
