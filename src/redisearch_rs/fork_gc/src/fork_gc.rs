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
};

use crate::reader::Reader;
use crate::util::{log_recv_error, read_with_timeout};
use crate::writer::Writer;

/// Poll timeout used by the parent-side pipe reader (3 minutes, matching
/// the original C `FGC_recvFixed`).
const POLL_TIMEOUT_MS: libc::c_int = 180_000;

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
    /// The returned [`Writer`] wraps an internal `FdWriter` that
    /// holds a `PhantomData<&'a mut ForkGC>`, so it statically borrows
    /// `self` — preventing two concurrent writers and ensuring the
    /// writer cannot outlive the `ForkGC` it came from. The concrete
    /// inner writer type is hidden behind `impl Write + '_`.
    pub fn writer(&mut self) -> Writer<impl Write + '_> {
        // Local [`Write`] adapter over the pipe fd. Holds a
        // `ManuallyDrop<File>` so dropping the writer does not close
        // the caller's fd, and a `PhantomData<&'a mut ForkGC>` so the
        // writer borrows the `ForkGC` for its entire lifetime.
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

        Writer::from_writer(ForkGCPipeWriter {
            // SAFETY: `pipe_write_fd` is an open writable fd maintained by
            // the C side's Fork GC state machine; `ManuallyDrop` prevents
            // `File::drop` from closing it.
            pipe_writer: ManuallyDrop::new(unsafe {
                io::PipeWriter::from_raw_fd(self.0.pipe_write_fd)
            }),
            _borrow: PhantomData,
        })
    }

    /// Return a readable handle to the GC pipe.
    ///
    /// Each call to [`Reader::recv_fixed`] delegates to
    /// [`read_with_timeout`] (3-minute poll) and retries on `EINTR`. On
    /// timeout or pipe error the error is logged and surfaced; the FFI
    /// trampoline maps it to `REDISMODULE_ERR`.
    pub fn reader(&mut self) -> Reader<impl Read + '_> {
        /// Local [`Read`] adapter over the pipe fd. Each call polls
        /// with a 3-minute timeout via [`read_with_timeout`] and loops
        /// on `EINTR` to match the original C `FGC_recvFixed` loop.
        struct ForkGCPipeReader<'a> {
            pipe_reader: ManuallyDrop<io::PipeReader>,
            _borrow: PhantomData<&'a mut ForkGC>,
        }

        impl Read for ForkGCPipeReader<'_> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                read_with_timeout(&mut *self.pipe_reader, buf, POLL_TIMEOUT_MS).inspect_err(|err| {
                    let what = if err.kind() == io::ErrorKind::TimedOut {
                        "timeout"
                    } else {
                        "error"
                    };
                    log_recv_error(what, err);
                })
            }
        }

        Reader::from_reader(ForkGCPipeReader {
            // SAFETY: `pipe_read_fd` is an open readable fd maintained
            // by the C side's Fork GC state machine; `ManuallyDrop`
            // prevents `File::drop` from closing it.
            pipe_reader: ManuallyDrop::new(unsafe {
                io::PipeReader::from_raw_fd(self.0.pipe_read_fd)
            }),
            _borrow: PhantomData,
        })
    }
}
