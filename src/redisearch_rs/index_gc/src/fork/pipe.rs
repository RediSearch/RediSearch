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

use crate::fork::wire::read_with_timeout;

use index_spec::IndexSpecWeakRef;

/// Poll timeout used by the parent-side pipe reader.
const POLL_TIMEOUT: Duration = Duration::from_mins(3);

/// Size of the parent's read-ahead buffer; see [`ForkPipe::with_read_ahead`].
const READ_AHEAD_SIZE: usize = 64 * 1024;

/// Pipe bytes the parent has read but not yet consumed.
struct ReadAhead {
    data: Box<[u8]>,
    start: usize,
    end: usize,
}

/// Owns the buffer [`ForkPipe::with_read_ahead`] installs, so it is uninstalled and
/// freed even when the closure unwinds.
struct ReadAheadScope<'a>(&'a mut ForkPipe);

impl Drop for ReadAheadScope<'_> {
    fn drop(&mut self) {
        let buffer = std::mem::replace(&mut self.0.0.readBuffer, std::ptr::null_mut());
        // SAFETY: `with_read_ahead` installed `buffer` from `Box::into_raw`, and no
        // reader outlives the closure, the only code that could reach it.
        drop(unsafe { Box::from_raw(buffer.cast::<ReadAhead>()) });
    }
}

/// Safe wrapper around [`ffi::IndexGC`].
#[repr(transparent)]
pub struct ForkPipe(ffi::IndexGC);

impl ForkPipe {
    /// Borrow a `ForkPipe` from a raw pointer provided by the C layer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must point to a valid, initialised [`ffi::IndexGC`].
    /// 2. For the chosen lifetime `'a`, no other reference to the same
    ///    struct may exist.
    pub unsafe fn from_ptr_mut<'a>(ptr: *mut ffi::IndexGC) -> &'a mut Self {
        // SAFETY: `#[repr(transparent)]` guarantees `*mut ffi::IndexGC` and
        // `*mut ForkPipe` have identical layout. Validity and aliasing are
        // the caller's responsibility per (1) and (2).
        unsafe { &mut *ptr.cast::<ForkPipe>() }
    }

    /// Return a writable handle to the GC pipe.
    ///
    /// The returned [`ForkPipeWriter`] holds a `PhantomData<&'a mut ForkPipe>`,
    /// so it statically borrows `self` — preventing two concurrent writers and
    /// ensuring the writer cannot outlive the `ForkPipe` it came from.
    pub fn writer(&mut self) -> ForkPipeWriter<'_> {
        ForkPipeWriter {
            // SAFETY: `pipe_write_fd` is an open writable fd maintained by
            // the C side's Fork GC state machine.
            pipe_writer: ManuallyDrop::new(unsafe {
                io::PipeWriter::from_raw_fd(self.0.pipe_write_fd)
            }),
            _borrow: PhantomData,
        }
    }

    /// Return a readable handle to the GC pipe.
    ///
    /// Each read delegates to [`read_with_timeout`] with [`POLL_TIMEOUT`] and
    /// retries on `EINTR`. On timeout or pipe error the error is surfaced.
    ///
    /// When `pipe_read_fd` is negative — tests deliberately set it to
    /// `-1` to simulate pipe failure — the returned reader yields
    /// `EBADF` on every read instead of constructing an `io::PipeReader`,
    /// which would otherwise trip a std-internal `fd != -1` precondition.
    pub fn reader(&mut self) -> ForkPipeReader<'_> {
        self.reader_with_timeout(POLL_TIMEOUT)
    }

    /// Like [`Self::reader`] but with a caller-supplied poll timeout.
    ///
    /// Intended for tests where the 3-minute default would be impractical.
    pub fn reader_with_timeout(&mut self, timeout: Duration) -> ForkPipeReader<'_> {
        // Existing C++ unit tests (`IndexGCTestTag.testPipeErrorDuringGC` and
        // `IndexGCTestTag.testPipeErrorDuringApply`) set `pipe_read_fd` to `-1`
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

        // SAFETY: a non-null `readBuffer` is the live `ReadAhead` that
        // `with_read_ahead` installed, and the reader's borrow of `self` keeps
        // this the only reference to it.
        let read_ahead = unsafe { self.0.readBuffer.cast::<ReadAhead>().as_mut() };

        ForkPipeReader {
            pipe_reader,
            read_ahead,
            timeout,
            _borrow: PhantomData,
        }
    }

    /// Run `f` with the parent's pipe reads going through a read-ahead buffer, so
    /// one poll and read can fetch many small messages instead of each message
    /// costing a poll and read per frame. Bytes still buffered when `f` returns
    /// are dropped with the buffer, so `f` must consume the whole stream it reads.
    /// A nested call reads through the buffer already installed.
    pub fn with_read_ahead<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        if !self.0.readBuffer.is_null() {
            return f(self);
        }
        let buffer = Box::new(ReadAhead {
            data: vec![0; READ_AHEAD_SIZE].into_boxed_slice(),
            start: 0,
            end: 0,
        });
        self.0.readBuffer = Box::into_raw(buffer).cast();
        let scope = ReadAheadScope(self);
        f(scope.0)
    }

    /// Return a weak reference to the GC's index spec.
    ///
    /// `WeakRef` is a plain `Copy` value, so this just copies it out of the
    /// struct. The returned [`IndexSpecWeakRef`] does not borrow `self`.
    pub const fn index_spec(&self) -> IndexSpecWeakRef {
        // SAFETY: `self.0.index` is a valid WeakRef for the lifetime of this ForkPipe.
        unsafe { IndexSpecWeakRef::from_raw(self.0.index) }
    }

    /// Whether the periodic callback requested trimming of empty numeric leaves
    /// on this cycle (mirrors `ForkPipe::cleanNumericEmptyNodes`).
    pub const fn clean_numeric_empty_nodes(&self) -> bool {
        self.0.cleanNumericEmptyNodes != 0
    }

    /// The GC-level statistics, which a [`GcStatsSink`](crate::stats::GcStatsSink) folds
    /// each applied delta into.
    pub const fn stats_mut(&mut self) -> &mut ffi::IndexGCStats {
        &mut self.0.stats
    }
}

/// [`Write`] adapter over `pipe_write_fd`. Holds a
/// `ManuallyDrop<io::PipeWriter>` so dropping the writer does not close
/// the caller's fd, and a `PhantomData<&'a mut ForkPipe>` makes the type
/// system behave as if the writer borrows the `ForkPipe` for its entire lifetime.
pub struct ForkPipeWriter<'a> {
    pipe_writer: ManuallyDrop<io::PipeWriter>,
    _borrow: PhantomData<&'a mut ForkPipe>,
}

impl Write for ForkPipeWriter<'_> {
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
/// surface `EBADF` on each read. The `PhantomData<&'a mut ForkPipe>` makes
/// the type system behave as if the reader borrows `ForkPipe` for its entire
/// lifetime.
pub struct ForkPipeReader<'a> {
    pipe_reader: Option<ManuallyDrop<io::PipeReader>>,
    read_ahead: Option<&'a mut ReadAhead>,
    timeout: Duration,
    _borrow: PhantomData<&'a mut ForkPipe>,
}

impl Read for ForkPipeReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // An invalidated pipe fails even with bytes still buffered.
        let Some(pr) = &mut self.pipe_reader else {
            return Err(io::Error::from_raw_os_error(libc::EBADF));
        };
        let Some(ahead) = self.read_ahead.as_deref_mut() else {
            return read_with_timeout(&mut **pr, buf, self.timeout);
        };
        if ahead.start == ahead.end {
            if buf.len() >= ahead.data.len() {
                return read_with_timeout(&mut **pr, buf, self.timeout);
            }
            ahead.end = read_with_timeout(&mut **pr, &mut ahead.data, self.timeout)?;
            ahead.start = 0;
        }
        let available = &ahead.data[ahead.start..ahead.end];
        let n = buf.len().min(available.len());
        buf[..n].copy_from_slice(&available[..n]);
        ahead.start += n;
        Ok(n)
    }
}
