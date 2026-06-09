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
    ops::{Deref, DerefMut},
    os::fd::FromRawFd,
    time::Duration,
};

use crate::util::read_with_timeout;

use index_spec::IndexSpecWeakRefMut;

/// Poll timeout used by the parent-side pipe reader.
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
    /// The returned [`ForkGCPipeWriter`] holds a `PhantomData<&'a mut ForkGC>`,
    /// so it statically borrows `self` — preventing two concurrent writers and
    /// ensuring the writer cannot outlive the `ForkGC` it came from.
    pub fn writer(&mut self) -> ForkGCPipeWriter<'_> {
        ForkGCPipeWriter {
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
    pub fn reader(&mut self) -> ForkGCPipeReader<'_> {
        self.reader_with_timeout(POLL_TIMEOUT)
    }

    /// Like [`Self::reader`] but with a caller-supplied poll timeout.
    ///
    /// Intended for tests where the 3-minute default would be impractical.
    pub fn reader_with_timeout(&mut self, timeout: Duration) -> ForkGCPipeReader<'_> {
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

        ForkGCPipeReader {
            pipe_reader,
            timeout,
            _borrow: PhantomData,
        }
    }

    /// Return a weak reference to the GC's index spec.
    ///
    /// Takes `&mut self` so that the returned [`IndexSpecWeakRefMut`] (and any
    /// [`IndexSpecStrongRefMut`] promoted from it) carries an exclusive borrow
    /// of this `ForkGC`, preventing a second strong reference from being
    /// created while the first is still alive.
    pub const fn index_spec(&mut self) -> IndexSpecWeakRefMut<'_> {
        // SAFETY: `self.0.index` is a valid WeakRef for the lifetime of this ForkGC.
        unsafe { IndexSpecWeakRefMut::from_raw(self.0.index) }
    }

    /// Split `self` into three non-overlapping views, each borrowing `self`
    /// for the same lifetime.
    ///
    /// - `&mut ForkGCLite` — access to GC-level statistics.
    /// - [`IndexSpecWeakRefMut`] — weak reference to the index spec.
    /// - [`ForkGCPipeReader`] — read end of the GC pipe.
    ///
    /// # Why this is safe
    ///
    /// [`IndexSpecWeakRefMut`] stores a *copy* of the `WeakRef` value (not a
    /// pointer into `ffi::ForkGC` memory), and [`ForkGCPipeReader`] stores a
    /// copy of the `pipe_read_fd` integer. The only live reference into
    /// `ffi::ForkGC` memory is `&mut ForkGCLite`. Soundness therefore requires
    /// that every method on [`ForkGCLite`] only accesses the `stats` fields and
    /// never touches `index` or `pipe_read_fd`.
    pub fn split(
        &mut self,
    ) -> (
        &mut ForkGCLite,
        IndexSpecWeakRefMut<'_>,
        ForkGCPipeReader<'_>,
    ) {
        // Use a raw pointer to read out the fields we need before creating the
        // &mut ForkGCLite, so the borrow checker doesn't see a conflict.
        // SAFETY: Derived from &mut self, so valid and exclusively owned.
        let ptr: *mut ffi::ForkGC = &raw mut self.0;

        // Copy the fields out while we have sole access and before any
        // references to the struct are created.
        // SAFETY: ptr is valid and non-null (derived from &mut self).
        let index = unsafe { (*ptr).index };
        let fd = unsafe { (*ptr).pipe_read_fd };

        // SAFETY: ForkGCLite is #[repr(transparent)] over ffi::ForkGC (same
        // layout as ForkGC). The returned reference borrows self for the same
        // lifetime. Callers must uphold the invariant described in the doc
        // comment above: every ForkGCLite method must only access stats fields.
        let lite = unsafe { &mut *(ptr.cast::<ForkGCLite>()) };

        // IndexSpecWeakRefMut stores a copy of the WeakRef value, so it does
        // not alias with &mut ForkGCLite.
        // SAFETY: self.0.index is a valid WeakRef for the lifetime of this ForkGC.
        let index_spec = unsafe { IndexSpecWeakRefMut::from_raw(index) };

        // ForkGCPipeReader stores a copy of the fd integer, so it does not
        // alias with &mut ForkGCLite.
        let pipe_reader = (fd >= 0).then(|| {
            // SAFETY: fd is non-negative (checked above) and refers to an open
            // readable fd maintained by the C side's Fork GC state machine.
            ManuallyDrop::new(unsafe { io::PipeReader::from_raw_fd(fd) })
        });

        (
            lite,
            index_spec,
            ForkGCPipeReader {
                pipe_reader,
                timeout: POLL_TIMEOUT,
                _borrow: PhantomData,
            },
        )
    }
}

impl Deref for ForkGC {
    type Target = ForkGCLite;

    fn deref(&self) -> &ForkGCLite {
        // SAFETY: ForkGC and ForkGCLite are both #[repr(transparent)] over
        // ffi::ForkGC and therefore have identical layout.
        unsafe { &*(&raw const *self).cast::<ForkGCLite>() }
    }
}

impl DerefMut for ForkGC {
    fn deref_mut(&mut self) -> &mut ForkGCLite {
        // SAFETY: Same as Deref. Derived from &mut self so no aliasing occurs.
        unsafe { &mut *(&raw mut *self).cast::<ForkGCLite>() }
    }
}

/// [`Write`] adapter over `pipe_write_fd`. Holds a
/// `ManuallyDrop<io::PipeWriter>` so dropping the writer does not close
/// the caller's fd, and a `PhantomData<&'a mut ForkGC>` makes the type
/// system behave as if the writer borrows the `ForkGC` for its entire lifetime.
pub struct ForkGCPipeWriter<'a> {
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
pub struct ForkGCPipeReader<'a> {
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

/// Safe lite wrapper around [`ffi::ForkGC`] which only allows access to it's
/// own fields and not other structs and stuff.
#[repr(transparent)]
pub struct ForkGCLite(ffi::ForkGC);

impl ForkGCLite {
    /// Update the GC-level statistics after applying a garbage collection delta.
    ///
    /// This is the GC-side half of `FGC_updateStats`.
    pub const fn update_gc_stats(
        &mut self,
        bytes_collected: usize,
        bytes_allocated: usize,
        ignored_last_block: bool,
    ) {
        self.0.stats.totalCollected += bytes_collected as isize;
        self.0.stats.totalCollected -= bytes_allocated as isize;
        self.0.stats.gcBlocksDenied += ignored_last_block as u64;
    }
}
