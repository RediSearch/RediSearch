/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    io::{self, Write},
    marker::PhantomData,
    mem::ManuallyDrop,
    os::fd::FromRawFd,
};

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
    /// The returned [`PipeWriter`] borrows `self`, which statically
    /// prevents two concurrent writers from interleaving pipe frames and
    /// ensures the writer cannot outlive the `ForkGC` it came from.
    pub fn writer(&mut self) -> PipeWriter<'_> {
        PipeWriter {
            // SAFETY: `pipe_write_fd` is an open writable fd maintained
            // by the C side's Fork GC state machine.
            pipe_writer: ManuallyDrop::new(unsafe {
                io::PipeWriter::from_raw_fd(self.0.pipe_write_fd)
            }),
            _borrow: PhantomData,
        }
    }
}

/// Borrowed write handle over a [`ForkGC`]'s pipe file descriptor.
///
/// Constructed via [`ForkGC::writer`]. Implements [`Write`] so it can
/// be passed directly to generic writer APIs like
/// [`fork_gc::pipe::send_fixed_or_exit`](crate::pipe::send_fixed_or_exit).
pub struct PipeWriter<'a> {
    // `ManuallyDrop` prevents `io::PipeWriter::drop` from closing the borrowed file descriptor.
    pipe_writer: ManuallyDrop<io::PipeWriter>,
    _borrow: PhantomData<&'a mut ForkGC>,
}

impl Write for PipeWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.pipe_writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.pipe_writer.flush()
    }
}
