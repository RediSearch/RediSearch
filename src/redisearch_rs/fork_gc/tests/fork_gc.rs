/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// All tests exercise `read_with_timeout`, which relies on the `poll` syscall
// that miri does not support.
#![cfg(not(miri))]

use fork_gc::ForkGC;
use std::{io, mem, os::fd::AsRawFd, time::Duration};

/// Construct a zeroed [`ffi::ForkGC`] backed by a real pipe pair.
///
/// Returns the raw struct together with the [`io::PipeReader`] and
/// [`io::PipeWriter`] that own the file descriptors. Both must be kept
/// alive for as long as the [`ForkGC`] is in use.
fn make_fork_gc() -> (ffi::ForkGC, io::PipeReader, io::PipeWriter) {
    let (pipe_reader, pipe_writer) = io::pipe().unwrap();
    // SAFETY: all pointer fields in ffi::ForkGC are zeroed (null), which is
    // a valid bit pattern. Only pipe_read_fd / pipe_write_fd are used by the
    // reader() / writer() methods exercised in these tests.
    let mut raw: ffi::ForkGC = unsafe { mem::zeroed() };
    raw.pipe_read_fd = pipe_reader.as_raw_fd();
    raw.pipe_write_fd = pipe_writer.as_raw_fd();
    (raw, pipe_reader, pipe_writer)
}

#[test]
fn roundtrip_through_fork_gc() {
    let (mut raw, _pipe_reader, _pipe_writer) = make_fork_gc();
    // SAFETY: raw is a valid ffi::ForkGC for the duration of the test.
    let fgc = unsafe { ForkGC::from_ptr_mut(&mut raw) };

    fgc.writer().send_fixed(b"hello").unwrap();

    let mut buf = [0u8; 5];
    fgc.reader().recv_fixed(&mut buf).unwrap();
    assert_eq!(&buf, b"hello");
}

#[test]
fn times_out_when_no_data_available() {
    let (mut raw, _pipe_reader, _pipe_writer) = make_fork_gc();
    // SAFETY: raw is a valid ffi::ForkGC for the duration of the test.
    let fgc = unsafe { ForkGC::from_ptr_mut(&mut raw) };

    let mut buf = [0u8; 5];
    let err = fgc
        .reader_with_timeout(Duration::from_millis(1))
        .recv_fixed(&mut buf)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::TimedOut);
}
