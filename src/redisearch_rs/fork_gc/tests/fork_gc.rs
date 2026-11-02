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
use fork_gc::util::{deserialize, serialize};
use std::{
    io::{self, Read, Write},
    mem,
    os::fd::AsRawFd,
    time::Duration,
};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Provide Redis allocator shims so the C dict functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

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

    fgc.writer().write_all(b"hello").unwrap();

    let mut buf = [0u8; 5];
    fgc.reader().read_exact(&mut buf).unwrap();
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
        .read_exact(&mut buf)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::TimedOut);
}

/// Small messages written ahead are fetched together: after the first is read
/// through the read-ahead, the rest are already off the pipe, and each still
/// decodes whole and in order.
#[test]
fn read_ahead_fetches_many_messages_in_one_read() {
    // Written before any read, so the batch must fit the smallest pipe a test may
    // get (a page, under per-user pipe limits); 100 small messages take about 1 KiB.
    const MESSAGES: u32 = 100;
    let (mut raw, pipe_reader, _pipe_writer) = make_fork_gc();
    // SAFETY: raw is a valid ffi::ForkGC for the duration of the test.
    let fgc = unsafe { ForkGC::from_ptr_mut(&mut raw) };
    for i in 0..MESSAGES {
        serialize(&mut fgc.writer(), i).unwrap();
    }

    fgc.with_read_ahead(|fgc| {
        assert_eq!(deserialize::<u32>(&mut fgc.reader(), "first").unwrap(), 0);
        assert!(
            !has_bytes_waiting(&pipe_reader),
            "the first read took every message the pipe held"
        );
        for i in 1..MESSAGES {
            assert_eq!(deserialize::<u32>(&mut fgc.reader(), "next").unwrap(), i);
        }
    });
}

/// A closure that unwinds still uninstalls the buffer, so the next cycle can
/// install its own.
#[test]
fn read_ahead_is_uninstalled_when_the_closure_unwinds() {
    let (mut raw, _pipe_reader, _pipe_writer) = make_fork_gc();
    // SAFETY: raw is a valid ffi::ForkGC for the duration of the test.
    let fgc = unsafe { ForkGC::from_ptr_mut(&mut raw) };

    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        fgc.with_read_ahead(|_| panic!("the closure unwinds"))
    }));

    assert!(unwound.is_err());
    assert!(raw.readBuffer.is_null());
}

/// A message larger than the read-ahead buffer still decodes, as do the small
/// ones around it, while the writer is still filling the pipe.
#[test]
fn read_ahead_decodes_messages_larger_than_its_buffer() {
    let (mut raw, _pipe_reader, pipe_writer) = make_fork_gc();
    let writer = std::thread::spawn(move || {
        let mut pipe_writer = pipe_writer;
        serialize(&mut pipe_writer, 1u8).unwrap();
        serialize(
            &mut pipe_writer,
            serde_bytes::Bytes::new(&vec![7; 200 * 1024]),
        )
        .unwrap();
        serialize(&mut pipe_writer, 2u8).unwrap();
    });
    // SAFETY: raw is a valid ffi::ForkGC for the duration of the test.
    let fgc = unsafe { ForkGC::from_ptr_mut(&mut raw) };

    fgc.with_read_ahead(|fgc| {
        assert_eq!(deserialize::<u8>(&mut fgc.reader(), "before").unwrap(), 1);
        let large: serde_bytes::ByteBuf = deserialize(&mut fgc.reader(), "large").unwrap();
        assert_eq!(large.as_ref(), vec![7; 200 * 1024]);
        assert_eq!(deserialize::<u8>(&mut fgc.reader(), "after").unwrap(), 2);
    });
    writer.join().unwrap();
}

/// A closed pipe still reads as the end of the stream through the read-ahead,
/// once the buffered bytes are consumed.
#[test]
fn read_ahead_reports_end_of_stream_after_the_buffered_bytes() {
    let (mut raw, _pipe_reader, pipe_writer) = make_fork_gc();
    // SAFETY: raw is a valid ffi::ForkGC for the duration of the test.
    let fgc = unsafe { ForkGC::from_ptr_mut(&mut raw) };
    let mut pipe_writer = pipe_writer;
    pipe_writer.write_all(b"tail").unwrap();
    // The ForkGC borrows this fd; dropping its owner closes the write end.
    drop(pipe_writer);

    fgc.with_read_ahead(|fgc| {
        let mut reader = fgc.reader();
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"tail");
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    });
}

fn has_bytes_waiting(pipe_reader: &io::PipeReader) -> bool {
    let mut fd = [nix::poll::PollFd::new(
        pipe_reader.as_raw_fd(),
        nix::poll::PollFlags::POLLIN,
    )];
    nix::poll::poll(&mut fd, 0).unwrap() > 0
}
