/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use fork_gc::util::read_with_timeout;
use std::{
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, RawFd},
        unix::net::UnixStream,
    },
    time::Duration,
};

#[test]
fn reads_available_data() {
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    writer.write_all(b"hello").unwrap();
    let mut buf = [0u8; 5];
    let n = read_with_timeout(&mut reader, &mut buf, Duration::from_secs(1)).unwrap();
    assert_eq!(n, 5);
    assert_eq!(&buf, b"hello");
}

#[test]
fn returns_eof_when_writer_closes() {
    let (mut reader, writer) = UnixStream::pair().unwrap();
    drop(writer);
    let mut buf = [0u8; 4];
    let n = read_with_timeout(&mut reader, &mut buf, Duration::from_secs(1)).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn times_out_when_no_data_available() {
    let (mut reader, _writer) = UnixStream::pair().unwrap();
    let mut buf = [0u8; 4];
    let err = read_with_timeout(&mut reader, &mut buf, Duration::from_millis(1)).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::TimedOut);
}

/// A reader that returns [`io::ErrorKind::Interrupted`] on the first `read`
/// call, then delegates to the underlying socket. This exercises the retry
/// loop that handles `EINTR` from the `read` syscall.
struct InterruptedOnFirstRead<'a> {
    inner: &'a mut UnixStream,
    interrupted: bool,
}

impl Read for InterruptedOnFirstRead<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.interrupted {
            self.interrupted = true;
            Err(io::Error::from(io::ErrorKind::Interrupted))
        } else {
            self.inner.read(buf)
        }
    }
}

impl AsRawFd for InterruptedOnFirstRead<'_> {
    fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }
}

#[test]
fn retries_after_interrupted_read() {
    let (mut reader, mut writer) = UnixStream::pair().unwrap();
    writer.write_all(b"hi").unwrap();
    let mut reader = InterruptedOnFirstRead {
        inner: &mut reader,
        interrupted: false,
    };
    let mut buf = [0u8; 2];
    let n = read_with_timeout(&mut reader, &mut buf, Duration::from_secs(1)).unwrap();
    assert_eq!(n, 2);
    assert_eq!(&buf, b"hi");
}

/// A reader whose `read` always fails with a non-`Interrupted` error, backed
/// by a real socket fd so that `poll` returns `POLLIN`.
struct FailingReader<'a> {
    socket: &'a UnixStream,
}

impl Read for FailingReader<'_> {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "injected read error",
        ))
    }
}

impl AsRawFd for FailingReader<'_> {
    fn as_raw_fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }
}

#[test]
fn propagates_non_interrupted_read_error() {
    let (reader, mut writer) = UnixStream::pair().unwrap();
    writer.write_all(b"x").unwrap();
    let mut reader = FailingReader { socket: &reader };
    let mut buf = [0u8; 1];
    let err = read_with_timeout(&mut reader, &mut buf, Duration::from_secs(1)).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
}

/// A reader backed by an always-invalid fd (`RawFd::MAX`), causing `poll` to
/// return `POLLNVAL` immediately without setting `POLLIN`. `read` is therefore
/// unreachable and panics if called.
struct InvalidFdReader;

impl Read for InvalidFdReader {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        unreachable!("read must not be called when poll reports POLLNVAL")
    }
}

impl AsRawFd for InvalidFdReader {
    fn as_raw_fd(&self) -> RawFd {
        // i32::MAX is always an invalid fd: the kernel fd limit is
        // configurable but capped well below i32::MAX.
        RawFd::MAX
    }
}

#[test]
fn returns_error_on_pollnval() {
    let mut reader = InvalidFdReader;
    let mut buf = [0u8; 4];
    let err = read_with_timeout(&mut reader, &mut buf, Duration::from_secs(1)).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert_eq!(
        err.to_string(),
        format!("poll error: revents=0x{:x} POLLNVAL", libc::POLLNVAL)
    );
}
