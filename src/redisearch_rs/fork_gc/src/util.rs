/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use redis_module::raw::RedisModule_ExitFromChild;
use std::{
    ffi::CStr,
    io::{self, Read},
    os::fd::AsRawFd,
};

/// Log a write error and terminate the forked child.
pub fn exit_on_write_error(err: io::Error) -> ! {
    // Write the error message to the logging mechanism as well as directly to `stderr`
    // to make sure it ends up somewhere.
    let message = format!("GC fork: broken pipe, exiting: {err}");
    eprintln!("{message}");
    tracing::warn!("{message}");

    // SAFETY: `RedisModule_ExitFromChild` is a function-pointer static
    // initialized by the Redis module loader before any module code
    // runs; it is never written after that, so reading it is sound.
    let exit_from_child = unsafe { RedisModule_ExitFromChild }
        .expect("RedisModule_ExitFromChild must be initialized");

    // SAFETY: terminates the child process; does not return.
    unsafe {
        exit_from_child(1);
    }

    unreachable!("RedisModule_ExitFromChild returned")
}

/// Read from `reader` with a timeout, returning the number of bytes
/// actually read.
///
/// Polls the reader's file descriptor for `POLLIN` with `timeout_ms`,
/// then delegates to [`Read::read`] when the fd is ready. Surfaces
/// timeouts as [`io::ErrorKind::TimedOut`] and `POLLHUP` / `POLLERR` /
/// `POLLNVAL` as [`io::ErrorKind::Other`]. `EINTR` from either `poll`
/// or the underlying read is handled internally by looping.
pub(crate) fn read_with_timeout<R: Read + AsRawFd>(
    reader: &mut R,
    buf: &mut [u8],
    timeout_ms: libc::c_int,
) -> io::Result<usize> {
    loop {
        let mut pfd = libc::pollfd {
            fd: reader.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };

        // SAFETY: `pfd` is a valid pointer to a single pollfd for the
        // duration of the call.
        let ret = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };

        match ret {
            -1 => {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }
            0 => return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out")),
            _ => {
                if pfd.revents & libc::POLLIN == 0 {
                    // POLLHUP / POLLERR / POLLNVAL
                    return Err(io::Error::new(io::ErrorKind::Other, "poll error"));
                }
                match reader.read(buf) {
                    Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    result => return result,
                }
            }
        }
    }
}

/// Log a pipe-read failure with the same prefix as the original C
/// `FGC_recvFixed`. `what` is either `"timeout"` or `"error"`.
pub(crate) fn log_recv_error(what: &str, err: &io::Error) {
    let errno_str = err
        .raw_os_error()
        .map(strerror)
        .unwrap_or_else(|| err.to_string());
    tracing::warn!("ForkGC - got {what} while reading from pipe. errno: {errno_str}");
}

/// Thin wrapper over `libc::strerror` returning an owned `String`.
fn strerror(errno: i32) -> String {
    // SAFETY: `strerror` returns a pointer to either a static string or
    // a thread-local buffer. We copy it out before returning.
    let ptr = unsafe { libc::strerror(errno) };
    if ptr.is_null() {
        return format!("errno {errno}");
    }
    // SAFETY: `ptr` is NUL-terminated per POSIX.
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}
