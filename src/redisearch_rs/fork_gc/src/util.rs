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
    io::{self, Read},
    os::fd::AsRawFd,
    time::Duration,
};

/// Log a write error and terminate the current process.
pub(crate) fn exit_on_write_error(err: io::Error) -> ! {
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

    // SAFETY: terminates the current process; does not return.
    unsafe {
        exit_from_child(1);
    }

    unreachable!("RedisModule_ExitFromChild returned")
}

/// Read from `reader` with a timeout, returning the number of bytes
/// actually read.
///
/// Polls the reader's file descriptor for `POLLIN` with `timeout`,
/// then delegates to [`Read::read`] when the fd is ready. Surfaces
/// timeouts as [`io::ErrorKind::TimedOut`] and `POLLHUP` / `POLLERR` /
/// `POLLNVAL` as [`io::ErrorKind::Other`]. `EINTR` from either `poll`
/// or the underlying read is handled internally by looping.
pub fn read_with_timeout<R: Read + AsRawFd>(
    reader: &mut R,
    buf: &mut [u8],
    timeout: Duration,
) -> io::Result<usize> {
    let timeout_ms = timeout.as_millis().min(libc::c_int::MAX as u128) as libc::c_int;

    let mut pfd = libc::pollfd {
        fd: reader.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };

    loop {
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
                // Reads from closed empty pipes return only `POLLHUP`, while reads from closed
                // unix domain sockets return `POLLIN | POLLHUP`. In both cases however, a
                // subsequent read doesn't block and returns 0, signalling EOF.
                if pfd.revents & (libc::POLLIN | libc::POLLHUP) != 0 {
                    match reader.read(buf) {
                        Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                        result => return result,
                    }
                } else {
                    return Err(io::Error::other(format!(
                        "poll error: revents=0x{:x}{}{}",
                        pfd.revents,
                        if pfd.revents & libc::POLLERR != 0 {
                            " POLLERR"
                        } else {
                            ""
                        },
                        if pfd.revents & libc::POLLNVAL != 0 {
                            " POLLNVAL"
                        } else {
                            ""
                        },
                    )));
                }
            }
        }
    }
}
