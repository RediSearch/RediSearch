/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_char;
use std::{
    io::{self, Read},
    os::fd::AsRawFd,
    time::Duration,
};

use hidden_string::HiddenStringRef;
use nix::poll::{PollFd, PollFlags};
use redis_module::raw::RedisModule_ExitFromChild;

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
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let mut pfd = PollFd::new(reader.as_raw_fd(), PollFlags::POLLIN);

    loop {
        match nix::poll::poll(std::slice::from_mut(&mut pfd), timeout_ms) {
            Err(nix::errno::Errno::EINTR) => continue,
            Err(e) => return Err(io::Error::from(e)),
            Ok(0) => return Err(io::Error::new(io::ErrorKind::TimedOut, "read timed out")),
            Ok(_) => {
                let revents = pfd
                    .revents()
                    .expect("poll returned unknown bits in revents");
                // Reads from closed empty pipes return only `POLLHUP`, while reads from closed
                // unix domain sockets return `POLLIN | POLLHUP`. In both cases however, a
                // subsequent read doesn't block and returns 0, signalling EOF.
                if revents.intersects(PollFlags::POLLIN | PollFlags::POLLHUP) {
                    match reader.read(buf) {
                        Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                        result => return result,
                    }
                } else {
                    return Err(io::Error::other(format!("poll error: revents={revents:?}")));
                }
            }
        }
    }
}

/// Wrap `bytes` in a temporary, non-owning [`HiddenStringRef`], pass it to `f`, then free it.
pub fn with_hidden_string_ref<R>(bytes: &[u8], f: impl FnOnce(HiddenStringRef<'_>) -> R) -> R {
    // SAFETY: NewHiddenString wraps `bytes` into a heap-allocated HiddenString;
    // we only need it for the duration of `f` and free it immediately after.
    let hidden_string =
        unsafe { ffi::NewHiddenString(bytes.as_ptr().cast::<c_char>(), bytes.len(), false) };
    // SAFETY: `hidden_string` was just allocated above and is non-null.
    let key = unsafe { HiddenStringRef::from_raw(hidden_string) };
    let result = f(key);
    // SAFETY: `hidden_string` was allocated by NewHiddenString and is no longer needed.
    unsafe { ffi::HiddenString_Free(hidden_string, false) };
    result
}
