/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    io::{self, Read},
    os::fd::AsRawFd,
    time::Duration,
};

use index_spec::{IndexSpecWeakRef, IndexSpecWriteGuard};
use nix::poll::{PollFd, PollFlags};
use redis_module::raw::RedisModule_ExitFromChild;

use crate::fork_gc::ForkGCPipeReader;
use crate::{ForkGC, GcApplyStats, HandleError, HandleOutcome};

/// Provides closure-scoped access to a live, write-locked [`IndexSpec`](index_spec::IndexSpec).
///
/// The closure prevents the [`IndexSpecWriteGuard`] from outliving the strong
/// reference that keeps the spec alive. Test implementations can provide the
/// same scope over an exclusively owned synthetic spec.
pub trait SpecWriteAccess {
    /// Promote and lock the spec, returning `None` if it has been deleted.
    fn with_write<T>(&mut self, apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>) -> T)
    -> Option<T>;

    /// [`with_write`](Self::with_write) for a fallible `apply`, reporting a
    /// deleted spec as [`HandleError::SpecDeleted`].
    ///
    /// Flattens the nested `Option<Result<..>>` the plain accessor would produce.
    fn try_with_write<T, C>(
        &mut self,
        apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>) -> Result<T, HandleError<C>>,
    ) -> Result<T, HandleError<C>> {
        self.with_write(apply).ok_or(HandleError::SpecDeleted)?
    }
}

impl SpecWriteAccess for IndexSpecWeakRef {
    fn with_write<T>(
        &mut self,
        apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>) -> T,
    ) -> Option<T> {
        let mut spec_ref = self.promote()?;
        let mut guard = spec_ref.write();
        Some(apply(&mut guard))
    }
}

/// Run the single-shot GC handler protocol shared by the per-index scanners
/// that apply one message per call (existing-docs, missing-docs):
///
/// 1. `receive` one message from the pipe; a `None` means the child sent the
///    terminator, so iteration is [`Done`](HandleOutcome::Done).
/// 2. Promote and write-lock the spec (gone → [`HandleError::SpecDeleted`]).
/// 3. `apply` the message, then flush the resulting [`GcApplyStats`] to both
///    the spec and the fork GC via [`GcApplyStats::apply`].
pub(crate) fn handle_one<M, C>(
    fgc: &mut ForkGC,
    receive: impl FnOnce(&mut ForkGCPipeReader<'_>) -> Result<Option<M>, HandleError<C>>,
    apply: impl FnOnce(M, &mut IndexSpecWriteGuard<'_>) -> Result<GcApplyStats, HandleError<C>>,
) -> Result<HandleOutcome, HandleError<C>> {
    let Some(message) = receive(&mut fgc.reader())? else {
        return Ok(HandleOutcome::Done);
    };

    let mut spec_ref = fgc.index_spec().promote().ok_or(HandleError::SpecDeleted)?;
    let mut guard = spec_ref.write();

    let stats = apply(message, &mut guard)?;
    stats.apply(fgc, &mut guard);

    Ok(HandleOutcome::Collected)
}

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
