/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Pipe I/O primitives used by the Fork GC child/parent protocol.

use std::io::{self, Write};

use redis_module::{logging::log_warning, raw::RedisModule_ExitFromChild};

/// Write all bytes of `buf` to `writer`.
///
/// Wraps [`Write::write_all`] as the canonical way to emit a fixed-size
/// frame to the Fork GC pipe. The returned [`io::Result`] lets callers
/// decide how to react; use [`send_fixed_or_exit`] when running inside
/// the forked child, where a broken pipe is unrecoverable.
pub fn send_fixed<W: Write + ?Sized>(writer: &mut W, buf: &[u8]) -> io::Result<()> {
    writer.write_all(buf)
}

/// Write all bytes of `buf` to `writer`, terminating the child process on
/// failure.
///
/// This is the variant the Fork GC child uses: if the pipe to the parent
/// breaks mid-write there is nothing productive the child can do, so we
/// log a warning and exit via [`RedisModule_ExitFromChild`].
///
/// # Panics
///
/// Panics if `RedisModule_ExitFromChild` has not been initialised by the
/// Redis module loader — a programmer error, not a runtime condition.
pub fn send_fixed_or_exit<W: Write + ?Sized>(writer: &mut W, buf: &[u8]) {
    if let Err(err) = send_fixed(writer, buf) {
        die_on_pipe_error(err);
    }
}

/// Log a broken-pipe warning and terminate the forked child.
///
/// `RedisModule_ExitFromChild` is declared as returning `int` but never
/// actually returns (it invokes `_exit` after Redis-side cleanup). The
/// trailing [`unreachable!`] exists to satisfy the `!` return type.
fn die_on_pipe_error(err: io::Error) -> ! {
    log_warning(format!("GC fork: broken pipe, exiting: {err}"));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_all_bytes_to_sink() {
        let mut sink: Vec<u8> = Vec::new();
        send_fixed(&mut sink, b"hello world").unwrap();
        assert_eq!(sink, b"hello world");
    }

    #[test]
    fn empty_buffer_is_a_no_op() {
        let mut sink: Vec<u8> = Vec::new();
        send_fixed(&mut sink, &[]).unwrap();
        assert!(sink.is_empty());
    }

    #[test]
    fn propagates_write_failure() {
        // `&mut [u8]` as a `Write` returns `WriteZero` once it's full.
        let mut backing = [0u8; 3];
        let mut slice: &mut [u8] = &mut backing;
        let err = send_fixed(&mut slice, b"hello").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    }
}
