/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI entrypoint for the Fork GC module.
//!
//! Functions in this crate are called from `src/fork_gc/*.c` and replace
//! their C counterparts symbol-for-symbol. This layer is a pure FFI
//! trampoline: it translates raw C inputs into safe Rust types and
//! delegates everything else — including Redis-specific failure
//! handling — to the `fork_gc` crate.

use std::ffi::c_void;

use fork_gc::ForkGC;

/// Write exactly `len` bytes from `buff` to the FGC pipe.
///
/// On error, logs the failure and terminates the child process via
/// `RedisModule_ExitFromChild(1)`.
///
/// # Safety
///
/// 1. `fgc` must point to a valid `ForkGC` whose `pipe_write_fd` is an open,
///    writable file descriptor.
/// 2. `buff` must point to a readable region of at least `len` bytes.
/// 3. `len` must be greater than zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_sendFixed(fgc: *mut ffi::ForkGC, buff: *const c_void, len: usize) {
    debug_assert!(len > 0, "buffer length cannot be 0");

    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(fgc) };
    // SAFETY: caller guarantees (2).
    let slice = unsafe { std::slice::from_raw_parts(buff.cast::<u8>(), len) };

    fgc.writer().send_fixed_or_exit(slice);
}

/// Write a length-prefixed buffer frame: a native-endian `size_t` header
/// followed by `len` payload bytes.
///
/// On error, logs the failure and terminates the child process via
/// `RedisModule_ExitFromChild(1)`. Mirrors `FGC_sendBuffer` in
/// `src/fork_gc/pipe.c`.
///
/// # Safety
///
/// 1. `fgc` must point to a valid `ForkGC` whose `pipe_write_fd` is an open,
///    writable file descriptor.
/// 2. If `len > 0`, `buff` must point to a readable region of at least
///    `len` bytes. When `len == 0`, `buff` is unused and may be anything
///    (including NULL).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_sendBuffer(fgc: *mut ffi::ForkGC, buff: *const c_void, len: usize) {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(fgc) };

    let slice = if len > 0 {
        // SAFETY: caller guarantees (2).
        unsafe { std::slice::from_raw_parts(buff.cast::<u8>(), len) }
    } else {
        &[]
    };

    fgc.writer().send_buffer_or_exit(slice);
}

/// Write the end-of-stream sentinel, signalling to the parent reader
/// that no more buffers will follow.
///
/// On error, logs the failure and terminates the child process via
/// `RedisModule_ExitFromChild(1)`. Mirrors `FGC_sendTerminator` in
/// `src/fork_gc/pipe.c`.
///
/// # Safety
///
/// `fgc` must point to a valid `ForkGC` whose `pipe_write_fd` is an open,
/// writable file descriptor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_sendTerminator(fgc: *mut ffi::ForkGC) {
    // SAFETY: caller guarantees fgc validity.
    let fgc = unsafe { ForkGC::from_ptr_mut(fgc) };

    fgc.writer().send_terminator_or_exit();
}
