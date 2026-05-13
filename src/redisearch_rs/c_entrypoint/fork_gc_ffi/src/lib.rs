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

use std::ffi::{c_char, c_int, c_void};

use fork_gc::{ForkGC, io_result_ext::IoResultExt, reader::RecvFrame};

use tracing::Level;
use tracing_log_error::log_error;

mod util;

/// Status code returned by Fork GC parent-side pipe-receive operations.
#[cheadergen::config(export)]
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FGCError {
    /// Data has been collected; more may follow.
    #[cheadergen(rename = "FGC_COLLECTED")]
    Collected = 0,
    /// No more data remains; iteration is complete.
    #[cheadergen(rename = "FGC_DONE")]
    Done = 1,
    /// Pipe error — the child process likely crashed.
    #[cheadergen(rename = "FGC_CHILD_ERROR")]
    ChildError = 2,
    /// Error on the parent side.
    #[cheadergen(rename = "FGC_PARENT_ERROR")]
    ParentError = 3,
    /// The index spec was deleted.
    #[cheadergen(rename = "FGC_SPEC_DELETED")]
    SpecDeleted = 4,
}

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

    fgc.writer().send_fixed(slice).unwrap_or_exit();
}

/// Write a length-prefixed buffer frame: a native-endian `size_t` header
/// followed by `len` payload bytes.
///
/// On error, logs the failure and terminates the child process via
/// `RedisModule_ExitFromChild(1)`.
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

    fgc.writer().send_buffer(slice).unwrap_or_exit();
}

/// Write the end-of-stream sentinel, signalling to the parent reader
/// that no more buffers will follow.
///
/// On error, logs the failure and terminates the child process via
/// `RedisModule_ExitFromChild(1)`.
///
/// # Safety
///
/// 1. `fgc` must point to a valid `ForkGC` whose `pipe_write_fd` is an open,
///    writable file descriptor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_sendTerminator(fgc: *mut ffi::ForkGC) {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(fgc) };

    fgc.writer().send_terminator().unwrap_or_exit();
}

/// Read exactly `len` bytes from the FGC pipe into `buf`.
///
/// Polls the pipe fd with a 3-minute timeout and retries on `EINTR`. On
/// timeout, read error, or unexpected EOF, logs a detailed warning
/// (matching the original C format) and returns `REDISMODULE_ERR`.
///
/// # Safety
///
/// 1. `fgc` must point to a valid `ForkGC` whose `pipe_read_fd` is an open,
///    readable file descriptor.
/// 2. `buf` must point to a writable region of at least `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FGC_recvFixed(
    fgc: *mut ffi::ForkGC,
    buf: *mut c_void,
    len: usize,
) -> c_int {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(fgc) };
    // SAFETY: caller guarantees (2).
    let slice = unsafe { std::slice::from_raw_parts_mut(buf.cast::<u8>(), len) };

    match fgc.reader().recv_fixed(slice) {
        Ok(()) => ffi::REDISMODULE_OK as c_int,
        Err(e) => {
            log_error!(e, level: Level::WARN, "ForkGC pipe read error");
            ffi::REDISMODULE_ERR as c_int
        }
    }
}

/// Read a length-prefixed buffer frame from the FGC pipe.
///
/// On receipt of a `SIZE_MAX` length prefix, writes `SIZE_MAX` to
/// `*len` and the `RECV_BUFFER_EMPTY` sentinel pointer to `*buf`. On a
/// zero-length prefix, writes `0` and a null pointer. Otherwise
/// allocates `len + 1` bytes (NUL-terminated) via the module allocator,
/// reads the payload, and stores the pointer in `*buf`; the caller is
/// responsible for releasing it with `rm_free`.
///
/// On read error (timeout, short stream, ...), returns `REDISMODULE_ERR`
/// and leaves `*buf` / `*len` unchanged.
///
/// # Safety
///
/// 1. `fgc` must point to a valid `ForkGC` whose `pipe_read_fd` is an
///    open, readable file descriptor.
/// 2. `buf` and `len` must point to writable `void*` and `size_t`
///    locations respectively.
#[unsafe(no_mangle)]
#[must_use]
pub unsafe extern "C" fn FGC_recvBuffer(
    fgc: *mut ffi::ForkGC,
    buf: *mut *mut c_void,
    len: *mut usize,
) -> c_int {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(fgc) };

    let frame = match fgc.reader().recv_buffer() {
        Ok(frame) => frame,
        Err(_) => return ffi::REDISMODULE_ERR as c_int,
    };

    let (ptr, payload_len) = util::frame_into_c_buffer(frame);

    // SAFETY: caller guarantees (2).
    unsafe { *buf = ptr };
    // SAFETY: caller guarantees (2).
    unsafe { *len = payload_len };

    ffi::REDISMODULE_OK as c_int
}

/// Receive a field header (field name + unique id).
///
/// Returns `FGC_COLLECTED` on success, `FGC_DONE` when no more fields remain,
/// or an error variant on pipe failure.
///
/// # Safety
///
/// 1. `fgc` must point to a valid `ForkGC` whose `pipe_read_fd` is an open,
///    readable file descriptor.
/// 2. `field_name` and `field_name_len` must point to writable `char*` and
///    `size_t` locations respectively.
/// 3. `id` must point to a writable `uint64_t` location.
#[unsafe(no_mangle)]
#[must_use]
pub unsafe extern "C" fn recvFieldHeader(
    fgc: *mut ffi::ForkGC,
    field_name: *mut *mut c_char,
    field_name_len: *mut usize,
    id: *mut u64,
) -> FGCError {
    // SAFETY: caller guarantees (1).
    let fgc = unsafe { ForkGC::from_ptr_mut(fgc) };
    let mut reader = fgc.reader();

    let frame = match reader.recv_buffer() {
        Ok(frame) => frame,
        Err(_) => return FGCError::ParentError,
    };

    if matches!(frame, RecvFrame::Terminator) {
        return FGCError::Done;
    }

    let mut id_bytes = [0u8; size_of::<u64>()];
    if reader.recv_fixed(&mut id_bytes).is_err() {
        return FGCError::ParentError;
    }

    let (name_ptr, name_len) = util::frame_into_c_buffer(frame);
    // SAFETY: caller guarantees (2).
    unsafe { *field_name = name_ptr.cast() };
    // SAFETY: caller guarantees (2).
    unsafe { *field_name_len = name_len };
    // SAFETY: caller guarantees (3).
    unsafe { *id = u64::from_ne_bytes(id_bytes) };

    FGCError::Collected
}
