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

    let mut writer = fgc.writer();

    fork_gc::pipe::send_fixed_or_exit(&mut writer, slice);
}
