/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use index_gc::ForkPipe;

/// Borrow the [`ForkPipe`] behind a pointer received from C.
///
/// A null `fgc` fails a debug assertion that reports the calling entrypoint;
/// release builds do not check.
///
/// # Safety
///
/// 1. `fgc` must point to a valid, initialised [`ffi::IndexGC`].
/// 2. For the chosen lifetime `'a`, no other reference to the same struct may
///    exist.
#[track_caller]
pub(crate) unsafe fn borrow_fork_pipe<'a>(fgc: *mut ffi::IndexGC) -> &'a mut ForkPipe {
    debug_assert!(!fgc.is_null(), "`fgc` must not be null");
    // SAFETY: caller guarantees (1), which implies `fgc` is non-null.
    let fgc = unsafe { NonNull::new_unchecked(fgc) };
    // SAFETY: caller guarantees (1) and (2).
    unsafe { ForkPipe::from_ptr_mut(fgc) }
}
