/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe read access to [`ffi::RSGlobalConfig`], the process-wide RediSearch configuration
//! defined in `src/config.c`.

/// Copies the process-wide configuration.
///
/// A copy rather than a `&'static `[`ffi::RSConfig`], which would assert that nothing writes
/// the static while it lives — `CONFIG SET` does, from client threads. Any pointer field can
/// also be freed by a later write.
///
/// Take one snapshot per operation rather than one per field: a struct copy is not atomic, so
/// this narrows the window in which an operation sees a setting change halfway through, but
/// does not close it.
#[inline]
pub fn get() -> ffi::RSConfig {
    // SAFETY: `RSGlobalConfig` is a statically initialised C global that lives for the whole
    // process, and `RSConfig` is `Copy`, so the value is read out without forming a reference.
    //
    // The read is deliberately unsynchronised, which is the one precondition this argument
    // does not establish: `CONFIG SET` writes these fields from client threads, so the copy
    // can race and tear. That is the behaviour of the C readers of the same fields, which
    // this wrapper exists to replace rather than to change; removing the race needs the C
    // side to store the fields atomically.
    unsafe { ffi::RSGlobalConfig }
}
