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
/// A copy rather than a `&'static ffi::RSConfig`, which would assert that nothing writes the
/// static while it lives — `FT.CONFIG SET` does, from client threads. The read is
/// unsynchronised, as it is on the C side, so it can tear against a concurrent write, and any
/// pointer field can be freed by a later one.
///
/// Take one snapshot per operation rather than one per field, so that an operation cannot
/// observe a setting changing halfway through.
pub fn get() -> ffi::RSConfig {
    // SAFETY: `RSGlobalConfig` is a statically initialised C global that lives for the whole
    // process, and `RSConfig` is `Copy`, so the value is read out without forming a reference.
    unsafe { ffi::RSGlobalConfig }
}
