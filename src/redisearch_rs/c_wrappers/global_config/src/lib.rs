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
//!
//! Every setting is read through its own accessor, each one load out of the static. There
//! is deliberately no whole-struct accessor: copying [`ffi::RSConfig`] is not atomic, so it
//! would not make the fields it copies agree with one another, while costing a large copy
//! and handing out owned pointers the caller never asked for. An operation that needs
//! several settings to agree resolves them into its own snapshot once, up front, and
//! passes that around.
//!
//! # Safety of these reads
//!
//! [`ffi::RSGlobalConfig`] is a statically initialised C global that lives for the whole
//! process, and every field read here is [`Copy`], so a value is read out without forming a
//! reference to the static. That is what makes these accessors safe to call.
//!
//! The reads are unsynchronised, which is the one thing that argument does not establish:
//! `CONFIG SET` writes these fields from client threads, so a read can race with a write.
//! This crate exists to replace the C readers of the same fields rather than to change what
//! they do; closing the race needs the C side to store the fields atomically and both the C
//! and Rust side to load them atomically.

use std::{
    ffi::{c_char, c_int},
    ptr::NonNull,
};

/// Defines one accessor per configuration field, reading it out of [`ffi::RSGlobalConfig`].
///
/// Every field declared here must be [`Copy`] and must not point at memory the caller
/// outlives, so that the module-level safety argument covers it without per-accessor
/// reasoning. A field that needs more than that — [`default_scorer`] — is written out
/// instead of declared here.
macro_rules! config_accessors {
    ($($(#[$doc:meta])* $name:ident() -> $ty:ty = $field:ident;)*) => {
        $(
            $(#[$doc])*
            #[inline]
            pub fn $name() -> $ty {
                // SAFETY: see this module's *Safety of these reads*.
                unsafe { ffi::RSGlobalConfig.$field }
            }
        )*
    };
}

config_accessors! {
    /// The Redis server version reported at module load, for gating features that need a
    /// minimum server.
    server_version() -> c_int = serverVersion;

    /// The cap on the number of results `FT.AGGREGATE` may return
    /// (`MAXAGGREGATERESULTS`).
    max_aggregate_results() -> usize = maxAggregateResults;

    /// Whether numeric indexes store doubles compressed to floats (`_NUMERIC_COMPRESS`).
    numeric_compress() -> bool = numericCompress;

    /// Whether an intersection iterator orders its children by factoring out unions rather
    /// than by estimated result count (`_PRIORITIZE_INTERSECT_UNION_CHILDREN`).
    prioritize_intersect_union_children() -> bool = prioritizeIntersectUnionChildren;
}

/// The name of the configured default scorer (`DEFAULT_SCORER`), or [`None`] when unset.
///
/// Reading the pointer is safe; dereferencing it is not. `FT.CONFIG SET DEFAULT_SCORER`
/// frees the old name before installing the new one, so the string can be freed while a
/// caller still holds this pointer. A caller that needs the name beyond the immediate read
/// must copy it rather than retain the pointer.
#[inline]
pub fn default_scorer() -> Option<NonNull<c_char>> {
    // SAFETY: see this module's *Safety of these reads*. Only the pointer is read here;
    // the string it points at is not.
    NonNull::new(unsafe { ffi::RSGlobalConfig.defaultScorer }.cast_mut())
}
