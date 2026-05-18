/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::RedisSearchCtx`].

use index_spec::IndexSpec;

/// A safe wrapper around a C [`ffi::RedisSearchCtx`].
///
/// Obtained from a raw pointer via [`SearchCtx::from_raw`] or
/// [`SearchCtx::from_raw_mut`].
#[repr(transparent)]
pub struct SearchCtx(ffi::RedisSearchCtx);

impl SearchCtx {
    /// Borrow a [`SearchCtx`] from a raw const pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid, non-null pointer to an `ffi::RedisSearchCtx`
    /// that is properly initialised and remains live for the returned
    /// lifetime `'a`.
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::RedisSearchCtx) -> &'a Self {
        // SAFETY: #[repr(transparent)] guarantees identical layout.
        // Validity and liveness are the caller's responsibility.
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Return the [`IndexSpec`] associated with this search context.
    pub fn spec(&self) -> &IndexSpec {
        debug_assert!(!self.0.spec.is_null(), "spec must not be null");
        // SAFETY: spec is a valid non-null IndexSpec* for a properly
        // initialised SearchCtx (invariant upheld by construction).
        unsafe { IndexSpec::from_raw(self.0.spec) }
    }
}
