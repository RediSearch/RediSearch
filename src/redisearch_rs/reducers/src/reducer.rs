/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// A safe wrapper around an `ffi::Reducer`.
#[repr(transparent)]
pub struct Reducer(ffi::Reducer);

impl Reducer {
    /// Create a `Reducer` wrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid non-null pointer to an `ffi::Reducer` that is properly initialized.
    ///    This also applies to any of its subfields.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::Reducer) -> &'a Self {
        // SAFETY: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }
}
