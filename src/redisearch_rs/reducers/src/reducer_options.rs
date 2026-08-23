/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use query_error::QueryError;

/// A safe wrapper around an `ffi::ReducerOptions`.
#[repr(transparent)]
pub struct ReducerOptions(ffi::ReducerOptions);

impl ReducerOptions {
    /// Create a `ReducerOptions` wrapper from a pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a [valid] pointer to an `ffi::ReducerOptions` that is properly initialized.
    ///    This also applies to any of its subfields.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw_mut<'a>(ptr: NonNull<ffi::ReducerOptions>) -> &'a mut Self {
        // SAFETY: ensured by caller (1.)
        unsafe { &mut *ptr.cast::<Self>().as_ptr() }
    }

    /// Get a reference to the `args` cursor.
    pub const fn args(&self) -> &ffi::ArgsCursor {
        // SAFETY: (1.) due to creation with `ReducerOptions::from_raw_mut`
        unsafe { self.0.args.as_ref().unwrap() }
    }

    /// Get a mutable reference to the query error.
    pub const fn status(&mut self) -> &mut QueryError {
        // SAFETY: (1.) due to creation with `ReducerOptions::from_raw_mut`
        unsafe { self.0.status.cast::<QueryError>().as_mut().unwrap() }
    }
}
