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
    /// Borrow `ptr` as an exclusive [`ReducerOptions`].
    ///
    /// # Safety
    ///
    /// 1. `ptr` must point to initialized [`ffi::ReducerOptions`] that remain [valid]
    ///    for the returned lifetime. The [`ffi::ArgsCursor`] and [`QueryError`]
    ///    pointees must remain initialized and alive for that lifetime.
    /// 2. The options and their [`QueryError`] pointee must be exclusively accessible
    ///    through this borrow for the returned lifetime. The [`ffi::ArgsCursor`]
    ///    pointee must not be mutated while borrowed through [`Self::args`].
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw_mut<'a>(ptr: NonNull<ffi::ReducerOptions>) -> &'a mut Self {
        // SAFETY: the caller guarantees validity and exclusive access for the returned lifetime.
        unsafe { ptr.cast::<Self>().as_mut() }
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
