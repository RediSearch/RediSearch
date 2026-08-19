/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{cell::UnsafeCell, ptr::NonNull};

/// A borrowed C request timeout whose state may change through C APIs.
///
/// Bindgen represents the C atomic flag and clock counter as ordinary Rust fields. Wrapping the
/// complete [`ffi::QueryRequestTimeout`] in [`UnsafeCell`] prevents a shared Rust reference from
/// promising that C will not mutate that state.
#[repr(transparent)]
pub struct QueryRequestTimeoutHandle {
    inner: UnsafeCell<ffi::QueryRequestTimeout>,
}

impl QueryRequestTimeoutHandle {
    /// Borrow a C request timeout without creating a reference to the bindgen type.
    ///
    /// Returns `None` when `timeout` is null.
    ///
    /// # Safety
    ///
    /// A non-null `timeout` must be aligned, initialized, and remain valid for `'a`. Its active
    /// source must not change while an operation is using the returned handle. Concurrent access
    /// is permitted only for the blocked-client flag and must use the C atomic timeout APIs; clock
    /// state and the source discriminator must not be accessed concurrently.
    pub unsafe fn from_raw<'a>(
        timeout: *mut ffi::QueryRequestTimeout,
    ) -> Option<&'a QueryRequestTimeoutHandle> {
        let timeout = NonNull::new(timeout)?;
        // SAFETY: the caller guarantees the pointer is valid for `'a`. `repr(transparent)` and
        // `UnsafeCell` preserve the wrapped C type's layout and alignment.
        Some(unsafe { &*timeout.cast::<QueryRequestTimeoutHandle>().as_ptr() })
    }

    pub(crate) const fn as_mut_ptr(&self) -> *mut ffi::QueryRequestTimeout {
        self.inner.get()
    }
}
