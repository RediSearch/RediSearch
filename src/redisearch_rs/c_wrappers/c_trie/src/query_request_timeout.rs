/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{cell::UnsafeCell, ptr::NonNull};

/// An aliasing-safe borrowed view of C-owned request timeout state.
///
/// Bindgen represents the atomic blocked-client marker and the clock counter in
/// [`ffi::QueryRequestTimeout`] as ordinary Rust fields. Creating a shared reference to that
/// bindgen type would therefore promise immutability while C may legally modify those fields.
/// This transparent [`UnsafeCell`] wrapper expresses that interior mutability without exposing
/// the fields to Rust.
///
/// The handle borrows rather than owns the C object. Trie wrappers pass its pointer only for the
/// duration of their synchronous FFI call; they never retain it. The C timeout state machine owns
/// source transitions and synchronization: transitions occur between execution cycles, the
/// blocked-client marker may be changed atomically during a cycle, and clock-counter calls for one
/// request are serialized. [`QueryRequestTimeoutHandle`] deliberately does not make those C
/// operations safe to overlap.
#[repr(transparent)]
pub struct QueryRequestTimeoutHandle {
    inner: UnsafeCell<ffi::QueryRequestTimeout>,
}

impl QueryRequestTimeoutHandle {
    /// Borrow a C request timeout without creating a reference to
    /// [`ffi::QueryRequestTimeout`].
    ///
    /// Returns `None` when `timeout` is null.
    ///
    /// # Safety
    ///
    /// A non-null `timeout` must be aligned, initialized, and remain valid for `'a`; `'a` must not
    /// extend beyond the request that owns it. No Rust reference to the underlying bindgen value,
    /// and no mutable Rust reference to the same storage, may coexist with the returned handle.
    ///
    /// The caller must also uphold the C API's cycle contract: the active source cannot change
    /// during an operation using this handle. Concurrent C access is limited to atomic publication
    /// or observation of the blocked-client marker; the source discriminator, clock deadline, and
    /// clock counter cannot be mutated concurrently.
    pub unsafe fn from_raw<'a>(
        timeout: *mut ffi::QueryRequestTimeout,
    ) -> Option<&'a QueryRequestTimeoutHandle> {
        let timeout = NonNull::new(timeout)?;
        // SAFETY: the caller guarantees the pointer is valid for `'a`. `repr(transparent)` and
        // `UnsafeCell` preserve the wrapped C type's layout and alignment.
        Some(unsafe { &*timeout.cast::<QueryRequestTimeoutHandle>().as_ptr() })
    }

    /// Exposes the borrowed object to a synchronous C operation.
    ///
    /// The pointer must not be retained after the operation returns.
    pub(crate) const fn as_mut_ptr(&self) -> *mut ffi::QueryRequestTimeout {
        self.inner.get()
    }
}
