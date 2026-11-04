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
/// Bindgen represents the atomic blocked-client marker, the atomic memory stop flag and the clock
/// counter in [`ffi::QueryRequestLimits`] as ordinary Rust fields. Creating a shared reference to
/// that bindgen type would therefore promise immutability while C may legally modify those fields.
/// This transparent [`UnsafeCell`] wrapper expresses that interior mutability without exposing
/// the fields to Rust.
///
/// The handle borrows rather than owns the C object. Trie wrappers pass its pointer only for the
/// duration of their synchronous FFI call; they never retain it. The C timeout state machine owns
/// source transitions and synchronization: transitions occur between execution cycles, the
/// blocked-client marker and the memory stop flag may be changed atomically while a request runs, and
/// clock-counter calls for one request are serialized. [`QueryRequestLimitsHandle`] deliberately
/// does not make those C operations safe to overlap.
#[repr(transparent)]
pub struct QueryRequestLimitsHandle {
    inner: UnsafeCell<ffi::QueryRequestLimits>,
}

impl QueryRequestLimitsHandle {
    /// Borrow a C request timeout without creating a reference to
    /// [`ffi::QueryRequestLimits`].
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
    /// or observation of the blocked-client marker and the memory stop flag; the source
    /// discriminator, clock deadline, and clock counter cannot be mutated concurrently.
    pub unsafe fn from_raw<'a>(
        timeout: *mut ffi::QueryRequestLimits,
    ) -> Option<&'a QueryRequestLimitsHandle> {
        let timeout = NonNull::new(timeout)?;
        // SAFETY: the caller guarantees the pointer is valid for `'a`. `repr(transparent)` and
        // `UnsafeCell` preserve the wrapped C type's layout and alignment.
        Some(unsafe { &*timeout.cast::<QueryRequestLimitsHandle>().as_ptr() })
    }

    /// The deadline of the active clock cycle, or `None` when the cycle uses
    /// any other timeout source.
    pub fn clock_deadline(&self) -> Option<ffi::timespec> {
        let timeout = self.inner.get();
        // SAFETY: `from_raw` guarantees the object is live, and its cycle
        // contract makes `kind` immutable while this handle is in use.
        let kind = unsafe { (*timeout).timeout.kind };
        if kind != ffi::QueryRequestTimeoutKind_QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE {
            return None;
        }
        // SAFETY: the object is live, as above. Taking the place's address
        // reads nothing.
        let clock = unsafe { &raw const (*timeout).timeout.source.clock };
        // SAFETY: `kind` selects `clock` as the active union member, and the
        // cycle contract keeps its deadline immutable. Reading only this field
        // leaves the non-atomic counter and the atomic marker untouched.
        Some(unsafe { (*clock).deadline })
    }

    /// Whether a blocked-client cycle has been marked as timed out.
    ///
    /// Reports `false` for every other source, the clock cycle included, so a
    /// caller that already enforces the clock deadline itself adds no clock
    /// checks here and leaves the request's shared counter alone. A walk driven
    /// by [`clock_deadline`](Self::clock_deadline) needs this as well: a
    /// blocked-client cycle has no deadline to enforce, only a flag to observe.
    pub fn blocked_client_timed_out(&self) -> bool {
        // SAFETY: `from_raw` guarantees the object is live, and its cycle
        // contract allows the blocked-client marker to be published or observed
        // atomically while this handle is in use, which is all this reads.
        unsafe { ffi::QueryRequestLimits_IsBlockedClientTimedOut(self.inner.get()) }
    }

    /// Exposes the borrowed object to a synchronous C operation.
    ///
    /// The pointer must not be retained after the operation returns.
    pub(crate) const fn as_mut_ptr(&self) -> *mut ffi::QueryRequestLimits {
        self.inner.get()
    }
}
