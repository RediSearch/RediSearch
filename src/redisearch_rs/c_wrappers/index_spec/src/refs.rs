/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Weak and strong references to a spec, through the C reference manager.

use std::ptr::NonNull;

use crate::{IndexSpecReadLock, IndexSpecWriteLock};

/// A weak reference to an [`IndexSpec`](crate::IndexSpec).
///
/// Just a copy of the underlying [`ffi::WeakRef`] handle — does not borrow any
/// owner and can be freely copied. Does not prevent the spec from being deleted.
/// Call [`promote`](Self::promote) to attempt promotion to an
/// [`IndexSpecStrongRef`], which keeps the spec alive via C-side refcounting.
pub struct IndexSpecWeakRef(ffi::WeakRef);

impl IndexSpecWeakRef {
    /// Wrap a raw [`ffi::WeakRef`].
    ///
    /// # Safety
    ///
    /// `weak` must be a valid weak reference.
    pub const unsafe fn from_raw(weak: ffi::WeakRef) -> Self {
        Self(weak)
    }

    /// Attempt to promote to a strong reference.
    ///
    /// Returns `None` if the spec has been deleted since the weak ref was obtained.
    pub fn promote(&self) -> Option<IndexSpecStrongRef> {
        // SAFETY: self.0 is a valid WeakRef (guaranteed by from_raw's caller).
        let strong_ref = unsafe { ffi::IndexSpecRef_Promote(self.0) };
        // SAFETY: StrongRef_Get is always safe to call on a valid StrongRef.
        let raw = unsafe { ffi::StrongRef_Get(strong_ref).cast::<ffi::IndexSpec>() };

        // When promotion fails, IndexSpecRef_Promote returns StrongRef { rm: NULL } —
        // no reference was acquired, so there is nothing to release.
        NonNull::new(raw).map(|spec| IndexSpecStrongRef { strong_ref, spec })
    }
}

/// A promoted strong reference to an [`IndexSpec`](crate::IndexSpec).
///
/// Obtained via [`IndexSpecWeakRef::promote`]. Keeps the spec alive for the
/// duration of this value and releases the strong reference on drop.
pub struct IndexSpecStrongRef {
    strong_ref: ffi::StrongRef,
    spec: NonNull<ffi::IndexSpec>,
}

impl IndexSpecStrongRef {
    /// Acquire the read lock for this `IndexSpec`.
    ///
    /// Any number of threads may hold the read lock together; the write lock
    /// waits until all of them have released it. Taking it also pauses
    /// incremental rehashing of the keys dict, which is what makes lookups
    /// through the returned guard safe while other readers run.
    ///
    /// The lock takes this reference and releases it after unlocking.
    ///
    /// # Aborts
    ///
    /// Aborts the process, through a C assertion rather than a panic, if this
    /// thread already holds a lock on any spec: the C lock bookkeeping is per
    /// thread and does not nest.
    pub fn read(self) -> IndexSpecReadLock {
        IndexSpecReadLock::new(self)
    }

    /// Acquire the write lock for this `IndexSpec`, required before modifying it.
    ///
    /// The lock takes this reference and releases it after unlocking.
    ///
    /// # Aborts
    ///
    /// As [`read`](Self::read) does, if this thread already holds a spec lock.
    pub fn write(self) -> IndexSpecWriteLock {
        IndexSpecWriteLock::new(self)
    }

    /// The spec this reference keeps alive.
    pub(crate) const fn as_non_null(&self) -> NonNull<ffi::IndexSpec> {
        self.spec
    }
}

impl Drop for IndexSpecStrongRef {
    fn drop(&mut self) {
        // SAFETY: strong_ref was obtained from IndexSpecRef_Promote and has
        // not yet been released.
        unsafe { ffi::IndexSpecRef_Release(self.strong_ref) };
    }
}
