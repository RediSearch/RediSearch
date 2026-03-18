/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    mem::{self, ManuallyDrop},
    ops::Deref,
    ptr,
    sync::Arc,
};

use crate::RsValue;

/// A reference-counted, shared pointer to an [`RsValue`].
///
/// Internally, this is a raw pointer to an [`RsValue`] that is either:
/// - **Static**: points to the global [`NULL_VALUE`] sentinel, requiring no
///   reference counting.
/// - **Heap-allocated**: backed by an [`Arc<RsValue>`](Arc), with manual
///   reference counting managed through raw pointer conversions.
///
/// # Cloning and dropping
///
/// [`Clone`] increments the [`Arc`] reference count (or cheaply copies the
/// pointer for static values). [`Drop`] decrements it and frees the allocation
/// when the count reaches zero.
#[expect(rustdoc::private_intra_doc_links)]
pub struct SharedRsValue {
    ptr: *const RsValue,
}

/// Static [`RsValue::Null`] value, used by [`SharedRsValue::null_static`]
/// to avoid heap allocation for null values.
static NULL_VALUE: RsValue = RsValue::Null;

impl SharedRsValue {
    /// Creates a [`SharedRsValue`] pointing to the static [`NULL_VALUE`].
    #[expect(rustdoc::private_intra_doc_links)]
    pub fn null_static() -> Self {
        Self {
            ptr: ptr::from_ref(&NULL_VALUE).cast(),
        }
    }

    /// Creates a new heap-allocated [`SharedRsValue`] backed by an [`Arc`].
    pub fn new(value: RsValue) -> Self {
        Self {
            ptr: Arc::into_raw(Arc::new(value)),
        }
    }

    /// Convert a [`SharedRsValue`] into a raw `*const RsValue` pointer.
    pub const fn into_raw(self) -> *const RsValue {
        let ptr = self.ptr;
        // The original [`SharedRsValue`] is forgotten to avoid decrementing it.
        mem::forget(self);
        ptr
    }

    /// Returns the underlying raw pointer without consuming `self`.
    pub const fn as_ptr(&self) -> *const RsValue {
        self.ptr
    }

    /// Convert a `*const RsValue` into a [`SharedRsValue`].
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer obtained from [`SharedRsValue::into_raw`].
    pub const unsafe fn from_raw(ptr: *const RsValue) -> Self {
        Self { ptr }
    }

    /// Returns `true` if this value points to the static [`NULL_VALUE`]
    /// rather than a heap-allocated [`Arc`].
    fn is_static(&self) -> bool {
        ptr::eq(self.ptr, &NULL_VALUE)
    }

    /// Replaces the stored [`RsValue`] in place.
    ///
    /// # Panics
    ///
    /// - Panics if this is a static null value (created via [`Self::null_static`]).
    /// - Panics if there are other outstanding clones sharing the same
    ///   allocation (i.e. the [`Arc`] strong count is greater than 1).
    pub fn set_value(&mut self, new_value: RsValue) {
        if self.is_static() {
            panic!("Cannot change the value of static NULL");
        }
        // SAFETY: `this.ptr` was obtained from `Arc::into_raw` and is not static (checked above).
        // `ManuallyDrop` prevents the `Arc` from being dropped, preserving the original ownership.
        let mut v = ManuallyDrop::new(unsafe { Arc::from_raw(self.ptr) });
        let value = Arc::get_mut(&mut v).expect("Failed to get mutable reference to inner value");
        *value = new_value;
    }

    /// Returns true if the two [`SharedRsValue`]s point to the same allocation similar
    /// to [`ptr::eq`].
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        this.ptr == other.ptr
    }

    /// Returns the reference count of this [`SharedRsValue`].
    pub fn refcount(this: &Self) -> usize {
        if this.is_static() {
            1
        } else {
            // SAFETY: `this.ptr` was obtained from `Arc::into_raw` and is not static (checked above).
            // `ManuallyDrop` prevents the `Arc` from being dropped, preserving the original ownership.
            let v = ManuallyDrop::new(unsafe { Arc::from_raw(this.ptr) });
            Arc::strong_count(&v)
        }
    }
}

impl Deref for SharedRsValue {
    type Target = RsValue;

    fn deref(&self) -> &Self::Target {
        if self.is_static() {
            &NULL_VALUE
        } else {
            // SAFETY: `self.ptr` was obtained from `Arc::into_raw` and the
            // `Arc` is kept alive for the lifetime of `self`.
            unsafe { &*self.ptr }
        }
    }
}

impl std::fmt::Debug for SharedRsValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl Clone for SharedRsValue {
    fn clone(&self) -> Self {
        if self.is_static() {
            Self { ptr: self.ptr }
        } else {
            // SAFETY: `self.ptr` was obtained from `Arc::into_raw` and is not static (checked above).
            // The original `Arc` is reconstructed, cloned to increment the refcount,
            // then forgotten to avoid decrementing it.
            let arc: Arc<RsValue> = unsafe { Arc::from_raw(self.ptr) };
            let cloned = Arc::clone(&arc);
            mem::forget(arc);
            Self {
                ptr: Arc::into_raw(cloned),
            }
        }
    }
}

impl Drop for SharedRsValue {
    fn drop(&mut self) {
        if !self.is_static() {
            // SAFETY: `self.ptr` was obtained from `Arc::into_raw` and is not static (checked above).
            // Reconstructing and dropping the `Arc` decrements the reference count.
            unsafe { drop(Arc::from_raw(self.ptr)) };
        }
    }
}

// SAFETY: The inner pointer is either the `&'static NULL_VALUE` (inherently
// Send + Sync) or an `Arc<RsValue>` raw pointer, and `Arc<T>` is Send + Sync
// when `T: Send + Sync`. `RsValue` satisfies both bounds.
unsafe impl Send for SharedRsValue {}

// SAFETY: The inner pointer is either the `&'static NULL_VALUE` (inherently
// Send + Sync) or an `Arc<RsValue>` raw pointer, and `Arc<T>` is Send + Sync
// when `T: Send + Sync`. `RsValue` satisfies both bounds.
unsafe impl Sync for SharedRsValue {}
