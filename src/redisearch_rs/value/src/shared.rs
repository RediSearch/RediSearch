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
};

use triomphe::Arc;

use crate::RsValue;

/// The total heap allocation size of an `RsValue` wrapped in a `triomphe::Arc`.
pub const SHARED_VALUE_SIZE: usize = 32;

// Ensure the size doesn't increase unexpectedly.
// The 8 bytes overhead is from the `triomphe::Arc` atomic refcount.
const _: () = assert!(SHARED_VALUE_SIZE == size_of::<RsValue>() + size_of::<usize>());

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
/// pointer for static values). [`Drop`] decrements it and, when the last
/// reference is dropped, recycles the allocation into a thread-local pool
/// (see [`crate::pool`]) instead of deallocating. If the pool is full, the
/// allocation is deallocated normally.
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
    ///
    /// Uses a thread-local pool to recycle allocations when available.
    pub fn new(value: RsValue) -> Self {
        Self {
            ptr: Arc::into_raw(crate::pool::pool_get(value).shareable()),
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
    pub fn is_null_static(&self) -> bool {
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
        if self.is_null_static() {
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
        if this.is_null_static() {
            1
        } else {
            // SAFETY: `this.ptr` was obtained from `Arc::into_raw` and is not static (checked above).
            // `ManuallyDrop` prevents the `Arc` from being dropped, preserving the original ownership.
            let v = ManuallyDrop::new(unsafe { Arc::from_raw(this.ptr) });
            Arc::strong_count(&v)
        }
    }

    pub fn new_num(num: f64) -> Self {
        Self::new(RsValue::Number(num))
    }

    pub fn new_string(str: Vec<u8>) -> Self {
        Self::new(RsValue::new_string(str))
    }

    pub const fn mem_size() -> usize {
        SHARED_VALUE_SIZE
    }
}

impl Deref for SharedRsValue {
    type Target = RsValue;

    fn deref(&self) -> &Self::Target {
        // SAFETY: `self.ptr` is either `&NULL_VALUE` (static) or was obtained
        // from `Arc::into_raw` and the `Arc` is kept alive for the lifetime of
        // `self`. Both cases produce a valid pointer.
        unsafe { &*self.ptr }
    }
}

impl std::fmt::Debug for SharedRsValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl Clone for SharedRsValue {
    fn clone(&self) -> Self {
        if self.is_null_static() {
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
        if !self.is_null_static() {
            // SAFETY: `self.ptr` was obtained from `Arc::into_raw` and is not static (checked above).
            // Reconstructing the `Arc` decrements the reference count.
            let arc = unsafe { Arc::from_raw(self.ptr) };

            // Convert this Arc into a UniqueArc if the Arc has exactly one strong reference,
            // otherwise None is returned and the Arc is dropped reducing its reference count.
            if let Some(unique_arc) = Arc::into_unique(arc) {
                // Release the UniqueArc to the pool to be recycled.
                crate::pool::pool_release(unique_arc);
            }
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
