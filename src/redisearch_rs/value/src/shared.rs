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
    ptr::{self, NonNull},
};

use triomphe::Arc;

use crate::Value;

/// The total heap allocation size of the `triomphe::Arc` inner struct.
/// Since that struct is inaccessible/hidden, the size is calculated manually,
/// which contains a `Value` and a `usize` atomic refcount.
pub const SHARED_VALUE_CONTENT_SIZE: usize = size_of::<Value>() + size_of::<usize>();

// Ensure the size doesn't increase unexpectedly.
const _: () = assert!(SHARED_VALUE_CONTENT_SIZE == 32);

/// A reference-counted, shared pointer to a [`Value`].
///
/// Internally, this is a raw pointer to a [`Value`] that is either:
/// - **Static**: points to the global [`NULL_VALUE`] sentinel, requiring no
///   reference counting.
/// - **Heap-allocated**: backed by an [`Arc<Value>`](Arc), with manual
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
#[repr(transparent)]
pub struct SharedValue {
    ptr: NonNull<Value>,
}

pub type SharedValueRef = ManuallyDrop<SharedValue>;
pub type SharedValueRefMut = ManuallyDrop<SharedValue>;

/// Static [`Value::Null`] value, used by [`SharedValue::null_static`]
/// to avoid heap allocation for null values.
static NULL_VALUE: Value = Value::Null;

impl SharedValue {
    /// Creates a [`SharedValue`] pointing to the static [`NULL_VALUE`].
    #[expect(rustdoc::private_intra_doc_links)]
    pub fn null_static() -> Self {
        Self {
            ptr: NonNull::from(&NULL_VALUE),
        }
    }

    /// Creates a new heap-allocated [`SharedValue`] backed by an [`Arc`].
    ///
    /// Uses a thread-local pool to recycle allocations when available.
    pub fn new(value: Value) -> Self {
        let ptr = Arc::into_raw(crate::pool::pool_get(value).shareable());
        Self {
            // SAFETY: `Arc::into_raw` always returns a non-null pointer.
            ptr: unsafe { NonNull::new_unchecked(ptr.cast_mut()) },
        }
    }

    /// Convert a [`SharedValue`] into a raw `*const Value` pointer.
    pub const fn into_raw(self) -> *const Value {
        let ptr = self.ptr.as_ptr();
        // The original [`SharedValue`] is forgotten to avoid decrementing it.
        mem::forget(self);
        ptr
    }

    /// Returns the underlying raw pointer without consuming `self`.
    pub const fn as_ptr(&self) -> *const Value {
        self.ptr.as_ptr()
    }

    /// Convert a `*const Value` into a [`SharedValue`].
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer obtained from [`SharedValue::into_raw`].
    pub const unsafe fn from_raw(ptr: *const Value) -> Self {
        // SAFETY: `ptr` was obtained from [`SharedValue::into_raw`], which
        // always returns a non-null pointer.
        Self {
            ptr: unsafe { NonNull::new_unchecked(ptr.cast_mut()) },
        }
    }

    /// Returns `true` if this value points to the static [`NULL_VALUE`]
    /// rather than a heap-allocated [`Arc`].
    pub fn is_null_static(&self) -> bool {
        ptr::eq(self.ptr.as_ptr(), &NULL_VALUE)
    }

    pub fn as_ref(&self) -> SharedValueRef {
        ManuallyDrop::new(unsafe { SharedValue::from_raw(self.as_ptr()) })
    }

    /// Replaces the stored [`Value`] in place.
    ///
    /// # Panics
    ///
    /// - Panics if this is a static null value (created via [`Self::null_static`]).
    /// - Panics if there are other outstanding clones sharing the same
    ///   allocation (i.e. the [`Arc`] strong count is greater than 1).
    pub fn set_value(&mut self, new_value: Value) {
        if self.is_null_static() {
            panic!("Cannot change the value of static NULL");
        }
        // SAFETY: `this.ptr` was obtained from `Arc::into_raw` and is not static (checked above).
        // `ManuallyDrop` prevents the `Arc` from being dropped, preserving the original ownership.
        let mut v = ManuallyDrop::new(unsafe { Arc::from_raw(self.ptr.as_ptr()) });
        let value = Arc::get_mut(&mut v).expect("Failed to get mutable reference to inner value");
        *value = new_value;
    }

    /// Returns true if the two [`SharedValue`]s point to the same allocation similar
    /// to [`ptr::eq`].
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        this.ptr == other.ptr
    }

    /// Returns the reference count of this [`SharedValue`].
    pub fn refcount(this: &Self) -> usize {
        if this.is_null_static() {
            1
        } else {
            // SAFETY: `this.ptr` was obtained from `Arc::into_raw` and is not static (checked above).
            // `ManuallyDrop` prevents the `Arc` from being dropped, preserving the original ownership.
            let v = ManuallyDrop::new(unsafe { Arc::from_raw(this.ptr.as_ptr()) });
            Arc::strong_count(&v)
        }
    }

    pub fn new_num(num: f64) -> Self {
        Self::new(Value::Number(num))
    }

    pub fn new_string(str: Vec<u8>) -> Self {
        Self::new(Value::new_string(str))
    }
}

impl Deref for SharedValue {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        // SAFETY: `self.ptr` is either `&NULL_VALUE` (static) or was obtained
        // from `Arc::into_raw` and the `Arc` is kept alive for the lifetime of
        // `self`. Both cases produce a valid pointer.
        unsafe { self.ptr.as_ref() }
    }
}

impl std::fmt::Debug for SharedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl Clone for SharedValue {
    fn clone(&self) -> Self {
        if self.is_null_static() {
            Self { ptr: self.ptr }
        } else {
            // SAFETY: `self.ptr` was obtained from `Arc::into_raw` and is not static (checked above).
            // The original `Arc` is reconstructed, cloned to increment the refcount,
            // then forgotten to avoid decrementing it.
            let arc: Arc<Value> = unsafe { Arc::from_raw(self.ptr.as_ptr()) };
            let cloned = Arc::clone(&arc);
            mem::forget(arc);
            let cloned_ptr = Arc::into_raw(cloned);
            Self {
                // SAFETY: `Arc::into_raw` always returns a non-null pointer.
                ptr: unsafe { NonNull::new_unchecked(cloned_ptr.cast_mut()) },
            }
        }
    }
}

impl Drop for SharedValue {
    fn drop(&mut self) {
        if !self.is_null_static() {
            // SAFETY: `self.ptr` was obtained from `Arc::into_raw` and is not static (checked above).
            // Reconstructing the `Arc` decrements the reference count.
            let arc = unsafe { Arc::from_raw(self.ptr.as_ptr()) };

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
// Send + Sync) or an `Arc<Value>` raw pointer, and `Arc<T>` is Send + Sync
// when `T: Send + Sync`. `Value` satisfies both bounds.
unsafe impl Send for SharedValue {}

// SAFETY: The inner pointer is either the `&'static NULL_VALUE` (inherently
// Send + Sync) or an `Arc<Value>` raw pointer, and `Arc<T>` is Send + Sync
// when `T: Send + Sync`. `Value` satisfies both bounds.
unsafe impl Sync for SharedValue {}
