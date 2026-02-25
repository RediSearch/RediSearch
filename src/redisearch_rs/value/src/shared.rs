/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::sync::Arc;

use crate::RsValue;

/// A shared RedisSearch dynamic value, backed by an `Arc<RsValue>`.
#[derive(Clone)]
pub struct SharedRsValue {
    inner: Arc<RsValue>,
}

impl SharedRsValue {
    /// Create a new shared RsValue wrapping an [`RsValue`]
    pub fn new(value: RsValue) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }

    /// Get a `*const RsValue` pointer from a [`SharedRsValue`].
    pub fn as_ptr(&self) -> *const RsValue {
        Arc::as_ptr(&self.inner)
    }

    /// Convert a [`SharedRsValue`] into a raw `*const RsValue` pointer.
    pub fn into_raw(self) -> *const RsValue {
        Arc::into_raw(self.inner)
    }

    /// Convert a `*const RsValue` back into a [`SharedRsValue`].
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer obtained from `SharedRsValue::into_raw`.
    pub unsafe fn from_raw(ptr: *const RsValue) -> Self {
        Self {
            // SAFETY: `ptr` is a valid pointer obtained from `SharedRsValue::into_raw`.
            inner: unsafe { Arc::from_raw(ptr) },
        }
    }

    /// Get a reference to the inner [`RsValue`].
    pub fn value(&self) -> &RsValue {
        &self.inner
    }

    /// Set a new [`RsValue`] for this [`SharedRsValue`].
    /// Only exactly one reference to the underlying [`RsValue`] must exist.
    ///
    /// # Panic
    ///
    /// Panics if more than one reference to the underlying [`RsValue`] exists.
    pub fn set_value(&mut self, new_value: RsValue) {
        let value =
            Arc::get_mut(&mut self.inner).expect("Failed to get mutable reference to inner value");

        *value = new_value;
    }

    /// Returns true if the two [`SharedRsValue`]s point to the same allocation in a vein similar to [`ptr::eq`][std::ptr::eq].
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.inner, &other.inner)
    }

    /// Returns the reference count of this [`SharedRsValue`].
    pub fn refcount(this: &Self) -> usize {
        Arc::strong_count(&this.inner)
    }
}

impl std::fmt::Debug for SharedRsValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value().fmt(f)
    }
}
