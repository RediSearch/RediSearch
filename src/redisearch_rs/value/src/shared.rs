/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ptr, sync::Arc};

use crate::{RsValueInternal, Value};

/// A heap-allocated and refcounted RedisSearch dynamic value.
/// This type is backed by [`Arc<RsValueInternal>`], but uses
/// the NULL pointer to encode an undefined value, and is FFI safe.
///
/// # Invariants
/// - If this pointer is non-NULL, it was obtained from `Arc::into_raw`.
/// - If it is NULL, it represents an undefined value.
/// - A non-null pointer represents one clone of said `Arc`, and as such, as
///   long as the [`SharedRsValue`] lives and holds a non-null pointer, the Arc
///   is still valid.
#[repr(C)]
pub struct SharedRsValue {
    /// Pointer representing the `Arc<RsValueInternal>`.
    ptr: *const RsValueInternal,
}

impl Default for SharedRsValue {
    fn default() -> Self {
        Self::undefined()
    }
}

impl Drop for SharedRsValue {
    fn drop(&mut self) {
        if self.ptr.is_null() {
            return;
        }
        // Safety: `self.ptr` is not null at this point,
        // and so must have been originated from a call to `from_internal`.
        // At least as long as `self` lives, this the strong reference
        // count of the backing `Arc` is at least 1.
        unsafe { Arc::decrement_strong_count(self.ptr) };
    }
}

impl Value for SharedRsValue {
    /// Wraps the passed [`RsValueInternal`] in an [`Arc`]
    /// allocated by the global allocator,
    /// and uses `Arc::into_raw` to convert it to a pointer and
    /// to ensure it doesn't get dropped. All constructors except
    /// of [`Self::undefined`] call this method to create a
    /// [`SharedRsValue`], which is relied upon in other method
    /// and trait implementations for this type, e.g. [`Drop`] and
    /// [`Clone`].
    fn from_internal(internal: RsValueInternal) -> Self {
        let internal = Arc::new(internal);

        Self {
            ptr: Arc::into_raw(internal),
        }
    }

    fn undefined() -> Self {
        Self {
            ptr: ptr::null_mut(),
        }
    }

    fn internal(&self) -> Option<&RsValueInternal> {
        if self.ptr.is_null() {
            return None;
        }
        // Safety: `self.ptr` is not null at this point,
        // and so must have been originated from a call to `from_internal`.
        // At least as long as `self` lives, this pointer is guaranteed
        // to be valid by the backing `Arc`.
        Some(unsafe { &*self.ptr })
    }
}

impl std::fmt::Debug for SharedRsValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.internal() {
            Some(internal) => f.debug_tuple("Defined").field(internal).finish(),
            None => f.debug_tuple("Undefined").finish(),
        }
    }
}

impl Clone for SharedRsValue {
    fn clone(&self) -> Self {
        if self.ptr.is_null() {
            return Self { ptr: ptr::null() };
        }

        // Safety: `self.ptr` is not null at this point,
        // and so must have been originated from a call to `from_internal`.
        // At least as long as `self` lives, this pointer is guaranteed
        // to be valid by the backing `Arc`.
        unsafe { Arc::increment_strong_count(self.ptr) };
        Self { ptr: self.ptr }
    }
}

// Safety: `SharedRsValue` is essentially just a `Arc<RsValueInternal>`.
// Below static assertion proves that `Arc<RsValueInternal>` is `Send`,
// and therefore `SharedRsValue` is `Send`
unsafe impl Send for SharedRsValue {}

// Safety: `SharedRsValue` is essentially just a `Arc<RsValueInternal>`.
// Below static assertion proves that `Arc<RsValueInternal>` is `Sync`,
// and therefore `SharedRsValue` is `Sync`
unsafe impl Sync for SharedRsValue {}

const _ASSERT_ARC_RS_VALUE_INTERNAL_IS_SEND_AND_SYNC: () = {
    const fn static_assert_send_and_sync<T: Send + Sync>() {}
    static_assert_send_and_sync::<Arc<RsValueInternal>>();
};
