/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{fmt, ptr, sync::Arc};

use crate::{RsValueInternal, Value, dynamic::DynRsValueRef};

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
// DAX: I don't think we need a raw pointer here. We're in safe rust and should use Arc directly.
// DAX: Only in the ffi boundary we need to convert to/from a raw pointer to the Arc imo.
// DAX: To make it nullable, wrap it in an Option. The identical safe type would be `Option<Arc<RsValueInternal>>`.
#[repr(transparent)]
pub struct SharedRsValue {
    /// Pointer representing the `Arc<RsValueInternal>`.
    ptr: *const RsValueInternal,
}

impl SharedRsValue {
    /// Consumes a [`SharedRsValue`] and returns the
    /// `*const RsValueInternal` it wraps.
    pub const fn into_raw(self) -> *const RsValueInternal {
        let ptr = self.ptr;
        std::mem::forget(self);
        ptr
    }

    /// Converts a reference to a [`SharedRsValue`] to a `*const RsValueInternal`.
    /// Does not increment the reference count, and as such the
    /// caller must ensure the `SharedRsValue`s reference count
    /// does not drop to 0 for the lifetime of the returned pointer.
    ///
    /// Furthermore, to avoid double freeing, when reconstructuring a
    /// [`SharedRsValue`] from the pointer obtained from this function,
    /// the resulting [`SharedRsValue`] must not be dropped, but rather
    /// [forgotten](std::mem::forget).
    pub const fn as_raw(&self) -> *const RsValueInternal {
        self.ptr
    }

    /// Restructure a [`SharedRsValue`] from a `*const RsValueInternal`,
    /// originating from either [`Self::into_raw`] or [`Self::as_raw`].
    ///
    /// In case the passed pointer originated from `as_raw`, in order
    /// to prevent
    ///
    /// # Safety
    /// - (1) `raw` must originate from a call to either [`SharedRsValue::into_raw`]
    ///   or [`SharedRsValue::as_raw`]
    /// - (2) In case `raw` originates from a call to [`SharedRsValue::as_raw`],
    ///   the returned [`SharedRsValue`] must not be dropped, but rather
    ///   [forgotten](std::mem::forget).
    pub const unsafe fn from_raw(raw: *const RsValueInternal) -> Self {
        Self { ptr: raw }
    }
}

impl Default for SharedRsValue {
    fn default() -> Self {
        Self::undefined()
    }
}

// DAX: No need for custom drop implementation when using Arc directly.
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
    // DAX: Would simply be `Self { inner: Some(Arc::new(internal)) }`
    fn from_internal(internal: RsValueInternal) -> Self {
        let internal = Arc::new(internal);

        Self {
            ptr: Arc::into_raw(internal),
        }
    }

    // DAX: Would simply be `Self { inner: None }`
    fn undefined() -> Self {
        Self {
            ptr: ptr::null_mut(),
        }
    }

    // DAX: Would simply be `self.inner.map(|arc| arc.as_ref())`
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

    fn to_dyn_ref(&self) -> DynRsValueRef<'_> {
        DynRsValueRef::from(self.clone())
    }

    fn to_shared(&self) -> SharedRsValue {
        self.clone()
    }
}

impl fmt::Debug for SharedRsValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.internal() {
            Some(internal) => internal.fmt(f),
            None => f.debug_tuple("Undefined").finish(),
        }
    }
}

// DAX: Would simply be `#[derive(Clone)]`
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
// DAX: Would not be needed, because Rust's type system automatically makes it `Send`.
unsafe impl Send for SharedRsValue {}

// Safety: `SharedRsValue` is essentially just a `Arc<RsValueInternal>`.
// Below static assertion proves that `Arc<RsValueInternal>` is `Sync`,
// and therefore `SharedRsValue` is `Sync`
// DAX: Would not be needed, because Rust's type system automatically makes it `Sync`.
unsafe impl Sync for SharedRsValue {}

const _ASSERT_ARC_RS_VALUE_INTERNAL_IS_SEND_AND_SYNC: () = {
    const fn static_assert_send_and_sync<T: Send + Sync>() {}
    static_assert_send_and_sync::<Arc<RsValueInternal>>();
};
