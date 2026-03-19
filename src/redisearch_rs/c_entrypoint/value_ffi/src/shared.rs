/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::{expect_shared_value, expect_value};
use value::{RsValue, SharedRsValue};

/// Decrement the reference count of the provided [`RsValue`] object. If this was
/// the last available reference, it frees the data.
///
/// # Safety
///
/// 1. `value` must point to a valid **owned** [`RsValue`] obtained from an
///    `RSValue_*` function (it will be consumed).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DecrRef(value: *const RsValue) {
    // SAFETY: ensured by caller (1.)
    let _ = unsafe { SharedRsValue::from_raw(value) };
}

/// Follows [`RsValue::Ref`] indirections and returns a pointer to the
/// innermost non-[`Ref`](RsValue::Ref) [`RsValue`].
///
/// The returned pointer borrows from the same allocation as `value`; no new
/// ownership is created.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Dereference(value: *const RsValue) -> *mut RsValue {
    // SAFETY: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    let value = value.fully_dereferenced_ref();

    std::ptr::from_ref(value).cast_mut()
}

/// Like [`RSValue_Dereference`], but also follows [`RsValue::Trio`]
/// indirections by recursing into the left element of each trio.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DereferenceRefAndTrio(value: *const RsValue) -> *mut RsValue {
    // SAFETY: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    let value = value.fully_dereferenced_ref_and_trio();

    std::ptr::from_ref(value).cast_mut()
}

/// Resets `value` to [`RsValue::Undefined`], dropping whatever it previously held.
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RsValue`] object.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Clear(value: *const RsValue) {
    // SAFETY: ensured by caller (1.)
    let mut shared_value = unsafe { expect_shared_value(value) };

    // Panics if more than 1 reference exists.
    shared_value.set_value(RsValue::Undefined);
}

/// Increments the reference count of `value` and returns a new owned pointer
/// to the same allocation.
///
/// The caller must ensure the returned pointer is eventually passed to
/// [`RSValue_DecrRef`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IncrRef(value: *const RsValue) -> *mut RsValue {
    // SAFETY: ensured by caller (1.)
    let shared_value = unsafe { expect_shared_value(value) };

    SharedRsValue::clone(&shared_value).into_raw().cast_mut()
}

/// Replaces the content of `dst` with an [`RsValue::Ref`] pointing to `src`.
///
/// `src`'s reference count is incremented; `dst`'s previous content is dropped.
///
/// # Panic
///
/// Panics if more than 1 reference exists to the `dst` [`RsValue`] object.
///
/// # Safety
///
/// 1. `dst` and `src` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeReference(dst: *const RsValue, src: *const RsValue) {
    // SAFETY: ensured by caller (1.)
    let mut shared_dst = unsafe { expect_shared_value(dst) };

    // SAFETY: ensured by caller (1.)
    let shared_src = unsafe { expect_shared_value(src) };

    let new_value = RsValue::Ref(SharedRsValue::clone(&shared_src));

    // Panics if more than 1 reference exists.
    shared_dst.set_value(new_value);
}

/// Like [`RSValue_MakeReference`], but **takes ownership** of `src` instead of
/// incrementing its reference count.
///
/// After this call, `src` must not be used or freed by the caller.
///
/// # Panic
///
/// Panics if more than 1 reference exists to the `dst` [`RsValue`] object.
///
/// # Safety
///
/// 1. `dst` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
/// 2. `src` must point to a valid **owned** [`RsValue`] obtained from an
///    `RSValue_*` function. Ownership is transferred to `dst`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeOwnReference(dst: *const RsValue, src: *const RsValue) {
    // SAFETY: ensured by caller (1.)
    let mut shared_dst = unsafe { expect_shared_value(dst) };

    // SAFETY: ensured by caller (2.)
    let shared_src = unsafe { SharedRsValue::from_raw(src) };

    let new_value = RsValue::Ref(shared_src);

    // Panics if more than 1 reference exists.
    shared_dst.set_value(new_value);
}

/// Replaces the pointer at `*dstpp` with a new clone of `src`.
///
/// The previous value at `*dstpp` is decremented (and potentially freed).
/// `src`'s reference count is incremented.
///
/// # Safety
///
/// 1. `dstpp` must be a valid, non-null pointer to a `*mut RsValue`.
/// 2. `*dstpp` must point to a valid **owned** [`RsValue`] obtained from an
///    `RSValue_*` function (it will be consumed).
/// 3. `src` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Replace(dstpp: *mut *mut RsValue, src: *const RsValue) {
    // SAFETY: ensured by caller (1.)
    let dst = unsafe { *dstpp };

    // SAFETY: ensured by caller (3.)
    let shared_src = unsafe { expect_shared_value(src) };

    let clone = SharedRsValue::clone(&shared_src);

    // SAFETY: ensured by caller (2.). Reconstructing the `SharedRsValue`
    // will decrement its refcount (and potentially free it).
    let _ = unsafe { SharedRsValue::from_raw(dst) };

    // SAFETY: ensured by caller (1.) — `dstpp` is valid and writable.
    unsafe {
        *dstpp = clone.into_raw().cast_mut();
    }
}

/// Returns the current reference count of `value`.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Refcount(value: *const RsValue) -> u16 {
    // SAFETY: ensured by caller (1.)
    let shared_value = unsafe { expect_shared_value(value) };

    SharedRsValue::refcount(&shared_value) as u16
}
