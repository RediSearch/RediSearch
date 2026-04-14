/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{SharedValue, SharedValueRef, SharedValueRefMut, Value};

/// Decrement the reference count of the provided [`RSValue`] object. If this was
/// the last available reference, it frees the data.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
/// 2. `value` **must not** be used or freed after this call, as this function takes ownership.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DecrRef(_value: SharedValue) {}

/// Follows [`Value::Ref`] indirections and returns a pointer to the
/// innermost non-[`Ref`](Value::Ref) [`Value`].
///
/// The returned pointer borrows from the same allocation as `value`; no new
/// ownership is created.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Dereference(value: SharedValueRef) -> SharedValueRef {
    let value = value.fully_dereferenced_ref();

    unsafe { std::mem::transmute(value) }
}

/// Like [`RSValue_Dereference`], but also follows [`Value::Trio`]
/// indirections by recursing into the left element of each trio.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DereferenceRefAndTrio(value: SharedValueRef) -> SharedValueRef {
    let value = value.fully_dereferenced_ref_and_trio();

    unsafe { std::mem::transmute(value) }
}

/// Resets `value` to [`Value::Undefined`], dropping whatever it previously held.
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RSValue`] object.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Clear(mut shared_value: SharedValueRefMut) {
    // Panics if more than 1 reference exists.
    shared_value.set_value(Value::Undefined);
}

/// Increments the reference count of `value` and returns a new owned pointer
/// to the same allocation.
///
/// The caller must ensure the returned pointer is eventually passed to
/// [`RSValue_DecrRef`].
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IncrRef(value: SharedValueRef) -> SharedValue {
    SharedValue::clone(&value)
}

/// Replaces the content of `dst` with an [`Value::Ref`] pointing to `src`.
///
/// `src`'s reference count is incremented; `dst`'s previous content is dropped.
///
/// # Panic
///
/// Panics if more than 1 reference exists to the `dst` [`RSValue`] object.
///
/// # Safety
///
/// 1. `dst` and `src` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeReference(mut dst: SharedValueRefMut, src: SharedValueRef) {
    let new_value = Value::Ref(SharedValue::clone(&src));

    // Panics if more than 1 reference exists.
    dst.set_value(new_value);
}

/// Like [`RSValue_MakeReference`], but **takes ownership** of `src` instead of
/// incrementing its reference count.
///
/// After this call, `src` must not be used or freed by the caller.
///
/// # Panic
///
/// Panics if more than 1 reference exists to the `dst` [`RSValue`] object.
///
/// # Safety
///
/// 1. `dst` must point to a valid [`RSValue`].
/// 2. `src` must point to a valid [`RSValue`]. Ownership is transferred to `dst`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeOwnReference(mut dst: SharedValueRefMut, src: SharedValue) {
    let new_value = Value::Ref(src);

    // Panics if more than 1 reference exists.
    dst.set_value(new_value);
}

/// Replaces the pointer at `*dstpp` with a new clone of `src`.
///
/// The previous value at `*dstpp` is decremented (and potentially freed).
/// `src`'s reference count is incremented.
///
/// # Safety
///
/// 1. `dstpp` must be a valid, non-null pointer to a `*mut RSValue`.
/// 2. `*dstpp` must point to a valid [`RSValue`] (it will be consumed).
/// 3. `src` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Replace(dstpp: &mut SharedValue, src: SharedValueRef) {
    let clone = SharedValue::clone(&src);

    *dstpp = clone;
}

/// Returns the current reference count of `value`.
///
/// # Safety
///
/// 1. `value` must point to a valid [`RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Refcount(value: SharedValueRef) -> u16 {
    SharedValue::refcount(&value) as u16
}
