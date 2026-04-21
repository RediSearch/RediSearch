/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RSValue;
use crate::util::{expect_shared_value, expect_value, into_rs_value, into_shared_value};
use value::{SharedValue, Value};

/// Decrement the reference count of the provided [`RSValue`] object. If this was
/// the last available reference, it frees the data.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
/// 2. `value` **must not** be used or freed after this call, as this function takes ownership.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DecrRef(value: *const RSValue) {
    // SAFETY: ensured by caller (1., 2.)
    let _ = unsafe { into_shared_value(value.cast_mut()) };
}

/// Follows [`Value::Ref`] indirections and returns a pointer to the
/// innermost non-[`Ref`](Value::Ref) [`Value`].
///
/// The returned pointer borrows from the same allocation as `value`; no new
/// ownership is created.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Dereference(value: *const RSValue) -> *mut RSValue {
    // SAFETY: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    let value = value.fully_dereferenced_ref();

    std::ptr::from_ref(value).cast_mut().cast()
}

/// Like [`RSValue_Dereference`], but also follows [`Value::Trio`]
/// indirections by recursing into the left element of each trio.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DereferenceRefAndTrio(value: *const RSValue) -> *mut RSValue {
    // SAFETY: ensured by caller (1.)
    let value = unsafe { expect_value(value) };

    let value = value.fully_dereferenced_ref_and_trio();

    std::ptr::from_ref(value).cast_mut().cast()
}

/// Resets `value` to [`Value::Undefined`], dropping whatever it previously held.
///
/// # Panic
///
/// Panics if more than 1 reference exists to this [`RSValue`] object.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Clear(value: *const RSValue) {
    // SAFETY: ensured by caller (1.)
    let mut shared_value = unsafe { expect_shared_value(value) };

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
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IncrRef(value: *const RSValue) -> *mut RSValue {
    // SAFETY: ensured by caller (1.)
    let shared_value = unsafe { expect_shared_value(value) };

    into_rs_value(SharedValue::clone(&shared_value))
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
/// 1. `dst` and `src` must be [valid], non-null pointers to [`RSValue`]s.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeReference(dst: *const RSValue, src: *const RSValue) {
    // SAFETY: ensured by caller (1.)
    let mut shared_dst = unsafe { expect_shared_value(dst) };

    // SAFETY: ensured by caller (1.)
    let shared_src = unsafe { expect_shared_value(src) };

    let new_value = Value::Ref(SharedValue::clone(&shared_src));

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
/// Panics if more than 1 reference exists to the `dst` [`RSValue`] object.
///
/// # Safety
///
/// 1. `dst` must be a [valid], non-null pointer to an [`RSValue`].
/// 2. `src` must be a [valid], non-null pointer to an [`RSValue`]. Ownership is transferred to `dst`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeOwnReference(dst: *const RSValue, src: *const RSValue) {
    // SAFETY: ensured by caller (1.)
    let mut shared_dst = unsafe { expect_shared_value(dst) };

    // SAFETY: ensured by caller (2.)
    let shared_src = unsafe { into_shared_value(src.cast_mut()) };

    let new_value = Value::Ref(shared_src);

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
/// 1. `dstpp` must be a [valid], non-null pointer to an `*mut RSValue`.
/// 2. `*dstpp` must be a [valid], non-null pointer to an [`RSValue`] (it will be consumed).
/// 3. `src` must be a [valid], non-null pointer to an [`RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Replace(dstpp: *mut *mut RSValue, src: *const RSValue) {
    // SAFETY: ensured by caller (1.)
    let dst = unsafe { *dstpp };

    // SAFETY: ensured by caller (3.)
    let shared_src = unsafe { expect_shared_value(src) };

    let clone = SharedValue::clone(&shared_src);

    // SAFETY: ensured by caller (2.). Reconstructing the `SharedRSValue`
    // will decrement its refcount (and potentially free it).
    let _ = unsafe { into_shared_value(dst) };

    // SAFETY: ensured by caller (1.) — `dstpp` is valid and writable.
    unsafe {
        *dstpp = into_rs_value(clone);
    }
}

/// Returns the current reference count of `value`.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Refcount(value: *const RSValue) -> u16 {
    // SAFETY: ensured by caller (1.)
    let shared_value = unsafe { expect_shared_value(value) };

    SharedValue::refcount(&shared_value) as u16
}
