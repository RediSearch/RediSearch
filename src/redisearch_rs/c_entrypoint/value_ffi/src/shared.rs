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
///    `RSValue_*` function returning an owned [`RsValue`] object.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DecrRef(value: *const RsValue) {
    // Safety: ensured by caller (1.)
    let _ = unsafe { SharedRsValue::from_raw(value) };
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Dereference(value: *const RsValue) -> *mut RsValue {
    let value = unsafe { expect_value(value) };

    let value = value.fully_dereferenced_ref();

    std::ptr::from_ref(value).cast_mut()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_DereferenceRefAndTrio(value: *const RsValue) -> *mut RsValue {
    let value = unsafe { expect_value(value) };

    let value = value.fully_dereferenced_ref_and_trio();

    std::ptr::from_ref(value).cast_mut()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Clear(value: *const RsValue) {
    let mut shared_value = unsafe { expect_shared_value(value) };

    shared_value.set_value(RsValue::Undefined);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_IncrRef(value: *const RsValue) -> *mut RsValue {
    let shared_value = unsafe { expect_shared_value(value) };

    SharedRsValue::clone(&shared_value).into_raw().cast_mut()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeReference(dst: *const RsValue, src: *const RsValue) {
    let mut shared_dst = unsafe { expect_shared_value(dst) };

    let shared_src = unsafe { expect_shared_value(src) };

    let new_value = RsValue::Ref(SharedRsValue::clone(&shared_src));
    shared_dst.set_value(new_value);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MakeOwnReference(dst: *const RsValue, src: *const RsValue) {
    let mut shared_dst = unsafe { expect_shared_value(dst) };

    let shared_src = unsafe { SharedRsValue::from_raw(src) };

    let new_value = RsValue::Ref(shared_src);
    shared_dst.set_value(new_value);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Replace(dstpp: *mut *mut RsValue, src: *const RsValue) {
    let _ = unsafe { SharedRsValue::from_raw(*dstpp) };

    let shared_src = unsafe { expect_shared_value(src) };

    let clone = SharedRsValue::clone(&shared_src);

    unsafe {
        *dstpp = clone.into_raw().cast_mut();
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Refcount(value: *const RsValue) -> u16 {
    let shared_value = unsafe { expect_shared_value(value) };

    SharedRsValue::refcount(&shared_value) as u16
}
