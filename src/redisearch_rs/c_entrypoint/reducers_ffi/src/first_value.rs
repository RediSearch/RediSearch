/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FIRST_VALUE reducer FFI.

use reducers::first_value::{FirstValueCtx, FirstValueReducer};
use rlookup::{RLookupKey, RLookupRow};
use std::{
    ffi::{c_int, c_void},
    ptr,
};

/// Create a FIRST_VALUE reducer; free it with [`fvFree`].
///
/// # Safety
///
/// 1. `retkey` must be a [valid] pointer to an [`RLookupKey`] that remains
///    alive for the lifetime of the returned reducer.
/// 2. `sortkey` is either null (no `BY` clause) or a [valid] pointer to an
///    [`RLookupKey`] that remains alive for the lifetime of the returned
///    reducer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn FirstValueReducer_Create(
    retkey: *const ffi::RLookupKey,
    sortkey: *const ffi::RLookupKey,
    ascending: bool,
) -> *mut ffi::Reducer {
    // SAFETY: ensured by caller (1.); `retkey` points to a valid `RLookupKey`
    // that outlives the returned reducer.
    let retkey: &RLookupKey = unsafe { &*retkey.cast::<RLookupKey>() };
    // SAFETY: ensured by caller (2.); when non-null, `sortkey` points to a
    // valid `RLookupKey` that outlives the returned reducer.
    let sortkey: Option<&RLookupKey> = unsafe { sortkey.cast::<RLookupKey>().as_ref() };

    let mut r = Box::new(FirstValueReducer::new(retkey, sortkey, ascending));

    r.reducer_mut()
        .set_new_instance(fvNewInstance)
        .set_add(fvAdd)
        .set_finalize(fvFinalize)
        .set_free_instance(fvFreeInstance)
        .set_free(fvFree);

    Box::into_raw(r).cast()
}

/// # Safety
///
/// 1. `r` must point to a [valid] `FirstValueReducer` masquerading as a `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fvNewInstance(r: *mut ffi::Reducer) -> *mut c_void {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<FirstValueReducer>().as_mut().unwrap() };

    ptr::from_mut(r.alloc_instance()).cast::<c_void>()
}

/// # Safety
///
/// 1. `r` must point to a [valid] `FirstValueReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `FirstValueCtx` masquerading as a void pointer.
/// 3. `srcrow` must point to a [valid] `ffi::RLookupRow`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fvAdd(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
    srcrow: *const ffi::RLookupRow,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<FirstValueReducer>().as_ref().unwrap() };
    // SAFETY: ensured by caller (2.)
    let fv = unsafe { ctx.cast::<FirstValueCtx>().as_mut().unwrap() };
    // SAFETY: ensured by caller (3.)
    let srcrow = unsafe { srcrow.cast::<RLookupRow>().as_ref().unwrap() };

    fv.add(r, srcrow);

    1 // C reducer->Add convention: always returns 1
}

/// # Safety
///
/// 1. `ctx` must point to a [valid] `FirstValueCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fvFinalize(_r: *mut ffi::Reducer, ctx: *mut c_void) -> *mut ffi::RSValue {
    // SAFETY: ensured by caller (1.)
    let fv = unsafe { ctx.cast::<FirstValueCtx>().as_ref().unwrap() };

    fv.finalize().into_raw() as *mut ffi::RSValue
}

/// # Safety
///
/// 1. `ctx` must point to a [valid] `FirstValueCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fvFreeInstance(_r: *mut ffi::Reducer, ctx: *mut c_void) {
    // SAFETY: ensured by caller (1.); `ctx` points to a valid, initialized
    // `FirstValueCtx`. After this call the pointee is logically uninitialized,
    // but the arena memory is freed later when `FirstValueReducer` (and its
    // `Bump`) is dropped.
    unsafe { ptr::drop_in_place(ctx.cast::<FirstValueCtx>()) }
}

/// # Safety
///
/// 1. `r` must point to a [valid] `FirstValueReducer` masquerading as a `ffi::Reducer`,
///    originally created by [`FirstValueReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fvFree(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller (1.); `r` originates from `Box::into_raw` of a
    // `Box<FirstValueReducer>` and is still owned by C, so we can reclaim it here.
    drop(unsafe { Box::from_raw(r.cast::<FirstValueReducer>()) });
}
