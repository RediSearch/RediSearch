/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! TOLIST reducer FFI.

use reducers::to_list::{ToListCtx, ToListReducer};
use rlookup::{RLookupKey, RLookupRow};
use std::{
    ffi::{c_int, c_void},
    ptr,
};

/// Create a TOLIST reducer; free it with [`tolistFree`].
///
/// # Safety
///
/// 1. `srckey` must be a [valid] pointer to an [`RLookupKey`] that remains
///    alive for the lifetime of the returned reducer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ToListReducer_Create(srckey: *const ffi::RLookupKey) -> *mut ffi::Reducer {
    // SAFETY: ensured by caller (1.); `srckey` points to a valid `RLookupKey`
    // that outlives the returned reducer.
    let srckey: &RLookupKey = unsafe { &*srckey.cast::<RLookupKey>() };

    let mut r = Box::new(ToListReducer::new(srckey));

    r.reducer_mut()
        .set_new_instance(tolistNewInstance)
        .set_add(tolistAdd)
        .set_finalize(tolistFinalize)
        .set_free_instance(tolistFreeInstance)
        .set_free(tolistFree);

    Box::into_raw(r).cast()
}

/// # Safety
///
/// 1. `r` must point to a [valid] `ToListReducer` masquerading as a `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tolistNewInstance(r: *mut ffi::Reducer) -> *mut c_void {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<ToListReducer>().as_mut().unwrap() };

    ptr::from_mut(r.alloc_instance()).cast::<c_void>()
}

/// # Safety
///
/// 1. `r` must point to a [valid] `ToListReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `ToListCtx` masquerading as a void pointer.
/// 3. `srcrow` must point to a [valid] `ffi::RLookupRow`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tolistAdd(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
    srcrow: *const ffi::RLookupRow,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<ToListReducer>().as_ref().unwrap() };
    // SAFETY: ensured by caller (2.)
    let tolist = unsafe { ctx.cast::<ToListCtx>().as_mut().unwrap() };
    // SAFETY: ensured by caller (3.)
    let srcrow = unsafe { srcrow.cast::<RLookupRow>().as_ref().unwrap() };

    tolist.add(r, srcrow);

    1 // C reducer->Add convention: always returns 1
}

/// # Safety
///
/// 1. `ctx` must point to a [valid] `ToListCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tolistFinalize(
    _r: *mut ffi::Reducer,
    ctx: *mut c_void,
) -> *mut ffi::RSValue {
    // SAFETY: ensured by caller (1.)
    let tolist = unsafe { ctx.cast::<ToListCtx>().as_ref().unwrap() };

    tolist.finalize().into_raw() as *mut ffi::RSValue
}

/// # Safety
///
/// 1. `ctx` must point to a [valid] `ToListCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tolistFreeInstance(_r: *mut ffi::Reducer, ctx: *mut c_void) {
    // SAFETY: ensured by caller (1.); `ctx` points to a valid, initialized
    // `ToListCtx`. After this call the pointee is logically uninitialized,
    // but the arena memory is freed later when `ToListReducer` (and its
    // `Bump`) is dropped.
    unsafe { ptr::drop_in_place(ctx.cast::<ToListCtx>()) }
}

/// # Safety
///
/// 1. `r` must point to a [valid] `ToListReducer` masquerading as a `ffi::Reducer`,
///    originally created by [`ToListReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn tolistFree(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller (1.); `r` originates from `Box::into_raw` of a
    // `Box<ToListReducer>` and is still owned by C, so we can reclaim it here.
    drop(unsafe { Box::from_raw(r.cast::<ToListReducer>()) });
}
