/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! SUM / AVG reducer FFI.

use reducers::sum::{SumCtx, SumReducer};
use rlookup::{RLookupKey, RLookupRow};
use std::{
    ffi::{c_int, c_void},
    ptr,
};

/// Create a SUM (`is_avg == false`) or AVG (`is_avg == true`) reducer;
/// free it with [`sumFree`].
///
/// # Safety
///
/// 1. `srckey` must be a [valid] pointer to an [`RLookupKey`] that remains
///    alive for the lifetime of the returned reducer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SumReducer_Create(
    srckey: *const ffi::RLookupKey,
    is_avg: bool,
) -> *mut ffi::Reducer {
    // SAFETY: ensured by caller (1.); `srckey` points to a valid `RLookupKey`
    // that outlives the returned reducer.
    let srckey: &RLookupKey = unsafe { &*srckey.cast::<RLookupKey>() };

    let mut r = Box::new(SumReducer::new(srckey, is_avg));

    r.reducer_mut()
        .set_new_instance(sumNewInstance)
        .set_add(sumAdd)
        .set_finalize(sumFinalize)
        .set_free(sumFree);

    Box::into_raw(r).cast()
}

/// # Safety
///
/// 1. `r` must point to a [valid] `SumReducer` masquerading as a `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sumNewInstance(r: *mut ffi::Reducer) -> *mut c_void {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<SumReducer>().as_mut().unwrap() };

    ptr::from_mut(r.alloc_instance()).cast::<c_void>()
}

/// # Safety
///
/// 1. `r` must point to a [valid] `SumReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `SumCtx` masquerading as a void pointer.
/// 3. `srcrow` must point to a [valid] `ffi::RLookupRow`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sumAdd(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
    srcrow: *const ffi::RLookupRow,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<SumReducer>().as_ref().unwrap() };
    // SAFETY: ensured by caller (2.)
    let sum = unsafe { ctx.cast::<SumCtx>().as_mut().unwrap() };
    // SAFETY: ensured by caller (3.)
    let srcrow = unsafe { srcrow.cast::<RLookupRow>().as_ref().unwrap() };

    sum.add(r, srcrow);

    1 // C reducer->Add convention: always returns 1
}

/// # Safety
///
/// 1. `r` must point to a [valid] `SumReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `SumCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sumFinalize(r: *mut ffi::Reducer, ctx: *mut c_void) -> *mut ffi::RSValue {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<SumReducer>().as_ref().unwrap() };
    // SAFETY: ensured by caller (2.)
    let sum = unsafe { ctx.cast::<SumCtx>().as_ref().unwrap() };

    sum.finalize(r).into_raw() as *mut ffi::RSValue
}

/// # Safety
///
/// 1. `r` must point to a [valid] `SumReducer` masquerading as a `ffi::Reducer`,
///    originally created by [`SumReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sumFree(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller (1.); `r` originates from `Box::into_raw` of a
    // `Box<SumReducer>` and is still owned by C, so we can reclaim it here.
    drop(unsafe { Box::from_raw(r.cast::<SumReducer>()) });
}
