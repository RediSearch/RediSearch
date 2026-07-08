/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! MIN / MAX reducer FFI.

use reducers::minmax::{MinMaxCtx, MinMaxReducer};
use rlookup::{RLookupKey, RLookupRow};
use std::{
    ffi::{c_int, c_void},
    ptr,
};

/// Create a MIN (`is_max == false`) or MAX (`is_max == true`) reducer;
/// free it with [`minmaxFree`].
///
/// # Safety
///
/// 1. `srckey` must be a [valid] pointer to an [`RLookupKey`] that remains
///    alive for the lifetime of the returned reducer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn MinMaxReducer_Create(
    srckey: *const ffi::RLookupKey,
    is_max: bool,
) -> *mut ffi::Reducer {
    // SAFETY: ensured by caller (1.); `srckey` points to a valid `RLookupKey`
    // that outlives the returned reducer.
    let srckey: &RLookupKey = unsafe { &*srckey.cast::<RLookupKey>() };

    let mut r = Box::new(MinMaxReducer::new(srckey, is_max));

    r.reducer_mut()
        .set_new_instance(minmaxNewInstance)
        .set_add(minmaxAdd)
        .set_finalize(minmaxFinalize)
        .set_free(minmaxFree);

    Box::into_raw(r).cast()
}

/// # Safety
///
/// 1. `r` must point to a [valid] `MinMaxReducer` masquerading as a `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn minmaxNewInstance(r: *mut ffi::Reducer) -> *mut c_void {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<MinMaxReducer>().as_mut().unwrap() };

    ptr::from_mut(r.alloc_instance()).cast::<c_void>()
}

/// # Safety
///
/// 1. `r` must point to a [valid] `MinMaxReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `MinMaxCtx` masquerading as a void pointer.
/// 3. `srcrow` must point to a [valid] `ffi::RLookupRow`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn minmaxAdd(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
    srcrow: *const ffi::RLookupRow,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<MinMaxReducer>().as_ref().unwrap() };
    // SAFETY: ensured by caller (2.)
    let minmax = unsafe { ctx.cast::<MinMaxCtx>().as_mut().unwrap() };
    // SAFETY: ensured by caller (3.)
    let srcrow = unsafe { srcrow.cast::<RLookupRow>().as_ref().unwrap() };

    minmax.add(r, srcrow);

    1 // C reducer->Add convention: always returns 1
}

/// # Safety
///
/// 1. `ctx` must point to a [valid] `MinMaxCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn minmaxFinalize(
    _r: *mut ffi::Reducer,
    ctx: *mut c_void,
) -> *mut ffi::RSValue {
    // SAFETY: ensured by caller (1.)
    let minmax = unsafe { ctx.cast::<MinMaxCtx>().as_ref().unwrap() };

    minmax.finalize().into_raw() as *mut ffi::RSValue
}

/// # Safety
///
/// 1. `r` must point to a [valid] `MinMaxReducer` masquerading as a `ffi::Reducer`,
///    originally created by [`MinMaxReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn minmaxFree(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller (1.); `r` originates from `Box::into_raw` of a
    // `Box<MinMaxReducer>` and is still owned by C, so we can reclaim it here.
    drop(unsafe { Box::from_raw(r.cast::<MinMaxReducer>()) });
}
